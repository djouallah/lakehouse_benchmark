"""The workload's dbt tests, once a day, for every leg: `python workload_test.py <record.json>`.

table_maintenance.yml runs it beside the compaction. The hourly builds no longer test
(workload_run_engine.py): the tests re-checked all history every hour, and the grain test alone
took 1,053 s of Trino's 1,431 s build (run 40050741886).

One reader checks every leg, Iceberg and Delta: stock Spark, the reference implementation
(bench/workload/spark_reader.py), running the spark tree's tests on the `spark` target, pointed at
each leg's tables in turn by WORKLOAD_LEG (workload/macros/generate_schema_name.sql reads it). So
dbt_trino_landing.fct_scada is tested by the same SQL as dbt_duckrun_landing.fct_scada. Until
2026-11-28 the reader was DuckDB, on the Iceberg legs only: DuckDB 2.0.1 refuses the Sail leg's
tables (lakehq/sail#3124), and it cannot reach the Delta legs' tables, which are outside the REST
catalog.

The record, per leg: every test node with its status, seconds, message, and peak RSS and spill.
workload_maintain.py publishes it beside the maintenance record.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from bench import auth, scrub
from bench.report import leak_check
from bench.workload.config import BENCH_ENGINE, DELTA_ENGINES, ENGINES, WorkloadConfig
from bench.workload.spark_reader import SparkReader

sys.path.insert(0, str(Path(__file__).resolve().parent))
from workload_run_engine import PROJECT, Dbt, PeakRss, node_timings, versions  # noqa: E402


def main(out: Path) -> int:
    os.environ["BENCH_ENGINE"] = "spark"
    cfg = WorkloadConfig.from_env()
    record = {
        "run_id": cfg.run_id,
        "run_url": cfg.run_url,
        "started": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tester": {},
        "legs": {},
    }
    os.chdir(PROJECT)
    dbt = Dbt("spark")
    reader = SparkReader(cfg)
    failed = False
    try:
        reader.setup()
        record["tester"] = {**versions("spark"), "spark": reader.version}
        for leg in ENGINES:
            # The REST catalog's bearer is fixed for the session: restart on a fresh one when it
            # has under 15 minutes left (PysparkIceberg.refresh). Delta tables are re-registered
            # per leg, so a restart loses nothing.
            reader.refresh()
            os.environ.update(cfg.dbt_env(auth.onelake_token()))
            os.environ["WORKLOAD_LEG"] = leg
            reader.use_leg(leg, leg in DELTA_ENGINES, cfg.tables_abfss)
            with PeakRss("spark") as rss:
                ok, seconds = dbt.run("test", callbacks=[rss.on_event])
            nodes = [n for n in node_timings(rss.nodes) if n["node"].startswith("test.")]
            record["legs"][BENCH_ENGINE[leg]] = {
                "leg": leg,
                "seconds": round(seconds, 3),
                "peak_rss_mb": rss.mb,
                "peak_spill_mb": rss.spill_mb,
                "tests": len(nodes),
                "tests_ok": sum(n["status"] in ("pass", "warn") for n in nodes),
                "nodes": nodes,
                **({} if ok or nodes else {"error": dbt.last_error}),
            }
            failed |= not ok
    finally:
        os.environ.pop("WORKLOAD_LEG", None)
        reader.close()
        record["finished"] = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        out.write_text(scrub.scrub(json.dumps(record, indent=2)) + "\n", encoding="utf-8")
        leak_check([out])
    for key, leg in record["legs"].items():
        passed = f"{leg['tests_ok']}/{leg['tests']}"
        scrub.safe_print(f"{key}: {passed} tests passed in {leg['seconds']} s")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1] if len(sys.argv) > 1 else "workload_tests.json").resolve()))
