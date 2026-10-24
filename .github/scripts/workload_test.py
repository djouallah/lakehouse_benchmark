"""The workload's dbt tests, once a day, for every leg: `python workload_test.py <record.json>`.

table_maintenance.yml runs it beside the compaction. The hourly builds no longer test
(workload_run_engine.py): the tests re-checked all history every hour, and the grain test alone
took 1,053 s of Trino's 1,431 s build (run 40050741886).

One engine checks every leg: the duckdb target's tests, pointed at each leg's tables in turn by
WORKLOAD_LEG (workload/macros/generate_schema_name.sql reads it), so dbt_trino_landing.fct_scada
is tested by the same SQL as dbt_duckdb_landing.fct_scada. No Spark, Trino or Sail to start.

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
from bench.workload.config import BENCH_ENGINE, ENGINES, WorkloadConfig

sys.path.insert(0, str(Path(__file__).resolve().parent))
from workload_run_engine import PROJECT, Dbt, PeakRss, node_timings, versions  # noqa: E402


def main(out: Path) -> int:
    os.environ["BENCH_ENGINE"] = "duckdb"
    cfg = WorkloadConfig.from_env()
    record = {
        "run_id": cfg.run_id,
        "run_url": cfg.run_url,
        "started": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tester": versions("duckdb"),
        "legs": {},
    }
    os.environ.setdefault("DUCKDB_TEMP_DIR", "/tmp/duckdb_spill")
    os.makedirs(os.environ["DUCKDB_TEMP_DIR"], exist_ok=True)
    os.chdir(PROJECT)
    dbt = Dbt("duckdb")
    failed = False
    try:
        for leg in ENGINES:
            # A fresh bearer per leg: the profile bakes it into DuckDB's ATTACH at connect.
            os.environ.update(cfg.dbt_env(auth.onelake_token(fresh=True)))
            os.environ["WORKLOAD_LEG"] = leg
            with PeakRss("duckdb") as rss:
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
        record["finished"] = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        out.write_text(scrub.scrub(json.dumps(record, indent=2)) + "\n", encoding="utf-8")
        leak_check([out])
    for key, leg in record["legs"].items():
        passed = f"{leg['tests_ok']}/{leg['tests']}"
        scrub.safe_print(f"{key}: {passed} tests passed in {leg['seconds']} s")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1] if len(sys.argv) > 1 else "workload_tests.json").resolve()))
