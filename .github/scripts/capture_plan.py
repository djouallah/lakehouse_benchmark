"""Run a few queries on one engine and keep its executed plan, with the rows each operator made.

For the results page's "Bad joins" tab (docs/index.html #plans): the same query on two engines,
the two plans side by side. This script only CAPTURES -- it writes each engine's raw plan, as the
engine reports it, to `<out>/<suite>_q<N>_<engine>_sf<SF>.json`. `plans_normalize.py` turns those
into the one shape the page reads, so the parsing can be fixed without running a query again.

READ-ONLY against OneLake, like run_engine.py, and the same engine classes set up and attach. Each
query runs once, cold, with the engine's own plan instrumentation on:

* DuckDB: `EXPLAIN (FORMAT JSON)` first -- the estimates, which survive a query that then fails --
  and the query itself with JSON profiling: actual rows and time per operator, and the estimate
  next to each one. Spill is the engine's own temp-directory probe.
* Trino: the query info (`/v1/query/{id}`): every fragment's plan with its estimates, and output
  rows per plan node from the operator summaries.
* Spark: the executed physical plan after `collect()` -- the final plan when AQE re-planned --
  with each node's SQL metrics (`numOutputRows`, spill).

A query that fails is still written: the error, the estimates when the engine gave them, and the
time it ran before failing. Bad joins are about exactly those.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from pathlib import Path

from bench import scrub
from bench.suite import suite_class
from bench.tpch.engines import get_engine
from bench.tpch.queries import load

TMP = Path(os.environ.get("RUNNER_TEMP", "."))


def _duckdb(engine, sql: str) -> dict:
    conn = engine._conn
    explain = TMP / "duckdb_explain.txt"
    explain.unlink(missing_ok=True)
    # The CLI's output goes to a file for one statement: the JSON is many lines, and the csv
    # reader the wrapper uses would split it.
    conn._send(
        f".mode list\n.separator @@\n.output {explain.as_posix()}\n"
        f"EXPLAIN (FORMAT JSON) {sql}\n;\n.output\n.mode csv"
    )
    text = explain.read_text(encoding="utf-8")
    estimates = json.loads(text[text.index("[") :] if "[" in text else "null")

    profile = TMP / "duckdb_profile.json"
    profile.unlink(missing_ok=True)
    conn.sql(
        # This order: the output file's extension is checked against the profiling type.
        f"SET enable_profiling = 'json'; SET profiling_output = '{profile.as_posix()}'; "
        "SET profiling_mode = 'detailed';"
    )
    if engine._spill is not None:
        engine._spill.take_peak()
    error, start = None, time.perf_counter()
    try:
        conn.sql(sql)
    except Exception as exc:  # noqa: BLE001 - a failed query is a result here
        error = scrub.scrub_exc(exc)
    seconds = time.perf_counter() - start
    spill = engine._spill.take_peak() if engine._spill is not None else None
    actual = json.loads(profile.read_text(encoding="utf-8")) if error is None else None
    if error is None:
        conn.sql("PRAGMA disable_profiling;")
    return {
        "seconds": seconds,
        "error": error,
        "spill_bytes": spill,
        "raw": {"explain": estimates, "profile": actual},
    }


def _trino(engine, sql: str) -> dict:
    from bench import trino

    cur = engine._conn.cursor()
    error, start = None, time.perf_counter()
    try:
        cur.execute(sql)
        cur.fetchall()
    except Exception as exc:  # noqa: BLE001 - a failed query is a result here
        error = scrub.scrub_exc(exc)
    seconds = time.perf_counter() - start
    info = None
    if cur.query_id:
        request = urllib.request.Request(
            f"http://127.0.0.1:{trino.PORT}/v1/query/{cur.query_id}",
            headers={"X-Trino-User": "bench"},
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            info = json.load(response)
    stats = (info or {}).get("queryStats", {})
    return {
        "seconds": seconds,
        "error": error,
        "spill_bytes": stats.get("spilledDataSize"),
        "raw": info,
    }


def _spark_node(node) -> dict:
    cls = node.getClass().getSimpleName()
    if cls == "AdaptiveSparkPlanExec":  # the final plan, after AQE re-planned at each stage
        return _spark_node(node.executedPlan())
    metrics = {}
    it = node.metrics().iterator()
    while it.hasNext():
        pair = it.next()
        metrics[pair._1()] = pair._2().value()
    if cls.endswith("QueryStageExec"):
        kids = [node.plan()]
    elif cls == "ReusedExchangeExec":
        kids = [node.child()]
    else:
        seq = node.children()
        kids = [seq.apply(i) for i in range(seq.size())]
    return {
        "name": node.nodeName(),
        "class": cls,
        "string": node.simpleString(25)[:600],
        "metrics": metrics,
        "children": [_spark_node(kid) for kid in kids],
    }


def _spark(engine, sql: str) -> dict:
    df = engine.session.sql(sql)
    error, start = None, time.perf_counter()
    try:
        df.collect()
    except Exception as exc:  # noqa: BLE001 - a failed query is a result here
        error = scrub.scrub_exc(exc)
    seconds = time.perf_counter() - start
    tree = _spark_node(df._jdf.queryExecution().executedPlan())
    return {"seconds": seconds, "error": error, "spill_bytes": None, "raw": tree}


CAPTURE = {
    "duckdb_iceberg": _duckdb,
    "trino_iceberg": _trino,
    "pyspark_iceberg": _spark,
    "pyspark_gluten_iceberg": _spark,
}


def _clean(text: str) -> str:
    """Registered secrets and anything token-shaped out: the file is a public artifact."""
    text = scrub.scrub(text)
    for token in scrub.find_token_shaped(text):
        text = text.replace(token, scrub.MASK)
    return text


def main() -> int:
    cfg = suite_class().from_env()
    if cfg.engine not in CAPTURE:
        raise SystemExit(f"no plan capture for {cfg.engine!r}; one of {sorted(CAPTURE)}")
    queries = [int(q) for q in os.environ["PLAN_QUERIES"].split(",") if q.strip()]
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "plans_raw")
    out.mkdir(parents=True, exist_ok=True)

    statements = load(cfg.engine, cfg.schema, cfg.sf, cfg.SQL_PATH, cfg.N_QUERIES)
    engine = get_engine(cfg.engine, cfg)
    engine.setup()
    failed = False
    try:
        for number in queries:
            engine.refresh()
            record = {
                "engine": cfg.engine,
                "version": engine.version,
                "suite": cfg.TEST,
                "query": number,
                "sf": cfg.sf,
                "run_url": os.environ.get("BENCH_RUN_URL", ""),
            }
            try:
                record.update(CAPTURE[cfg.engine](engine, statements[number - 1]))
            except Exception as exc:  # noqa: BLE001 - the capture itself broke: say so, go on
                record.update(error=f"capture failed: {scrub.scrub_exc(exc)}", raw=None)
                failed = True
            path = out / f"{cfg.TEST}_q{number}_{cfg.engine}_sf{cfg.sf}.json"
            path.write_text(_clean(json.dumps(record, default=str)), encoding="utf-8")
            took = record.get("seconds")
            scrub.safe_print(
                f"  Q{number} {f'{took:.1f}s' if took is not None else '-'} "
                f"{record.get('error') or 'ok'} -> {path} ({path.stat().st_size // 1024} KiB)"
            )
            if record.get("error") and cfg.engine == "duckdb_iceberg":
                break  # an out-of-memory DuckDB process is no place for the next query
    finally:
        engine.close()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
