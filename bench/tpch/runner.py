"""Run the suite's statements cold (then warm, for TPC-H), timing each. Both suites come here.

REPLACES cells 15, 16 and 17.

WHAT CHANGED BEYOND THE MEASUREMENT FIXES IN engines/base.py:

* A FAILING QUERY NO LONGER KILLS THE RUN. Cell 15 had no try/except, so one engine raising on one
  statement lost all 22 timings and both passes. Here a query failure becomes a row with
  `status='error'` and a scrubbed message, and the remaining queries still run. That matters most
  for exactly the engines most likely to fail -- Polars and LakeSail on Q9/Q18/Q21 at SF>=10 --
  because "Sail completed 20 of 22, and here are the two it could not" is a result, while a dead
  job is not.
* SETUP IS ITS OWN ROW rather than being added to query 1's duration. See store.Row.
* `exclude_list=[]` (a mutable default argument, and a real bug waiting to happen) is gone; there
  was never a caller that passed it.
"""

from __future__ import annotations

import contextlib
import time

from bench import scrub
from bench.store import EngineResult, Row
from bench.tpch.queries import load


def _time(fn, *args) -> tuple[float, object, Exception | None]:
    """Run `fn`, returning (elapsed, value, exception). Never raises.

    `Exception` and not `BaseException`: a Ctrl-C or a SystemExit must still stop the run, and an
    OOM kill is not an exception at all -- the kernel takes the process out and the job reports
    exit 137 with nothing in the log.
    """
    start = time.perf_counter()
    try:
        value = fn(*args)
    except Exception as exc:  # noqa: BLE001 - stored as a result row by the caller
        return time.perf_counter() - start, None, exc
    return time.perf_counter() - start, value, None


def order(n_statements: int, hard_first: tuple[int, ...] = ()) -> list[int]:
    """Query numbers in run order: `hard_first` as given, then the rest ascending."""
    first = [q for q in hard_first if 1 <= q <= n_statements]
    return first + [q for q in range(1, n_statements + 1) if q not in first]


def run_pass(
    engine, statements: list[str], run_type: str, hard_first: tuple[int, ...] = ()
) -> list[Row]:
    """One full pass over the statements, hard ones first (config.HARD_FIRST)."""
    rows: list[Row] = []
    refresh = getattr(engine, "refresh", None)
    for number in order(len(statements), hard_first):
        sql = statements[number - 1]
        if refresh is not None:
            # Untimed: re-minting a credential is not the query's work. A failure here is left
            # to surface as the statement's own error, with whatever credential is still in place.
            with contextlib.suppress(Exception):
                refresh()
        duration, count, exc = _time(engine.execute, sql)
        if exc is None:
            rows.append(Row(run_type, "query", number, round(duration, 4), rows=count))
            scrub.safe_print(f"  Q{number:<2} {run_type:<4} {duration:8.3f}s  {count} rows")
        else:
            rows.append(
                Row(run_type, "query", number, None, status="error", error=scrub.scrub_exc(exc))
            )
            message = scrub.scrub_exc(exc, 160)
            scrub.safe_print(f"  Q{number:<2} {run_type:<4} {'ERROR':>8}  {message}")
    return rows


def benchmark(engine, cfg) -> EngineResult:
    """Attach, run each of the suite's passes (cold, then warm for TPC-H). Always closes the engine.

    A setup failure returns `status='setup_failed'` with the one setup row rather than raising:
    the caller writes the artifact either way, so a failed engine still appears in the results
    with a reason instead of vanishing from the chart.
    """
    statements = load(engine.name, cfg.schema, cfg.sf, cfg.SQL_PATH, cfg.N_QUERIES)
    result = EngineResult(version="unknown")

    setup_duration, _, exc = _time(engine.setup)
    # Best-effort: if the engine's package never imported, there is no version to read, and that
    # is not a reason to lose the setup failure we are about to record.
    with contextlib.suppress(Exception):
        result.version = engine.version

    if exc is not None:
        result.status = "setup_failed"
        result.rows.append(
            Row("cold", "setup", 0, None, status="error", error=scrub.scrub_exc(exc))
        )
        scrub.safe_print(f"  setup FAILED: {scrub.scrub_exc(exc, 400)}")
        engine.close()
        return result

    result.rows.append(Row("cold", "setup", 0, round(setup_duration, 4)))
    scrub.safe_print(f"  setup {setup_duration:.3f}s")

    try:
        # A second pass runs the SAME statements again, immediately. Whatever the engine cached --
        # chDB's filesystem cache, DuckDB's buffer pool, the OS page cache -- is what the warm
        # numbers measure. The suite's config says how many passes there are.
        for run_type in cfg.PASSES:
            result.rows += run_pass(engine, statements, run_type, cfg.HARD_FIRST)
    finally:
        engine.close()

    return result


def totals(result: EngineResult) -> dict[str, float]:
    """Total seconds per pass, counting only statements that succeeded."""
    out: dict[str, float] = {}
    for row in result.rows:
        if row.status == "ok" and row.dur is not None:
            out[row.run_type] = out.get(row.run_type, 0.0) + row.dur
    return out
