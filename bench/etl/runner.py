"""Time one engine's load, and record it the way the TPC-H runner does.

REPLACES cells 26 and 29 (`run_test_engine`, `run_test`). Two rows per engine instead of the
notebook's one Delta append:

* `phase='setup'` -- session start, catalog attach, storage credential. The notebook timed
  DuckDB's `ATTACH` inside its load and Sail's server start outside it; this makes the split the
  same for every engine, and the same as store.Row documents for TPC-H.
* `phase='load'`, `query=1` -- everything the notebook's `<engine>_clean_csv` did: drop and
  create the table, read the CSVs from OneLake, transform, write, commit. `rows` is the row count
  read back from the table afterwards, untimed: every engine applying one filter to one set of
  files should land one number, and a chart cannot show which of them did not.

One pass, `run_type='cold'`: the notebook ran once, and a second write of the same input would
measure the same thing again.

The row count and the FILE LAYOUT are both read back from the catalog after the clock stops, and
both are checks rather than results: six engines applying one filter to one set of files should
land one row count, and the layout says what the load time cannot -- Spark at the default split
wrote ~400 fragments where the streaming writers wrote whole files. Neither read is timed and
neither can fail the run.
"""

from __future__ import annotations

import contextlib

from bench import scrub
from bench.etl.config import EtlConfig
from bench.store import EngineResult, Row
from bench.tpch.runner import _time


def benchmark(engine, cfg: EtlConfig, files: list[str]) -> EngineResult:
    """Setup, load, count. Always closes the engine; never raises for an engine failure."""
    result = EngineResult(version="unknown")

    setup_duration, _, exc = _time(engine.setup)
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
        duration, _, exc = _time(engine.load, files)
        if exc is not None:
            result.rows.append(
                Row("cold", "load", 1, None, status="error", error=scrub.scrub_exc(exc))
            )
            scrub.safe_print(f"  load  {'ERROR':>8}  {scrub.scrub_exc(exc, 400)}")
            return result
        rows = None
        try:
            rows = int(engine.row_count())
        except Exception as count_exc:  # noqa: BLE001 - the count is a check, not the result
            scrub.safe_print(f"  warning: row count failed: {scrub.scrub_exc(count_exc, 200)}")
        result.rows.append(Row("cold", "load", 1, round(duration, 4), rows=rows))
        scrub.safe_print(
            f"  load {duration:8.3f}s  {len(files)} files -> {cfg.schema}"
            + (f"  {rows:,} rows" if rows is not None else "")
        )
        written = _layout(engine)
        if written:
            scrub.safe_print(f"  wrote {written}")
    finally:
        engine.close()

    return result


def _layout(engine) -> str | None:
    """What the engine actually left in the lakehouse, asked of the engine itself.

    Optional, because the answer has to come from something the job already has: the pyiceberg
    engines read the snapshot summary of the table they hold, Spark reads Iceberg's own `files`
    metadata table. DuckDB's and Sail's jobs carry neither pyiceberg nor a metadata-table reader,
    and adding a package to a measured environment to print a log line is not a trade worth
    making -- they print nothing. A failure here is logged, never raised: the measurement is
    already done by the time this runs.
    """
    report = getattr(engine, "layout", None)
    if report is None:
        return None
    try:
        return report()
    except Exception as exc:  # noqa: BLE001 - a check on the result, not the result
        scrub.safe_print(f"  warning: layout read failed: {scrub.scrub_exc(exc, 200)}")
        return None


def load_row(result: EngineResult) -> Row | None:
    return next((r for r in result.rows if r.phase == "load"), None)
