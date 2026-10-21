"""Merge the ETL matrix artifacts into one immutable run file and flatten the history to CSV.

The ETL twin of publish.py, sharing its pieces -- `merge()`, `write_csv()`, `leak_check()` --
from bench/report.py. What differs is the shape of a run: one `load` row per engine instead of
22 queries, so the step summary's table is the ETL's own.

Inputs : parts/*.json               -- one per engine, from etl_run_engine.py
Outputs: results/etl/<run>.json     -- the immutable record, committed
         docs/data/etl_results.csv  -- flattened history, committed
         $GITHUB_STEP_SUMMARY

The dashboard (docs/index.html) reads the CSV and draws every chart and table; nothing else here
renders the results.

SEPARATE DIRECTORIES, ON PURPOSE. `store.load_all` globs `*.json` in one directory,
non-recursively, so `results/etl/` is invisible to the TPC-H publish and `results/` to this one.
Two benchmarks, two histories, one file format.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from bench.charts import LABEL
from bench.etl.config import DEFAULT_FILES, ETL_ENGINES
from bench.etl.runner import load_row
from bench.report import leak_check, merge, write_csv
from bench.store import Run, load_all, write_run


def summarize(run: Run) -> list[dict]:
    """Per-engine load time, attach time, rows and error, fastest first."""
    out = []
    for engine in ETL_ENGINES:
        result = run.engines.get(engine)
        if result is None:
            continue
        row = load_row(result)
        setup = next((r for r in result.rows if r.phase == "setup"), None)
        error = None
        if result.status == "setup_failed" and setup is not None:
            error = setup.error
        elif row is not None and row.status != "ok":
            error = row.error
        out.append(
            {
                "engine": engine,
                "label": LABEL[engine],
                "version": result.version,
                "status": result.status,
                "setup": setup.dur if setup is not None else None,
                "load": row.dur if row is not None and row.status == "ok" else None,
                "rows": row.rows if row is not None else None,
                "error": error,
            }
        )
    return sorted(out, key=lambda r: r["load"] if r["load"] is not None else float("inf"))


def _table(rows: list[dict]) -> list[str]:
    """The step summary's table: this run, per engine."""
    lines = [
        "| Engine | Version | Load | Attach | Rows | Error |",
        "|---|---|---:|---:|---:|---|",
    ]
    for row in rows:
        load = f"{row['load']:,.1f}s" if row["load"] is not None else "—"
        setup = f"{row['setup']:,.1f}s" if row["setup"] is not None else "—"
        count = f"{row['rows']:,}" if row["rows"] is not None else "—"
        error = (row["error"] or "").replace("|", "\\|").replace("\n", " ")[:300]
        lines.append(
            f"| {row['label']} | `{row['version']}` | {load} | {setup} | {count} | "
            f"{'`' + error + '`' if error else '—'} |"
        )
    return lines


def write_step_summary(run: Run, rows: list[dict]) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if not target:
        return
    lines = [
        f"## Light ETL, {run.sf} files — {run.cpu} vCPU, {run.mem_gb} GB",
        "",
        *_table(rows),
        "",
        f"`{run.run_started_at}` · commit `{run.git_sha}`",
    ]
    with open(target, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def any_engine_loaded(run: Run) -> bool:
    """A run where every load failed records a broken configuration, not a measurement."""
    return any(
        row is not None and row.status == "ok"
        for row in (load_row(result) for result in run.engines.values())
    )


def main() -> int:
    parts_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "parts")
    results_dir = Path("results") / "etl"
    files = int(os.environ.get("ETL_FILES", str(DEFAULT_FILES)))

    run = merge(parts_dir, files)
    run.test = "etl"
    rows = summarize(run)

    if not any_engine_loaded(run):
        write_step_summary(run, rows)
        print(
            "::error::every engine failed to load -- nothing was measured, so no results file is "
            "being committed. Read the per-engine logs in the artifacts."
        )
        return 1

    run_path = write_run(results_dir, run)
    print(f"wrote {run_path}")

    csv = Path("docs") / "data" / "etl_results.csv"
    write_csv(load_all(results_dir), csv)
    leak_check([run_path, csv])
    write_step_summary(run, rows)

    best = rows[0]
    print(f"::notice::fastest load: {best['label']} at {best['load']:,.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
