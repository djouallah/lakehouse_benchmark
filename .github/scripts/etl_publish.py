"""Merge the ETL matrix artifacts into one immutable run file, then render docs/etl.

The ETL twin of publish.py, sharing its pieces -- `merge()`, `write_csv()`, `leak_check()` --
from bench/report.py. What differs is the shape of a run: one `load` row per engine instead of
22 queries, so the tables and charts are the ETL's own.

Inputs : parts/*.json               -- one per engine, from etl_run_engine.py
Outputs: results/etl/<run>.json     -- the immutable record, committed
         docs/etl/charts/*.png      -- light and dark, committed
         docs/etl/RESULTS.md        -- the table view, committed
         docs/data/etl_results.csv  -- flattened history, committed
         README.md                  -- the Gluten/Velox vs Spark-OSS block only (bench/speedup.py)
         $GITHUB_STEP_SUMMARY

SEPARATE DIRECTORIES, ON PURPOSE. `store.load_all` globs `*.json` in one directory,
non-recursively, so `results/etl/` is invisible to the TPC-H publish and `results/` to this one.
Two benchmarks, two histories, one file format.

THE CHARTS AND RESULTS.md ARE THE HEADLINE_FILES VIEW. Every run is recorded -- the JSON, the
CSV, the step summary -- but only a run at the headline file count rewrites docs/etl/charts and
docs/etl/RESULTS.md, because that chart is the 1000-file one at a path that never changes.
bench/etl/config.py says what a FILES=100 run did to it before this guard existed.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from bench.charts import LABEL, RECENT_RUNS
from bench.etl import charts
from bench.etl.config import DEFAULT_FILES, ETL_ENGINES, HEADLINE_FILES
from bench.etl.runner import load_row
from bench.report import leak_check, merge, write_csv
from bench.speedup import write_readme
from bench.store import Run, load_all, write_run

# Printed under the table whenever Gluten is on it. LEARNING.md has the upstream PRs.
GLUTEN_NOTE = (
    "Gluten/Velox: Velox does not read the CSVs. Open-source Gluten has no CSV reader on Spark "
    "4.x, so plain Spark reads and parses them, and Velox runs only the filter, casts and Parquet "
    "write after that."
)


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
    """The step summary's table (this run) and RESULTS.md's (means, with a Runs column)."""
    runs = any("runs" in row for row in rows)
    lines = [
        "| Engine | Version | Load | Attach |" + (" Runs |" if runs else "") + " Rows | Error |",
        "|---|---|---:|---:|" + ("---:|" if runs else "") + "---:|---|",
    ]
    for row in rows:
        load = f"{row['load']:,.1f}s" if row["load"] is not None else "—"
        setup = f"{row['setup']:,.1f}s" if row["setup"] is not None else "—"
        count = f"{row['rows']:,}" if row["rows"] is not None else "—"
        error = (row["error"] or "").replace("|", "\\|").replace("\n", " ")[:300]
        n = f" {row['runs']} |" if runs else ""
        lines.append(
            f"| {row['label']} | `{row['version']}` | {load} | {setup} |{n} {count} | "
            f"{'`' + error + '`' if error else '—'} |"
        )
    return lines


def write_results_md(run: Run, rows: list[dict], table, path: Path) -> None:
    counts = {r["rows"] for r in rows if r["rows"] is not None}
    lines = [
        "# Light ETL results",
        "",
        f"{run.sf} AEMO daily CSV files read from OneLake, filtered, cast and written as one "
        f"Iceberg table per engine, on {run.cpu} vCPU / {run.mem_gb} GB "
        f"({run.runner}, Python {run.python}).",
        "",
        f"Last run: `{run.run_started_at}` · commit `{run.git_sha}`"
        + (f" · [Actions run]({run.run_url})" if run.run_url else ""),
        "",
        "## Per engine",
        "",
        *_table(rows),
        "",
        f"Each engine is the mean of its own last {RECENT_RUNS} runs at this file count, or of "
        "as many as it has (Runs). Load = drop and create the table, read the CSVs, transform, "
        "write, commit. Attach = session start and catalog attach, timed separately and "
        "excluded from Load. Rows is the count read back from the table after the latest run; "
        "every engine applies the same filter, so "
        + (
            "they agree."
            if len(counts) <= 1
            else f"**the {len(counts)} distinct counts above mean one of them does not.**"
        ),
        "",
        *([GLUTEN_NOTE, ""] if any(r["engine"] == "pyspark_gluten_iceberg" for r in rows) else []),
        "## History",
        "",
        f"{table.num_rows:,} timed rows across "
        f"{len(set(table.column('run_id').to_pylist()))} runs.",
        "Raw data: one immutable JSON per run under [`results/etl/`](../../results/etl/), "
        "flattened to [`data/etl_results.csv`](../data/etl_results.csv).",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


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
    docs = Path("docs")
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

    table = load_all(results_dir)
    write_csv(table, docs / "data" / "etl_results.csv")
    write_readme()

    if files == HEADLINE_FILES:
        subtitle = (
            f"{run.sf} CSV files → Iceberg on OneLake · {run.cpu} vCPU {run.mem_gb:.0f} GB · "
            f"{run.run_started_at[:10]}"
        )
        for path in charts.render_all(table, files, docs / "etl" / "charts", subtitle):
            print(f"wrote {path}")
        write_results_md(
            run, charts.recent_summary(table, files), table, docs / "etl" / "RESULTS.md"
        )
    else:
        print(
            f"::notice::FILES={files} is not the headline count ({HEADLINE_FILES}): the run and "
            "the CSV are committed; docs/etl/charts and docs/etl/RESULTS.md are left as they are."
        )
    leak_check(
        [
            run_path,
            docs / "etl" / "RESULTS.md",
            docs / "data" / "etl_results.csv",
            Path("README.md"),
        ]
    )
    write_step_summary(run, rows)

    best = rows[0]
    print(f"::notice::fastest load: {best['label']} at {best['load']:,.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
