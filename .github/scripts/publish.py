"""Merge the matrix artifacts into one immutable run file, then render the public surface.

THE SINGLE WRITER of results, and the only job that commits to the repo. Serves BOTH query
suites: bench.yml runs it with BENCH_SUITE=tpch and tpcds.yml with BENCH_SUITE=tpcds, and
everything that differs between them -- where the runs and the docs live, the CSV, the headline
scale, the title, the statement count -- is read off the suite's config class (bench/suite.py).

Inputs : parts/*.json               -- one per engine, from run_engine.py
Outputs: <RESULTS_DIR>/<run>.json   -- the immutable record, committed (results/, results/tpcds/)
         <DOCS_DIR>/charts/*.png    -- light and dark, committed (docs/charts/, docs/tpcds/charts/)
         <DOCS_DIR>/RESULTS.md      -- the table view, committed
         <CSV>                      -- flattened history, committed (docs/data/<suite>_results.csv)
         README.md                  -- the Gluten/Velox vs Spark-OSS block only (bench/speedup.py)
         $GITHUB_STEP_SUMMARY       -- the per-run signal on the Actions page

SEPARATE DIRECTORIES PER SUITE, ON PURPOSE. `store.load_all` globs `*.json` in one directory,
non-recursively, so results/tpcds/ is invisible to the TPC-H publish and results/ to the TPC-DS
one -- the split etl_publish.py already uses for the ETL. Two histories, one file format.

It touches nothing in Azure: no pyiceberg, no credentials, no network. That is why
requirements/report.txt has no azure-identity in it and the job needs no `id-token` permission.

THE LEAK CHECK IS NOT OPTIONAL, and it lives in bench/report.py with `merge` and `write_csv` --
the three pieces every publish script shares, including the concurrency benchmark's, which is a
package module and cannot sibling-import this script.
"""

from __future__ import annotations

import math
import os
import sys
from pathlib import Path

from bench.report import leak_check, merge, write_csv
from bench.speedup import write_readme
from bench.store import Run, latest_per_engine, load_all, read_run, write_run
from bench.suite import suite_class
from bench.tpch import charts

LABEL = charts.LABEL

# The per-query chart wraps at this many queries per band: TPC-H's 22 stay on one row, TPC-DS's
# 99 become three rows of 33. `per_row` below is the band width that spreads a suite evenly.
PER_ROW_MAX = 33


def summarize(run: Run, engines: tuple[str, ...]) -> list[dict]:
    """Per-engine totals for the markdown tables."""
    out = []
    for engine in engines:
        result = run.engines.get(engine)
        if result is None:
            continue
        totals = {"cold": 0.0, "warm": 0.0}
        failed = []
        for row in result.rows:
            if row.phase != "query":
                continue
            if row.status == "ok" and row.dur is not None:
                totals[row.run_type] += row.dur
            elif row.status == "error" and row.run_type == "cold":
                failed.append(row.query)
        setup = next((r.dur for r in result.rows if r.phase == "setup"), None)
        out.append(
            {
                "engine": engine,
                "label": LABEL[engine],
                "version": result.version,
                "status": result.status,
                "setup": setup,
                "cold": totals["cold"],
                "warm": totals["warm"],
                "failed": failed,
            }
        )
    return sorted(out, key=lambda r: r["cold"] if r["cold"] else float("inf"))


def _table(rows: list[dict], passes: tuple[str, ...] = ("cold", "warm")) -> list[str]:
    """One column per pass the suite runs: TPC-DS has no warm column at all."""
    totals = " | ".join(f"{p.capitalize()} total" for p in passes)
    lines = [
        f"| Engine | Version | {totals} | Attach | Failed queries |",
        "|---|---|" + "---:|" * len(passes) + "---:|---|",
    ]
    for row in rows:
        failed = ", ".join(f"Q{q}" for q in row["failed"]) if row["failed"] else "—"
        cells = " | ".join(f"{row[p]:,.1f}s" if row[p] else "—" for p in passes)
        setup = f"{row['setup']:,.1f}s" if row["setup"] else "—"
        lines.append(f"| {row['label']} | `{row['version']}` | {cells} | {setup} | {failed} |")
    return lines


def only_passes(run: Run, passes: tuple[str, ...]) -> Run:
    """`run` with every row outside the suite's passes dropped.

    TPC-DS ran cold then warm until it went to one pass; its older runs still carry warm rows,
    and the headline must not show a pass the suite no longer runs.
    """
    for result in run.engines.values():
        result.rows = [r for r in result.rows if r.run_type in passes]
    return run


def _rel(target: Path, start: Path) -> str:
    """`target` as a forward-slash path relative to directory `start`, for a markdown link."""
    return os.path.relpath(target, start).replace(os.sep, "/")


def write_results_md(run: Run, rows: list[dict], table, path: Path, suite) -> None:
    """The table view.

    Required, not decorative: the light-mode palette carries a contrast WARN on two of the four
    slots, and the dataviz relief rule says a chart that cannot clear 3:1 must be accompanied by
    visible labels or a table. This is that table. It is also where a reader checks a number a
    chart only shows as a bar.
    """
    lines = [
        "# Results",
        "",
        f"{suite.TITLE}-like, scale factor {run.sf}, {suite.N_QUERIES} queries, on {run.cpu} vCPU "
        f"/ {run.mem_gb} GB ({run.runner}, Python {run.python}).",
        "",
        f"Last run: `{run.run_started_at}` · commit `{run.git_sha}`"
        + (f" · [Actions run]({run.run_url})" if run.run_url else ""),
        "",
        "## Latest run",
        "",
        "Each engine's most recent run at this scale; the newest run may not include every engine.",
        "",
        *_table(rows, suite.PASSES),
        "",
        (
            f"Cold = first pass after attaching the catalog. Warm = the identical "
            f"{suite.N_QUERIES} statements run again immediately. Attach is timed separately and "
            "excluded from both totals."
            if "warm" in suite.PASSES
            else "One cold pass, the first after attaching the catalog. Attach is timed "
            "separately and excluded from the total."
        ),
        "",
    ]

    failures = [
        (engine, row)
        for engine, result in run.engines.items()
        for row in result.rows
        if row.status == "error"
    ]
    if failures:
        lines += ["## Failures", "", "| Engine | Pass | Query | Error |", "|---|---|---|---|"]
        seen = set()
        for engine, row in failures:
            key = (engine, row.query, row.error)
            if key in seen:
                continue
            seen.add(key)
            message = (row.error or "").replace("|", "\\|").replace("\n", " ")[:300]
            lines.append(f"| {LABEL[engine]} | {row.run_type} | Q{row.query} | `{message}` |")
        lines.append("")

    lines += [
        "## Per query, latest run",
        "",
        "Seconds, cold pass. `—` means the query failed; see Failures above.",
        "",
    ]
    engines = [r["engine"] for r in rows]
    lines.append("| Query | " + " | ".join(LABEL[e] for e in engines) + " |")
    lines.append("|---" * (len(engines) + 1) + "|")
    by_query: dict[int, dict[str, str]] = {}
    for engine, result in run.engines.items():
        for row in result.rows:
            if row.phase == "query" and row.run_type == "cold":
                by_query.setdefault(row.query, {})[engine] = (
                    f"{row.dur:,.2f}" if row.status == "ok" and row.dur is not None else "—"
                )
    for query in sorted(by_query):
        cells = " | ".join(by_query[query].get(e, "—") for e in engines)
        lines.append(f"| Q{query} | {cells} |")

    results_dir, csv = Path(suite.RESULTS_DIR), Path(suite.CSV)
    lines += [
        "",
        "## History",
        "",
        f"{table.num_rows:,} timed statements across "
        f"{len(set(table.column('run_id').to_pylist()))} runs.",
        f"Raw data: one immutable JSON per run under [`{suite.RESULTS_DIR}/`]"
        f"({_rel(results_dir, path.parent)}/), "
        f"flattened to [`{_rel(csv, Path('docs'))}`]({_rel(csv, path.parent)}).",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_headline_docs(run: Run, rows: list[dict], table, suite, engines: tuple[str, ...]) -> None:
    """Charts and RESULTS.md, drawn from every engine with a stored run."""
    import pyarrow as pa
    import pyarrow.compute as pc

    docs = Path(suite.DOCS_DIR)
    shown = table.filter(
        pc.and_(
            pc.is_in(table["engine"], value_set=pa.array(list(engines))),
            pc.is_in(table["run_type"], value_set=pa.array(list(suite.PASSES))),
        )
    )
    subtitle = (
        f"{suite.TITLE} SF {run.sf} · {run.cpu} vCPU {run.mem_gb:.0f} GB · "
        f"{run.run_started_at[:10]} · OneLake Iceberg REST catalog"
    )
    per_row = math.ceil(suite.N_QUERIES / math.ceil(suite.N_QUERIES / PER_ROW_MAX))
    for path in charts.render_all(
        shown,
        run.sf,
        docs / "charts",
        subtitle,
        test=suite.TEST,
        n_queries=suite.N_QUERIES,
        per_row=per_row,
        totals_sfs=suite.TOTALS_SFS,
    ):
        print(f"wrote {path}")
    write_results_md(run, rows, shown, docs / "RESULTS.md", suite)


def write_step_summary(run: Run, rows: list[dict], suite) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if not target:
        return
    lines = [
        f"## {suite.TITLE} SF {run.sf} — {run.cpu} vCPU, {run.mem_gb} GB",
        "",
        *_table(rows, suite.PASSES),
        "",
        f"`{run.run_started_at}` · commit `{run.git_sha}`",
    ]
    with open(target, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main() -> int:
    suite = suite_class()
    parts_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "parts")
    results_dir = Path(suite.RESULTS_DIR)
    docs = Path(suite.DOCS_DIR)
    csv = Path(suite.CSV)
    # Off the class, not `from_env`: this job has no Fabric secrets to build a config from.
    sf = int(os.environ.get(suite.SF_ENV, str(suite.HEADLINE_SF)))

    run = merge(parts_dir, sf)
    run.test = suite.TEST
    # Every engine this run measured, not just the suite's roster: a dispatch with an engine the
    # suite leaves out (Sail on TPC-DS, run 36648458503) otherwise summarized to nothing and
    # crashed on rows[0] below, after the measurement and before the commit.
    ran = suite.ENGINES + tuple(e for e in run.engines if e not in suite.ENGINES)
    rows = summarize(run, ran)

    if not any_engine_produced_a_measurement(run):
        write_step_summary(run, rows, suite)
        print(
            "::error::no engine got past its attach -- nothing was measured, so no "
            "results file is being committed. Read the per-engine logs in the artifacts."
        )
        return 1

    run_path = write_run(results_dir, run)
    print(f"wrote {run_path}")

    table = load_all(results_dir)
    write_csv(table, csv)
    # Every engine with a stored run is on the charts, the suite's roster first.
    stored = set(table.column("engine").to_pylist())
    every = suite.ENGINES + tuple(sorted(stored - set(suite.ENGINES)))
    write_readme()

    # RESULTS.md and the per-query chart are the HEADLINE_SF view. The totals chart spans
    # TOTALS_SFS, so a run at any of those scales redraws the docs too -- from the latest stored
    # headline run, which is what the per-scale pages show whichever scale just ran. A run at a
    # scale outside both (SF=1) is recorded and rewrites nothing. etl_publish.py says why.
    headline = run if sf == suite.HEADLINE_SF else None
    if headline is None and sf in suite.TOTALS_SFS:
        stored = sorted(results_dir.glob(f"*_sf{suite.HEADLINE_SF}_*.json"))
        headline = read_run(stored[-1]) if stored else None
    if headline is not None:
        latest = latest_per_engine(results_dir, suite.HEADLINE_SF, every, headline)
        latest = only_passes(latest, suite.PASSES)
        write_headline_docs(latest, summarize(latest, every), table, suite, every)
    else:
        print(
            f"::notice::SF={sf} is not the headline scale ({suite.HEADLINE_SF}): the run and the "
            f"CSV are committed; {docs}/charts and {docs}/RESULTS.md are left as they are."
        )
    leak_check([run_path, docs / "RESULTS.md", csv, Path("README.md")])
    write_step_summary(run, rows, suite)

    best = rows[0]
    print(f"::notice::fastest cold: {best['label']} at {best['cold']:,.1f}s")
    return 0


def any_engine_produced_a_measurement(run: Run) -> bool:
    """True if at least one engine attached and ran a query, whether it finished or failed.

    A failed query IS a result since 2026-10-10: the run stops at it (run_engine.py) and the page
    reads `failed` from it. What is still not a result is a run where nothing got past the
    attach -- run 35486545501 landed a file whose only row was a setup timing.

    The artifacts and the step summary are still produced either way -- that is where you go to
    read the errors. Only the COMMIT is suppressed.
    """
    return any(
        row.phase == "query" for result in run.engines.values() for row in result.rows
    )


if __name__ == "__main__":
    sys.exit(main())
