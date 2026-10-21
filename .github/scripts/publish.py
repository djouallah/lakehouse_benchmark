"""Merge the matrix artifacts into one immutable run file and flatten the suite's history to CSV.

THE SINGLE WRITER of results, and the only job that commits to the repo. Serves BOTH query
suites: bench.yml runs it with BENCH_SUITE=tpch and tpcds.yml with BENCH_SUITE=tpcds, and
everything that differs between them -- where the runs live, the CSV, the headline scale, the
title -- is read off the suite's config class (bench/suite.py).

Inputs : parts/*.json               -- one per engine, from run_engine.py
Outputs: <RESULTS_DIR>/<run>.json   -- the immutable record, committed (results/, results/tpcds/)
         <CSV>                      -- flattened history, committed (docs/data/<suite>_results.csv)
         $GITHUB_STEP_SUMMARY       -- the per-run signal on the Actions page

The dashboard (docs/index.html) reads the CSV and draws every chart and table; nothing else here
renders the results.

SEPARATE DIRECTORIES PER SUITE, ON PURPOSE. `store.load_all` globs `*.json` in one directory,
non-recursively, so results/tpcds/ is invisible to the TPC-H publish and results/ to the TPC-DS
one -- the split etl_publish.py already uses for the ETL. Two histories, one file format.

It touches nothing in Azure: no pyiceberg, no credentials, no network. That is why
requirements/report.txt has no azure-identity in it and the job needs no `id-token` permission.

THE LEAK CHECK IS NOT OPTIONAL, and it lives in bench/report.py with `merge` and `write_csv` --
the three pieces every publish script shares (this one and etl_publish.py).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from bench.charts import LABEL
from bench.report import leak_check, merge, write_csv
from bench.store import Run, load_all, write_run
from bench.suite import suite_class


def summarize(run: Run, engines: tuple[str, ...]) -> list[dict]:
    """Per-engine totals for the step summary's table.

    A run that failed a query has NO total, the way the page shows it: the sum of the queries
    before the failure is not a time for the suite. Its row still lists what failed.
    """
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
        complete = not failed and result.status == "ok"
        out.append(
            {
                "engine": engine,
                "label": LABEL[engine],
                "version": result.version,
                "status": result.status,
                "setup": setup,
                "cold": totals["cold"] if complete else None,
                "warm": totals["warm"] if complete else None,
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

    write_csv(load_all(results_dir), csv)
    leak_check([run_path, csv])
    write_step_summary(run, rows, suite)

    best = rows[0]
    if best["cold"]:
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
    return any(row.phase == "query" for result in run.engines.values() for row in result.rows)


if __name__ == "__main__":
    sys.exit(main())
