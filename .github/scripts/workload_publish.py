"""Grade and record a workload run: `python workload_publish.py parts [--write] [--check]`.

Always: the per-leg table and the parity verdict, to stdout and the job summary.
--write: results/workload/<run_id>.json, the run's immutable record. NOTHING under docs/: the
         dashboard does not show this test until every leg passes (workload.yml).
--check: exit 1 unless every leg in the run built and the legs agree.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from bench.report import leak_check
from bench.workload import parity

REPO = Path(__file__).resolve().parents[2]
RESULTS = REPO / "results" / "workload"


def load(parts: Path) -> dict[str, dict]:
    legs = {}
    for path in sorted(parts.glob("workload_*.json")):
        part = json.loads(path.read_text(encoding="utf-8"))
        legs[part["engine"]] = part
    return legs


def summary(legs: dict[str, dict], grade: dict) -> str:
    lines = [
        "## dbt workload (Iceberg)",
        "",
        "| leg | status | build s | models ok | tests ok | fct_summary rows | mw_sum | price_sum |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for engine, leg in legs.items():
        # A retry re-runs only what failed, so its results override the build's, node by node.
        merged = {n["node"]: n for n in leg.get("nodes") or []}
        merged |= {n["node"]: n for n in leg.get("retry_nodes") or []}
        nodes = list(merged.values())
        models = [n for n in nodes if n["node"].startswith("model.")]
        tests = [n for n in nodes if n["node"].startswith("test.")]
        fp = leg.get("fingerprint") or {}
        lines.append(
            f"| {engine} | {leg['status']} | {leg.get('build_seconds', '')} "
            f"| {sum(n['status'] == 'success' for n in models)}/{len(models)} "
            f"| {sum(n['status'] in ('pass', 'warn') for n in tests)}/{len(tests)} "
            f"| {fp.get('rows_total', '')} | {fp.get('mw_sum', '')} | {fp.get('price_sum', '')} |"
        )
    lines.append("")
    for engine, leg in legs.items():
        if leg.get("error"):
            lines += [f"**{engine} error:**", "```", leg["error"][:1500], "```", ""]
    if grade["baseline"] and len(grade["engines"]) > 1:
        lines.append(f"Parity baseline: **{grade['baseline']}**, REL_TOL {parity.REL_TOL:.0e}")
        for engine, diffs in grade["diffs"].items():
            cells = ", ".join(f"{k} {v:.2e}" for k, v in diffs.items())
            lines.append(f"- {engine}: {cells}")
        lines.append("")
    if grade["failures"]:
        lines.append("**PARITY FAILED**")
        lines += [f"- {f}" for f in grade["failures"]]
    elif len(grade["engines"]) > 1:
        lines.append(f"**PARITY OK** across {', '.join(grade['engines'])}")
    else:
        lines.append("Parity not graded: fewer than two legs built.")
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    parts = Path(argv[1]) if len(argv) > 1 and not argv[1].startswith("--") else Path("parts")
    legs = load(parts)
    fps = {e: leg["fingerprint"] for e, leg in legs.items() if leg.get("fingerprint")}
    grade = parity.grade(fps)
    text = summary(legs, grade)
    print(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write(text)

    if "--write" in argv and legs:
        run_id = next(iter(legs.values()))["run_id"]
        RESULTS.mkdir(parents=True, exist_ok=True)
        out = RESULTS / f"{run_id}.json"
        record = {
            "run_id": run_id,
            "run_url": os.environ.get("BENCH_RUN_URL", ""),
            "started": os.environ.get("BENCH_RUN_STARTED_AT", ""),
            "legs": legs,
            "parity": grade,
        }
        out.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        leak_check([out])
        print(f"wrote {out.relative_to(REPO)}")

    if "--check" in argv:
        failed = [e for e, leg in legs.items() if leg["status"] != "success"]
        if failed:
            print(f"::error::legs failed: {', '.join(failed)}")
        if grade["failures"]:
            print("::error::parity failed: " + "; ".join(grade["failures"]))
        return 1 if failed or grade["failures"] or not legs else 0
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
