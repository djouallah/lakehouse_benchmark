"""Grade and record a workload run: `python workload_publish.py parts [--write] [--check]`.

Always: the per-leg table and the parity verdict, to stdout and the job summary.
--write: results/workload/<run_id>.json, the run's immutable record, and docs/data/workload.json,
         the page's copy of the latest run (the Real workload tab), keyed by bench engine id.
--check: exit 1 unless every leg in the run built and the legs agree.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from bench.report import leak_check
from bench.workload import parity
from bench.workload.config import BENCH_ENGINE

REPO = Path(__file__).resolve().parents[2]
RESULTS = REPO / "results" / "workload"
PAGE_DATA = REPO / "docs" / "data" / "workload.json"


def load(parts: Path) -> dict[str, dict]:
    legs = {}
    for path in sorted(parts.glob("workload_*.json")):
        part = json.loads(path.read_text(encoding="utf-8"))
        legs[part["engine"]] = part
    return legs


def counts(leg: dict) -> dict[str, int]:
    """Models and tests, built and passed. A retry re-runs only what failed, so its results
    override the build's, node by node."""
    merged = {n["node"]: n for n in leg.get("nodes") or []}
    merged |= {n["node"]: n for n in leg.get("retry_nodes") or []}
    nodes = list(merged.values())
    models = [n for n in nodes if n["node"].startswith("model.")]
    tests = [n for n in nodes if n["node"].startswith("test.")]
    return {
        "models": len(models),
        "models_ok": sum(n["status"] == "success" for n in models),
        "tests": len(tests),
        "tests_ok": sum(n["status"] in ("pass", "warn") for n in tests),
    }


def dag(legs: dict[str, dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for leg in legs.values():
        for name, node in (leg.get("dag") or {}).items():
            entry = out.setdefault(name, {"materialized": node["materialized"], "parents": []})
            entry["parents"] = sorted(set(entry["parents"]) | set(node["parents"]))
    return out


def adapter(versions: dict[str, str]) -> str:
    """The leg's dbt adapter and its version, e.g. "dbt-duckdb 1.10.1"."""
    name = next((k for k in versions if k.startswith("dbt-") and k != "dbt-core"), "")
    return f"{name} {versions[name]}".strip() if name else ""


def model_seconds(leg: dict) -> dict[str, float]:
    """Seconds per model; a retry's result overrides the build's."""
    merged = {n["node"]: n for n in leg.get("nodes") or []}
    merged |= {n["node"]: n for n in leg.get("retry_nodes") or []}
    return {
        uid.rsplit(".", 1)[-1]: n["seconds"]
        for uid, n in merged.items()
        if uid.startswith("model.") and n["status"] == "success"
    }


def page_data(legs: dict[str, dict], grade: dict) -> dict:
    """The latest run as the Real workload tab reads it: one entry per bench engine id."""
    out = {
        "run_id": next(iter(legs.values()))["run_id"],
        "run_url": os.environ.get("BENCH_RUN_URL") or next(iter(legs.values())).get("run_url", ""),
        "finished": max(leg.get("finished", "") for leg in legs.values()),
        "baseline": BENCH_ENGINE.get(grade["baseline"], grade["baseline"]),
        "rel_tol": parity.REL_TOL,
        "parity_ok": len(grade["engines"]) > 1 and not grade["failures"],
        # The project's DAG as dbt published it, the union over the legs that built: every tree
        # has the same model names, so the union of their edges is the project's DAG.
        "dag": dag(legs),
        "legs": {},
    }
    base = next((leg.get("fingerprint") for e, leg in legs.items() if e == grade["baseline"]), None)
    for engine, leg in legs.items():
        fp = leg.get("fingerprint") or {}
        entry = {
            "target": engine,
            "status": leg["status"],
            "build_seconds": leg.get("build_seconds"),
            **counts(leg),
            "rows": fp.get("rows_total"),
            "same_rows": bool(base and fp and fp.get("rows_total") == base.get("rows_total")),
            "diff": grade["diffs"].get(engine, {}),
            "versions": leg.get("versions", {}),
            "adapter": adapter(leg.get("versions", {})),
            "model_seconds": model_seconds(leg),
            "error": (leg.get("error") or "")[:300],
        }
        out["legs"][BENCH_ENGINE[engine]] = entry
    return out


def summary(legs: dict[str, dict], grade: dict) -> str:
    lines = [
        "## dbt workload (Iceberg)",
        "",
        "| leg | status | build s | models ok | tests ok | fct_summary rows | mw_sum | price_sum |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for engine, leg in legs.items():
        c = counts(leg)
        fp = leg.get("fingerprint") or {}
        lines.append(
            f"| {engine} | {leg['status']} | {leg.get('build_seconds', '')} "
            f"| {c['models_ok']}/{c['models']} | {c['tests_ok']}/{c['tests']} "
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
        PAGE_DATA.write_text(json.dumps(page_data(legs, grade), indent=2) + "\n", encoding="utf-8")
        leak_check([out, PAGE_DATA])
        print(f"wrote {out.relative_to(REPO)} and {PAGE_DATA.relative_to(REPO)}")

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
