"""Record a workload run: `python workload_publish.py parts [--write] [--check]`.

Always: the per-leg table, to stdout and the job summary.
--write: results/workload/<run_id>.json, the run's immutable record, and docs/data/workload.json,
         the page's copy of the latest state (the Real workload tab), keyed by bench engine id.
         An engine this run asked for (WORKLOAD_ENGINES) that left no part had its runner killed
         -- the out-of-memory case, exit 143, nothing uploaded -- and stays on the page as
         "died", carried forward from the previous page data with its last good run. An engine
         this run did not ask for is carried forward unchanged.
--check: exit 1 when a leg failed or died.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

from bench.report import leak_check
from bench.workload.config import BENCH_ENGINE, ENGINES

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
    """The project's DAG: the union of the legs' edges, with each model's description and notes
    (the shared patch files give every leg the same ones)."""
    out: dict[str, dict] = {}
    for leg in legs.values():
        for name, node in (leg.get("dag") or {}).items():
            entry = out.setdefault(name, {"materialized": node["materialized"], "parents": []})
            entry["parents"] = sorted(set(entry["parents"]) | set(node["parents"]))
            for field in ("description", "differences"):
                if node.get(field) and not entry.get(field):
                    entry[field] = node[field]
    return out


def model_config(leg: dict) -> dict[str, dict]:
    """How this leg builds each model, from its own manifest."""
    fields = ("materialized", "strategy", "unique_key", "merge_clauses", "path", "sql_lines")
    return {
        name: {f: node[f] for f in fields if f in node}
        for name, node in (leg.get("dag") or {}).items()
    }


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


def files_behind(leg: dict) -> int | None:
    """Daily files the leg has not folded yet: its own assert_all_daily_files_processed_* tests
    count them ("Got N results", a warning) and pass at zero. None when they did not run."""
    merged = {n["node"]: n for n in leg.get("nodes") or []}
    merged |= {n["node"]: n for n in leg.get("retry_nodes") or []}
    found = None
    for uid, n in merged.items():
        if ".assert_all_daily_files_processed_" not in uid:
            continue
        m = re.search(r"Got (\d+) result", n.get("message") or "")
        count = int(m.group(1)) if m else (0 if n["status"] == "pass" else None)
        if count is not None:
            found = max(found or 0, count)
    return found


def page_data(
    legs: dict[str, dict], previous: dict | None = None, asked: list[str] | None = None
) -> dict:
    """The latest state as the Real workload tab reads it: one entry per bench engine id.

    `previous` is the page data before this run, `asked` the engines this run was given."""
    previous = previous or {}
    asked = list(legs) if asked is None else asked
    first = next(iter(legs.values()), {})
    # Every runner dead leaves no part to read the run from: the workflow's own id then.
    run_id = first.get("run_id") or os.environ.get("GITHUB_RUN_ID", "")
    run_url = os.environ.get("BENCH_RUN_URL") or first.get("run_url", "")
    out = {
        "run_id": run_id,
        "run_url": run_url,
        "finished": max((leg.get("finished", "") for leg in legs.values()), default=""),
        # The project's DAG as dbt published it, the union over the legs that built: every tree
        # has the same model names, so the union of their edges is the project's DAG.
        "dag": dag(legs) or previous.get("dag", {}),
        "legs": {},
    }
    before = previous.get("legs") or {}
    for engine, leg in legs.items():
        old = before.get(BENCH_ENGINE[engine], {})
        ok = leg["status"] == "success"
        out["legs"][BENCH_ENGINE[engine]] = {
            "target": engine,
            "status": leg["status"],
            "build_seconds": leg.get("build_seconds"),
            "peak_rss_mb": leg.get("peak_rss_mb"),
            "process_limit": leg.get("process_limit"),
            "files_behind": files_behind(leg),
            **counts(leg),
            "versions": leg.get("versions", {}),
            "adapter": adapter(leg.get("versions", {})),
            "model_seconds": model_seconds(leg),
            "model_rows": leg.get("model_rows", {}),
            "model_config": model_config(leg),
            "error": (leg.get("error") or "")[:300],
            "last_ok": (
                {"run_id": run_id, "run_url": run_url, "finished": leg.get("finished", "")}
                if ok
                else old.get("last_ok")
            ),
        }
    for engine in ENGINES:
        old = before.get(BENCH_ENGINE[engine])
        if engine in legs or not old:
            continue
        if engine in asked:
            # Asked for and no part: the runner was killed before it could upload one.
            old = {**old, "status": "died", "error": "runner died (likely out of memory)"}
        out["legs"][BENCH_ENGINE[engine]] = old
    return out


def summary(legs: dict[str, dict]) -> str:
    lines = [
        "## dbt workload (Iceberg)",
        "",
        "| leg | status | files/run | build s | peak RSS MB | files behind | models ok "
        "| tests ok |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for engine, leg in legs.items():
        c = counts(leg)
        behind = files_behind(leg)
        lines.append(
            f"| {engine} | {leg['status']} | {leg.get('process_limit', '')} "
            f"| {leg.get('build_seconds', '')} "
            f"| {leg.get('peak_rss_mb', '')} | {'' if behind is None else behind} "
            f"| {c['models_ok']}/{c['models']} | {c['tests_ok']}/{c['tests']} |"
        )
    lines.append("")
    for engine, leg in legs.items():
        if leg.get("error"):
            lines += [f"**{engine} error:**", "```", leg["error"][:1500], "```", ""]
    return "\n".join(lines) + "\n"


def asked_engines() -> list[str] | None:
    """The engines this run was given (the land job's matrix), so a missing part means died."""
    raw = os.environ.get("WORKLOAD_ENGINES")
    return json.loads(raw) if raw else None


def main(argv: list[str]) -> int:
    parts = Path(argv[1]) if len(argv) > 1 and not argv[1].startswith("--") else Path("parts")
    legs = load(parts)
    asked = asked_engines()
    text = summary(legs)
    print(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write(text)

    if "--write" in argv and (legs or asked):
        run_id = next(iter(legs.values()), {}).get("run_id") or os.environ.get("GITHUB_RUN_ID")
        RESULTS.mkdir(parents=True, exist_ok=True)
        out = RESULTS / f"{run_id}.json"
        record = {
            "run_id": run_id,
            "run_url": os.environ.get("BENCH_RUN_URL", ""),
            "started": os.environ.get("BENCH_RUN_STARTED_AT", ""),
            "legs": legs,
        }
        out.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        previous = json.loads(PAGE_DATA.read_text(encoding="utf-8")) if PAGE_DATA.exists() else {}
        page = page_data(legs, previous, asked)
        PAGE_DATA.write_text(json.dumps(page, indent=2) + "\n", encoding="utf-8")
        leak_check([out, PAGE_DATA])
        print(f"wrote {out.relative_to(REPO)} and {PAGE_DATA.relative_to(REPO)}")

    if "--check" in argv:
        failed = [e for e, leg in legs.items() if leg["status"] != "success"]
        died = [e for e in (asked or []) if e not in legs]
        if failed:
            print(f"::error::legs failed: {', '.join(failed)}")
        if died:
            print(f"::error::runner died, no result: {', '.join(died)}")
        return 1 if failed or died or not legs else 0
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
