"""The table maintenance record, published:
`workload_maintain.py publish <record> [--tests <tests record>] [--write]`.

.github/scripts/maintain (apache/iceberg-go) does the maintenance and writes the record: per table,
before/after counts and each step's result or its exact error. workload_test.py runs the dbt tests
on every leg and writes the tests record; it goes into the same files under "tests". This prints
the step summary, and with --write writes results/maintenance/<run_id>.json plus
docs/data/maintenance.json, the Real workload tab's copy. table_maintenance.yml's commit loop
re-runs it after every reset onto main.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from bench.report import leak_check
from bench.workload.config import BENCH_ENGINE, ENGINES

REPO = Path(__file__).resolve().parents[2]

RESULTS = REPO / "results" / "maintenance"
PAGE_DATA = REPO / "docs" / "data" / "maintenance.json"


def totals(entries: list[dict]) -> dict:
    """One engine's sums over its tables."""

    def total(side: str, field: str):
        values = [t[side][field] for t in entries if t.get(side) and t[side].get(field) is not None]
        return sum(values) if values else None

    return {
        "tables": len(entries),
        "errors": sum("error" in t for t in entries),
        "skipped": sum("skipped" in t for t in entries),
        "files_before": total("before", "data_files"),
        "files_after": total("after", "data_files"),
        "bytes_after": total("after", "bytes"),
        "snapshots_before": total("before", "snapshots"),
        "snapshots_after": total("after", "snapshots"),
        "compacted": sum((t.get("compact") or {}).get("rewritten", 0) for t in entries),
        "compacted_into": sum((t.get("compact") or {}).get("added", 0) for t in entries),
        "compacted_bytes": sum((t.get("compact") or {}).get("rewritten_bytes", 0) for t in entries),
        "expired": sum(t.get("expired") or 0 for t in entries),
        "orphans_deleted": sum((t.get("orphans") or {}).get("deleted", 0) for t in entries),
        "orphans_failed": sum((t.get("orphans") or {}).get("failed", 0) for t in entries),
        "orphan_bytes": sum((t.get("orphans") or {}).get("bytes", 0) for t in entries),
    }


def test_results(tests: dict | None) -> dict:
    """Each leg's daily tests as the page reads them: counts, and the tests that did not pass."""
    if not tests:
        return {}
    return {
        "run_url": tests.get("run_url", ""),
        "finished": tests.get("finished", ""),
        "legs": {
            key: {
                "tests": leg["tests"],
                "tests_ok": leg["tests_ok"],
                "seconds": leg.get("seconds"),
                "not_passed": {
                    n["node"].rsplit(".", 1)[-1]: n["status"]
                    for n in leg.get("nodes", [])
                    if n["status"] not in ("pass", "warn")
                },
                **({"error": leg["error"]} if leg.get("error") else {}),
            }
            for key, leg in tests.get("legs", {}).items()
        },
    }


def page_data(record: dict) -> dict:
    """The latest run as the Real workload tab reads it, keyed by bench engine id."""
    engines = {}
    for engine in ENGINES:
        entries = [t for t in record["tables"] if t["engine"] == engine]
        if entries:
            engines[BENCH_ENGINE[engine]] = {
                "totals": totals(entries),
                "tables": {
                    t["table"]: {k: v for k, v in t.items() if k not in ("engine", "table")}
                    for t in entries
                },
            }
    return {
        "run_id": record["run_id"],
        "run_url": record["run_url"],
        "started": record["started"],
        "finished": record["finished"],
        "settings": record["settings"],
        "versions": record["versions"],
        "engines": engines,
        "tests": test_results(record.get("tests")),
    }


def summary(record: dict) -> str:
    s = record["settings"]
    lines = [
        f"## Table maintenance: compact to {s['target_mb']} MiB, expire > {s['expire_days']:g}d, "
        f"orphans > {s['orphan_days']:g}d",
        "",
        "| table | data files | snapshots | compacted | expired | orphans deleted | s | error |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for t in record["tables"]:
        b, a = t.get("before") or {}, t.get("after") or {}
        c, o = t.get("compact") or {}, t.get("orphans") or {}
        err = t.get("skipped") or "; ".join(f"{k}: {v}" for k, v in (t.get("error") or {}).items())
        lines.append(
            f"| `{t['table']}` | {b.get('data_files', '')} → {a.get('data_files', '')} "
            f"| {b.get('snapshots', '')} → {a.get('snapshots', '')} "
            f"| {c.get('rewritten', '')} → {c.get('added', '')} | {t.get('expired', '')} "
            f"| {o.get('deleted', '')} ({o.get('bytes', 0) / 1e9:.2f} GB) | {t.get('seconds', '')} "
            f"| {err.replace('|', '/')} |"
        )
    tests = test_results(record.get("tests")).get("legs", {})
    if tests:
        lines += ["", "| leg | tests passed | s | not passed |", "|---|---:|---:|---|"]
        for key, t in tests.items():
            bad = ", ".join(f"{k} ({v})" for k, v in t["not_passed"].items()) or t.get("error", "")
            lines.append(f"| {key} | {t['tests_ok']}/{t['tests']} | {t['seconds']} | {bad[:300]} |")
    return "\n".join(lines) + "\n"


def publish(record_path: Path, write: bool, tests_path: Path | None = None) -> int:
    record = json.loads(record_path.read_text(encoding="utf-8"))
    if tests_path is not None and tests_path.exists():
        record["tests"] = json.loads(tests_path.read_text(encoding="utf-8"))
    text = summary(record)
    print(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write(text)
    if write:
        RESULTS.mkdir(parents=True, exist_ok=True)
        out = RESULTS / f"{record['run_id']}.json"
        out.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        PAGE_DATA.write_text(json.dumps(page_data(record), indent=2) + "\n", encoding="utf-8")
        leak_check([out, PAGE_DATA])
        print(f"wrote {out.relative_to(REPO)} and {PAGE_DATA.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 3 or sys.argv[1] != "publish":
        sys.exit(__doc__)
    tests = Path(sys.argv[sys.argv.index("--tests") + 1]) if "--tests" in sys.argv else None
    sys.exit(publish(Path(sys.argv[2]), "--write" in sys.argv, tests))
