"""Write a part for every engine whose runner died, read back from its job log.

A runner out of memory (exit 143, no traceback; LEARNING.md) takes the whole job down: the
upload step and every post step are skipped, so the engine leaves no part, `publish` never hears
of it, and the run vanishes from the page (Polars TPC-H SF=300, runs 40249009029, 40252372530).
The job LOG survives. This reads it on the publish runner: the queries it finished, with their
times, and the next query in run order as the one it died in. `publish` then reads the part like
any other failed run: failed at that query, no total.

Only for a FAILED job that names its engine (run_engine.py's first line) and left no part.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

from bench.store import EngineResult, Row, write_engine_part
from bench.suite import suite_class
from bench.tpch.runner import order

KILLED = (
    "Killed: the runner died during this query with no error of its own"
    " (on these runners, out of memory)"
)
# The package whose version the job prints (bench.yml's pip freeze line), per Python engine.
PACKAGE = {
    "polars_iceberg": "polars",
    "duckdb_iceberg": "duckdb",
    "chdb_iceberg": "chdb",
    "lakesail_iceberg": "pysail",
}
STAMP = r"^\S+Z "  # the timestamp GitHub puts on every log line
ENGINE_LINE = re.compile(STAMP + r"(\w+) \| .+ SF=\d+ \| namespace ", re.M)
SETUP_LINE = re.compile(STAMP + r"\s+setup ([\d.]+)s$", re.M)
QUERY_LINE = re.compile(STAMP + r"\s+Q(\d+)\s+(cold|warm)\s+([\d.]+)s\s+(\d+) rows$", re.M)


def gh(*args: str) -> str:
    return subprocess.run(
        ["gh", "api", *args], check=True, capture_output=True, encoding="utf-8"
    ).stdout


def part_from_log(log: str, suite) -> tuple[str, EngineResult] | None:
    found = ENGINE_LINE.search(log)
    if not found:
        return None
    engine = found.group(1)
    version = "unknown"
    if engine in PACKAGE:
        pinned = re.search(STAMP + re.escape(PACKAGE[engine]) + r"==(\S+)$", log, re.M)
        version = pinned.group(1) if pinned else version
    setup = SETUP_LINE.search(log)
    if not setup:
        rows = [Row("cold", "setup", 0, None, status="error", error=KILLED)]
        return engine, EngineResult(version=version, status="setup_failed", rows=rows)
    rows = [Row("cold", "setup", 0, float(setup.group(1)))]
    done = set()
    for q, run_type, dur, count in QUERY_LINE.findall(log):
        rows.append(Row(run_type, "query", int(q), float(dur), rows=int(count)))
        done.add(int(q))
    pending = [q for q in order(suite.N_QUERIES, getattr(suite, "HARD_FIRST", ())) if q not in done]
    if pending:
        rows.append(Row("cold", "query", pending[0], None, status="error", error=KILLED))
    return engine, EngineResult(version=version, rows=rows)


def main() -> int:
    parts = Path(sys.argv[1] if len(sys.argv) > 1 else "parts")
    repo, run_id = os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_RUN_ID"]
    suite = suite_class()
    jobs = json.loads(gh(f"repos/{repo}/actions/runs/{run_id}/jobs?per_page=100"))["jobs"]
    for job in jobs:
        if job["conclusion"] != "failure":
            continue
        # Logs carry the steps' colour codes, which gh prints only when asked to.
        log = gh("--allow-escape-sequences", f"repos/{repo}/actions/jobs/{job['id']}/logs")
        found = part_from_log(log, suite)
        if found is None:
            continue
        engine, result = found
        if (parts / f"{engine}.json").exists():
            continue
        path = write_engine_part(parts, engine, result)
        last = result.rows[-1]
        print(f"{engine}: the runner died at Q{last.query}; recorded from the job log -> {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
