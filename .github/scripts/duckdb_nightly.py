"""Install the newest DuckDB 2.0 nightly CLI whose extensions are published.

WHY NOT artifacts.duckdb.org/v2.0-cyanoptera/duckdb-cli-linux-amd64.tar.gz. That link is the CLI
from the LAST nightly, and the last nightly usually has no extensions: DuckDB's `Main (alphaN)`
workflow builds the CLI every night, but its "Deploy extensions" job -- the one that publishes
extensions.duckdb.org/v2.0.0-alphaN/... -- is skipped whenever any extension build in the matrix
fails. Checked 2026-10-06: it ran on 1 of the last 10 nightlies, and the newest CLI got a 404 for
iceberg.duckdb_extension, so `ATTACH ... TYPE ICEBERG` could not even load.

So: walk the nightlies newest first and take the first whose CLI artifact still exists and whose
EXTENSIONS are all on extensions.duckdb.org. `DUCKDB_RUN_ID` pins one instead (a workflow passes
the run its generator used, so generator and reader are one build).

Writes the binary under $RUNNER_TEMP/duckdb, adds it to $GITHUB_PATH, and outputs `run-id` and
`version`. Needs `gh` and GH_TOKEN: artifact downloads are authenticated even on a public repo.
"""

from __future__ import annotations

import io
import json
import os
import re
import stat
import subprocess
import sys
import tarfile
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

REPO = "duckdb/duckdb"
BRANCH = "v2.0-cyanoptera"
ARTIFACT = "duckdb-cli-linux-amd64.tar.gz"
PLATFORM = "linux_amd64"
# What the DuckDB jobs load: the catalog (iceberg, which reads manifests with avro), storage
# (azure, httpfs for the REST calls) and the TPC-DS generator.
EXTENSIONS = ("iceberg", "avro", "azure", "httpfs", "tpcds")
_RUN_NAME = re.compile(r"^Main \(alpha(\d+)\)$")


def alpha(run_name: str) -> int | None:
    """`Main (alpha44357)` -> 44357; anything else -> None."""
    match = _RUN_NAME.match(run_name)
    return int(match.group(1)) if match else None


def version(number: int) -> str:
    return f"v2.0.0-alpha{number}"


def extension_url(number: int, extension: str) -> str:
    base = f"https://extensions.duckdb.org/{version(number)}/{PLATFORM}"
    return f"{base}/{extension}.duckdb_extension.gz"


def pick(runs: list[dict], missing_extensions, has_artifact, log=print) -> tuple[dict, int]:
    """The first run (newest first) that is a nightly, has its CLI artifact and every extension.

    Pure apart from the two callables, so tests/test_duckdb_nightly.py can drive it.
    """
    for run in runs:
        number = alpha(run.get("name", ""))
        if number is None:
            continue
        if not has_artifact(run["id"]):
            log(f"  alpha{number} (run {run['id']}): CLI artifact gone")
            continue
        missing = missing_extensions(number)
        if missing:
            log(f"  alpha{number} (run {run['id']}): not published: {', '.join(missing)}")
            continue
        return run, number
    raise RuntimeError(f"no {BRANCH} nightly has a CLI artifact and {', '.join(EXTENSIONS)}")


# --- I/O ----------------------------------------------------------------------------------------


def _gh(path: str) -> dict:
    out = subprocess.run(["gh", "api", path], capture_output=True, text=True, check=True)
    return json.loads(out.stdout)


def _missing_extensions(number: int) -> list[str]:
    missing = []
    for extension in EXTENSIONS:
        request = urllib.request.Request(
            extension_url(number, extension), method="HEAD", headers={"User-Agent": "curl/8"}
        )
        try:
            urllib.request.urlopen(request, timeout=30).close()
        except urllib.error.HTTPError:
            missing.append(extension)
    return missing


def _artifact(run_id: int) -> dict | None:
    found = _gh(f"repos/{REPO}/actions/runs/{run_id}/artifacts?name={ARTIFACT}")["artifacts"]
    live = [a for a in found if not a["expired"]]
    return live[0] if live else None


def _has_artifact(run_id: int) -> bool:
    return _artifact(run_id) is not None


def _nightlies() -> list[dict]:
    query = f"branch={BRANCH}&event=workflow_dispatch&per_page=100"
    runs = _gh(f"repos/{REPO}/actions/runs?{query}")["workflow_runs"]
    return sorted(runs, key=lambda r: r["created_at"], reverse=True)


def _unpack(blob: bytes, dest: Path) -> Path:
    """The artifact is a zip (classic) or the raw .tar.gz (unzipped upload); the CLI is inside."""
    if blob[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(blob)) as archive:
            inner = [n for n in archive.namelist() if not n.endswith("/")]
            if len(inner) == 1 and inner[0].endswith((".tar.gz", ".tgz")):
                return _unpack(archive.read(inner[0]), dest)
            archive.extractall(dest)
    else:
        with tarfile.open(fileobj=io.BytesIO(blob), mode="r:*") as archive:
            archive.extractall(dest, filter="data")
    binary = next(p for p in dest.rglob("duckdb") if p.is_file())
    binary.chmod(binary.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return binary


def _append(env_file: str, line: str) -> None:
    path = os.environ.get(env_file)
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def main() -> int:
    pinned = os.environ.get("DUCKDB_RUN_ID", "").strip()
    if pinned:
        run = _gh(f"repos/{REPO}/actions/runs/{pinned}")
        run, number = pick([run], _missing_extensions, _has_artifact)
        print(f"pinned run {pinned}: {version(number)}")
    else:
        print(f"newest {BRANCH} nightly with {', '.join(EXTENSIONS)} published:")
        run, number = pick(_nightlies(), _missing_extensions, _has_artifact)

    artifact = _artifact(run["id"])
    blob = subprocess.run(
        ["gh", "api", f"repos/{REPO}/actions/artifacts/{artifact['id']}/zip"],
        capture_output=True,
        check=True,
    ).stdout
    dest = Path(os.environ.get("RUNNER_TEMP", ".")) / "duckdb"
    dest.mkdir(parents=True, exist_ok=True)
    binary = _unpack(blob, dest)

    print(
        f"{version(number)} from run {run['id']} ({run['head_sha'][:10]}, {run['created_at']}) "
        f"-> {binary}"
    )
    _append("GITHUB_PATH", str(binary.parent))
    _append("GITHUB_OUTPUT", f"run-id={run['id']}")
    _append("GITHUB_OUTPUT", f"version={version(number)}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
