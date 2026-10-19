"""Install the newest DuckDB 2.0 nightly CLI whose extensions can be had.

WHY NOT artifacts.duckdb.org/v2.0-cyanoptera/duckdb-cli-linux-amd64.tar.gz. That link is the CLI
from the LAST nightly, and the last nightly usually has no extensions: DuckDB's `Main (alphaN)`
workflow builds the CLI every night, but its "Deploy extensions" job -- the one that publishes
extensions.duckdb.org/v2.0.0-alphaN/... -- is skipped whenever any extension build in the matrix
fails. Checked 2026-10-06: it ran on 1 of the last 10 nightlies, and the newest CLI got a 404 for
iceberg.duckdb_extension, so `ATTACH ... TYPE ICEBERG` could not even load.

AND NOT ONLY THE DEPLOYED ONES. The deploy also waits on every platform: alpha48043 (run
38578625719, 2026-10-14) built every Linux extension but sat `queued` on the osx_amd64 builds, so
nothing was deployed -- while the Linux builds sat in the run's own artifacts. So a nightly is
usable when its extensions are on extensions.duckdb.org (signed) OR in its artifacts
(`EXTENSION_ARTIFACTS`, unsigned). Those go into a local repository under $RUNNER_TEMP, and
bench/duckdb_cli.py runs the CLI with `-unsigned` and that repository when DUCKDB_EXTENSION_REPO
is set.

So: walk the nightlies newest first and take the first whose CLI artifact still exists and whose
EXTENSIONS are published or in its artifacts. `DUCKDB_RUN_ID` pins one instead (a workflow passes
the run its generator used, so generator and reader are one build). Then load all of them once,
so a bad install fails here and not mid-benchmark.

Writes the binary under $RUNNER_TEMP/duckdb, adds it to $GITHUB_PATH, and outputs `run-id`,
`version` and `extensions` (published|artifacts). Needs `gh` and GH_TOKEN: artifact downloads are
authenticated even on a public repo.
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
# (azure, httpfs for the REST calls). Not tpcds: TPC-DS data comes from tpcgen-cli since
# 2026-10-20 (bench/tpcds/generate.py), and a nightly is no longer held back waiting for it.
EXTENSIONS = ("iceberg", "avro", "azure", "httpfs")
# Where each one sits among a nightly's run artifacts when it was never deployed: DuckDB's
# .github/config/{cloud,core}.cmake builds, and the Linux release build's own extensions.
EXTENSION_ARTIFACTS = {
    "iceberg": "cloud-extensions-linux_amd64",
    "azure": "cloud-extensions-linux_amd64",
    "avro": "core-extensions-linux_amd64",
    "httpfs": "release-core-extensions-linux-amd64",
}
# Set (in $GITHUB_ENV) to the local repository when the extensions came from artifacts.
REPO_ENV = "DUCKDB_EXTENSION_REPO"
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


def pick(
    runs: list[dict], missing_extensions, has_artifact, has_extension_artifacts, log=print
) -> tuple[dict, int, str]:
    """The first run (newest first) that is a nightly with its CLI artifact and every extension,
    and where the extensions come from: `published`, else `artifacts`.

    Pure apart from the three callables, so tests/test_duckdb_nightly.py can drive it.
    """
    for run in runs:
        number = alpha(run.get("name", ""))
        if number is None:
            continue
        if not has_artifact(run["id"]):
            log(f"  alpha{number} (run {run['id']}): CLI artifact gone")
            continue
        missing = missing_extensions(number)
        if not missing:
            return run, number, "published"
        if has_extension_artifacts(run["id"]):
            log(f"  alpha{number} (run {run['id']}): not published, taking its artifacts")
            return run, number, "artifacts"
        log(
            f"  alpha{number} (run {run['id']}): not published: {', '.join(missing)}; "
            "no extension artifacts"
        )
    raise RuntimeError(f"no {BRANCH} nightly has a CLI artifact and {', '.join(EXTENSIONS)}")


def extract_extensions(blobs: dict[str, bytes], number: int, repo: Path) -> list[Path]:
    """EXTENSIONS out of their artifact zips (by artifact name) into `repo`, in the layout DuckDB
    reads a local repository with: <repo>/<version>/<platform>/<name>.duckdb_extension. The zips
    already hold that layout; only the members named in EXTENSIONS are written."""
    written = []
    for extension in EXTENSIONS:
        group = EXTENSION_ARTIFACTS[extension]
        member = f"{version(number)}/{PLATFORM}/{extension}.duckdb_extension"
        with zipfile.ZipFile(io.BytesIO(blobs[group])) as archive:
            if member not in archive.namelist():
                raise RuntimeError(f"{member} is not in {group}")
            target = repo / member
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(member))
        written.append(target)
    return written


def check_script(repo: str | None) -> str:
    """Install and load every extension the way a session will, then list what loaded."""
    lines = []
    if repo:
        lines += [
            f"SET custom_extension_repository = '{repo}';",
            f"SET autoinstall_extension_repository = '{repo}';",
        ]
    lines += [f"INSTALL {e}; LOAD {e};" for e in EXTENSIONS]
    lines.append(
        "SELECT extension_name, extension_version, install_mode FROM duckdb_extensions() "
        "WHERE loaded ORDER BY 1;"
    )
    return "\n".join(lines) + "\n"


# --- I/O ----------------------------------------------------------------------------------------


def _gh(path: str) -> dict:
    out = subprocess.run(["gh", "api", path], capture_output=True, text=True, check=True)
    return json.loads(out.stdout)


def _gh_bytes(path: str) -> bytes:
    return subprocess.run(["gh", "api", path], capture_output=True, check=True).stdout


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


def _extension_artifacts(run_id: int) -> dict[str, dict]:
    """The run's live artifacts among EXTENSION_ARTIFACTS' groups, by name."""
    groups = set(EXTENSION_ARTIFACTS.values())
    found = _gh(f"repos/{REPO}/actions/runs/{run_id}/artifacts?per_page=100")["artifacts"]
    return {a["name"]: a for a in found if a["name"] in groups and not a["expired"]}


def _has_extension_artifacts(run_id: int) -> bool:
    return set(_extension_artifacts(run_id)) == set(EXTENSION_ARTIFACTS.values())


def _nightlies() -> list[dict]:
    """Two listings, merged: either can come back stale. ETL run 37427037888 got a repo-wide
    listing whose newest nightly was 19 days old -- no newer run in it at all -- while the same
    query a few minutes later had all of them, so it ran alpha42230 instead of alpha44357."""
    listings = (
        f"repos/{REPO}/actions/runs?branch={BRANCH}&event=workflow_dispatch&per_page=100",
        f"repos/{REPO}/actions/workflows/Main.yml/runs?branch={BRANCH}&per_page=100",
    )
    runs = {run["id"]: run for path in listings for run in _gh(path)["workflow_runs"]}
    return sorted(runs.values(), key=lambda r: r["created_at"], reverse=True)


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


def _check(binary: Path, repo: str | None) -> None:
    cmd = [str(binary), "-batch", "-bail", *(["-unsigned"] if repo else [])]
    out = subprocess.run(cmd, input=check_script(repo), capture_output=True, text=True)
    print(out.stdout)
    if out.returncode or out.stderr.strip():
        raise RuntimeError(f"loading {', '.join(EXTENSIONS)} failed: {out.stderr.strip()}")


def _append(env_file: str, line: str) -> None:
    path = os.environ.get(env_file)
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def main() -> int:
    sources = (_missing_extensions, _has_artifact, _has_extension_artifacts)
    pinned = os.environ.get("DUCKDB_RUN_ID", "").strip()
    if pinned:
        run, number, source = pick([_gh(f"repos/{REPO}/actions/runs/{pinned}")], *sources)
        print(f"pinned run {pinned}: {version(number)}")
    else:
        print(f"newest {BRANCH} nightly with {', '.join(EXTENSIONS)} published or in artifacts:")
        runs = _nightlies()
        newest = next((r for r in runs if alpha(r.get("name", "")) is not None), None)
        if newest is not None:
            print(f"  newest listed: {newest['name']} ({newest['created_at']})")
        run, number, source = pick(runs, *sources)

    temp = Path(os.environ.get("RUNNER_TEMP", "."))
    dest = temp / "duckdb"
    dest.mkdir(parents=True, exist_ok=True)
    artifact = _artifact(run["id"])
    binary = _unpack(_gh_bytes(f"repos/{REPO}/actions/artifacts/{artifact['id']}/zip"), dest)

    repo = None
    if source == "artifacts":
        blobs = {
            name: _gh_bytes(f"repos/{REPO}/actions/artifacts/{a['id']}/zip")
            for name, a in _extension_artifacts(run["id"]).items()
        }
        repo = str((temp / "duckdb-extensions").resolve())
        for path in extract_extensions(blobs, number, Path(repo)):
            print(f"  {path}")

    print(
        f"{version(number)} from run {run['id']} ({run['head_sha'][:10]}, {run['created_at']}) "
        f"-> {binary}, extensions {source}"
    )
    _check(binary, repo)
    _append("GITHUB_PATH", str(binary.parent))
    if repo:
        _append("GITHUB_ENV", f"{REPO_ENV}={repo}")
    _append("GITHUB_OUTPUT", f"run-id={run['id']}")
    _append("GITHUB_OUTPUT", f"version={version(number)}")
    _append("GITHUB_OUTPUT", f"extensions={source}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
