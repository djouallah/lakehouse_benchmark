"""Install the newest usable Linux x86_64 chdb-core wheel.

Auto mode selects the newer candidate from main nightly artifacts and published stable/RC releases.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

REPO = "chdb-io/chdb-core"
WORKFLOW = "build_linux_x86_wheels.yml"
NIGHTLY_ARTIFACT_PREFIXES = (
    "chdb-core-nightly-linux-x86_64",
    "chdb-core-wheel-linux-x86_64",
)
CHANNELS = {"auto", "release", "nightly"}


@dataclass(frozen=True)
class Candidate:
    source: str
    timestamp: str
    label: str
    wheel_name: str
    url: str = ""
    artifact_id: int = 0


def compatible_wheel(name: str) -> bool:
    """Return whether the wheel supports this benchmark runner."""
    return (
        name.startswith("chdb_core-")
        and not name.startswith("chdb_core_lite-")
        and "manylinux" in name
        and name.endswith("x86_64.whl")
        and "cp314t" not in name
    )


def release_candidate(release: dict) -> Candidate | None:
    if release.get("draft") or not release.get("published_at"):
        return None
    wheels = [asset for asset in release.get("assets", []) if compatible_wheel(asset["name"])]
    if not wheels:
        return None
    wheel = wheels[0]
    return Candidate(
        source="release",
        timestamp=release["published_at"],
        label=f"release:{release['tag_name']}",
        wheel_name=wheel["name"],
        url=wheel["browser_download_url"],
    )


def valid_nightly_run(run: dict) -> bool:
    repository = (run.get("head_repository") or {}).get("full_name")
    return (
        run.get("status") == "completed"
        and run.get("conclusion") == "success"
        and run.get("event") in {"push", "schedule"}
        and run.get("head_branch") == "main"
        and repository == REPO
    )


def nightly_candidate(run: dict, artifacts: list[dict]) -> Candidate | None:
    if not valid_nightly_run(run):
        return None
    matches = [
        artifact
        for artifact in artifacts
        if not artifact.get("expired")
        and any(artifact["name"].startswith(prefix) for prefix in NIGHTLY_ARTIFACT_PREFIXES)
    ]
    if not matches:
        return None
    artifact = matches[0]
    sha = run["head_sha"]
    return Candidate(
        source="nightly",
        timestamp=run["created_at"],
        label=f"nightly:{sha[:10]}@{run['id']}/{artifact['id']}",
        wheel_name=artifact["name"],
        artifact_id=artifact["id"],
    )


def pick(
    releases: list[dict],
    runs: list[dict],
    artifacts_for_run,
    channel: str = "auto",
    log=print,
) -> Candidate:
    if channel not in CHANNELS:
        raise ValueError(f"unknown chdb-core channel {channel!r}; expected {', '.join(CHANNELS)}")

    candidates = []
    if channel in {"auto", "release"}:
        candidates.extend(candidate for item in releases if (candidate := release_candidate(item)))

    if channel in {"auto", "nightly"}:
        for run in sorted(runs, key=lambda item: item.get("created_at", ""), reverse=True):
            if not valid_nightly_run(run):
                continue
            candidate = nightly_candidate(run, artifacts_for_run(run["id"]))
            if candidate:
                candidates.append(candidate)
                break
            log(f"  nightly run {run['id']}: no live dedicated wheel artifact")

    if not candidates:
        raise RuntimeError(f"no usable chdb-core {channel} Linux x86_64 wheel")
    return max(candidates, key=lambda candidate: candidate.timestamp)


def resolve_reference(reference: str, release_for_tag, run_for_id, artifacts_for_run) -> Candidate:
    if reference.startswith("release:"):
        tag = reference.removeprefix("release:")
        candidate = release_candidate(release_for_tag(tag))
    elif reference.startswith("nightly:"):
        revision, separator, raw_ids = reference.removeprefix("nightly:").rpartition("@")
        raw_run_id, id_separator, raw_artifact_id = raw_ids.partition("/")
        if (
            not separator
            or not id_separator
            or not revision
            or not raw_run_id.isdigit()
            or not raw_artifact_id.isdigit()
        ):
            raise ValueError(f"invalid chdb-core source {reference!r}")
        run = run_for_id(int(raw_run_id))
        if not run.get("head_sha", "").startswith(revision):
            raise RuntimeError(f"chdb-core source {reference!r} no longer matches its run")
        artifact_id = int(raw_artifact_id)
        artifacts = [
            artifact for artifact in artifacts_for_run(run["id"]) if artifact["id"] == artifact_id
        ]
        candidate = nightly_candidate(run, artifacts)
    else:
        raise ValueError(f"invalid chdb-core source {reference!r}")

    if candidate is None or candidate.label != reference:
        raise RuntimeError(f"chdb-core source {reference!r} is unavailable")
    return candidate


def _gh(path: str) -> dict | list:
    result = subprocess.run(["gh", "api", path], capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def _artifacts(run_id: int) -> list[dict]:
    return _gh(f"repos/{REPO}/actions/runs/{run_id}/artifacts?per_page=100")["artifacts"]


def _release(tag: str) -> dict:
    return _gh(f"repos/{REPO}/releases/tags/{quote(tag, safe='')}")


def _run(run_id: int) -> dict:
    return _gh(f"repos/{REPO}/actions/runs/{run_id}")


def _download(candidate: Candidate, directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    if candidate.source == "release":
        target = directory / candidate.wheel_name
        request = urllib.request.Request(
            candidate.url, headers={"User-Agent": "lakehouse-benchmark"}
        )
        with urllib.request.urlopen(request, timeout=300) as response, target.open("wb") as output:
            shutil.copyfileobj(response, output)
        return target

    archive = directory / "artifact.zip"
    with archive.open("wb") as output:
        subprocess.run(
            ["gh", "api", f"repos/{REPO}/actions/artifacts/{candidate.artifact_id}/zip"],
            stdout=output,
            check=True,
        )
    with zipfile.ZipFile(archive) as bundle:
        names = [name for name in bundle.namelist() if compatible_wheel(Path(name).name)]
        if len(names) != 1:
            raise RuntimeError(
                f"expected one compatible wheel in {candidate.wheel_name}, found {len(names)}"
            )
        target = directory / Path(names[0]).name
        with bundle.open(names[0]) as source, target.open("wb") as output:
            shutil.copyfileobj(source, output)
        return target


def _append(env_file: str, line: str) -> None:
    path = os.environ.get(env_file)
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def main() -> int:
    channel = os.environ.get("CHDB_CORE_CHANNEL", "auto").strip().lower()
    source = os.environ.get("CHDB_CORE_SOURCE_INPUT", "").strip()
    if source:
        candidate = resolve_reference(source, _release, _run, _artifacts)
    else:
        releases = _gh(f"repos/{REPO}/releases?per_page=100")
        runs = _gh(f"repos/{REPO}/actions/workflows/{WORKFLOW}/runs?branch=main&per_page=100")[
            "workflow_runs"
        ]
        candidate = pick(releases, runs, _artifacts, channel)
    print(
        f"selected chdb-core {candidate.label} from {candidate.timestamp}: {candidate.wheel_name}"
    )
    _append("GITHUB_OUTPUT", f"source={candidate.label}")
    if os.environ.get("CHDB_CORE_RESOLVE_ONLY", "").lower() in {"1", "true"}:
        return 0

    root = Path(os.environ.get("RUNNER_TEMP", ".")) / "chdb-core-latest"
    wheel = _download(candidate, root)
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--force-reinstall", "--no-deps", str(wheel)],
        check=True,
    )
    subprocess.run([sys.executable, "-m", "pip", "check"], check=True)

    from importlib.metadata import version

    import chdb

    wrapper_version = version("chdb")
    core_version = version("chdb-core")
    engine_version = str(chdb.query("SELECT version()", "CSV")).strip()
    print(f"chdb wrapper: {wrapper_version}")
    print(f"chdb-core: {core_version} ({candidate.label})")
    print(f"module: {chdb.__file__}")
    print(f"engine: {engine_version}")

    _append("GITHUB_ENV", f"CHDB_CORE_SOURCE={candidate.label}")
    _append("GITHUB_OUTPUT", f"version={core_version}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
