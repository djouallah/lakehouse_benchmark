"""Selection rules for chdb-core stable, RC and nightly wheels."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / ".github" / "scripts" / "chdb_core_latest.py"
_spec = importlib.util.spec_from_file_location("chdb_core_latest", _SCRIPT)
latest = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = latest
_spec.loader.exec_module(latest)


def _asset(name="chdb_core-26.9.0-cp39-abi3-manylinux2014_x86_64.whl"):
    return {"name": name, "browser_download_url": f"https://example.test/{name}"}


def _release(tag, published, assets=None, draft=False, prerelease=False):
    return {
        "tag_name": tag,
        "published_at": published,
        "draft": draft,
        "prerelease": prerelease,
        "assets": [_asset()] if assets is None else assets,
    }


def _run(run_id, created, **changes):
    run = {
        "id": run_id,
        "created_at": created,
        "status": "completed",
        "conclusion": "success",
        "event": "schedule",
        "head_branch": "main",
        "head_sha": "abcdef0123456789",
        "head_repository": {"full_name": latest.REPO},
    }
    run.update(changes)
    return run


def _artifact(artifact_id=7, name="chdb-core-nightly-linux-x86_64-abcdef"):
    return {"id": artifact_id, "name": name, "expired": False}


def test_release_selection_includes_rc_but_skips_one_without_a_linux_wheel():
    releases = [
        _release("v27.1.0-rc.1", "2026-10-10T03:00:00Z", assets=[], prerelease=True),
        _release("v27.0.0", "2026-10-09T03:00:00Z"),
        _release("v26.9.2-rc.2", "2026-10-08T03:00:00Z", prerelease=True),
    ]
    picked = latest.pick(releases, [], lambda run_id: [], channel="release")
    assert picked.label == "release:v27.0.0"


def test_auto_uses_a_newer_successful_main_daily_wheel():
    releases = [_release("v27.0.0", "2026-10-09T03:00:00Z")]
    runs = [_run(42, "2026-10-10T03:00:00Z")]
    picked = latest.pick(releases, runs, lambda run_id: [_artifact()])
    assert picked.label == "nightly:abcdef0123@42/7"


def test_daily_runs_are_sorted_even_if_the_api_listing_is_stale():
    runs = [
        _run(41, "2026-10-09T03:00:00Z"),
        _run(42, "2026-10-10T03:00:00Z"),
    ]
    artifacts = {41: [_artifact(6)], 42: [_artifact(7)]}
    picked = latest.pick([], runs, artifacts.__getitem__, channel="nightly")
    assert picked.label == "nightly:abcdef0123@42/7"


@pytest.mark.parametrize(
    ("changes", "artifact"),
    [
        ({"event": "pull_request"}, _artifact()),
        ({"head_branch": "feature"}, _artifact()),
        ({"head_repository": {"full_name": "somebody/fork"}}, _artifact()),
        ({"conclusion": "failure"}, _artifact()),
        ({}, {**_artifact(), "expired": True}),
        ({}, _artifact(name="chdb-artifacts-linux-x86_64")),
    ],
)
def test_auto_rejects_untrusted_or_unusable_daily_artifacts(changes, artifact):
    release = _release("v27.0.0", "2026-10-09T03:00:00Z")
    run = _run(42, "2026-10-10T03:00:00Z", **changes)
    picked = latest.pick([release], [run], lambda run_id: [artifact], log=lambda line: None)
    assert picked.label == "release:v27.0.0"


def test_auto_keeps_a_newer_release_than_the_last_daily():
    release = _release("v27.0.0", "2026-10-10T03:00:00Z")
    run = _run(42, "2026-10-09T03:00:00Z")
    picked = latest.pick([release], [run], lambda run_id: [_artifact()])
    assert picked.label == "release:v27.0.0"


def test_nightly_channel_fails_when_no_artifact_is_available():
    with pytest.raises(RuntimeError, match="nightly"):
        latest.pick(
            [],
            [_run(42, "2026-10-10T03:00:00Z")],
            lambda run_id: [],
            channel="nightly",
        )


def test_only_full_linux_x86_wheels_are_compatible():
    assert latest.compatible_wheel("chdb_core-26.9.0-cp39-abi3-manylinux2014_x86_64.whl")
    assert not latest.compatible_wheel("chdb_core_lite-26.9.0-cp39-abi3-manylinux2014_x86_64.whl")
    assert not latest.compatible_wheel("chdb_core-26.9.0-cp39-abi3-macosx_11_0_arm64.whl")
    assert not latest.compatible_wheel("chdb_core-26.9.0-cp314t-cp314t-manylinux2014_x86_64.whl")


def test_release_reference_resolves_the_same_release():
    release = _release("v27.0.0", "2026-10-10T03:00:00Z")
    candidate = latest.resolve_reference(
        "release:v27.0.0",
        lambda tag: release,
        lambda run_id: None,
        lambda run_id: [],
    )
    assert candidate.label == "release:v27.0.0"


def test_nightly_reference_resolves_the_same_run_and_artifact():
    run = _run(42, "2026-10-10T03:00:00Z")
    candidate = latest.resolve_reference(
        "nightly:abcdef0123@42/7",
        lambda tag: None,
        lambda run_id: run,
        lambda run_id: [_artifact()],
    )
    assert candidate.artifact_id == 7


def test_nightly_reference_rejects_a_changed_revision():
    run = _run(42, "2026-10-10T03:00:00Z")
    with pytest.raises(RuntimeError, match="no longer matches"):
        latest.resolve_reference(
            "nightly:0000000000@42/7",
            lambda tag: None,
            lambda run_id: run,
            lambda run_id: [_artifact()],
        )


def test_nightly_reference_rejects_a_missing_artifact():
    run = _run(42, "2026-10-10T03:00:00Z")
    with pytest.raises(RuntimeError, match="unavailable"):
        latest.resolve_reference(
            "nightly:abcdef0123@42/99",
            lambda tag: None,
            lambda run_id: run,
            lambda run_id: [_artifact()],
        )


@pytest.mark.parametrize(
    "reference",
    ["", "nightly:abc", "nightly:abc@nope", "nightly:abc@42", "other:v1"],
)
def test_invalid_source_reference_fails_loudly(reference):
    with pytest.raises(ValueError, match="invalid"):
        latest.resolve_reference(
            reference, lambda tag: None, lambda run_id: None, lambda run_id: []
        )
