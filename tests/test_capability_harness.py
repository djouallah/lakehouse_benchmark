""".github/scripts/capability/harness.py: the verdict of a read made with the files gone."""

import importlib.util
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = _ROOT / ".github" / "scripts" / "capability" / "harness.py"
_spec = importlib.util.spec_from_file_location("harness", _SCRIPT)
harness = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(harness)


def _fails():
    raise FileNotFoundError("data file is gone")


def test_right_answer_and_a_failing_full_scan_is_a_yes():
    detail = harness.read_without_files("q", lambda: [(33,)], [(33,)], _fails, "4 of 4 gone")
    assert "returns [(33,)]" in detail


def test_a_read_that_opens_the_deleted_files_is_a_no():
    with pytest.raises(harness.Refused, match="so it opens them"):
        harness.read_without_files("q", _fails, [(33,)], _fails, "4 of 4 gone")


def test_a_wrong_answer_is_a_no():
    with pytest.raises(harness.Refused, match="expected"):
        harness.read_without_files("q", lambda: [(None,)], [(33,)], _fails, "4 of 4 gone")


def test_an_engine_that_skips_missing_files_proves_nothing():
    with pytest.raises(harness.Broken, match="skips missing files"):
        harness.read_without_files("q", lambda: [(220,)], [(220,)], lambda: [(220,)], "3 of 4")


def test_the_stats_files_do_not_overlap_and_hold_the_expected_answers():
    bounds = [(min(i for i, _ in f), max(i for i, _ in f)) for f in harness.STATS_FILES]
    assert all(a[1] < b[0] for a, b in zip(bounds, bounds[1:], strict=False))
    holding = [f for f in harness.STATS_FILES if any(i == harness.PRUNE_ID for i, _ in f)]
    assert len(holding) == 1
    assert [(v,) for i, v in holding[0] if i == harness.PRUNE_ID] == harness.PRUNE_EXPECTED
    assert [(bounds[-1][1],)] == harness.MAX_EXPECTED
