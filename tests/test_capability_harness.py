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


def test_a_run_that_asks_some_probes_keeps_the_other_readings(tmp_path, monkeypatch):
    import json

    path = tmp_path / "spark.json"
    old = [{"key": "session"}, {"key": "a", "outcome": "no"}, {"key": "b", "outcome": "no"}]
    path.write_text(json.dumps({"engine": "spark", "rows": old}), encoding="utf-8")
    monkeypatch.setenv("RESULTS_FILE", str(path))
    monkeypatch.setenv("CAPABILITY_PROBES", "b,c")
    assert harness.wanted("session") and harness.wanted("create_table")
    assert harness.wanted("b") and not harness.wanted("a")
    assert harness.any_wanted([("g", "q", "session"), ("g", "q", "b")])
    assert not harness.any_wanted([("g", "q", "session"), ("g", "q", "create_table")])
    fresh = [{"key": "session"}, {"key": "b", "outcome": "supported"}, {"key": "c"}]
    harness.save_results({"engine": "spark", "version": "v", "rows": fresh})
    rows = json.loads(path.read_text(encoding="utf-8"))["rows"]
    assert [r["key"] for r in rows] == ["session", "a", "b", "c"]
    assert rows[1]["outcome"] == "no" and rows[2]["outcome"] == "supported"


def test_no_probe_list_asks_everything_and_replaces_the_file(tmp_path, monkeypatch):
    import json

    path = tmp_path / "spark.json"
    path.write_text(json.dumps({"engine": "spark", "rows": [{"key": "a"}]}), encoding="utf-8")
    monkeypatch.setenv("RESULTS_FILE", str(path))
    monkeypatch.delenv("CAPABILITY_PROBES", raising=False)
    assert harness.wanted("anything")
    harness.save_results({"engine": "spark", "version": "v", "rows": [{"key": "b"}]})
    assert [r["key"] for r in json.loads(path.read_text(encoding="utf-8"))["rows"]] == ["b"]
