"""The results file: round-trip, immutability, and the shape charts.py depends on."""

from __future__ import annotations

import json

import pytest

from bench.store import (
    EngineResult,
    Row,
    Run,
    load_all,
    read_engine_part,
    run_filename,
    write_engine_part,
    write_run,
)
from bench.tpcds.config import TpcdsConfig
from bench.tpch.config import ENGINES, TpchConfig
from bench.tpch.runner import benchmark, order, run_pass, totals


def _run(run_id="1", stamp="2026-09-20T03:17:44Z", sf=10):
    run = Run(
        run_id=run_id,
        run_started_at=stamp,
        sf=sf,
        cpu=4,
        mem_gb=15.6,
        runner="linux-6.8",
        python="3.12.7",
    )
    run.engines["duckdb_iceberg"] = EngineResult(
        version="1.5.5",
        rows=[
            Row("cold", "setup", 0, 2.5),
            Row("cold", "query", 1, 1.25, rows=4),
            Row("cold", "query", 2, None, status="error", error="boom"),
            Row("warm", "query", 1, 0.4, rows=4),
        ],
    )
    return run


def test_filename_sorts_chronologically_and_is_unique(tmp_path):
    name = run_filename(_run())
    assert name == "2026-09-20T03-17Z_sf10_1.json"
    assert name < run_filename(_run(run_id="2", stamp="2026-09-21T03:17:44Z"))


def test_runs_are_immutable(tmp_path):
    """Immutability is what makes concurrent jobs incapable of a merge conflict; silently
    clobbering would give that property away for nothing."""
    write_run(tmp_path, _run())
    with pytest.raises(FileExistsError):
        write_run(tmp_path, _run())


def test_round_trip_through_load_all(tmp_path):
    write_run(tmp_path, _run())
    table = load_all(tmp_path)
    assert table.num_rows == 4
    rows = table.to_pylist()
    assert {r["engine"] for r in rows} == {"duckdb_iceberg"}
    assert rows[0]["run_started_at"] == "2026-09-20T03:17:44Z"
    # The setup row is its own row, not folded into query 1.
    setup = [r for r in rows if r["phase"] == "setup"]
    assert len(setup) == 1 and setup[0]["query"] == 0
    errored = [r for r in rows if r["status"] == "error"]
    assert len(errored) == 1 and errored[0]["dur"] is None


def test_load_all_spans_multiple_runs(tmp_path):
    write_run(tmp_path, _run(run_id="1"))
    write_run(tmp_path, _run(run_id="2", stamp="2026-09-21T03:17:44Z"))
    assert load_all(tmp_path).num_rows == 8


def test_load_all_is_empty_but_typed_on_a_fresh_repo(tmp_path):
    """The first run publishes into an empty results/ directory; the charts must not crash."""
    table = load_all(tmp_path)
    assert table.num_rows == 0
    assert "engine" in table.column_names and "dur" in table.column_names


def test_engine_part_round_trip(tmp_path):
    original = EngineResult(version="4.4.0", rows=[Row("cold", "query", 1, 1.5, rows=7)])
    path = write_engine_part(tmp_path, "chdb_iceberg", original)
    name, restored = read_engine_part(path)
    assert name == "chdb_iceberg"
    assert restored.version == "4.4.0"
    assert restored.rows[0].dur == 1.5 and restored.rows[0].rows == 7


def test_null_errors_are_omitted_from_the_file(tmp_path):
    """Most rows succeed; writing `"error": null` 178 times bloats every committed file."""
    payload = json.loads(write_run(tmp_path, _run()).read_text(encoding="utf-8"))
    rows = payload["engines"]["duckdb_iceberg"]["rows"]
    assert "error" not in rows[0]
    assert rows[2]["error"] == "boom"


class _Fake:
    """An engine that fails exactly one query."""

    name = "duckdb_iceberg"
    version = "0.0-fake"

    def setup(self):
        pass

    def execute(self, sql):
        if "SUBSTRING" in sql:
            raise RuntimeError("boom")
        return 7

    def close(self):
        self.closed = True


class _Passes(_Fake):
    def execute(self, sql):
        return 7


class _FailsToAttach(_Fake):
    def setup(self):
        raise RuntimeError("catalog refused the token")


def test_a_failing_query_is_recorded_not_raised():
    """Cell 15 had no try/except: one bad statement crashed the job. Q22 is the last statement,
    so the run is complete up to it and the failure is a row."""
    result = benchmark(_Fake(), TpchConfig(workspace_id="w", lakehouse_id="l", sf=10))
    assert result.status == "ok"
    assert len(result.rows) == 23  # 1 setup + 22 cold
    assert sum(1 for r in result.rows if r.status == "error") == 1  # Q22
    assert totals(result).keys() == {"cold"}


def test_the_runner_takes_the_statement_count_from_the_suite():
    """Same runner, same fake engine, the TPC-DS config: 99 statements, one cold pass."""
    result = benchmark(_Passes(), TpcdsConfig(workspace_id="w", lakehouse_id="l", sf=10))
    assert result.status == "ok"
    assert len(result.rows) == 1 + 99
    assert {r.query for r in result.rows if r.phase == "query"} == set(range(1, 100))
    assert {r.run_type for r in result.rows} == {"cold"}


def test_tpcds_runs_its_hard_queries_first_and_every_query_once():
    """A run that dies on Q64 should die in its first minutes, not after 98 easy queries."""
    cfg = TpcdsConfig(workspace_id="w", lakehouse_id="l", sf=10)
    ran = [r.query for r in benchmark(_Passes(), cfg).rows if r.phase == "query"]
    assert ran[: len(cfg.HARD_FIRST)] == list(cfg.HARD_FIRST)
    assert sorted(ran) == list(range(1, 100))
    assert ran[len(cfg.HARD_FIRST) :] == sorted(ran[len(cfg.HARD_FIRST) :])


def test_each_query_number_runs_its_own_statement():
    """Reordering must not shift which SQL a query number gets."""
    seen = []

    class _Records(_Fake):
        def execute(self, sql):
            seen.append(sql)
            return 1

    statements = [f"SELECT {n}" for n in range(1, 6)]
    rows = run_pass(_Records(), statements, "cold", hard_first=(4, 2))
    assert [r.query for r in rows] == [4, 2, 1, 3, 5]
    assert seen == ["SELECT 4", "SELECT 2", "SELECT 1", "SELECT 3", "SELECT 5"]


def test_a_run_stops_at_the_first_failed_query():
    """All or nothing: nothing after a failure runs."""

    class _FailsOn2(_Fake):
        def execute(self, sql):
            if sql == "SELECT 2":
                raise RuntimeError("out of spill")
            return 1

    statements = [f"SELECT {n}" for n in range(1, 6)]
    rows = run_pass(_FailsOn2(), statements, "cold", hard_first=(4, 2))
    assert [(r.query, r.status) for r in rows] == [(4, "ok"), (2, "error")]


def test_tpch_keeps_query_order():
    assert TpchConfig.HARD_FIRST == ()
    assert order(22) == list(range(1, 23))


def test_a_failing_attach_is_recorded_not_raised():
    result = benchmark(_FailsToAttach(), TpchConfig(workspace_id="w", lakehouse_id="l", sf=10))
    assert result.status == "setup_failed"
    assert len(result.rows) == 1
    assert "catalog refused the token" in result.rows[0].error


def test_every_engine_has_a_chart_label():
    from bench.charts import DARK, LABEL, LIGHT

    for engine in ENGINES:
        assert engine in LABEL and engine in LIGHT and engine in DARK


def test_a_part_with_an_unknown_key_still_loads(tmp_path):
    """The part and the publisher can be different code, and run 35580753996 paid for it.

    Its engine jobs wrote `verdict` into every row; the publish hit a rejected push, the
    regenerate loop reset onto a main that no longer had the field, and `Row(**row)` raised
    TypeError with six green engines already measured. The rejected-push loop rebuilds on
    whatever main has become, so the publisher is always liable to be newer than its parts.
    """
    part = write_engine_part(
        tmp_path,
        "duckdb_iceberg",
        EngineResult(
            "1.5.5",
            rows=[
                Row("cold", "load", 1, 2.5, rows=7),
            ],
        ),
    )
    payload = json.loads(part.read_text(encoding="utf-8"))
    for row in payload["rows"]:
        row["verdict"] = None  # a field that has since been removed
        row["something_new"] = "x"  # and one that does not exist yet
    part.write_text(json.dumps(payload), encoding="utf-8")

    name, restored = read_engine_part(part)
    assert name == "duckdb_iceberg"
    assert restored.rows[0].dur == 2.5 and restored.rows[0].rows == 7


class _Refreshing(_Fake):
    """Records the order of refresh and execute calls; its refresh sometimes raises."""

    def __init__(self):
        self.calls = []

    def refresh(self):
        self.calls.append("refresh")
        if len(self.calls) == 1:
            raise RuntimeError("token endpoint down")

    def execute(self, sql):
        self.calls.append("execute")
        return 7


def test_refresh_runs_before_every_statement_and_never_fails_one():
    """An hour-long pass outlives the credential; the runner offers a refresh each statement."""
    engine = _Refreshing()
    result = benchmark(engine, TpcdsConfig(workspace_id="w", lakehouse_id="l", sf=10))
    assert engine.calls == ["refresh", "execute"] * 99
    assert all(r.status == "ok" for r in result.rows)
