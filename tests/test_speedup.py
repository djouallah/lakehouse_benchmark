"""bench/speedup.py: README's Gluten/Velox vs Spark-OSS block, built from the committed CSVs."""

from __future__ import annotations

import csv

from bench.speedup import END, START, write_readme

COLUMNS = ["run_id", "run_started_at", "sf", "test", "engine", "run_type", "phase", "dur", "status"]


def _csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(COLUMNS)
        writer.writerows(rows)


def _query_run(run_id, started, sf, engine, durs, test="tpch", failed=0):
    return [
        [run_id, started, sf, test, engine, "cold", "query", d, "error" if i < failed else "ok"]
        for i, d in enumerate(durs)
    ]


def test_block_is_rewritten_with_means_failed_and_not_run(tmp_path, monkeypatch):
    monkeypatch.setattr("bench.tpch.config.TpchConfig.N_QUERIES", 2)
    spark, gluten = "pyspark_iceberg", "pyspark_gluten_iceberg"
    _csv(
        tmp_path / "docs/data/tpch_results.csv",
        _query_run("a", "2026-01-01", 10, spark, [5, 5])
        + _query_run("b", "2026-01-02", 10, spark, [10, 10])
        + _query_run("c", "2026-01-01", 10, gluten, [2, 3])
        + _query_run("d", "2026-01-01", 100, spark, [1, 1], failed=1)
        + _query_run("e", "2026-01-01", 100, gluten, [4, 4]),
    )
    _csv(
        tmp_path / "docs/data/etl_results.csv",
        [["f", "2026-01-01", 1000, "etl", spark, "cold", "load", 30, "ok"]],
    )
    readme = tmp_path / "README.md"
    readme.write_text(f"# Top\n\n{START}\nstale\n{END}\n\nkept\n", encoding="utf-8")

    write_readme(readme, tmp_path)

    text = readme.read_text(encoding="utf-8")
    assert text.startswith("# Top\n") and text.endswith("\n\nkept\n") and "stale" not in text
    assert "| TPC-H | SF=10 | 15.0s | 5.0s | 3.0x |" in text
    assert "| TPC-H | SF=100 | failed | 8.0s | — |" in text
    assert "| Light ETL | 1,000 files | 30.0s | — | — |" in text


def test_a_run_stopped_at_its_first_query_reads_failed():
    """All or nothing: the published failure is one error row, and that alone says `failed`."""
    from bench.speedup import SPARK, query_totals

    rows = [dict(zip(COLUMNS, r)) for r in _query_run("a", "2026-01-01", 100, SPARK, [""], "tpcds", 1)]
    assert query_totals(rows, "tpcds", 99) == {(SPARK, 100): None}
    assert query_totals([], "tpcds", 99) == {}


def test_readme_without_markers_is_left_alone(tmp_path):
    readme = tmp_path / "README.md"
    readme.write_text("# Top\n", encoding="utf-8")
    write_readme(readme, tmp_path)
    assert readme.read_text(encoding="utf-8") == "# Top\n"
