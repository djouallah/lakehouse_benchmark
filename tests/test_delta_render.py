""".github/scripts/capability/render_delta.py and delta_probe.py: the Delta support grid."""

import importlib
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_SCRIPTS = _ROOT / ".github" / "scripts" / "capability"
sys.path.insert(0, str(_SCRIPTS))

render_delta = importlib.import_module("render_delta")
delta_probe = importlib.import_module("delta_probe")

PAGE = (_ROOT / "docs" / "index.html").read_text(encoding="utf-8")


def _reading(engine, outcomes):
    return {
        "engine": engine,
        "version": "1",
        "run": "1",
        "url": "https://github.com/djouallah/lakehouse_benchmark/actions/runs/1",
        "date": "2026-11-21",
        "rows": [
            {"key": k, "group": "g", "question": k, "outcome": o, "detail": f"{k} said {o}"}
            for k, o in outcomes.items()
        ],
    }


def test_every_probe_has_a_row_and_every_row_a_probe():
    probes = {key for _, _, key in delta_probe.PROBES} - {"session"}
    rows = {key for _, key in render_delta.ROWS}
    assert probes == rows


def test_every_row_is_in_one_group_and_the_grid_opens_on_core():
    grouped = [label for labels in render_delta.GROUPS.values() for label in labels]
    assert sorted(grouped) == sorted(label for label, _ in render_delta.ROWS)
    assert len(grouped) == len(set(grouped))
    assert list(render_delta.GROUPS)[0] == "Core"


def test_the_grid_is_only_the_engines_that_write_delta_in_sql():
    assert render_delta.ENGINES == ("spark", "sail", "trino", "duckdb", "chdb")
    for engine in render_delta.ENGINES:
        assert (_SCRIPTS / f"delta_capability_{engine}.py").exists()
        assert (_ROOT / "requirements" / f"capability_delta_{engine}.txt").exists()


def test_cells_quote_the_reading_and_columns_sort_by_yes():
    keys = [key for _, key in render_delta.ROWS]
    data = {
        "duckdb": _reading("duckdb", {k: "supported" for k in keys}),
        "chdb": _reading("chdb", {k: ("no" if k == "update" else "no-op") for k in keys}),
    }
    grid = render_delta.site_matrix(data)
    assert list(grid["engines"]) == ["duckdb_iceberg", "chdb_iceberg"]
    update = next(r for r in grid["rows"] if r["label"] == "UPDATE")
    assert update["cells"]["chdb_iceberg"] == {"o": "no", "n": "update said no"}
    assert update["cells"]["duckdb_iceberg"] == {"o": "supported"}


def test_a_probe_with_no_reading_is_not_probed():
    grid = render_delta.site_matrix({"spark": _reading("spark", {"create_table": "supported"})})
    cells = {r["label"]: r["cells"]["pyspark_iceberg"]["o"] for r in grid["rows"]}
    assert cells["CREATE TABLE at a path"] == "supported"
    assert cells["VACUUM"] == "skipped"


def test_sail_is_sent_sparks_sql_byte_for_byte():
    """Sail and Spark run DeltaProbe's statements: neither may override one."""
    spark = importlib.import_module("delta_capability_spark").SparkDelta
    sail = importlib.import_module("delta_capability_sail").SailDelta
    allowed = {
        "start",
        "execute",
        "close",
        "CLIENT",
        "TITLE",
        "PREFIX",
        "__module__",
        "__doc__",
        "__qualname__",
        "__firstlineno__",
        "__static_attributes__",
    }
    for cls in (spark, sail):
        assert set(vars(cls)) <= allowed, set(vars(cls)) - allowed


def test_the_delta_support_tab_is_on_the_page():
    assert '{ id: "delta-features", title: "Delta support", subs: [DELTA_RANK, DELTA] }' in PAGE
    assert '<section class="cap" id="delta" hidden></section>' in PAGE
    assert 'json: "data/delta.json"' in PAGE
