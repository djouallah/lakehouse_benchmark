""".github/scripts/capability/render_readme.py: RESULTS.md and the site grid, from readings."""

import importlib.util
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = _ROOT / ".github" / "scripts" / "capability" / "render_readme.py"
_spec = importlib.util.spec_from_file_location("render_readme", _SCRIPT)
render_readme = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(render_readme)


def _row(key, outcome="supported", detail=""):
    return {"key": key, "group": "g", "question": key, "outcome": outcome, "detail": detail}


DATA = {
    "duckdb": {
        "engine": "duckdb",
        "version": "v2.0.0-alpha1",
        "run": "1",
        "url": "https://github.com/djouallah/lakehouse_benchmark/actions/runs/1",
        "date": "2026-10-09",
        "rows": [
            _row("create_table"),
            _row("merge_update_only"),
            _row("merge_delete_only", "no", "400 Duplicate types: add-snapshot"),
            _row("merge_insert_only", "no-op", "nothing written"),
            _row("merge_by_source_only"),
        ],
    },
    "polars": {
        "engine": "polars",
        "version": "2.0.0",
        "run": "1",
        "date": "2026-10-09",
        "rows": [_row("sink_append")],
    },
}


def test_cell_takes_the_worst_outcome_and_keeps_the_reasons():
    rows = {r["key"]: r for r in DATA["duckdb"]["rows"]}
    outcome, why = render_readme.cell(rows, ["merge_update_only", "merge_insert_only"])
    assert outcome == "no-op"
    assert [r["key"] for r in why] == ["merge_insert_only"]


def test_unprobed_is_a_dash_and_absent_engine_is_na():
    line = next(ln for ln in render_readme.render(DATA).splitlines() if ln.startswith("| UPDATE |"))
    assert line == "| UPDATE | na | — |"


def test_refusals_get_a_note_quoting_the_detail():
    text = render_readme.render(DATA)
    merge = next(ln for ln in text.splitlines() if ln.startswith("| MERGE with one action |"))
    assert merge == "| MERGE with one action | na | no ¹ |"
    assert "merge_delete_only: `400 Duplicate types: add-snapshot`" in text


def test_duckdb_isolation_rows_fill_the_duckdb_column():
    data = {
        **DATA,
        "duckdb_isolation": {
            "engine": "duckdb_isolation",
            "version": "v2.0.0-alpha1",
            "run": "1",
            "date": "2026-10-09",
            "configs": {},
            "levels": {},
            "transactions": [],
            "combos": [],
            "rows": [_row("race_append"), _row("race_delete", "no", "lost: final [...]")],
        },
    }
    lines = render_readme.render(data).splitlines()
    assert "| Concurrent append: both kept | — | yes |" in lines
    assert any(
        ln.startswith("| Concurrent writer: DELETE loses nothing") and "| na | no " in ln
        for ln in lines
    )


def test_every_row_is_in_exactly_one_site_group():
    placed = [label for labels in render_readme.GROUPS.values() for label in labels]
    assert sorted(placed) == sorted(label for label, _ in render_readme.ROWS)


def test_site_matrix_uses_the_site_engine_keys_and_each_runs_own_url():
    site = render_readme.site_matrix(DATA)
    assert set(site["engines"]) == {"polars_iceberg", "duckdb_iceberg"}
    assert site["engines"]["duckdb_iceberg"]["url"].startswith(
        "https://github.com/djouallah/lakehouse_benchmark/"
    )
    update = next(r for r in site["rows"] if r["label"] == "UPDATE")
    assert update["cells"] == {"polars_iceberg": {"o": "na"}, "duckdb_iceberg": {"o": "skipped"}}


def test_nothing_the_catalog_decides_is_probed_or_shown():
    """The capability suite tests engines: no row, probe or text whose answer is the catalog's."""
    labels = {label for label, _ in render_readme.ROWS}
    for gone in ("MERGE INTO / upsert", "INSERT OVERWRITE, whole table", "format-version 3"):
        assert gone not in labels
    scripts = (_ROOT / ".github" / "scripts" / "capability").glob("*.py")
    for script in scripts:
        text = script.read_text(encoding="utf-8")
        for word in ("blocked", "BLOCKED", "def format_v3", "def insert_overwrite"):
            assert word not in text, f"{script.name}: {word}"
