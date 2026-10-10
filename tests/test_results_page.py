"""docs/index.html, the interactive results page: every engine has its chart colours and label."""

from __future__ import annotations

from pathlib import Path

from bench.charts import DARK, LABEL, LIGHT

PAGE = (Path(__file__).parent.parent / "docs" / "index.html").read_text(encoding="utf-8")


def rows(name: str) -> str:
    """The text of one `const NAME = [ ... ];` table on the page."""
    start = PAGE.index(f"const {name} = [")
    return PAGE[start : PAGE.index("\n];", start)]


def test_every_engine_has_its_chart_colours_and_label():
    for engine, label in LABEL.items():
        assert f"--{engine}: {LIGHT[engine]};" in PAGE, engine
        assert f"--{engine}: {DARK[engine]};" in PAGE, engine
        assert f'{engine}: "{label}"' in PAGE, engine


def test_every_engine_has_an_engines_row():
    for engine in LABEL:
        assert f'engine: "{engine}"' in rows("ENGINE_ROWS"), engine


def test_every_engine_has_a_dbt_row():
    for engine in LABEL:
        assert f'engine: "{engine}"' in rows("DBT_ROWS"), engine


def test_the_storage_token_tab_is_the_catalogs_top_tab():
    assert '{ id: "catalogs", title: "Catalogs", subs: [STORAGE] }' in PAGE
    assert 'const STORAGE = { id: "storage", title: "Storage token",' in PAGE


def test_every_engine_has_a_workload_row():
    for engine in LABEL:
        assert f'engine: "{engine}"' in rows("WORKLOAD_ROWS"), engine


def test_the_real_workload_is_a_top_tab():
    assert '{ id: "workload", title: "Real workload", subs: [WORKLOAD] }' in PAGE


def test_the_workload_merge_cells_match_the_iceberg_support_tab():
    """The Real workload tab's MERGE column never disagrees with the probe that measured it."""
    import json
    import re

    cap = json.loads((Path(__file__).parent.parent / "docs" / "data" / "capability.json").read_text(encoding="utf-8"))
    merge = next(r for r in cap["rows"] if r["label"] == "MERGE with one action")
    table = rows("WORKLOAD_ROWS")
    for engine in cap["engines"]:
        start = table.index(f'engine: "{engine}"')
        cell = re.search(r"merge: \{ ok: (true|false)", table[start:]).group(1)
        assert (cell == "true") == (merge["cells"][engine]["o"] == "supported"), engine
