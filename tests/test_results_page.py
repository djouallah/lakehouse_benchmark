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
        row = rows("ENGINE_ROWS").split(f'engine: "{engine}"')[1].split("{ engine:")[0]
        for field in ('install: "', 'runs: "', 'scheduling: "'):
            assert field in row, (engine, field)
        assert 'execution: "Columnar"' in row or 'execution: "Row"' in row, engine
        # The overview's cluster facts split the engines on this field.
        assert 'machines: "Cluster"' in row or 'machines: "One"' in row, engine


def test_every_engine_has_a_dbt_row():
    for engine in LABEL:
        assert f'engine: "{engine}"' in rows("DBT_ROWS"), engine


def test_the_lakehouse_is_the_first_tab_and_the_landing_page():
    """The premise first: any engine, one catalog; then how a commit works."""
    assert (
        rows("GROUPS")
        .split("\n")[1]
        .strip()
        .startswith('{ id: "lakehouse", title: "Lakehouse", subs: [OVERVIEW, ARCH, ANATOMY] },')
    )
    assert "const state = { suite: OVERVIEW.id," in PAGE


def test_every_engine_has_a_workload_row():
    for engine in LABEL:
        assert f'engine: "{engine}"' in rows("WORKLOAD_ROWS"), engine


def test_the_real_workload_is_the_scorecards_first_column_not_a_tab():
    assert 'id: "workload"' not in rows("GROUPS")
    first = '{ id: "workload", title: "My workload", href: WORKLOAD_REPO,'
    assert rows("SC_COLS").split("[", 1)[1].lstrip().startswith(first)


def test_future_is_the_last_top_tab():
    assert rows("GROUPS").rstrip().endswith('{ id: "future", title: "Future", subs: [NEXT] },')


def test_the_workload_merge_cells_match_the_iceberg_support_tab():
    """The Scorecard's real workload MERGE check never disagrees with the probe that measured it."""
    import json
    import re

    cap_path = Path(__file__).parent.parent / "docs" / "data" / "capability.json"
    cap = json.loads(cap_path.read_text(encoding="utf-8"))
    merge = next(r for r in cap["rows"] if r["label"] == "MERGE with two actions (UPDATE + INSERT)")
    table = rows("WORKLOAD_ROWS")
    for engine in cap["engines"]:
        if merge["cells"][engine]["o"] == "na":
            continue  # the engine has no DML (Polars): its dbt adapter's merge decides
        start = table.index(f'engine: "{engine}"')
        cell = re.search(r"merge: \{ ok: (true|false)", table[start:]).group(1)
        assert (cell == "true") == (merge["cells"][engine]["o"] == "supported"), engine


def test_the_scorecard_has_no_hand_picked_groups():
    """Rows are ordered by the data (workload, then Light ETL time), not by tiers."""
    assert "WORKLOAD_TIERS" not in PAGE
    assert "tier:" not in rows("WORKLOAD_ROWS")


def test_maintenance_is_a_group_of_the_iceberg_support_grid():
    import json

    assert '{ id: "features", title: "Iceberg support", subs: [CAPABILITY] }' in PAGE
    cap_path = Path(__file__).parent.parent / "docs" / "data" / "capability.json"
    cap = json.loads(cap_path.read_text(encoding="utf-8"))
    maintenance = [r["label"] for r in cap["rows"] if r["group"] == "Maintenance"]
    assert maintenance == ["Compaction", "Rewrite manifests"]


def test_the_iceberg_support_grid_opens_on_core():
    """Core, the first chip, holds only the minimum an engine needs to be usable."""
    import json

    cap_path = Path(__file__).parent.parent / "docs" / "data" / "capability.json"
    cap = json.loads(cap_path.read_text(encoding="utf-8"))
    assert cap["groups"][0] == "Core"
    assert "Advanced" not in cap["groups"]
    assert [r["label"] for r in cap["rows"] if r["group"] == "Core"] == [
        "CREATE TABLE",
        "CREATE TABLE AS SELECT",
        "INSERT / append",
        "INSERT ... SELECT",
        "DELETE",
        "UPDATE",
        "MERGE with two actions (UPDATE + INSERT)",
        "INSERT OVERWRITE, whole table",
        "TRUNCATE",
        "Drop table with purge",
        "Create / drop namespace",
        "CREATE SCHEMA IF NOT EXISTS, existing schema",
        "Concurrent append: both kept",
        "Concurrent writer: DELETE loses nothing",
        "Concurrent writer: UPDATE loses nothing",
    ]


def test_file_pruning_and_query_plans_share_the_less_work_tab():
    assert '{ id: "work", title: "Less work", subs: [PRUNING, PLANS] }' in PAGE


def test_bug_reports_then_the_scorecard_come_just_before_future():
    bar = [line.strip() for line in rows("GROUPS").split("\n") if "bar: false" not in line]
    assert bar[-3] == '{ id: "bugs", title: "Bug reports", subs: [REPORTS] },'
    assert bar[-2] == '{ id: "summary", title: "Scorecard", subs: [SCORECARD] },'
