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


def test_the_lakehouse_is_the_first_tab_and_the_landing_page():
    """The premise first: any engine, one catalog; then how a commit works."""
    assert (
        rows("GROUPS")
        .split("\n")[1]
        .strip()
        .startswith('{ id: "lakehouse", title: "Lakehouse", subs: [OVERVIEW, ARCH, ANATOMY] },')
    )
    assert "const state = { suite: OVERVIEW.id," in PAGE


def test_the_real_workload_tab_reads_the_measured_workload():
    """The Real workload tab draws the latest run: a card per leg and dbt's own DAG."""
    assert (
        'const REAL_WORKLOAD = { id: "workload", title: "Real workload", '
        'json: "data/workload.json" };' in PAGE
    )
    assert "const { W, H, NW, NH, pos } = dagLayout(w.dag);" in PAGE
    for gone in ("SC_COLS", "REAL_COLS", "DBT_ROWS", "WORKLOAD_ROWS"):
        assert gone not in PAGE, gone


def test_issue_links_that_left_the_dbt_rows_are_still_on_the_page():
    """The Bug reports tab is built from the links on this page; none may drop off."""
    for link in (
        "https://github.com/StarRocks/starrocks/pull/66975",
        "https://github.com/datamindedbe/incubator-dbt-polars/issues/32",
        "https://github.com/lakehq/sail/pull/2571",
        "https://github.com/pola-rs/polars/issues/30074",
        "https://github.com/duckdb/duckdb/issues/27503",
    ):
        assert link in PAGE, link


def test_future_is_the_last_top_tab():
    assert rows("GROUPS").rstrip().endswith('{ id: "future", title: "Future", subs: [NEXT] },')


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


def test_bug_reports_then_the_real_workload_come_just_before_future():
    bar = [line.strip() for line in rows("GROUPS").split("\n") if "bar: false" not in line]
    assert bar[-3] == '{ id: "bugs", title: "Bug reports", subs: [REPORTS] },'
    assert bar[-2] == '{ id: "summary", title: "Real workload", subs: [REAL_WORKLOAD] },'
