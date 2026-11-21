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
        .startswith(
            '{ id: "lakehouse", title: "Lakehouse", '
            "subs: [TIMELINE, OVERVIEW, ARCH, ANATOMY, PRUNING, PLANS, ENGINES] },"
        )
    )
    assert "const state = { suite: TIMELINE.id," in PAGE


def test_the_real_workload_tab_reads_the_measured_workload():
    """The Real workload tab draws the latest run: a card per leg and dbt's own DAG."""
    assert (
        'const REAL_WORKLOAD = { id: "workload", title: "Real workload write", '
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


def test_one_table_format_tab_with_iceberg_and_delta():
    import json

    assert '{ id: "features", title: "Table format support", subs: [CAPABILITY, DELTA] }' in PAGE
    cap_path = Path(__file__).parent.parent / "docs" / "data" / "capability.json"
    cap = json.loads(cap_path.read_text(encoding="utf-8"))
    maintenance = [r["label"] for r in cap["rows"] if r["group"] == "Maintenance"]
    assert maintenance == ["Compaction", "Rewrite manifests", "Add existing Parquet files"]


def test_both_formats_share_their_groups_and_basic_is_the_mvp():
    """Basic, the first group, is the minimum to use a format: attach or register, create, read,
    append, append safely. Changing existing rows is Writes."""
    import json

    data = Path(__file__).parent.parent / "docs" / "data"
    cap = json.loads((data / "capability.json").read_text(encoding="utf-8"))
    delta = json.loads((data / "delta.json").read_text(encoding="utf-8"))
    assert cap["groups"] == delta["groups"]
    assert cap["groups"][0] == "Basic"
    assert [r["label"] for r in cap["rows"] if r["group"] == "Basic"] == [
        "Attach the REST catalog",
        "SELECT a table by its catalog name",
        "CREATE TABLE",
        "CREATE TABLE AS SELECT",
        "INSERT / append",
        "INSERT ... SELECT",
        "Concurrent append: both kept",
    ]
    for grid in (cap, delta):
        basic = {r["label"] for r in grid["rows"] if r["group"] == "Basic"}
        assert not basic & {"DELETE", "UPDATE", "CREATE OR REPLACE TABLE AS SELECT"}


def test_engines_and_issues_are_not_header_links_and_issues_is_under_bug_reports():
    groups = rows("GROUPS")
    assert 'id: "engines"' not in groups and 'id: "issues"' not in groups
    assert 'href="#engines">Engines</a>' not in PAGE and 'href="#issues">Issues</a>' not in PAGE
    assert PAGE.index('id="reports"') < PAGE.index('<section class="details" id="issues"')


def test_file_pruning_and_query_plans_are_under_lakehouse():
    assert "subs: [TIMELINE, OVERVIEW, ARCH, ANATOMY, PRUNING, PLANS, ENGINES] }" in PAGE
    assert 'id: "work"' not in rows("GROUPS")


def test_simple_write_real_workload_write_then_complex_read_are_the_last_top_tabs():
    bar = [line.strip() for line in rows("GROUPS").split("\n") if "bar: false" not in line]
    assert bar[-3].startswith('{ id: "write", title: "Simple write", subs: [SUITES[0]] },')
    assert bar[-2] == '{ id: "summary", title: "Real workload write", subs: [REAL_WORKLOAD] },'
    assert bar[-1].startswith('{ id: "read", title: "Complex read", subs: [SUITES[1]] },')


def test_bug_reports_is_a_header_link_next_to_about():
    assert '{ id: "bugs", title: "Bug reports", subs: [REPORTS], bar: false },' in PAGE
    assert (
        '<a class="top-link" id="reports-link" href="#reports">Bug reports</a>'
        '<a class="top-link" id="about-link" href="#about">About</a>'
    ) in PAGE
