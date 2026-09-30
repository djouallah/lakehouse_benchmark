"""The SQL, and the per-engine rewriting that the notebook got wrong.

Everything here runs offline, for both query suites -- sql/tpch.sql's 22 statements and
sql/tpcds.sql's 99. These are the failures that would otherwise surface twenty minutes into a
real run, after the data has been generated and the catalog attached.
"""

from __future__ import annotations

import re

import pytest

from bench.tpcds.config import TpcdsConfig
from bench.tpch.config import ENGINES, TpchConfig
from bench.tpch.queries import (
    BACKTICK_ALIASES,
    IDENT_STYLE,
    N_QUERIES,
    SQL_PATH,
    load,
    render,
    rewrite_identifiers,
)

SCHEMA = "CH0010"
SF = 10
SUITES = (TpchConfig, TpcdsConfig)


def _load(suite, engine: str, schema: str = SCHEMA, sf: int = SF) -> list[str]:
    return load(engine, schema, sf, suite.SQL_PATH, suite.N_QUERIES)


def test_twenty_two_statements():
    assert len(load("duckdb_iceberg", SCHEMA, SF)) == N_QUERIES


def test_ninety_nine_statements():
    assert len(_load(TpcdsConfig, "duckdb_iceberg")) == 99


@pytest.mark.parametrize("suite", SUITES)
@pytest.mark.parametrize("engine", ENGINES)
def test_no_placeholder_survives(suite, engine):
    """A leftover `{schema}` reaches the engine as a syntax error at query time, not here."""
    for index, statement in enumerate(_load(suite, engine), start=1):
        assert "{" not in statement, f"{engine} Q{index} still has a placeholder"
        assert "}" not in statement


@pytest.mark.parametrize("suite", SUITES)
@pytest.mark.parametrize("engine", ENGINES)
def test_every_table_reference_is_rewritten(suite, engine):
    """No reference is left in the source spelling.

    The bug this catches: cell 15 stripped backticks for Polars and left `CH0010.lineitem`, which
    Polars parses as a relation named CH0010 and cannot resolve.
    """
    style = IDENT_STYLE[engine]
    first = suite.TABLES[0]
    joined = "\n".join(_load(suite, engine))
    if style == "backticked":
        # The qualified name must survive as ONE quoted identifier.
        assert f"`{SCHEMA}.{first}`" in joined
        assert not re.search(rf"(?<!`){re.escape(SCHEMA)}\.\w+(?!`)", joined)
    elif style == "quoted":
        # Same single identifier, ANSI quoting. Daft rejects a backtick outright.
        assert f'"{SCHEMA}.{first}"' in joined
        assert "`" not in joined
        assert not re.search(rf'(?<!"){re.escape(SCHEMA)}\.\w+(?!")', joined)
    else:
        assert f"{SCHEMA}.{first}" in joined
        if engine in BACKTICK_ALIASES:
            assert f"`{SCHEMA}." not in joined  # its aliases are backticked, its tables not
        else:
            assert "`" not in joined


@pytest.mark.parametrize("engine", ENGINES)
def test_double_quoted_aliases_reach_only_engines_that_parse_them(engine):
    """Sail reads `AS "order count"` as a string and fails eight TPC-DS statements on it."""
    joined = "\n".join(_load(TpcdsConfig, engine))
    if engine in BACKTICK_ALIASES:
        assert '"' not in joined
        assert "AS `order count`" in joined
        assert "AS `>120 days`" in joined
    else:
        assert 'AS "order count"' in joined


def test_polars_and_chdb_keep_the_qualified_name_quoted():
    """Both engines lack a second namespace level, for different reasons.

    chDB: the OneLake catalog reports `<namespace>.<table>` as one name.
    Polars: no catalog at all, so the engine registers frames under the full dotted name.
    """
    assert IDENT_STYLE["polars_iceberg"] == "backticked"
    assert IDENT_STYLE["chdb_iceberg"] == "backticked"
    # Daft has the same flat namespace and rejects backticks, so it gets the quoted spelling.
    assert IDENT_STYLE["daft_iceberg"] == "quoted"


@pytest.mark.parametrize("suite", SUITES)
def test_every_table_is_referenced(suite):
    joined = "\n".join(_load(suite, "duckdb_iceberg"))
    for table in suite.TABLES:
        assert f"{SCHEMA}.{table}" in joined, f"{suite.TITLE}: no query touches {table}"


def test_q11_threshold_scales_with_sf():
    """Q11's `(0.0001 / {SF})` is the only use of SF in the whole script.

    Dropping it would silently change how many rows Q11 returns at every scale factor but the one
    it was written for -- and no timing chart could ever show that.
    """
    for sf in (1, 10, 100):
        statements = load("duckdb_iceberg", f"CH{sf:04d}", sf)
        assert f"0.0001 / {sf}" in statements[10], f"Q11 lost its SF scaling at SF={sf}"


def test_sf_appears_exactly_once_in_the_source():
    assert SQL_PATH.read_text(encoding="utf-8").count("{SF}") == 1


def test_tpcds_is_a_fixed_point_of_the_rewrite():
    """sql/build_tpcds_sql.py's guarantee, re-checked on the committed file.

    Running bench/tpcds/rewrite.py over sql/tpcds.sql must change nothing: every table reference
    is already `{schema}.`-qualified and aliased, and every other occurrence of a table name (a
    column alias, a qualifier, a column reference through an alias) is left alone. A bare
    `store_sales` would resolve on DuckDB (its DEFAULT_SCHEMA) and fail on every engine that
    needs the qualified name -- a dialect gap that is not one.
    """
    from bench.tpcds.rewrite import qualify

    text = TpcdsConfig.SQL_PATH.read_text(encoding="utf-8")
    assert qualify(text) == text
    assert text.count("`{schema}.") >= TpcdsConfig.N_QUERIES


def test_tpcds_is_a_fixed_point_of_portable():
    """sql/tpcds.sql carries bench/tpcds/rewrite.py `portable()`: no bare date literal, no
    date + integer, no grouping() sum in ORDER BY -- each a statement Trino rejected."""
    from bench.tpcds.rewrite import portable

    text = TpcdsConfig.SQL_PATH.read_text(encoding="utf-8")
    assert portable(text) == text
    assert "d_date + 5 " not in text
    assert "INTERVAL '5' DAY" in text


def test_tpcds_only_uses_the_schema_placeholder():
    """render() is str.format: any other brace in the file is a KeyError at load time."""
    text = TpcdsConfig.SQL_PATH.read_text(encoding="utf-8")
    assert set(re.findall(r"\{[^}]*\}", text)) == {"{schema}"}
    assert text.count("-- Query ") == TpcdsConfig.N_QUERIES


def test_unknown_engine_is_rejected():
    with pytest.raises(ValueError, match="unknown engine"):
        load("clickhouse_local", SCHEMA, SF)


def test_render_is_idempotent_on_schema_name():
    """A schema name that is a prefix of another must not be double-rewritten."""
    raw = "SELECT * FROM `{schema}.lineitem` JOIN `{schema}.orders` ON 1=1"
    rendered = render(raw, "CH0010", 10)
    assert rewrite_identifiers(rendered, "duckdb_iceberg", "CH0010") == (
        "SELECT * FROM CH0010.lineitem JOIN CH0010.orders ON 1=1"
    )
