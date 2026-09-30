"""Load the 22 TPC-H statements and write their table references the way each engine needs.

REPLACES cells 14 and the identifier half of cell 15.

sql/tpch.sql is cell 14's f-string body, lifted verbatim: 22 statements separated by `;`, every
table reference written as a BACKTICKED, SCHEMA-QUALIFIED name -- `{schema}.lineitem`. All 87
references are backticked uniformly (checked), which is what makes the rewriting below a clean
regex rather than a parser.

THE ONE CHANGE FROM CELL 15. It had:

    NEEDS_BACKTICKS = {'chdb_iceberg'}

Polars was therefore handed `CH0010.lineitem` with the backticks stripped, which it parses as a
relation named CH0010 -- `ComputeError: relation 'CH0010' was not found`. Polars has no catalog
namespace to resolve the prefix against.

The fix is on the Polars side and it is why the set below has two members: the engine registers
each frame into its SQLContext under the FULL dotted name (`CH0010.lineitem`), so keeping the
backticks makes the SQL ask for exactly that one quoted identifier. Same trick chDB needs, for
the same underlying reason -- neither engine has a second namespace level, so the qualified name
has to survive as a single identifier rather than being parsed as schema + table.
"""

from __future__ import annotations

import re
from pathlib import Path

from bench.config import SQL_DIR

# The TPC-H defaults. `load` takes any suite's file and count -- sql/tpcds.sql is written
# to the same convention (backticked `{schema}.table`, `;`-separated) so TPC-DS goes
# through here too.
SQL_PATH = SQL_DIR / "tpch.sql"

N_QUERIES = 22

# How each engine wants `{schema}.lineitem` written.
#
#   backticked  The qualified name must survive as ONE identifier.
#               chDB: the OneLake catalog reports `<namespace>.<table>` as a single name,
#               and ClickHouse has no second namespace level to put it in -- unquoted it
#               resolves as a database named CH0010.
#               Polars: no catalog at all, but the engine registers each frame under the
#               full dotted name, so the backticks ask for that exact key.
#   quoted      The same single identifier, but DOUBLE-QUOTED. Daft registers temp tables
#               under the full dotted name like Polars, and then rejects backticks outright:
#               "Daft only supports delimited identifiers with double-quotes, found `".
#               All 22 queries failed on that alone -- a quoting style, not a dialect gap.
#   dotted      Ordinary schema-qualified SQL. DuckDB resolves it through the attached
#               catalog's DEFAULT_SCHEMA; LakeSail through its OneLake catalog.
IDENT_STYLE = {
    "chdb_iceberg": "backticked",
    "polars_iceberg": "backticked",
    # Daft: a flat namespace like Polars, but ANSI quoting -- see the note above.
    "daft_iceberg": "quoted",
    "duckdb_iceberg": "dotted",
    # Spark has real multi-level namespaces, same as LakeSail.
    "pyspark_iceberg": "dotted",
    # Stock Spark plus a file cache; the dialect is Spark's.
    "pyspark_alluxio_iceberg": "dotted",
    # Spark with a native executor underneath; the parser is still Spark's.
    "pyspark_gluten_iceberg": "dotted",
    "lakesail_iceberg": "dotted",
    # catalog.database.table, and `SET CATALOG onelake` makes `CH0010.lineitem` resolve.
    "starrocks_iceberg": "dotted",
    # catalog.schema.table; the connection's default catalog and schema resolve the rest.
    "trino_iceberg": "dotted",
}


# Engines whose parser reads a double-quoted name as a STRING, so `AS "order count"` -- the
# TPC-DS spec's own alias in Q16, Q32, Q50, Q62, Q92, Q94, Q95 and Q99 -- is a parse error.
#   LakeSail: its parser has an `allow_double_quote_identifier` switch, but the analyzer always
#   builds ParserOptions::default() (off), and `spark.sql.ansi.doubleQuotedIdentifiers` (the
#   setting Spark takes, bench/tpch/engines/pyspark_iceberg.py) is listed in Sail's config but
#   never reaches it (sail-sql-analyzer/src/parser.rs at v0.7.2). The same alias in backticks is
#   the same column name, so only the quoting changes.
BACKTICK_ALIASES = frozenset({"lakesail_iceberg"})

# `AS "..."`: in sql/tpch.sql and sql/tpcds.sql a double quote only ever opens a column alias.
_QUOTED_ALIAS = re.compile(r'\bAS\s+"([^"`]+)"', re.IGNORECASE)


def render(sql: str, schema: str, sf: int) -> str:
    """Substitute the two placeholders.

    `{SF}` appears exactly once, in Q11's `(0.0001 / {SF})` -- the correct TPC-H scaling of
    the value threshold. Dropping it would make Q11 return the wrong number of rows at every
    scale factor but one, in a way no timing chart could reveal. test_queries.py pins it.
    """
    return sql.format(schema=schema, SF=sf)


def style_for(engine: str) -> str:
    try:
        return IDENT_STYLE[engine]
    except KeyError:
        raise ValueError(
            f"unknown engine {engine!r}; expected one of {sorted(IDENT_STYLE)}"
        ) from None


def rewrite_identifiers(sql: str, engine: str, schema: str) -> str:
    """Rewrite every `schema.table` reference into `engine`'s spelling.

    Runs on ALREADY-RENDERED sql, so the pattern matches the real schema name rather than the
    `{schema}` placeholder.
    """
    style = style_for(engine)
    if style == "backticked":
        return sql
    pattern = re.compile(rf"`{re.escape(schema)}\.(\w+)`")
    if style == "quoted":
        return pattern.sub(rf'"{schema}.\1"', sql)
    return pattern.sub(rf"{schema}.\1", sql)


def load(
    engine: str,
    schema: str,
    sf: int,
    path: Path | None = None,
    expected: int = N_QUERIES,
) -> list[str]:
    """The suite's statements, rendered and rewritten for `engine`, in query order.

    Splitting on `;` is safe here and not in general: neither sql/tpch.sql nor sql/tpcds.sql
    contains a semicolon inside a string literal (checked at extraction, and test_queries.py
    re-checks the count for both).
    """
    raw = (path or SQL_PATH).read_text(encoding="utf-8")
    rendered = rewrite_identifiers(render(raw, schema, sf), engine, schema)
    if engine in BACKTICK_ALIASES:
        rendered = _QUOTED_ALIAS.sub(r"AS `\1`", rendered)
    statements = [s.strip() for s in rendered.split(";") if s.strip()]
    if len(statements) != expected:
        raise ValueError(
            f"expected {expected} statements in {path or SQL_PATH}, found {len(statements)}"
        )
    return statements
