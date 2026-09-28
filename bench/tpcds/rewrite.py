"""Qualify the bare table names in DuckDB's TPC-DS queries the way sql/tpch.sql spells them.

`tpcds_queries()` hands out the 99 statements with BARE table names. sql/tpch.sql's convention --
every table reference a backticked, schema-qualified `{schema}.table` -- is what
bench/tpch/queries.py rewrites per engine, so the same loader serves both suites. This module
turns one into the other; sql/build_tpcds_sql.py runs it and tests/test_queries.py checks that
the committed file is a fixed point of it.

A TOKENIZER, NOT A PARSER, and the four rules it needs, each one found by the builder's check
rather than assumed:

1. A table name is a REFERENCE only after FROM or JOIN, or after a comma whose governing clause
   -- the nearest SELECT/FROM/WHERE/... keyword scanning back at the same parenthesis depth --
   is FROM (`from store_sales, date_dim`). Anywhere else it is something that happens to share
   the name: a column alias (q31's `sum(ss_ext_sales_price) AS store_sales`, q51's
   `store.cume_sales store_sales`), a select-list column (q49's `SELECT channel, item, ...`), a
   column reference through an alias (`ss1.store_sales`), or a subquery alias (q49's `store`).
2. ... unless the next token is a dot. `store_sales.ss_item_sk` in a WHERE clause names the
   TABLE as a qualifier; the reference to rewrite is the one in the FROM list, not this.
3. String literals and comments are skipped whole: 'store', 'catalog' and 'web' are channel
   labels in q14 and q76.
4. EVERY REFERENCE GETS AN ALIAS. Ten queries qualify columns with the bare table name
   (`store_sales.ss_sold_date_sk = date_dim.d_date_sk`). DuckDB and Spark resolve that against a
   schema-qualified table; chDB, Polars and Daft cannot, because for them the table's whole
   name is the single identifier `DS0010.store_sales` (bench/tpch/queries.py explains that
   spelling). `` `{schema}.store_sales` AS store_sales `` is the one form that reads the same on
   all seven engines, so a reference that carries no alias of its own is given its bare name.
"""

from __future__ import annotations

import re

from bench.tpcds.config import TABLES

_TABLES = frozenset(TABLES)

# A single-quoted literal ('' is the escaped quote), a line comment, a word, or one other char.
_TOKEN = re.compile(r"'(?:[^']|'')*'|--[^\n]*|\w+|\S")

_INTRODUCERS = frozenset({"from", "join", ","})

# Keywords that open a clause. Scanning back from a comma, the first of these at the same
# parenthesis depth says which list the comma belongs to; only FROM's (and JOIN's) list holds
# tables. ON and USING are deliberately absent: they sit INSIDE the FROM clause, and q40 writes
# `JOIN catalog_returns ON (...), warehouse` -- the comma after a join condition is still a
# FROM-list comma.
_CLAUSES = frozenset(
    {
        "select", "from", "where", "group", "order", "having", "when", "then", "else", "case",
        "set", "values", "with", "limit", "union", "intersect", "except", "qualify", "window",
        "partition", "by", "join",
    }
)  # fmt: skip

# What may follow a table reference WITHOUT being its alias. Anything else that is a word is one.
_NOT_AN_ALIAS = frozenset(
    {
        "as", "on", "using", "where", "group", "order", "having", "limit", "qualify", "window",
        "union", "intersect", "except", "join", "left", "right", "inner", "full", "cross",
        "outer", "natural", "lateral", "and", "or", "when", "then", "else", "end", "select",
        "from",
    }
)  # fmt: skip


def qualify(sql: str) -> str:
    """`sql` with every table REFERENCE spelled `` `{schema}.<table>` ``, aliased if it was not.

    Idempotent: on already-qualified text every table name is preceded by `.` (inside the
    backticks) or by `AS` (the alias), and neither is an introducer.
    """
    tokens = [(m.start(), m.end(), m.group(0)) for m in _TOKEN.finditer(sql)]

    def significant(index: int, step: int) -> str:
        """The next/previous token that is not a comment, lowercased; '' at either end."""
        index += step
        while 0 <= index < len(tokens) and tokens[index][2].startswith("--"):
            index += step
        return tokens[index][2].lower() if 0 <= index < len(tokens) else ""

    def in_from_list(index: int) -> bool:
        """Is the comma before token `index` separating FROM-list items?"""
        depth = 0
        for j in range(index - 1, -1, -1):
            token = tokens[j][2].lower()
            if token.startswith("--"):
                continue
            if token == ")":
                depth += 1
            elif token == "(":
                if depth == 0:
                    return False  # the comma belongs to a parenthesised list, not a FROM
                depth -= 1
            elif depth == 0 and token in _CLAUSES:
                return token in ("from", "join")
        return False

    out, last = [], 0
    for index, (start, end, text) in enumerate(tokens):
        if text not in _TABLES:
            continue
        before, after = significant(index, -1), significant(index, +1)
        if before not in _INTRODUCERS or after == ".":
            continue
        if before == "," and not in_from_list(index):
            continue
        replacement = f"`{{schema}}.{text}`"
        has_alias = after == "as" or (after.isidentifier() and after not in _NOT_AN_ALIAS)
        if not has_alias:
            replacement += f" AS {text}"
        out.append(sql[last:start])
        out.append(replacement)
        last = end
    out.append(sql[last:])
    return "".join(out)


# A 'YYYY-MM-DD' literal and what sits before it, so one already inside cast( is left alone.
_DATE_LITERAL = re.compile(r"'\d{4}-\d{2}-\d{2}'")
_CAST_OPEN = re.compile(r"cast\s*\(\s*$", re.IGNORECASE)
# ORDER BY's grouping() sum in q70 and q86, where the select list names it `lochierarchy`.
_ORDER_BY_GROUPING = re.compile(r"WHEN grouping\(\w+\)\+grouping\(\w+\) = 0 THEN", re.IGNORECASE)


def portable(sql: str) -> str:
    """`sql` in standard SQL where DuckDB's copy leans on leniency Trino does not have.

    Same answers on every engine (sql/build_tpcds_sql.py checks the row counts against the
    originals); each rule is a statement Trino rejected at SF=1 (smoke run 36371429297):

    1. A bare date literal compared with a date -- `d_date BETWEEN '2002-02-01' AND ...`,
       `d_date = '2000-01-03'`, `d_date IN ('2000-06-30', ...)` (q16, q32, q58, q83, q92, q94,
       q95) -- becomes `cast('...' AS date)`, the spelling the other end of those BETWEENs
       already uses. Trino compares no date with a varchar.
    2. q72's `d1.d_date + 5` becomes `+ INTERVAL '5' DAY`: date plus integer is not SQL.
    3. q70 and q86 order by `grouping(a)+grouping(b)` inside a CASE; the spec writes it
       `lochierarchy`, the select list's own name for it, and Trino (like StarRocks, #79806)
       rejects the grouping() form in ORDER BY.

    Idempotent, like `qualify`.
    """

    def date(match: re.Match) -> str:
        if _CAST_OPEN.search(sql[: match.start()]):
            return match.group(0)
        return f"cast({match.group(0)} AS date)"

    sql = _DATE_LITERAL.sub(date, sql)
    sql = sql.replace("d1.d_date + 5 ", "d1.d_date + INTERVAL '5' DAY ")
    return _ORDER_BY_GROUPING.sub("WHEN lochierarchy = 0 THEN", sql)
