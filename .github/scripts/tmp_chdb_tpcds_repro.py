"""Minimal repros for the TPC-DS queries chDB fails. Temporary: deleted after the issue is filed."""

import re
from pathlib import Path

from chdb import session

s = session.Session()
for setting in ("SET join_use_nulls = 1", "SET union_default_mode = 'DISTINCT'"):
    s.query(setting)

# Empty tables: the failures are analysis errors, raised before any row is read.
TABLES = {
    "item": "i_item_sk Int64, i_category Nullable(String), i_brand Nullable(String)",
    "store_sales": "ss_item_sk Nullable(Int64), ss_sold_date_sk Nullable(Int64), "
    "ss_store_sk Nullable(Int64), ss_sales_price Nullable(Decimal(7, 2))",
    "catalog_sales": "cs_item_sk Nullable(Int64), cs_sold_date_sk Nullable(Int64), "
    "cs_call_center_sk Nullable(Int64), cs_sales_price Nullable(Decimal(7, 2))",
    "date_dim": "d_date_sk Int64, d_year Nullable(Int32), d_moy Nullable(Int32)",
    "store": "s_store_sk Int64, s_store_name Nullable(String), s_company_name Nullable(String)",
    "call_center": "cc_call_center_sk Int64, cc_name Nullable(String)",
}
for name, columns in TABLES.items():
    s.query(f"CREATE TABLE {name} ({columns}) ENGINE = Memory")

text = Path("sql/tpcds.sql").read_text()


def query(n: int) -> str:
    body = re.search(rf"(?ms)^-- Query {n:02d}\n(.*?)(?=^-- Query |\Z)", text).group(1)
    return re.sub(r"`\{schema\}\.(\w+)`", r"\1", body).strip().rstrip(";")


q47 = query(47)
CASES = {
    "version": "SELECT version()",
    "const_two_way": "WITH v1 AS (SELECT 1 AS a), v2 AS (SELECT v1.a FROM v1, v1 AS x) SELECT * FROM v2 WHERE a = 1",
    "const_three_way": "WITH v1 AS (SELECT 1 AS a), v2 AS (SELECT v1.a FROM v1, v1 AS x, v1 AS y) SELECT * FROM v2 WHERE a = 1",
    "const_three_way_select": "WITH v1 AS (SELECT 1 AS a), v2 AS (SELECT v1.a FROM v1, v1 AS x, v1 AS y) SELECT a FROM v2",
    "const_three_way_alias": "WITH v1 AS (SELECT 1 AS a), v2 AS (SELECT v1.a AS a FROM v1, v1 AS x, v1 AS y) SELECT * FROM v2 WHERE a = 1",
    "const_three_way_join_on": "WITH v1 AS (SELECT 1 AS a), v2 AS (SELECT v1.a FROM v1 CROSS JOIN v1 AS x CROSS JOIN v1 AS y) SELECT * FROM v2 WHERE a = 1",
    "subquery_three_way": "SELECT * FROM (SELECT v1.a FROM (SELECT 1 AS a) AS v1, (SELECT 1 AS a) AS x, (SELECT 1 AS a) AS y) WHERE a = 1",
    "tables_three_way": "SELECT * FROM (SELECT t1.d_year FROM date_dim AS t1, date_dim AS t2, date_dim AS t3) WHERE d_year = 1",
    "old_analyzer": "WITH v1 AS (SELECT 1 AS a), v2 AS (SELECT v1.a FROM v1, v1 AS x, v1 AS y) SELECT * FROM v2 WHERE a = 1 SETTINGS allow_experimental_analyzer = 0",
    "q47_alias_fix": q47.replace("v1.d_year,", "v1.d_year AS d_year,"),
    "q18_cast_keep_nullable": "SELECT avg(CAST(x AS decimal(12, 2))) FROM (SELECT CAST(NULL, 'Nullable(Float64)') AS x UNION ALL SELECT 1.5) SETTINGS cast_keep_nullable = 1",
    "q18_cast_null_literal": "SELECT CAST(NULL AS decimal(12, 2))",
}

for name, sql in CASES.items():
    try:
        print(f"{name:24} OK    {str(s.query(sql, 'CSV')).strip()[:80]}")
    except Exception as exc:
        print(f"{name:24} FAIL  {str(exc).splitlines()[0][:300]}")
