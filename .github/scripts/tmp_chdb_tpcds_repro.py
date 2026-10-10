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
    "q47_full": q47,
    "q47_select_list": q47.replace("SELECT *\nFROM v2", "SELECT d_year, sum_sales\nFROM v2"),
    "q47_no_outer_filter": q47.replace("WHERE d_year = 1999\n  AND", "WHERE"),
    "q57_full": query(57),
    # The minimal shape: a CTE that projects a window over an aggregate, read by a second CTE
    # through a self-join, then filtered by the bare name.
    "min_window_selfjoin": "WITH v1 AS (SELECT d_year, sum(d_moy) AS s, "
    "rank() OVER (ORDER BY d_year) AS rn FROM date_dim GROUP BY d_year), "
    "v2 AS (SELECT v1.d_year, v1_lag.s AS psum FROM v1, v1 AS v1_lag WHERE v1.rn = v1_lag.rn + 1) "
    "SELECT * FROM v2 WHERE d_year = 1999",
    "min_window_nojoin": "WITH v1 AS (SELECT d_year, sum(d_moy) AS s, "
    "rank() OVER (ORDER BY d_year) AS rn FROM date_dim GROUP BY d_year), "
    "v2 AS (SELECT v1.d_year FROM v1) SELECT * FROM v2 WHERE d_year = 1999",
    "min_selfjoin_nowindow": "WITH v1 AS (SELECT d_year, d_moy AS rn FROM date_dim), "
    "v2 AS (SELECT v1.d_year, v1_lag.rn AS r FROM v1, v1 AS v1_lag WHERE v1.rn = v1_lag.rn + 1) "
    "SELECT * FROM v2 WHERE d_year = 1999",
    "min_selfjoin_threeway": "WITH v1 AS (SELECT d_year, d_moy AS rn FROM date_dim), "
    "v2 AS (SELECT v1.d_year, v1_lag.rn AS r, v1_lead.rn AS l FROM v1, v1 AS v1_lag, v1 AS v1_lead "
    "WHERE v1.rn = v1_lag.rn + 1 AND v1.rn = v1_lead.rn - 1) "
    "SELECT * FROM v2 WHERE d_year = 1999",
}

for name, sql in CASES.items():
    try:
        print(f"{name:24} OK    {str(s.query(sql, 'CSV')).strip()[:80]}")
    except Exception as exc:
        print(f"{name:24} FAIL  {str(exc).splitlines()[0][:300]}")
