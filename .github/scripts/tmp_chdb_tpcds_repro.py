"""Minimal repros for the TPC-DS queries chDB fails. Temporary: deleted after the issue is filed."""

from chdb import session

s = session.Session()
for setting in ("SET join_use_nulls = 1", "SET union_default_mode = 'DISTINCT'"):
    s.query(setting)

CASES = {
    "version": "SELECT version()",
    # Q49: a subquery aliased `catalog`; the same query aliased `web` works.
    "q49_alias_web": "SELECT web.x FROM (SELECT 1 AS x) AS web",
    "q49_alias_catalog": "SELECT catalog.x FROM (SELECT 1 AS x) AS catalog",
    # Q47/Q57: a qualified column `v1.d_year` projected from a CTE, then filtered by its bare name.
    "q47_single": "WITH v1 AS (SELECT 1 AS d_year), v2 AS (SELECT v1.d_year FROM v1) "
    "SELECT * FROM v2 WHERE d_year = 1",
    "q47_selfjoin": "WITH v1 AS (SELECT 1 AS d_year, 1 AS rn), "
    "v2 AS (SELECT v1.d_year FROM v1, v1 AS v1_lag WHERE v1.rn = v1_lag.rn) "
    "SELECT * FROM v2 WHERE d_year = 1",
    # Q18: CAST of a nullable column to a non-nullable type, inside avg, with and without ROLLUP.
    "q18_cast": "SELECT avg(CAST(x AS decimal(12, 2))) FROM (SELECT CAST(NULL, 'Nullable(Float64)') AS x UNION ALL SELECT 1.5)",
    "q18_cast_rollup": "SELECT k, avg(CAST(x AS decimal(12, 2))) FROM (SELECT 'a' AS k, CAST(NULL, 'Nullable(Float64)') AS x UNION ALL SELECT 'a', 1.5) GROUP BY ROLLUP (k)",
    # Q36/Q66: an aggregate aliased with the name of the column it aggregates (ClickHouse#9715).
    "q36_alias": "SELECT sum(x) AS x, rank() OVER (ORDER BY sum(x)) FROM (SELECT 1 AS x)",
    # Q75: SUM over a CASE mixing Decimal and Float64 (ClickHouse#106707).
    "q75_variant": "SELECT sum(CASE WHEN x > 0 THEN CAST(x AS Decimal(9, 2)) ELSE 0.0 END) FROM (SELECT 1 AS x)",
}

for name, sql in CASES.items():
    try:
        print(f"{name:20} OK    {str(s.query(sql, 'CSV')).strip()}")
    except Exception as exc:
        print(f"{name:20} FAIL  {str(exc).splitlines()[0][:400]}")
