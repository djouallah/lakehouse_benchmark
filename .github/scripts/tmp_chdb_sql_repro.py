"""TEMPORARY. Minimal reproductions, no Iceberg and no TPC-DS data, of the SQL failures that keep
chDB out of TPC-DS (smoke run 39182268676 on chdb-core 26.10.0.dev2610180851), to tell the ones
already filed from the ones that are not."""

from __future__ import annotations

import chdb


def q(title: str, sql: str) -> None:
    print(f"\n=== {title}\n{sql}")
    try:
        print("  OK   ->", str(chdb.query(sql, "CSV")).strip())
    except Exception as exc:  # noqa: BLE001 - reporting is the point
        print("  FAIL ->", " ".join(str(exc).split())[:400])


print("chdb", chdb.__version__, "engine", str(chdb.query("SELECT version()", "CSV")).strip())

# Q18: CAST of a NULL to a non-Nullable type.
q("Q18 shape: CAST(NULL AS DECIMAL)", "SELECT CAST(NULL AS Decimal(12, 2))")
q(
    "Q18 shape: CAST of a Nullable column holding NULL",
    "SELECT avg(CAST(x AS Decimal(12, 2))) FROM values('x Nullable(Decimal(7, 2))', (1.5), (NULL))",
)
q(
    "Q18 shape, with cast_keep_nullable = 1",
    "SELECT CAST(NULL AS Decimal(12, 2)) SETTINGS cast_keep_nullable = 1",
)

# Q36/Q66: an alias with a column's own name, then that column inside another aggregate (#9715).
q("Q36 shape", "SELECT sum(number) AS number, max(number) FROM numbers(10)")

# Q47/Q57: a CTE column referenced by its bare name after a self-join of three (#34697?).
base = "WITH v1 AS (SELECT number AS k, number * 2 AS d_year FROM numbers(3))"
q(
    "Q47 shape, two copies of v1",
    f"{base}, v2 AS (SELECT v1.d_year FROM v1, v1 AS v1_lag WHERE v1.k = v1_lag.k) "
    "SELECT * FROM v2 WHERE d_year = 2",
)
q(
    "Q47 shape, three copies of v1",
    f"{base}, v2 AS (SELECT v1.d_year FROM v1, v1 AS v1_lag, v1 AS v1_lead "
    "WHERE v1.k = v1_lag.k AND v1.k = v1_lead.k) SELECT * FROM v2 WHERE d_year = 2",
)
q(
    "Q47 shape, three copies, the inner names listed",
    f"{base}, v2 AS (SELECT v1.d_year FROM v1, v1 AS v1_lag, v1 AS v1_lead "
    "WHERE v1.k = v1_lag.k AND v1.k = v1_lead.k) SELECT name FROM system.one, "
    "(SELECT * FROM v2 LIMIT 0) FORMAT Null",
)
q(
    "Q47 shape, three copies, DESCRIBE",
    f"DESCRIBE ({base}, v2 AS (SELECT v1.d_year FROM v1, v1 AS v1_lag, v1 AS v1_lead "
    "WHERE v1.k = v1_lag.k AND v1.k = v1_lead.k) SELECT * FROM v2)",
)
