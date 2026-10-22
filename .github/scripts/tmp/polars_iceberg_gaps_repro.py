"""Polars Iceberg gaps on a main build: Python API surface, MAX from metadata, SQL parity.

Local pyiceberg SqlCatalog in a temp dir, no secrets. Prints every error verbatim for the
upstream issues.
"""

import glob
import inspect
import os
import tempfile

import polars as pl
from pyiceberg.catalog.sql import SqlCatalog

print("polars", pl.__version__, pl.build_info().get("git", {}))

d = tempfile.mkdtemp()
cat = SqlCatalog("lake", uri=f"sqlite:///{d}/c.db", warehouse=f"file://{d}")


def sink(name, df, **kw):
    df.lazy().sink_iceberg(name, catalog=cat, **kw)


sink(
    "sales.orders",
    pl.DataFrame({"id": [1, 2], "v": [10, 20]}),
    create_if_not_exists=True,
    create_namespace_if_not_exists=True,
)
sink("sales.orders", pl.DataFrame({"id": [3], "v": [30]}))
first = cat.load_table("sales.orders").snapshots()[0].snapshot_id

print("\n== A. Polars Iceberg API surface")
print("pl.*iceberg*:", [n for n in dir(pl) if "iceberg" in n.lower()])
print("DataFrame.*iceberg*:", [n for n in dir(pl.DataFrame) if "iceberg" in n.lower()])
print("LazyFrame.*iceberg*:", [n for n in dir(pl.LazyFrame) if "iceberg" in n.lower()])
print("sink_iceberg", inspect.signature(pl.LazyFrame.sink_iceberg))
print("write_iceberg", inspect.signature(pl.DataFrame.write_iceberg))
print("scan_iceberg", inspect.signature(pl.scan_iceberg))
print("pl.Catalog methods:", [n for n in dir(pl.Catalog) if not n.startswith("_")])

print("\n== B. MAX / MIN / COUNT with every data file deleted")
sink("sales.stats", pl.DataFrame({"id": [1, 2, 3, 4]}), create_if_not_exists=True)
tbl = cat.load_table("sales.stats")
files = glob.glob(f"{d}/sales.db/stats/data/*.parquet")
for f in files:
    os.remove(f)
print(f"deleted {len(files)} data files")
for label, expr in [("max", pl.col("id").max()), ("min", pl.col("id").min()), ("len", pl.len())]:
    try:
        print(label, "->", pl.scan_iceberg(tbl).select(expr).collect().item())
    except Exception as e:
        print(label, "->", f"{type(e).__name__}: {e}")

print("\n== C. SQL on an attached catalog")
STATEMENTS = [
    "SELECT count(*) AS n FROM sales.orders",
    # already filed: #29892-#29896, #22185
    "CREATE TABLE sales.t2 (id BIGINT, v BIGINT)",
    "CREATE TABLE sales.t3 AS SELECT * FROM sales.orders",
    "INSERT INTO sales.orders VALUES (4, 40)",
    "INSERT INTO sales.orders SELECT id + 10, v FROM sales.orders",
    "DELETE FROM sales.orders WHERE id = 1",
    "UPDATE sales.orders SET v = 0 WHERE id = 1",
    "DROP TABLE sales.orders",
    "MERGE INTO sales.orders t USING (SELECT 1 AS id, 0 AS v) s ON t.id = s.id "
    "WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)",
    # parity gaps
    "TRUNCATE TABLE sales.orders",
    "INSERT OVERWRITE TABLE sales.orders SELECT * FROM sales.orders WHERE id > 1",
    "INSERT OVERWRITE sales.orders SELECT * FROM sales.orders WHERE id > 1",
    f"SELECT count(*) AS n FROM sales.orders FOR VERSION AS OF {first}",
    f"SELECT count(*) AS n FROM sales.orders VERSION AS OF {first}",
    "SELECT count(*) AS n FROM sales.orders FOR SYSTEM_TIME AS OF '2100-01-01 00:00:00'",
    "CREATE SCHEMA IF NOT EXISTS newns",
    "CREATE SCHEMA IF NOT EXISTS lake.newns",
    "ALTER TABLE sales.orders ADD COLUMN c BIGINT",
]
for sql in STATEMENTS:
    ctx = pl.SQLContext().register_catalog("lake", cat, default=True)
    try:
        out = ctx.execute(sql, eager=True)
        print(f"OK   {sql}\n     -> {out.to_dicts()[:3]}")
    except Exception as e:
        print(f"ERR  {sql}\n     -> {type(e).__name__}: {e}")
print("\nnamespaces after:", cat.list_namespaces())
print("tables in sales:", cat.list_tables("sales"))
print("snapshots of sales.orders:", len(cat.load_table("sales.orders").snapshots()))
