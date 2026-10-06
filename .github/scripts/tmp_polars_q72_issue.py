"""TEMPORARY: standalone repro for a Polars issue (TPC-DS Q72, scan_iceberg vs scan_parquet).

Needs only: polars, duckdb, pyiceberg[sql-sqlite,pyarrow], psutil.
"""

import threading
import time

import duckdb
import polars as pl
import psutil
from pyiceberg.catalog.sql import SqlCatalog

SF = 1
TABLES = [
    "catalog_sales", "inventory", "warehouse", "item", "customer_demographics",
    "household_demographics", "date_dim", "promotion", "catalog_returns",
]  # fmt: skip
Q72 = """
SELECT i_item_desc,
       w_warehouse_name,
       d1.d_week_seq,
       sum(CASE
               WHEN p_promo_sk IS NULL THEN 1
               ELSE 0
           END) no_promo,
       sum(CASE
               WHEN p_promo_sk IS NOT NULL THEN 1
               ELSE 0
           END) promo,
       count(*) total_cnt
FROM catalog_sales AS catalog_sales
JOIN inventory AS inventory ON (cs_item_sk = inv_item_sk)
JOIN warehouse AS warehouse ON (w_warehouse_sk=inv_warehouse_sk)
JOIN item AS item ON (i_item_sk = cs_item_sk)
JOIN customer_demographics AS customer_demographics ON (cs_bill_cdemo_sk = cd_demo_sk)
JOIN household_demographics AS household_demographics ON (cs_bill_hdemo_sk = hd_demo_sk)
JOIN date_dim d1 ON (cs_sold_date_sk = d1.d_date_sk)
JOIN date_dim d2 ON (inv_date_sk = d2.d_date_sk)
JOIN date_dim d3 ON (cs_ship_date_sk = d3.d_date_sk)
LEFT OUTER JOIN promotion AS promotion ON (cs_promo_sk=p_promo_sk)
LEFT OUTER JOIN catalog_returns AS catalog_returns ON (cr_item_sk = cs_item_sk
                                    AND cr_order_number = cs_order_number)
WHERE d1.d_week_seq = d2.d_week_seq
  AND inv_quantity_on_hand < cs_quantity
  AND d3.d_date > d1.d_date + INTERVAL '5' DAY
  AND hd_buy_potential = '>10000'
  AND d1.d_year = 1999
  AND cd_marital_status = 'D'
GROUP BY i_item_desc,
         w_warehouse_name,
         d1.d_week_seq
ORDER BY total_cnt DESC NULLS FIRST,
         i_item_desc NULLS FIRST,
         w_warehouse_name NULLS FIRST,
         d1.d_week_seq NULLS FIRST
LIMIT 100
"""

# 1. TPC-DS data with DuckDB's dsdgen, one parquet file per table.
con = duckdb.connect()
con.sql("INSTALL tpcds; LOAD tpcds")
con.sql(f"CALL dsdgen(sf = {SF})")
for t in TABLES:
    con.sql(f"COPY {t} TO '{t}.parquet' (FORMAT parquet)")

# 2. The same data as Iceberg tables in a local SQLite catalog.
catalog = SqlCatalog(
    "local", uri="sqlite:///catalog.db", warehouse="file://" + __import__("os").getcwd()
)
catalog.create_namespace_if_not_exists("tpcds")
for t in TABLES:
    data = con.sql(f"SELECT * FROM {t}").to_arrow_table()
    if catalog.table_exists(f"tpcds.{t}"):
        catalog.drop_table(f"tpcds.{t}")
    catalog.create_table(f"tpcds.{t}", schema=data.schema).append(data)


def run(label, frames):
    peak, stop = [0], threading.Event()
    proc = psutil.Process()

    def sample():
        while not stop.wait(0.1):
            peak[0] = max(peak[0], proc.memory_info().rss)

    threading.Thread(target=sample, daemon=True).start()
    started = time.perf_counter()
    out = pl.SQLContext(frames).execute(Q72).collect(engine="streaming")
    took = time.perf_counter() - started
    stop.set()
    print(
        f"{label}: {out.height} rows in {took:.1f}s, peak RSS {peak[0] / 2**30:.2f} GiB", flush=True
    )


print("polars", pl.__version__, "pyiceberg", __import__("pyiceberg").__version__)
run("scan_parquet", {t: pl.scan_parquet(f"{t}.parquet") for t in TABLES})
run("scan_iceberg", {t: pl.scan_iceberg(catalog.load_table(f"tpcds.{t}")) for t in TABLES})
