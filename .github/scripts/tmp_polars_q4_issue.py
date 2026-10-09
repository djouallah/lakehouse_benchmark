# ruff: noqa: E501
"""TEMPORARY: standalone repro for a Polars issue (TPC-DS Q4 OOM on main, fine on 2.0.0).

Needs only: polars, duckdb, pyiceberg[sql-sqlite,pyarrow], psutil.
Each case runs in a child process, killed at 12 GiB RSS or 300 s.
"""

import os
import subprocess
import sys
import time

import psutil

SF = int(os.environ.get("SF", "10"))
TABLES = ["customer", "store_sales", "catalog_sales", "web_sales", "date_dim"]
Q4 = """
WITH year_total AS
  (SELECT c_customer_id customer_id,
          c_first_name customer_first_name,
          c_last_name customer_last_name,
          c_preferred_cust_flag customer_preferred_cust_flag,
          c_birth_country customer_birth_country,
          c_login customer_login,
          c_email_address customer_email_address,
          d_year dyear,
          sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,
          's' sale_type
   FROM customer AS customer,
        store_sales AS store_sales,
        date_dim AS date_dim
   WHERE c_customer_sk = ss_customer_sk
     AND ss_sold_date_sk = d_date_sk
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year
   UNION ALL SELECT c_customer_id customer_id,
                    c_first_name customer_first_name,
                    c_last_name customer_last_name,
                    c_preferred_cust_flag customer_preferred_cust_flag,
                    c_birth_country customer_birth_country,
                    c_login customer_login,
                    c_email_address customer_email_address,
                    d_year dyear,
                    sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2)) year_total,
                    'c' sale_type
   FROM customer AS customer,
        catalog_sales AS catalog_sales,
        date_dim AS date_dim
   WHERE c_customer_sk = cs_bill_customer_sk
     AND cs_sold_date_sk = d_date_sk
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year
   UNION ALL SELECT c_customer_id customer_id,
                    c_first_name customer_first_name,
                    c_last_name customer_last_name,
                    c_preferred_cust_flag customer_preferred_cust_flag,
                    c_birth_country customer_birth_country,
                    c_login customer_login,
                    c_email_address customer_email_address,
                    d_year dyear,
                    sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2)) year_total,
                    'w' sale_type
   FROM customer AS customer,
        web_sales AS web_sales,
        date_dim AS date_dim
   WHERE c_customer_sk = ws_bill_customer_sk
     AND ws_sold_date_sk = d_date_sk
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.customer_preferred_cust_flag
FROM year_total t_s_firstyear,
     year_total t_s_secyear,
     year_total t_c_firstyear,
     year_total t_c_secyear,
     year_total t_w_firstyear,
     year_total t_w_secyear
WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_c_secyear.customer_id
  AND t_s_firstyear.customer_id = t_c_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_secyear.customer_id
  AND t_s_firstyear.sale_type = 's'
  AND t_c_firstyear.sale_type = 'c'
  AND t_w_firstyear.sale_type = 'w'
  AND t_s_secyear.sale_type = 's'
  AND t_c_secyear.sale_type = 'c'
  AND t_w_secyear.sale_type = 'w'
  AND t_s_firstyear.dyear = 2001
  AND t_s_secyear.dyear = 2001+1
  AND t_c_firstyear.dyear = 2001
  AND t_c_secyear.dyear = 2001+1
  AND t_w_firstyear.dyear = 2001
  AND t_w_secyear.dyear = 2001+1
  AND t_s_firstyear.year_total > 0
  AND t_c_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND CASE
          WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
          ELSE NULL
      END > CASE
                WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
                ELSE NULL
            END
  AND CASE
          WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
          ELSE NULL
      END > CASE
                WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
                ELSE NULL
            END
ORDER BY t_s_secyear.customer_id NULLS FIRST,
         t_s_secyear.customer_first_name NULLS FIRST,
         t_s_secyear.customer_last_name NULLS FIRST,
         t_s_secyear.customer_preferred_cust_flag NULLS FIRST
LIMIT 100
"""


def generate():
    import duckdb
    from pyiceberg.catalog.sql import SqlCatalog

    con = duckdb.connect()
    con.sql("INSTALL tpcds; LOAD tpcds")
    con.sql(f"CALL dsdgen(sf = {SF})")
    catalog = SqlCatalog("local", uri="sqlite:///catalog.db", warehouse="file://" + os.getcwd())
    catalog.create_namespace_if_not_exists("tpcds")
    for t in TABLES:
        # DuckDB writes the files with the table's Iceberg field ids, then add_files registers them.
        schema = con.sql(f"SELECT * FROM {t} LIMIT 0").to_arrow_table().schema
        tbl = catalog.create_table(f"tpcds.{t}", schema=schema)
        ids = ", ".join(f"{f.name}: {f.field_id}" for f in tbl.schema().fields)
        con.sql(f"COPY {t} TO '{t}.parquet' (FORMAT parquet, FIELD_IDS {{{ids}}})")
        tbl.add_files([f"file://{os.getcwd()}/{t}.parquet"])


def child(case):
    import polars as pl
    from pyiceberg.catalog.sql import SqlCatalog

    catalog = SqlCatalog("local", uri="sqlite:///catalog.db", warehouse="file://" + os.getcwd())
    if case == "scan_parquet":
        frames = {t: pl.scan_parquet(f"{t}.parquet") for t in TABLES}
    else:
        stats = case == "scan_iceberg"
        frames = {
            t: pl.scan_iceberg(catalog.load_table(f"tpcds.{t}"), use_metadata_statistics=stats)
            for t in TABLES
        }
    lf = pl.SQLContext(frames).execute(Q4)
    if case == "scan_iceberg":
        print(lf.explain(engine="streaming"), flush=True)
    started = time.perf_counter()
    out = lf.collect(engine="streaming")
    print(f"RESULT {out.height} rows in {time.perf_counter() - started:.1f}s", flush=True)


def run(case):
    proc = psutil.Popen([sys.executable, __file__, case], stdout=subprocess.PIPE, text=True)
    peak, started, outcome = 0, time.perf_counter(), None
    while proc.poll() is None:
        try:
            peak = max(peak, proc.memory_info().rss)
        except psutil.NoSuchProcess:
            break
        if peak > 12 * 2**30:
            outcome = "KILLED at 12 GiB RSS"
            proc.kill()
        elif time.perf_counter() - started > 300:
            outcome = "KILLED after 300 s"
            proc.kill()
        time.sleep(0.1)
    out = proc.communicate()[0]
    result = [line for line in out.splitlines() if line.startswith("RESULT")]
    took = time.perf_counter() - started
    outcome = outcome or (result[0][7:] if result else f"exit {proc.returncode}")
    print(f"{case:40} {outcome}; after {took:.1f}s, peak RSS {peak / 2**30:.2f} GiB", flush=True)
    if case == "scan_iceberg":
        print("--- optimized plan (scan_iceberg) ---\n" + out.split("RESULT")[0], flush=True)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        child(sys.argv[1])
    else:
        import polars as pl

        print(f"polars {pl.__version__}, TPC-DS SF={SF}", flush=True)
        generate()
        for case in ("scan_parquet", "scan_iceberg", "scan_iceberg_no_metadata_statistics"):
            run(case)
