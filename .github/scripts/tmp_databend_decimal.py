"""TEMPORARY. Why does Databend fail every TPC-H query that reads a DECIMAL column?

"Parquet argument error: EOF: Not enough bytes to decode" on 17/22 queries (run 36520116389), and
the 5 that pass read no decimals. The TPC-H parquet is rewritten by pyarrow (bench/tpch/generate.py
add_field_ids), which stores decimal(15,2) as FIXED_LEN_BYTE_ARRAY. This reads real OneLake files
and synthetic pyarrow decimals from local disk in the stock image, outside Iceberg, to split
"decimal encoding" from "Iceberg reader".
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from bench import onelake
from bench.suite import suite_class

IMAGE = sys.argv[1] if len(sys.argv) > 1 else "datafuselabs/databend:v1.2.949-nightly"
DATA = Path("/tmp/dec")


def download(cfg, table: str) -> Path:
    fs = onelake.file_system(cfg)
    prefix = f"{cfg.lakehouse_id}/Tables/{cfg.schema}/{table}/data"
    name = next(p.name for p in fs.get_paths(prefix, recursive=True) if p.name.endswith(".parquet"))
    out = DATA / f"{table}.parquet"
    out.write_bytes(fs.get_file_client(name).download_file().readall())
    meta = pq.ParquetFile(out).metadata
    print(f"{table}: {meta.num_rows} rows, {meta.num_row_groups} row groups, by {meta.created_by}")
    schema = pq.ParquetFile(out).schema
    for i in range(len(schema)):
        col = schema.column(i)
        print(f"  {col.name}: {col.physical_type} {col.logical_type} len={col.length}")
    return out


def synthetic() -> None:
    values = pa.array([123.45, -7.01, 99999999.99, None] * 1000, pa.float64())
    table = pa.table({"k": pa.array(range(4000)), "d": values.cast(pa.decimal128(15, 2))})
    pq.write_table(table, DATA / "dec_flba.parquet")
    pq.write_table(table, DATA / "dec_int.parquet", store_decimal_as_integer=True)
    for name in ("dec_flba", "dec_int"):
        col = pq.ParquetFile(DATA / f"{name}.parquet").schema.column(1)
        print(f"{name}: d {col.physical_type} {col.logical_type} len={col.length}")


def sql(conn, statement: str) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(statement)
            print(f"  PASS  {statement}\n        {cur.fetchall()[:3]}")
    except Exception as exc:  # noqa: BLE001 - reporting is the point
        print(f"  FAIL  {statement}\n        {str(exc)[:600]}")


def main() -> int:
    import pymysql

    DATA.mkdir(parents=True, exist_ok=True)
    cfg = suite_class("tpch").from_env()
    download(cfg, "supplier")
    download(cfg, "customer")
    synthetic()

    subprocess.run(
        ["docker", "run", "-d", "--name", "db", "-v", f"{DATA}:/data:ro"]
        + ["-e", "QUERY_DEFAULT_USER=databend", "-e", "QUERY_DEFAULT_PASSWORD=databend"]
        + ["-p", "127.0.0.1:3307:3307", IMAGE],
        check=True,
    )
    deadline = time.time() + 300
    while True:
        try:
            conn = pymysql.connect(
                host="127.0.0.1", port=3307, user="databend", password="databend", autocommit=True
            )
            break
        except Exception:  # noqa: BLE001 - not up yet
            if time.time() > deadline:
                raise
            time.sleep(5)

    sql(conn, "SELECT version()")
    sql(conn, "CREATE STAGE IF NOT EXISTS d URL = 'fs:///data/'")
    for src in ("'fs:///data/{f}'", "@d/{f}"):
        for f, cols in (
            (
                "supplier.parquet",
                ["count(*)", "sum(s_suppkey)", "sum(s_acctbal)", "min(s_acctbal)"],
            ),
            ("customer.parquet", ["sum(c_custkey)", "sum(c_acctbal)"]),
            ("dec_flba.parquet", ["sum(k)", "sum(d)"]),
            ("dec_int.parquet", ["sum(k)", "sum(d)"]),
        ):
            where = src.format(f=f)
            for col in cols:
                sql(conn, f"SELECT {col} FROM {where}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
