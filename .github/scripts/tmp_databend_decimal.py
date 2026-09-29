"""TEMPORARY. Upstream repro: Databend misreads Iceberg decimals stored as FIXED_LEN_BYTE_ARRAY.

Local only: Databend's own docker/it-iceberg-catalogs compose (Iceberg REST + MinIO) is already up.
pyiceberg writes a decimal(15,2) table -- pyarrow stores it as FIXED_LEN_BYTE_ARRAY(7), which the
Iceberg spec allows. A second table holds the same rows in a parquet written with INT64 decimals
(add_files). Databend reads both through the Iceberg catalog, and the FLBA file through a stage.
"""

from __future__ import annotations

import subprocess
import sys
import time

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import DecimalType, LongType, NestedField

IMAGE = sys.argv[1] if len(sys.argv) > 1 else "datafuselabs/databend:v1.2.949-nightly"
S3 = {
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "admin",
    "s3.secret-access-key": "password",
    "s3.region": "us-east-1",
}


def physical(path: str) -> str:
    fs = pafs.S3FileSystem(
        endpoint_override="localhost:9000",
        scheme="http",
        access_key="admin",
        secret_key="password",
        region="us-east-1",
    )
    col = pq.ParquetFile(path.removeprefix("s3://"), filesystem=fs).schema.column(1)
    return f"{col.physical_type} {col.logical_type}"


def iceberg() -> None:
    catalog = load_catalog("rest", uri="http://localhost:8181", **S3)
    catalog.create_namespace_if_not_exists("demo")
    schema = Schema(
        NestedField(1, "k", LongType(), required=False),
        NestedField(2, "d", DecimalType(15, 2), required=False),
    )
    rows = pa.table(
        {
            "k": pa.array([1, 2, 3], pa.int64()),
            "d": pa.array([123.45, -7.01, 99999.99]).cast(pa.decimal128(15, 2)),
        }
    )
    for name in ("dec_flba", "dec_int64"):
        if catalog.table_exists(f"demo.{name}"):
            catalog.drop_table(f"demo.{name}")
    flba = catalog.create_table("demo.dec_flba", schema=schema)
    flba.append(rows)

    int64 = catalog.create_table("demo.dec_int64", schema=schema)
    path = "s3://warehouse/demo/external/dec_int64.parquet"
    fs = pafs.S3FileSystem(
        endpoint_override="localhost:9000",
        scheme="http",
        access_key="admin",
        secret_key="password",
        region="us-east-1",
    )
    pq.write_table(rows, path.removeprefix("s3://"), filesystem=fs, store_decimal_as_integer=True)
    int64.add_files([path])

    for table in (flba, int64):
        for task in table.scan().plan_files():
            print(f"{table.name()}: {task.file.file_path}  d is {physical(task.file.file_path)}")


def query(conn, statement: str) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(statement)
            print(f"  OK    {statement}\n        {cur.fetchall()}")
    except Exception as exc:  # noqa: BLE001 - reporting is the point
        print(f"  ERROR {statement}\n        {exc}")


def main() -> int:
    import pymysql

    iceberg()
    subprocess.run(
        ["docker", "run", "-d", "--name", "db", "--network", "host", IMAGE],
        check=True,
    )
    deadline = time.time() + 300
    while True:
        try:
            conn = pymysql.connect(host="127.0.0.1", port=3307, user="root", autocommit=True)
            break
        except Exception:  # noqa: BLE001 - not up yet
            if time.time() > deadline:
                raise
            time.sleep(3)

    query(conn, "SELECT version()")
    props = " ".join(f"\"{k}\"='{v}'" for k, v in (S3 | {"s3.path-style-access": "true"}).items())
    query(
        conn,
        "CREATE CATALOG ice TYPE=ICEBERG CONNECTION=(TYPE='rest' "
        f"ADDRESS='http://localhost:8181' WAREHOUSE='s3://warehouse/demo' {props})",
    )
    for table in ("dec_int64", "dec_flba"):
        query(conn, f"SELECT k FROM ice.demo.{table} ORDER BY k")
        query(conn, f"SELECT k, d FROM ice.demo.{table} ORDER BY k")
    # The same FLBA data file, read as plain parquet through a stage.
    query(
        conn,
        "CREATE STAGE s URL='s3://warehouse/demo/' "
        "CONNECTION=(ENDPOINT_URL='http://localhost:9000' "
        "ACCESS_KEY_ID='admin' SECRET_ACCESS_KEY='password' REGION='us-east-1')",
    )
    query(conn, "LIST @s PATTERN = '.*dec_flba/data/.*[.]parquet'")
    query(
        conn,
        "SELECT k, d FROM @s (PATTERN => '.*dec_flba/data/.*[.]parquet', FILE_FORMAT => 'parquet') "
        "ORDER BY k",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
