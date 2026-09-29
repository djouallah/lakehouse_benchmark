"""TEMPORARY. Upstream repro: Databend misreads Iceberg decimals stored as FIXED_LEN_BYTE_ARRAY.

Local only: an Iceberg REST catalog (apache/iceberg-rest-fixture) on a file:// warehouse that the
catalog, pyiceberg and Databend all see at /tmp/wh. A decimal(15,2) table gets a parquet written
with pyarrow's defaults -- FIXED_LEN_BYTE_ARRAY(7), which the Iceberg spec allows -- via add_files.
A second table holds the same rows written by pyiceberg's append (INT64). Databend reads both
through the Iceberg catalog, and the FLBA file as plain parquet through a stage.
"""

from __future__ import annotations

import subprocess
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import DecimalType, LongType, NestedField

IMAGE = sys.argv[1] if len(sys.argv) > 1 else "datafuselabs/databend:v1.2.949-nightly"
WH = "/tmp/wh"


def physical(uri: str) -> str:
    col = pq.ParquetFile(uri.removeprefix("file://")).schema.column(1)
    return f"{col.physical_type} {col.logical_type}"


def iceberg() -> None:
    catalog = load_catalog("rest", uri="http://localhost:8181")
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
    # pyarrow's default layout for decimal(15,2): FIXED_LEN_BYTE_ARRAY(7).
    flba = catalog.create_table("demo.dec_flba", schema=schema)
    path = f"{WH}/dec_flba.parquet"
    pq.write_table(rows, path)
    flba.add_files([f"file://{path}"])

    # pyiceberg's own write: INT64.
    int64 = catalog.create_table("demo.dec_int64", schema=schema)
    int64.append(rows)

    for table in (flba, int64):
        for task in table.scan().plan_files():
            print(f"{table.name()}: {task.file.file_path}  d is {physical(task.file.file_path)}")
        print(f"  pyiceberg reads: {table.scan().to_arrow().to_pylist()}")


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
        ["docker", "run", "-d", "--name", "db", "--network", "host", "-v", f"{WH}:{WH}", IMAGE],
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
    query(
        conn,
        "CREATE CATALOG ice TYPE=ICEBERG CONNECTION=(TYPE='rest' "
        f"ADDRESS='http://localhost:8181' WAREHOUSE='file://{WH}' \"root\"='/')",
    )
    for table in ("dec_int64", "dec_flba"):
        query(conn, f"SELECT k FROM ice.demo.{table} ORDER BY k")
        query(conn, f"SELECT k, d FROM ice.demo.{table} ORDER BY k")
    # The same FLBA data file, read as plain parquet through a stage.
    query(conn, f"CREATE STAGE s URL='fs://{WH}/'")
    query(conn, "SELECT k, d FROM @s/dec_flba.parquet (FILE_FORMAT => 'parquet') ORDER BY k")
    return 0


if __name__ == "__main__":
    sys.exit(main())
