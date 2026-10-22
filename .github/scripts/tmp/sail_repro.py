"""Minimal repros for Sail issues, against an in-process Sail server. No OneLake, no secrets.

Each case prints OK or the exception, so one run shows which statements fail on this build.
TEMPORARY: deleted with tmp_sail_repro.yml once the issues are filed.
"""

import os
import sys
import tempfile

from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

server = SparkConnectServer()
server.start()
_, port = server.listening_address
spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()

work = tempfile.mkdtemp()


def case(name, fn):
    try:
        out = fn()
        print(f"[OK]   {name}: {out}")
    except Exception as exc:  # noqa: BLE001 - every case reports, none stops the run
        msg = str(exc).strip().splitlines()[0][:300]
        print(f"[FAIL] {name}: {type(exc).__name__}: {msg}")


def fresh(name, fmt):
    spark.sql(f"CREATE TABLE {name} (id INT, v INT) USING {fmt} LOCATION '{os.path.join(work, name)}'")
    spark.sql(f"INSERT INTO {name} VALUES (1, 10), (2, 20), (3, 30)")


# TRUNCATE TABLE: Spark SQL's statement; on Iceberg, Spark runs it as a metadata-only delete.
fresh("ice", "iceberg")
case("iceberg TRUNCATE TABLE", lambda: spark.sql("TRUNCATE TABLE ice").collect())
case("iceberg count after", lambda: spark.sql("SELECT count(*) FROM ice").collect())

fresh("ice_ctl", "iceberg")
case("iceberg DELETE FROM (control)", lambda: spark.sql("DELETE FROM ice_ctl").collect())
case("iceberg count after (control)", lambda: spark.sql("SELECT count(*) FROM ice_ctl").collect())

fresh("pq", "parquet")
case("parquet TRUNCATE TABLE", lambda: spark.sql("TRUNCATE TABLE pq").collect())

spark.stop()
server.stop()
sys.stdout.flush()
os._exit(0)
