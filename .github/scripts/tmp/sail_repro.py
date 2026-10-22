"""Minimal repros for Sail issues, against an in-process Sail server. No OneLake, no secrets.

Each case prints OK or the exception, so one run shows which statements fail on this build.
TEMPORARY: deleted with tmp_sail_repro.yml once the issues are filed.
"""

import os
import sys
import tempfile
import traceback

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


# 1. GROUP BY ALL (Spark 3.4+)
case(
    "group_by_all",
    lambda: spark.sql(
        "SELECT k, count(*) AS n FROM VALUES (1), (1), (2) AS t(k) GROUP BY ALL ORDER BY k"
    ).collect(),
)
case(
    "group_by_ordinal (control)",
    lambda: spark.sql(
        "SELECT k, count(*) AS n FROM VALUES (1), (1), (2) AS t(k) GROUP BY 1 ORDER BY k"
    ).collect(),
)

# 0. DESCRIBE without TABLE (lakehq/sail#2712), as gluten_create_table_as.sql runs it.
spark.sql("CREATE OR REPLACE TEMPORARY VIEW dv AS SELECT 1 AS a, 'x' AS b")
case("describe_table_keyword (control)", lambda: spark.sql("DESCRIBE TABLE dv").collect())
case("describe_no_keyword", lambda: spark.sql("describe dv").collect())

# 2. string_view: where a collected string column comes back as an Arrow view type.
pq = os.path.join(work, "pq")
spark.sql(
    "SELECT * FROM VALUES ('/a/b/x1.CSV'), ('/a/b/x2.CSV'), ('/a/b/x1.CSV') AS t(path)"
).write.parquet(pq)
spark.read.parquet(pq).createOrReplaceTempView("files")
expr = "substring_index(element_at(split(path, '/'), -1), '.', 1)"

case("parquet select", lambda: spark.sql("SELECT path FROM files").collect())
case("parquet distinct", lambda: spark.sql("SELECT DISTINCT path FROM files").collect())
case("parquet expr", lambda: spark.sql(f"SELECT {expr} AS file FROM files").collect())
case(
    "parquet distinct expr",
    lambda: spark.sql(f"SELECT DISTINCT {expr} AS file FROM files").collect(),
)
for label, e in [
    ("split", "split(path, '/')[0]"),
    ("element_at", "element_at(split(path, '/'), -1)"),
    ("substring_index", "substring_index(path, '.', 1)"),
    ("upper", "upper(path)"),
    ("concat", "concat(path, 'x')"),
]:
    case(
        f"parquet distinct {label}",
        lambda e=e: spark.sql(f"SELECT DISTINCT {e} AS s FROM files").collect(),
    )
    case(
        f"parquet group by {label}",
        lambda e=e: spark.sql(f"SELECT {e} AS s FROM files GROUP BY 1").collect(),
    )
case(
    "values distinct substring_index",
    lambda: spark.sql(
        "SELECT DISTINCT substring_index(p, '.', 1) AS s FROM VALUES ('x1.CSV'), ('x1.CSV') AS t(p)"
    ).collect(),
)
spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW v AS SELECT {expr} AS file FROM files")
case("temp view distinct", lambda: spark.sql("SELECT DISTINCT file FROM v").collect())
case(
    "temp view distinct schema",
    lambda: spark.sql("SELECT DISTINCT file FROM v").schema.simpleString(),
)

csv = os.path.join(work, "csv")
os.makedirs(csv)
with open(os.path.join(csv, "x1.CSV"), "w") as f:
    f.write("a\n1\n2\n")
spark.sql(
    "CREATE OR REPLACE TEMPORARY VIEW raw (a STRING) USING csv "
    f"OPTIONS (path '{csv}', header 'true')"
)
spark.sql(
    "CREATE OR REPLACE TEMPORARY VIEW w AS "
    "SELECT a, substring_index(element_at(split(input_file_name(), '/'), -1), '.', 1) AS file "
    "FROM raw"
)
case("csv input_file_name distinct", lambda: spark.sql("SELECT DISTINCT file FROM w").collect())

# The workload's shape: an Iceberg table holding the path, a temp view over it, DISTINCT.
ice = os.path.join(work, "ice")
try:
    spark.sql(f"CREATE TABLE ice (path STRING) USING iceberg LOCATION '{ice}'")
    spark.sql("INSERT INTO ice SELECT path FROM files")
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW iv AS SELECT {expr} AS file FROM ice")
    case("iceberg select", lambda: spark.sql("SELECT path FROM ice").collect())
    case("iceberg distinct", lambda: spark.sql("SELECT DISTINCT path FROM ice").collect())
    case("iceberg temp view distinct", lambda: spark.sql("SELECT DISTINCT file FROM iv").collect())
    case(
        "iceberg temp view distinct cast",
        lambda: spark.sql(
            "SELECT CAST(file AS STRING) FROM (SELECT DISTINCT file FROM iv)"
        ).collect(),
    )
except Exception:  # noqa: BLE001
    print("[SKIP] iceberg cases:")
    traceback.print_exc()

spark.stop()
server.stop()
sys.stdout.flush()
os._exit(0)
