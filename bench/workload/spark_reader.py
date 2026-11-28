"""Stock Spark reading every workload leg's tables, Iceberg and Delta, for the daily dbt tests.

Spark is the reference reader: one session, the same SQL (the spark tree's tests), whatever engine
wrote the table. Since 2026-11-28 it replaces DuckDB as table maintenance's tester. DuckDB 2.0.1
refuses every Sail table (OneLake writes `last-partition-id: -1` for Sail's createTable, which sends
no partition-spec: lakehq/sail#3124), and it reads only through the Iceberg REST catalog, so the
Delta legs were never tested.

SPARK 4.1, THE BENCHMARK'S OWN PIN. Iceberg 1.12.0 published no Spark 4.2 runtime to Maven
(iceberg-spark-runtime-4.2_2.13:1.12.0 is a 404), and Delta 4.4.1 ships for 4.1 as well as 4.2
(delta-spark_4.1_2.13). So bench's stock Spark session (bench/tpch/engines/pyspark_iceberg.py:
the OIDC storage credential, the `onelake` REST catalog) plus Delta's jar, session extension and
`spark_catalog`, as bench/capability/engines/pyspark_delta.py sets them.

Two catalogs, one per format, switched per leg by `use_leg`:
  * an Iceberg leg reads `onelake`, the REST catalog its tables are in;
  * a Delta leg reads `spark_catalog`, Delta's catalog over the in-memory session catalog, where
    each of the leg's tables is registered by its OneLake path (CREATE TABLE ... LOCATION, which
    writes nothing: the table's log is already there).
dbt-spark renders a ref as `schema.table`, which the current catalog resolves.
"""

from __future__ import annotations

from bench import scrub
from bench.tpch.engines.pyspark_iceberg import CATALOG, PysparkIceberg

ICEBERG = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0"
DELTA = "io.delta:delta-spark_4.1_2.13:4.4.1"
# hadoop-azure at Spark 4.1's own hadoop.version (pyspark_iceberg.py says why).
PACKAGES = f"{ICEBERG},{DELTA},org.apache.hadoop:hadoop-azure:3.4.2"
DELTA_CATALOG = "spark_catalog"
LAYERS = ("landing", "mart")


class SparkReader(PysparkIceberg):
    name = "spark"

    def _extra_config(self) -> dict[str, str]:
        return {
            "spark.jars.packages": PACKAGES,
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "io.delta.sql.DeltaSparkSessionExtension"
            ),
            f"spark.sql.catalog.{DELTA_CATALOG}": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.session.timeZone": "UTC",
            # dbt-spark's macros are written for stock Spark, where "x" is a string; bench's
            # session reads it as an identifier, for TPC-DS (workload_run_engine.start_gluten).
            "spark.sql.ansi.doubleQuotedIdentifiers": "false",
        }

    @property
    def version(self) -> str:
        return f"{super().version} + {DELTA.split(':', 1)[1]}"

    def use_leg(self, leg: str, delta: bool, tables_abfss: str) -> list[str]:
        """Point the session at one leg's tables. For a Delta leg, register every Delta table
        under Tables/dbt_<leg>_<layer>/; the names registered, as schema.table."""
        spark = self._spark
        if not delta:
            spark.catalog.setCurrentCatalog(CATALOG)
            return []
        spark.catalog.setCurrentCatalog(DELTA_CATALOG)
        jvm = spark.sparkContext._jvm
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        registered = []
        for layer in LAYERS:
            schema = f"dbt_{leg}_{layer}"
            root = jvm.org.apache.hadoop.fs.Path(f"{tables_abfss}/{schema}")
            fs = root.getFileSystem(conf)
            if not fs.exists(root):
                continue
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
            for status in fs.listStatus(root):
                path = status.getPath()
                log = jvm.org.apache.hadoop.fs.Path(path, "_delta_log")
                if not (status.isDirectory() and fs.exists(log)):
                    continue
                name = path.getName()
                spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {schema}.{name} USING delta "
                    f"LOCATION '{path.toString()}'"
                )
                registered.append(f"{schema}.{name}")
        scrub.safe_print(f"  {leg}: Delta tables registered: {', '.join(registered) or 'none'}")
        return registered
