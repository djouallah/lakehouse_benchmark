"""Apache Spark with Delta Lake, for the Delta support probes.

NOT THE ICEBERG SESSION. Delta ships one `delta-spark` build per Spark minor, and the newest is
for Spark 4.2 (io.delta:delta-spark_4.2_2.13:4.4.1, built against Spark 4.2.0); there is none for
4.3. The Iceberg grid stays on Spark 4.1 because Iceberg 1.12 stops there, so this leg runs its own
pin: pyspark 4.2.x (requirements/capability_delta_spark.txt) and hadoop-azure 3.5.0, Spark 4.2's
own hadoop.version, which a mismatched hadoop-azure splits the classpath against.

Storage is the same as the benchmark's Spark: hadoop-azure's WorkloadIdentityTokenProvider reading
the GitHub OIDC assertion from a file rewritten every 4 minutes (bench/tpch/engines/pyspark_iceberg
says why). Tables live in the session catalog, which is in memory, each one created with a
LOCATION under Tables/_bench_delta.
"""

from __future__ import annotations

import os
import threading
from pathlib import Path

from bench import auth, scrub
from bench.config import ONELAKE_DFS, Config

DELTA = "io.delta:delta-spark_4.2_2.13:4.4.1"
PACKAGES = f"{DELTA},org.apache.hadoop:hadoop-azure:3.5.0"
ASSERTION_REFRESH_S = 240


class PysparkDelta:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._spark = None
        self._stop = threading.Event()
        self._assertion = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "onelake-oidc-assertion"

    @property
    def session(self):
        return self._spark

    @property
    def version(self) -> str:
        import pyspark

        return f"{pyspark.__version__} + {DELTA.split(':', 1)[1]}"

    def _write_assertion(self) -> None:
        self._assertion.write_text(auth._github_oidc_assertion(), encoding="utf-8")

    def _keep_fresh(self) -> None:
        while not self._stop.wait(ASSERTION_REFRESH_S):
            try:
                self._write_assertion()
            except Exception as exc:  # noqa: BLE001 - a failed refresh must not kill the run
                scrub.safe_print(f"  warning: assertion refresh failed: {exc}")

    def setup(self) -> None:
        from pyspark.sql import SparkSession

        if os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL"):
            self._write_assertion()
            threading.Thread(target=self._keep_fresh, daemon=True).start()

        abfs = {
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": (
                "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider"
            ),
            "fs.azure.account.oauth2.msi.tenant": os.environ.get("AZURE_TENANT_ID", ""),
            "fs.azure.account.oauth2.client.id": os.environ.get("AZURE_CLIENT_ID", ""),
            "fs.azure.account.oauth2.token.file": str(self._assertion),
        }
        builder = (
            SparkSession.builder.master("local[4]")
            .appName("bench-delta-capability")
            .config("spark.jars.packages", PACKAGES)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            # VACUUM ... RETAIN 0 HOURS is what the vacuum probe asks; Delta refuses it otherwise.
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.shuffle.partitions", "8")
        )
        for key, value in abfs.items():
            builder = builder.config(f"spark.hadoop.{key}.{ONELAKE_DFS}", value)
            builder = builder.config(f"spark.hadoop.{key}", value)
        self._spark = builder.getOrCreate()
        hadoop = self._spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
        scrub.safe_print(f"  pyspark {self.version} on hadoop {hadoop}")

    def close(self) -> None:
        self._stop.set()
        if self._spark is not None:
            try:
                self._spark.stop()
            except Exception as exc:  # noqa: BLE001 - teardown is best effort
                scrub.safe_print(f"  warning: spark.stop() failed: {exc}")
            self._spark = None
