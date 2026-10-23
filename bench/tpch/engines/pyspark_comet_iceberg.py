"""Spark exactly as pyspark_iceberg, with DataFusion Comet executing what it can natively. A TRIAL.

WHY NOW. Comet was rejected on `abfss://` (LEARNING.md, "DataFusion Comet"). Main has since given
its native PARQUET scan ABFS through object_store, authenticated by asking hadoop-azure, with
WorkloadIdentityTokenProvider supported (apache/datafusion-comet#6059, 2026-10-11), and one object
store per scan partition (#6753). Its native ICEBERG reader still lists no Azure scheme
(`builtin_storage_schemes` in native/core/src/execution/operators/iceberg_common.rs), so the
Iceberg scans stay on the JVM; joins, aggregations and shuffle can still run in Comet. This run
measures what that is worth against Spark-OSS. No dashboard row until the numbers say so.

THE SNAPSHOT, NOT A RELEASE. 1.1.0 (2026-10-06) predates both PRs. Apache's snapshot repository
publishes main nightly as comet-spark-spark4.1_2.13:1.2.0-SNAPSHOT under a timestamped file name,
which is printed at setup with its sha256: that is what identifies the build a run measured.

THE APP CLASSPATH, as Gluten (pyspark_gluten_iceberg.py): CometShuffleManager is a shuffle
manager, loaded by Spark's own loader, and Comet's Iceberg integration reflects into Iceberg's
classes, which must then sit in the same loader -- so the Iceberg/ABFS jars go there too.

MEMORY. Comet allocates off-heap, but here the JVM still scans every Iceberg file, so the heap
keeps more than Gluten's 3g: 6g heap, 6g off-heap, about the 11g+ the Spark leg uses.

THE CREDENTIAL is pyspark_iceberg's own (the OIDC token file through
WorkloadIdentityTokenProvider), unchanged: the JVM scans use it, and Comet's native Parquet scan
can resolve the same keys through hadoop-azure.
"""

from __future__ import annotations

import hashlib
import os
import re
import urllib.request
from pathlib import Path

from bench import scrub
from bench.tpch.engines.pyspark_gluten_iceberg import fetch_packages
from bench.tpch.engines.pyspark_iceberg import PysparkIceberg

SNAPSHOTS = (
    "https://repository.apache.org/content/groups/snapshots/"
    "org/apache/datafusion/comet-spark-spark4.1_2.13/1.2.0-SNAPSHOT/"
)

HEAP = "6g"
OFF_HEAP = "6g"


def fetch_comet_jar() -> Path:
    """The newest snapshot jar, once per job, and which build it is."""
    with urllib.request.urlopen(SNAPSHOTS + "maven-metadata.xml") as response:
        metadata = response.read().decode()
    # The plain jar: extension jar, no classifier (sources and javadoc carry one).
    match = re.search(
        r"<snapshotVersion>\s*<extension>jar</extension>\s*<value>([^<]+)</value>", metadata
    )
    if match is None:
        raise RuntimeError(f"no jar in {SNAPSHOTS}maven-metadata.xml")
    name = f"comet-spark-spark4.1_2.13-{match.group(1)}.jar"
    target = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / name
    if not target.exists():
        urllib.request.urlretrieve(SNAPSHOTS + name, target)
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    scrub.safe_print(f"  comet {name} sha256 {digest[:16]}")
    return target


def comet_conf() -> dict[str, str]:
    """The Comet session keys. Runs before the JVM starts, so the heap size takes effect."""
    os.environ["SPARK_DRIVER_MEMORY"] = HEAP
    return {
        "spark.driver.extraClassPath": str(fetch_comet_jar())
        + os.pathsep
        + f"{fetch_packages()}/*",
        "spark.plugins": "org.apache.spark.CometPlugin",
        "spark.shuffle.manager": "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager",
        "spark.memory.offHeap.enabled": "true",
        "spark.memory.offHeap.size": OFF_HEAP,
        # Every operator Comet leaves to Spark is logged with its reason: the trial's other answer.
        "spark.comet.explain.fallback.enabled": "true",
    }


class PysparkCometIceberg(PysparkIceberg):
    name = "pyspark_comet_iceberg"

    def _extra_config(self) -> dict[str, str]:
        return comet_conf()
