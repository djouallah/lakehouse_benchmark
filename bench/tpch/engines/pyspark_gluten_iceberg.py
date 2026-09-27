"""Spark exactly as pyspark_iceberg, with Gluten + Velox executing the plan natively.

NO RELEASED PACKAGE, BUT A NIGHTLY ONE. Gluten's releases stop at Spark 3.5; Apache's nightly
builds carry a Velox bundle for Spark 4.1 (since 1.7.0-SNAPSHOT, 2026-06). Compiling Velox in
CI was the alternative -- hours cold, tens of GB of disk -- and is deliberately not done: if the
nightly cannot read OneLake, Gluten is out.

SPARK 4.1.1, NOT 4.1.3. The bundle warns "Spark runtime version 4.1.3 is not matched with
Gluten's fully tested version 4.1.1", so this engine pins its own pyspark to what Gluten tests
(requirements/pyspark_gluten_iceberg.txt). Iceberg's runtime and hadoop-azure are per-minor, so
pyspark_iceberg's PACKAGES serve 4.1.1 unchanged.

THE CREDENTIAL IS A OneLake SAS, not the OIDC token file every other Spark engine uses. Velox
reads data files itself, through its own ABFS connector, and that connector knows three auth
types: SharedKey, OAuth WITH A CLIENT SECRET, and SAS (velox/.../abfs/AzureClientProviderImpl.cpp).
This app registration has no secret and OneLake has no account key, so SAS it is: a user
delegation SAS minted at setup from the same OIDC credential, read+list, scoped to the lakehouse.
Velox's key, `fs.azure.sas.fixed.token.<account>`, is also hadoop-azure's, so the JVM side
(Iceberg metadata, any fallback scan) reads with the same token.

THE SAS LIVES AN HOUR, SO A LONG RUN RESTARTS THE JVM. OneLake caps a user delegation key at one
hour, and TPC-DS at SF=100 runs Gluten longer: run 35998250865 answered Q1-Q74 and then failed
every statement from Q75 on, 54 minutes in, `401 Unauthorized`. Swapping the key in a live session
does not work -- Gluten builds Velox's ABFS config ONCE, at backend init
(`hiveConnectorConfig_ = createHiveConnectorConfig(backendConf_)`, cpp/velox/compute/
VeloxBackend.cc), and Velox caches the filesystem per account after that; hadoop-azure's cached
FileSystem holds its copy too. A new SAS reaches Velox only in a NEW JVM -- which is exactly what
the base engine's `refresh()` already does for the catalog bearer, so all this engine adds is the
SAS's expiry to the clock it watches (`_storage_conf`). At SF<=30 it never fires; at SF=100 about
every forty minutes. The Velox cache starts empty again after a restart, so the next statements
read cold -- a real cost, deliberately left in the numbers.

ACCOUNT-SCOPED KEYS ONLY. Velox registers a provider for every key starting with
`fs.azure.account.auth.type` by cutting the account off the end, so the base engine's unscoped
copy crashed the native backend at startup: `substr: __pos (which is 27) > __size (which is 26)`.

THE SNAPSHOT IS OVERWRITTEN NIGHTLY under the same name, so the jar's sha256 is printed at
setup: that, not the file name, is what identifies the build a run measured.

MEMORY. Velox allocates OFF-HEAP, so the 11g heap every other Spark run gets would leave it
nothing on a 16GB runner. The heap shrinks to 3g and Velox gets 9g; the total is the same. The
vendors lean further (Google's Dataproc guidance 6:1 off-heap to heap, Gluten's benchmark 7:1),
but in local mode the one JVM also plans every query and holds Iceberg's metadata.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import shutil
import subprocess
import urllib.request
from pathlib import Path

from bench import auth, scrub
from bench.tpch.engines.pyspark_iceberg import PACKAGES, PysparkIceberg

# The JDK 17 directory, because every Spark job sets up Temurin 17. amd64: the hosted runners.
GLUTEN_JAR_URL = (
    "https://nightlies.apache.org/gluten/nightly-release-jdk17/"
    "gluten-velox-bundle-spark4.1_2.13-linux_amd64-1.8.0-SNAPSHOT.jar"
)

HEAP = "3g"
OFF_HEAP = "9g"


def fetch_gluten_jar() -> Path:
    """Download the bundle once per job and print which build it is."""
    target = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / Path(GLUTEN_JAR_URL).name
    if not target.exists():
        urllib.request.urlretrieve(GLUTEN_JAR_URL, target)
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    scrub.safe_print(f"  gluten bundle {target.name} sha256 {digest[:16]}")
    return target


def gluten_conf() -> dict[str, str]:
    """The Gluten session keys. Shared with smoke_sql.py, so phase 1 runs the same plugin.

    Also shrinks the driver heap, which only works because it runs before the JVM starts:
    spark-submit reads SPARK_DRIVER_MEMORY when pyspark launches the gateway.
    """
    os.environ["SPARK_DRIVER_MEMORY"] = HEAP
    return {
        # THE APP CLASSPATH, NOT spark.jars. ColumnarShuffleManager lives in Spark's own
        # org.apache.spark.shuffle.sort package and calls package-private classes there; loaded
        # through spark.jars' child classloader it is a different runtime package, and every
        # shuffle died with IllegalAccessError on BypassMergeSortShuffleWriter.
        "spark.driver.extraClassPath": str(fetch_gluten_jar()),
        "spark.plugins": "org.apache.gluten.GlutenPlugin",
        # ANSI STAYS ON, as in stock Spark. Gluten's default answer to it is to fall back
        # WHOLESALE -- every node tagged "does not support ansi mode", so Velox ran nothing -- and
        # this asks Velox to execute ANSI instead. Not ANSI off: doubleQuotedIdentifiers only
        # works under ANSI, and eight TPC-DS statements alias `AS "order count"`.
        "spark.gluten.sql.ansiFallback.enabled": "false",
        "spark.memory.offHeap.enabled": "true",
        "spark.memory.offHeap.size": OFF_HEAP,
        "spark.shuffle.manager": "org.apache.spark.shuffle.sort.ColumnarShuffleManager",
        # GLUTEN'S OWN TPC-DS BENCHMARK CONFIG (tools/workload in apache/gluten, and Intel's
        # tuning guide), not guesses. Shuffled hash joins stay forced (the default: native
        # sort-merge "still has some performance issues"); instead a long chain of joins falls
        # back to row operators -- the documented Q72 answer (apache/gluten#8417).
        "spark.gluten.sql.columnar.physicalJoinOptimizeEnable": "true",
        "spark.gluten.sql.columnar.physicalJoinOptimizationLevel": "18",
        # Runtime bloom filters on every probe-side scan, however small.
        "spark.sql.optimizer.runtime.bloomFilter.enabled": "true",
        "spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold": "0",
        # Velox otherwise reserves 30% more memory than it asks for, as headroom.
        "spark.gluten.memory.overAcquiredMemoryRatio": "0",
        # Gluten's Arrow/netty buffers need reflective access on JDK 17.
        "spark.driver.extraJavaOptions": "-Dio.netty.tryReflectionSetAccessible=true",
    }


# OneLake's ceiling for a user delegation key, and so for the SAS signed with it.
SAS_LIFETIME = dt.timedelta(hours=1)


def onelake_sas(workspace_id: str, lakehouse_id: str, write: bool = False) -> tuple[str, float]:
    """A read+list user delegation SAS on the lakehouse directory, signed via Entra.

    `write` adds create, write and delete, for the ETL: there Spark writes the data files and
    manifests over hadoop-azure with this same token, and DROP ... PURGE deletes them.

    Returned with its expiry as epoch seconds -- about 55 minutes out, since the start is
    backdated five for clock skew and the whole window is capped at SAS_LIFETIME.
    """
    from azure.storage.filedatalake import (
        DataLakeServiceClient,
        DirectorySasPermissions,
        generate_directory_sas,
    )

    from bench.config import ONELAKE_DFS

    start = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=5)
    expiry = start + SAS_LIFETIME
    service = DataLakeServiceClient(f"https://{ONELAKE_DFS}", credential=auth.credential())
    key = service.get_user_delegation_key(start, expiry)
    sas = generate_directory_sas(
        account_name="onelake",
        file_system_name=workspace_id,
        directory_name=lakehouse_id,
        credential=key,
        permission=DirectorySasPermissions(
            read=True, list=True, create=write, write=write, delete=write
        ),
        expiry=expiry,
        start=start,
    )
    scrub.register(sas)
    return sas, expiry.timestamp()


def fetch_packages() -> Path:
    """pyspark_iceberg's PACKAGES, resolved onto disk BEFORE the JVM starts.

    ONE CLASSLOADER FOR GLUTEN AND ICEBERG. Gluten has to sit on the app classpath (see
    extraClassPath above), and spark.jars.packages lands Iceberg in a CHILD loader the app loader
    cannot see -- so Gluten's Iceberg offload died with NoClassDefFoundError on
    SparkBatchQueryScan for every query. extraClassPath is read at JVM launch, so the jars must
    already exist: the Ivy jar pyspark ships resolves them here, same coordinates, same
    transitive set. The spark.jars.packages copy stays and is harmless -- Spark's loader asks its
    parent first.
    """
    import pyspark

    target = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "gluten-classpath"
    target.mkdir(parents=True, exist_ok=True)
    ivy = next((Path(pyspark.__file__).parent / "jars").glob("ivy-*.jar"))
    for coordinate in PACKAGES.split(","):
        group, artifact, version = coordinate.split(":")
        subprocess.run(
            [
                "java",
                "-jar",
                str(ivy),
                "-dependency",
                group,
                artifact,
                version,
                "-confs",
                "default",
                "-retrieve",
                f"{target}/[artifact]-[revision](-[classifier]).[ext]",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
    jars = sorted(target.glob("*.jar"))
    scrub.safe_print(f"  {len(jars)} Iceberg/ABFS jars on the app classpath beside Gluten")
    return target


# Where the bundle's libcurl looks for CA certificates -- it was built on CentOS -- and where
# Ubuntu actually keeps them.
CENTOS_CA_PATHS = (
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
)
UBUNTU_CA_BUNDLE = "/etc/ssl/certs/ca-certificates.crt"


def link_ca_bundle() -> None:
    """Give Velox's libcurl its CA bundle at the CentOS path, on a CI runner only.

    Without it every native read died on the first TLS handshake to OneLake: "Fail to get a new
    connection for: https://onelake.blob.fabric.microsoft.com. Problem with the SSL CA cert
    (path? access rights?)". The path is compiled into libcurl, and libcurl reads no env var.
    """
    if not os.environ.get("GITHUB_ACTIONS"):
        return
    for path in CENTOS_CA_PATHS:
        if not Path(path).exists():
            subprocess.run(["sudo", "mkdir", "-p", str(Path(path).parent)], check=True)
            subprocess.run(["sudo", "ln", "-sf", UBUNTU_CA_BUNDLE, path], check=True)


# Velox's own file cache, the counterpart of DuckDB's and StarRocks' Data Cache: an SSD tier sized
# from the FREE DISK at setup, in front of a 1GB memory tier, Gluten's default, since heap and
# off-heap already hold 12 of the runner's 16GB. It caches only what Velox reads itself; a scan
# Gluten hands back to the JVM still goes through hadoop-azure.
#
# WAS A FIXED 8GB, sized to the SF=10 working set and never revisited: at SF=200 that is a sixth of
# lineitem, while StarRocks' Data Cache held 33.6GB of the same data (run 36318638699). Half the
# free disk, not all of it: shuffle and Velox spill write to the same disk, and the cache does not
# give space back to them.
SSD_CACHE_FREE_FRACTION = 0.5
SSD_CACHE_MIN_GIB = 8
MEM_CACHE_SIZE = "1GB"


def cache_conf() -> dict[str, str]:
    cache_dir = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "velox-cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    free_gib = shutil.disk_usage(cache_dir).free / 2**30
    ssd_gib = max(SSD_CACHE_MIN_GIB, int(free_gib * SSD_CACHE_FREE_FRACTION))
    scrub.safe_print(
        f"  velox cache {ssd_gib}GB ssd ({free_gib:.0f}GB free) at {cache_dir} "
        f"+ {MEM_CACHE_SIZE} memory"
    )
    prefix = "spark.gluten.sql.columnar.backend.velox"
    return {
        f"{prefix}.cacheEnabled": "true",
        f"{prefix}.memCacheSize": MEM_CACHE_SIZE,
        f"{prefix}.ssdCachePath": str(cache_dir),
        f"{prefix}.ssdCacheSize": f"{ssd_gib}GB",
        f"{prefix}.ssdCacheShards": "4",
        # The SSD tier refuses to start above an 8MB read unit ("Velox currently only support up
        # to 8MB load quantum size on SSD cache"); Gluten's default is 256MB.
        f"{prefix}.loadQuantum": "8MB",
    }


class PysparkGlutenIceberg(PysparkIceberg):
    name = "pyspark_gluten_iceberg"

    # The TPC-H and TPC-DS runs only read; the ETL's subclass writes.
    sas_write = False

    def _extra_config(self) -> dict[str, str]:
        link_ca_bundle()
        conf = gluten_conf() | cache_conf()
        conf["spark.driver.extraClassPath"] += os.pathsep + f"{fetch_packages()}/*"
        return conf

    def _storage_conf(self, abfs: dict[str, str], account: str) -> dict[str, str]:
        sas, sas_expiry = onelake_sas(
            self.cfg.workspace_id, self.cfg.lakehouse_id, write=self.sas_write
        )
        # The base setup has already set _expires from the catalog bearer; the session is good
        # until the first of the two runs out, and the base refresh() restarts it then.
        self._expires = min(self._expires, sas_expiry)
        scrub.safe_print(f"  onelake SAS for Velox and hadoop-azure, valid {SAS_LIFETIME}")
        return {
            f"spark.hadoop.fs.azure.account.auth.type.{account}": "SAS",
            f"spark.hadoop.fs.azure.sas.fixed.token.{account}": sas,
        }
