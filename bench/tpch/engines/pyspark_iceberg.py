"""Apache Spark against the OneLake Iceberg REST catalog.

THE REFERENCE IMPLEMENTATION, and the point of comparison for LakeSail -- which is Spark without
the JVM, and which this benchmark had no way to judge until now.

SINGLE-NODE SPARK, `local[4]`. Not a cluster number, and nobody should read it as one. The
alternative shape, `local-cluster[1,3,M]`, is documented by Spark itself as "only for unit tests":
it forks a second JVM, gives Spark 3 of 4 cores where every other engine here gets 4, and splits
16GB into two heaps. That measures a memory split, not an engine.

THE STORAGE CREDENTIAL, which is the whole reason this engine took research rather than an hour.

Spark needs TWO credentials: one for the REST catalog (a bearer token, easy) and one for the
abfss:// reads, which go through hadoop-azure. ABFS's built-in providers want a CLIENT SECRET,
and this app registration deliberately has none -- it is OIDC-federated.

`WorkloadIdentityTokenProvider` (HADOOP-18610, Hadoop 3.4.1+) is the way out, and it fits this
repo exactly. It reads a JWT CLIENT ASSERTION FROM A FILE and posts it to Entra itself:

    grant_type=client_credentials, client_id=..., client_assertion=<file contents>,
    client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer,
    scope=https://storage.azure.com/.default

That is byte-for-byte the exchange `ClientAssertionCredential` already performs in bench/auth.py,
against the same app registration and the same api://AzureADTokenExchange federated credential.
A GitHub OIDC assertion IS that JWT. So: write it to a file, set five config keys, done. No Java,
no client secret, and no credential vending -- which costs ~7s per table cold (measured on DuckDB)
and would have made Spark the only engine paying it.

The default token-file path is AKS's projected-volume location, but the path is just a constructor
argument and the provider does a plain file read, so any file works.

AND UNLIKE EVERY OTHER ENGINE HERE, Spark can refresh. DuckDB bakes the token into ATTACH, chDB
into CREATE DATABASE, LakeSail into an env var read once at startup -- all three capture a STRING
and are hard-bounded by its lifetime. ABFS re-reads the assertion file on every token refresh, so
the daemon thread below keeps Spark alive indefinitely. GitHub's OIDC assertion lives about five
minutes, hence the four-minute rewrite.

THE CATALOG BEARER CANNOT, so a long run restarts the session. It is a literal header, fixed for
the life of the catalog (see _catalog_conf), and once it expires every table not already in the
catalog cache is a 401. TPC-DS first touches ship_mode at Q62, income_band at Q64, time_dim at Q66
and web_page at Q77 -- runs 35956230538 (SF=30) and 35956245381 (SF=60) both lost exactly those
statements from Q62 on, about 95 minutes in, `NotAuthorizedException`. So `refresh()`, which the
runner calls before every statement and outside the timer, compares the clock with the bearer's
expiry and, with under TOKEN_MIN_LIFETIME_SECONDS left, stops the session, shuts the JVM down and
runs setup again on a freshly minted bearer. Gluten watches its SAS on the same clock. The catalog
and manifest caches start empty after a restart, so the next statements pay their REST lookups
again: a real cost, left in the numbers.
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from pathlib import Path

from bench import auth, scrub
from bench.config import (
    CATALOG_CACHE_SECONDS,
    ICEBERG_ENDPOINT,
    ONELAKE_DFS,
    TOKEN_MIN_LIFETIME_SECONDS,
    Config,
)

CATALOG = "onelake"

# PINNED AS A PAIR, and the pin is on Spark, not Iceberg. Iceberg ships one runtime per Spark
# MINOR, built against Spark internals, so pyspark is held at 4.1.x in
# requirements/pyspark_iceberg.txt and these two coordinates follow from that.
#
# SPARK 4.2 WAS TRIED AND REVERTED (runs 35564906115, 35568051831, 35568044894). No released
# Iceberg speaks 4.2 -- 1.11 stopped at 4.1 and 1.12 left 4.2 out of the branch on purpose -- so
# it meant iceberg-spark-runtime-4.2_2.13:1.13.0-SNAPSHOT, a nightly of Iceberg main, plus
# hadoop-azure 3.5.0 to match Spark 4.2.0's own hadoop.version. That combination WORKED for the
# ETL and was faster than this one (960.6s against 1155.4s for 1000 files, 68 files of 15.6 MB
# either way), and then HUNG THE READ BENCHMARK: Q1-Q20 answered cold in 1-13s each at SF=1 and
# Q21 -- the lineitem self-join -- was still running 48 minutes later. One pin serves both
# benchmarks, so the one that hangs 22 queries decides it.
#
# hadoop-azure MUST equal Spark 4.1's own hadoop.version (3.4.2) or the classpath splits between
# it and Spark's bundled hadoop-client-api and reads die with NoSuchMethodError. 3.4.2 also clears
# the 3.4.1 floor where WorkloadIdentityTokenProvider was backported.
PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.hadoop:hadoop-azure:3.4.2"
)

ASSERTION_REFRESH_S = 240


def _catalog_conf(name: str, warehouse: str) -> dict[str, str]:
    """Every `spark.sql.catalog.<name>.*` key, as a dict rather than a builder chain.

    A dict rather than fifteen more `.config()` links in the builder chain, because the chain had
    grown long enough that the abfs loop below it read as the end of the configuration when it is
    not. Same keys, same order, applied in one loop.
    """
    prefix = f"spark.sql.catalog.{name}"
    return {
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        f"{prefix}.uri": ICEBERG_ENDPOINT,
        f"{prefix}.warehouse": warehouse,
        # rest.auth.type=none + a literal header, NOT the `token` property. Setting `token`
        # selects Iceberg's OAuth2 manager, whose token-refresh-enabled defaults to TRUE and
        # which then calls POST <uri>/v1/oauth/tokens -- an endpoint Fabric's own /v1/config
        # does not advertise. The catalog is metadata-only and read-only.
        #
        # The cost of that choice is that the bearer is a STRING, fixed for the life of the
        # catalog instance -- Hadoop's provider above mints its own storage tokens from the OIDC
        # assertion forever, but nothing here can re-open the catalog with a new one. The cache
        # carries a table loaded early past the bearer's expiry; a table first touched later
        # cannot be, which is why `refresh()` restarts the session instead (module docstring).
        f"{prefix}.rest.auth.type": "none",
        f"{prefix}.header.Authorization": f"Bearer {auth.onelake_token()}",
        # PIN THE FileIO. Iceberg's ResolvingFileIO maps abfss:// to ADLSFileIO when
        # iceberg-azure is on the classpath, and ADLSFileIO uses the Azure SDK -- it ignores
        # every fs.azure.* key in `setup` and fails with a credential error that looks nothing
        # like a classpath problem. HadoopFileIO is what routes reads through the provider.
        f"{prefix}.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
        # Iceberg's catalog cache expires after 30 SECONDS by default, so across 44 statements
        # Spark alone would keep re-resolving tables over REST while the others answer from
        # cache. See config.CATALOG_CACHE_SECONDS.
        f"{prefix}.cache-enabled": "true",
        f"{prefix}.cache.expiration-interval-ms": str(CATALOG_CACHE_SECONDS * 1000),
        # THE CATALOG CACHE KEEPS THE TABLE OBJECT, NOT ITS MANIFESTS. Scan planning still
        # fetches the manifest list and every manifest over ABFS for each statement -- a cold
        # open() (HEAD + GET) on a small file, times up to 8 tables per query. That is the ~5s
        # floor under Q11, Q16 and Q22, which touch no lineitem at all. Off by default in
        # Iceberg; same 15-minute lifetime as the catalog cache, so it is still one number for
        # every engine.
        f"{prefix}.io.manifest.cache-enabled": "true",
        f"{prefix}.io.manifest.cache.expiration-interval-ms": str(CATALOG_CACHE_SECONDS * 1000),
    }


def _shutdown_gateway() -> None:
    """Kill the py4j gateway, and with it the JVM, so the next builder launches a new one.

    `SparkSession.stop()` leaves the JVM running, and a SparkContext created in it again would
    find Gluten's native backend already initialised on the old SAS. Stopping py4j is not enough
    either: PythonGatewayServer exits only on EOF from its stdin, so close that and wait.
    """
    from pyspark import SparkContext

    gateway = SparkContext._gateway
    if gateway is not None:
        proc = getattr(gateway, "proc", None)
        gateway.shutdown()
        if proc is not None:
            proc.stdin.close()
            try:
                proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
    SparkContext._gateway = None
    SparkContext._jvm = None


class PysparkIceberg:
    name = "pyspark_iceberg"

    # When the earliest credential baked into the session expires, epoch seconds: the catalog
    # bearer here, and a variant's own (Gluten's SAS) folded in by _storage_conf.
    _expires = float("inf")

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._spark = None
        self._stop = threading.Event()
        self._refresher: threading.Thread | None = None
        self._assertion: Path | None = None

    @property
    def version(self) -> str:
        """pyspark's version, plus the Iceberg jar's own build when a session is up.

        The Iceberg half is what makes a nightly legible: the coordinate says 1.13.0-SNAPSHOT
        for every run, `IcebergBuild.fullVersion()` says WHICH one (version plus the git commit
        it was built from). Best-effort by construction -- this is read right after setup(),
        including after a setup that FAILED, so no session, no JVM and an Iceberg that never
        loaded all fall back to the pyspark version alone.
        """
        import pyspark

        version = pyspark.__version__
        try:
            build = self._spark._jvm.org.apache.iceberg.IcebergBuild
            return f"{version} + iceberg {build.fullVersion()}"
        except Exception:  # noqa: BLE001 - a version string must never fail a run
            return version

    @property
    def session(self):
        """The live SparkSession -- None before setup(). The ETL engine reuses this setup."""
        return self._spark

    def _write_assertion(self) -> None:
        """Refresh the OIDC assertion on disk, where ABFS will re-read it."""
        assert self._assertion is not None
        self._assertion.write_text(auth._github_oidc_assertion(), encoding="utf-8")

    def _keep_assertion_fresh(self, stop: threading.Event) -> None:
        while not stop.wait(ASSERTION_REFRESH_S):
            try:
                self._write_assertion()
            except Exception as exc:  # noqa: BLE001 - a failed refresh must not kill the run
                scrub.safe_print(f"  warning: assertion refresh failed: {exc}")

    def setup(self) -> None:
        from pyspark.sql import SparkSession

        scratch = Path(os.environ.get("RUNNER_TEMP", "/tmp"))
        self._assertion = scratch / "onelake-oidc-assertion"

        # Only in Actions. On a laptop there is no OIDC endpoint, and ABFS falls back to whatever
        # the four fs.azure keys below resolve to -- which is nothing, so it fails loudly.
        if os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL"):
            self._write_assertion()
            # A new Event per setup, handed to the thread: setup runs again after close() when
            # Gluten restarts on fresh credentials, and the old thread must stay stopped.
            self._stop = threading.Event()
            self._refresher = threading.Thread(
                target=self._keep_assertion_fresh, args=(self._stop,), daemon=True
            )
            self._refresher.start()

        account = ONELAKE_DFS
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
            .appName("bench-tpch-onelake")
            .config("spark.jars.packages", PACKAGES)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            # NO WEB UI. Not cosmetic: with the UI on, Spark's listener bus RETAINS job, stage
            # and task metadata in memory -- 1000 jobs, 1000 stages, 100000 tasks by default --
            # and across 44 statements in one session that accumulates into GC pressure on an
            # 11GB heap. It is the best available explanation for Spark being the only engine
            # here whose WARM pass is slower than its cold one (644s vs 565s at SF=10). Nothing
            # in CI ever opens the UI.
            .config("spark.ui.enabled", "false")
            # And the `[Stage 0:===>]` lines, which flood the log artifact.
            .config("spark.ui.showConsoleProgress", "false")
            # 200 shuffle partitions on 4 cores is scheduling overhead, not parallelism, and it is
            # the usual reason small-scale Spark looks worse than it is. NOT a thumb on the scale:
            # every other engine here is told it has 4 threads too.
            .config("spark.sql.shuffle.partitions", "8")
            # BUT 8 IS ALSO THE FLOOR AQE COALESCES *DOWN* FROM, and it cannot go back up. At
            # SF=10 that leaves each shuffle partition carrying a large slice of lineitem, which
            # is where spill on an 11GB heap comes from. initialPartitionNum lets AQE start fine
            # and shrink to what the data needs.
            #
            # It has to be this setting rather than simply raising shuffle.partitions, because
            # Iceberg's adaptive split sizing targets
            # max(spark.default.parallelism, spark.sql.shuffle.partitions) -- so raising that one
            # would ALSO multiply the scan task count and shrink every split. One knob, one job.
            #
            # Past SF=100 the count grows with the data, so each task's slice of a shuffle --
            # the hash table Q18 aggregates and Q21 joins into -- stays the size it is at SF=100,
            # the largest scale Spark and Gluten are known to finish TPC-H at.
            .config(
                "spark.sql.adaptive.coalescePartitions.initialPartitionNum",
                str(max(64, 64 * self.cfg.sf // 100)),
            )
            # SHUFFLE FILES GO WHEN THE QUERY DOES. Spark otherwise deletes them only when the JVM
            # garbage-collects the shuffle, forced every 30 minutes, so a session running one
            # query after another keeps every finished query's shuffle on disk: Gluten at TPC-H
            # SF=300 filled the runner's ~105 GB 15 minutes into the queries and took the runner
            # down with it (run 36304713315). Gluten's ColumnarShuffleManager deletes its files on
            # the same unregister call.
            .config("spark.sql.classic.shuffleDependency.fileCleanup.enabled", "true")
            # DOUBLE QUOTES ARE IDENTIFIERS, as in the SQL standard and every other engine here.
            # Spark's default reads "order count" as a STRING literal, so eight TPC-DS statements
            # that alias a column `AS "order count"` or `AS "30 days"` -- the spec's own text --
            # were parse errors. With ANSI mode (Spark 4's default) this flag makes the parser
            # standard on that one point. It changes no plan and no TPC-H statement.
            .config("spark.sql.ansi.doubleQuotedIdentifiers", "true")
            .config("spark.sql.defaultCatalog", CATALOG)
        )
        for key, value in _catalog_conf(CATALOG, self.cfg.warehouse).items():
            builder = builder.config(key, value)
        # _catalog_conf just put the bearer in the header; refresh() watches its expiry.
        self._expires = auth.token_expires_on()
        for key, value in self._storage_conf(abfs, account).items():
            builder = builder.config(key, value)

        # HOW BYTES COME OFF ONELAKE, which is where Spark's time goes: warm is barely faster than
        # cold, and Q6 -- four columns of lineitem, no join -- takes 11s where Sail takes 3s.
        #
        # hadoop-azure's defaults are a readahead queue depth of 2 and 4MB requests, so each of the
        # four task streams has at most 8MB in flight per round-trip to the OneLake proxy. A
        # Parquet column chunk of tens of MB becomes a chain of 4MB REST calls, two at a time. The
        # read-buffer pool is 16 buffers JVM-wide, so depth 4 across 4 streams fills it exactly, and
        # 16 x 8MB is 128MB of heap out of 11GB. Not 16MB on this runner. NOT a thumb on the scale:
        # DuckDB, chDB and Sail all issue many concurrent range reads per file by default.
        for key, value in {
            "fs.azure.readaheadqueue.depth": "4",
            "fs.azure.read.request.size": str(8 * 1024 * 1024),
            "fs.azure.read.readahead.blocksize": str(8 * 1024 * 1024),
        }.items():
            builder = builder.config(f"spark.hadoop.{key}", value)

        for key, value in self._extra_config().items():
            builder = builder.config(key, value)

        self._spark = builder.getOrCreate()
        self._spark.catalog.setCurrentCatalog(CATALOG)

        hadoop = self._spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
        scrub.safe_print(
            f"  pyspark {self.version} on hadoop {hadoop}, catalog {CATALOG} "
            f"(local[4], {os.environ.get('SPARK_DRIVER_MEMORY', 'default')} driver heap)"
        )

    def _storage_conf(self, abfs: dict[str, str], account: str) -> dict[str, str]:
        """The ABFS credential keys as Spark sees them. A variant may swap the credential."""
        conf = {}
        for key, value in abfs.items():
            # Account-scoped AND unscoped: Fabric's own table metadata mixes schemes, with
            # abfss:// in `location` and abfs:// in the `write.data.path` property, and the
            # account-scoped keys only match one host spelling.
            conf[f"spark.hadoop.{key}.{account}"] = value
            conf[f"spark.hadoop.{key}"] = value
        return conf

    def _extra_config(self) -> dict[str, str]:
        """Session keys a variant adds on top of everything above. Stock Spark adds none."""
        return {}

    def refresh(self) -> None:
        """Restart on fresh credentials once the session's have under 15 minutes left."""
        if self._expires - time.time() > TOKEN_MIN_LIFETIME_SECONDS:
            return
        start = time.perf_counter()
        self.close()
        _shutdown_gateway()
        # A fresh bearer, not the cached one: the new session's catalog cache is empty, so every
        # table is loaded again over REST on whatever bearer the header carries.
        auth.onelake_token(fresh=True)
        self.setup()
        scrub.safe_print(
            f"  credentials within {TOKEN_MIN_LIFETIME_SECONDS // 60} min of expiry: "
            f"Spark restarted in {time.perf_counter() - start:.1f}s"
        )

    def execute(self, sql: str) -> int:
        return len(self._spark.sql(sql).collect())

    def close(self) -> None:
        """Stop the JVM. Safe to call twice; never raises.

        Same hazard LakeSail documents: a Spark JVM that outlives the script would hold the job
        open until its timeout.
        """
        self._stop.set()
        if self._spark is not None:
            try:
                self._spark.stop()
            except Exception as exc:  # noqa: BLE001 - teardown is best-effort by design
                scrub.safe_print(f"  warning: spark.stop() failed: {exc}")
            self._spark = None
        if self._assertion is not None:
            self._assertion.unlink(missing_ok=True)
            self._assertion = None
