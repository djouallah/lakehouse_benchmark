"""LakeSail (Sail) against the OneLake Iceberg REST catalog, over Spark Connect.

Port of cell 12's `lakesail_iceberg` branch. Sail is a Rust Spark replacement with no JVM: the
process starts an in-process Spark Connect server and talks to it over gRPC on localhost.

TWO THINGS THE NOTEBOOK COULD IGNORE AND THIS CANNOT:

1. THE SERVER OUTLIVES THE SCRIPT. Sail's gRPC server runs on background threads that are not
   daemons, so a process that merely finishes `main()` hangs forever. A Fabric notebook cell does
   not care -- the kernel stays alive anyway. Here it would burn the job's entire timeout. Hence
   `close()` in a `finally`, plus the hard `timeout-minutes` on the workflow step behind it.

2. THE MEMORY POOL IS UNBOUNDED BY DEFAULT, and unbounded means it never spills: DataFusion
   spills a sort or an aggregation only when its pool says no. So at TPC-H SF=30 Sail did not
   slow down, it took the runner with it -- OOM-killed 46s into Q18, no traceback, no row. The
   pool is now `fair` with a ceiling under the runner's RAM (`setup`). Sort and aggregation spill
   to the OS temp directory; a HASH JOIN'S BUILD SIDE CANNOT SPILL in DataFusion 54, so a join
   that outgrows the pool fails as "Resources exhausted" -- one failed statement, like chDB's
   memory limit, instead of a dead job. An earlier version of this file invented
   `SAIL_EXECUTION__MEMORY_LIMIT`; that is not a real setting, and Sail validates its config
   STRICTLY -- it refused to start rather than ignoring the unknown key.

ALSO: `grpcio-status==1.48.2` from cell 3 is deliberately NOT pinned here. That pin exists to
fight Fabric's preinstalled protobuf/grpcio stack; outside Fabric it actively breaks
pyspark-client, which declares its own floors.
"""

from __future__ import annotations

import os

from bench import auth, scrub
from bench.config import CATALOG_CACHE_SECONDS, Config
from bench.tpch.engines.base import restart_on_fresh_token

# DataFusion's memory pool ceiling. See setup().
POOL_BYTES = 10 * 1024**3


class LakesailIceberg:
    name = "lakesail_iceberg"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._server = None
        self._spark = None
        self._expires = float("inf")

    @property
    def version(self) -> str:
        from importlib.metadata import version

        return version("pysail")

    @property
    def session(self):
        """The live Spark Connect session, None before setup(). Reused by the ETL engine."""
        return self._spark

    def setup(self) -> None:
        from pysail.spark import SparkConnectServer
        from pyspark.sql import SparkSession

        token = auth.onelake_token()
        self._expires = auth.token_expires_on()

        # Sail is configured by environment, read once at server start -- so the token is captured
        # here, and `refresh` renews it by starting a new server.
        os.environ["SAIL_OPTIMIZER__ENABLE_JOIN_REORDER"] = "true"
        os.environ["SAIL_EXECUTION__COLLECT_STATISTICS"] = "true"
        # A BOUNDED POOL, SO SAIL SPILLS (module docstring). `runtime.memory_pool.type` and
        # `runtime.memory_pool.fair.max_size` in crates/sail-common/src/config/application.yaml
        # (v0.7.1), whose defaults are `unbounded` and 64 GiB. Fair, not greedy: FairSpillPool
        # shares the ceiling among the spilling operators instead of letting the first one take
        # it all. 10 GiB leaves the rest of the 16 GB runner to the Python client, the gRPC
        # buffers and the reads in flight, which the pool does not account for.
        #
        # An earlier version set SAIL_EXECUTION__MEMORY_LIMIT, which is not a real key, and Sail
        # refused to start at all ("unknown field: found `memory_limit`").
        os.environ["SAIL_RUNTIME__MEMORY_POOL__TYPE"] = "fair"
        os.environ["SAIL_RUNTIME__MEMORY_POOL__FAIR__MAX_SIZE"] = str(POOL_BYTES)
        # AND JOINS THAT CAN SPILL. The pool alone took SF=30 from a dead runner to 21/22, and the
        # one left was Q18: "Failed to allocate ... for HashJoinInput[3] with 1665.0 MB already
        # allocated" (run 36238299506). A hash join's build side cannot spill in DataFusion 54; a
        # sort-merge join's buffered side can. `optimizer.prefer_hash_join` (default true) picks
        # sort-merge for the shuffled joins instead; small build sides still broadcast. With it,
        # SF=30 went 22/22 (run 36238967358).
        #
        # ONLY WHERE HASH JOINS OUTGROW THE POOL. Sort-merge costs the join-heavy queries 3-5x at
        # SF=10 -- Q7 35.0s against 6-11s, Q8 38.3s against 8-14s (run 36240159354) -- where
        # every hash table fits. So sort-merge once the dataset passes half the pool: SF=10
        # (~2.7 GiB) keeps hash joins, SF=30 (~8 GiB) and up spill.
        spill_joins = self.cfg.estimated_gib > POOL_BYTES / 2**30 / 2
        os.environ["SAIL_OPTIMIZER__PREFER_HASH_JOIN"] = "false" if spill_joins else "true"
        #
        # TRIED AND REVERTED: SAIL_PARQUET__PUSHDOWN_FILTERS=true (+ REORDER_FILTERS), Sail's
        # late-materialization switch, off by default. Run 35512613884 at SF=10: the queries it
        # should help most -- Q6, Q12, Q19, selective predicates on lineitem -- came out the
        # WORST of four runs (5.9s, 6.3s, 7.2s against 3.2-4.6s, 3.4-5.6s, 4.3-7.2s), and the
        # rest sat inside run-to-run noise. Over object storage the row-filter pass costs extra
        # range requests, and that is what it measured. Sail's other read-side defaults --
        # global footer and statistics caches, page index, pruning, bloom filters -- are already
        # on, so there is nothing left to switch.
        # THE STORAGE TOKEN, which is separate from the catalog token below.
        #
        # Sail does NOT implement credential vending -- it says so and then carries on:
        #
        #   WARN sail_iceberg::table_format] Iceberg REST catalog table CH0001.lineitem
        #   advertises vended storage credentials, which is not implemented yet
        #
        # so it falls back to building a credential from the environment. And the environment is
        # actively misleading here: this job exports AZURE_CLIENT_ID and AZURE_TENANT_ID for the
        # OIDC login, so Sail found a service principal, tried a client-credentials flow with no
        # secret, and every query died with
        #
        #   400 Bad Request: {"error":"invalid_request","error_description":"Identity not found"}
        #
        # AZURE_STORAGE_TOKEN takes precedence over that whole chain. It is what Sail's own
        # OneLake example sets, with a token for the same https://storage.azure.com/ audience we
        # already hold -- so DuckDB, chDB and LakeSail all authenticate storage with one bearer
        # token and none of them pays for vending.
        os.environ["AZURE_STORAGE_TOKEN"] = token

        # The two cache settings do NOT cache the table. In Sail 0.7 they cache the namespace's
        # table LISTING, consulted by list_tables only; every statement still calls loadTable
        # once per table it touches, which is the per-table WARN line in the log. Kept so the
        # constant applies the day Sail caches the loaded table. See config.CATALOG_CACHE_SECONDS
        # and lakehq/sail#2629.
        os.environ["SAIL_CATALOG__LIST"] = (
            f'[{{type="onelake", name="onelake", url="{self.cfg.warehouse}", '
            f'api="iceberg", bearer_token="{token}", '
            f'table_cache_type="session", table_cache_ttl_secs={CATALOG_CACHE_SECONDS}, '
            f'database_cache_type="session", database_cache_ttl_secs={CATALOG_CACHE_SECONDS}}}]'
        )

        self._server = SparkConnectServer()
        self._server.start()
        _, port = self._server.listening_address
        self._spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
        # No `USE SCHEMA`: the statements arrive schema-qualified (`CH0010.lineitem`, the `dotted`
        # style in bench/tpch/queries.py), so the catalog resolves them without a
        # current schema.
        scrub.safe_print(f"  pysail {self.version} listening on {port}")

    def refresh(self) -> None:
        """A new server on a fresh token: Sail reads it from env vars once, at start."""
        restart_on_fresh_token(self, self._expires)

    def execute(self, sql: str) -> int:
        """Run and count.

        `.collect()`, not the notebook's `.show()`. `.show()` implies `limit(20)`, so the five
        queries ending in `LIMIT 100` (Q2, Q3, Q10, Q18, Q21) were being asked for a fifth of the
        rows every other engine computed. All 22 results are <=100 rows, so collecting is free.
        """
        return len(self._spark.sql(sql).collect())

    def close(self) -> None:
        """Stop the session and the server. Safe to call twice; never raises.

        A failure here must not mask the benchmark's own exception, and a half-stopped server is
        still better than a hung job.
        """
        for attr in ("_spark", "_server"):
            handle = getattr(self, attr, None)
            if handle is None:
                continue
            try:
                handle.stop()
            except Exception as exc:  # noqa: BLE001 - teardown is best-effort by design
                scrub.safe_print(f"  warning: {attr}.stop() failed: {exc}")
            setattr(self, attr, None)
