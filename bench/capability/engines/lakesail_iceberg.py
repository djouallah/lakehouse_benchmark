"""LakeSail (Sail) against the OneLake Iceberg REST catalog, over Spark Connect.

Sail is a Rust Spark replacement with no JVM: the process starts an in-process Spark Connect
server and talks to it over gRPC on localhost.

THE SERVER OUTLIVES THE SCRIPT. Sail's gRPC server runs on background threads that are not
daemons, so a process that merely finishes `main()` hangs forever. Hence `close()` in a
`finally`, and the probe script's hard `os._exit`.

Sail validates its config STRICTLY: an unknown key (an earlier version set the invented
SAIL_EXECUTION__MEMORY_LIMIT) stops it from starting at all rather than being ignored.
"""

from __future__ import annotations

import os

from bench import auth, scrub
from bench.config import CATALOG_CACHE_SECONDS, ICEBERG_ENDPOINT, Config


class LakesailIceberg:
    name = "lakesail_iceberg"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._server = None
        self._spark = None

    @property
    def version(self) -> str:
        from importlib.metadata import version

        return version("pysail")

    @property
    def session(self):
        """The live Spark Connect session, None before setup()."""
        return self._spark

    def setup(self, race_endpoint: str | None = None) -> None:
        """`race_endpoint`, when given, is a second catalog, `race`, on the race proxy."""
        from pysail.spark import SparkConnectServer
        from pyspark.sql import SparkSession

        token = auth.onelake_token()

        # Sail is configured by environment, read once at server start -- so the token is captured
        # here and never refreshed, the same ceiling DuckDB has.
        os.environ["SAIL_OPTIMIZER__ENABLE_JOIN_REORDER"] = "true"
        os.environ["SAIL_EXECUTION__COLLECT_STATISTICS"] = "true"
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
        # already hold -- so DuckDB and LakeSail both authenticate storage with one bearer token
        # and neither pays for vending.
        os.environ["AZURE_STORAGE_TOKEN"] = token

        # The two cache settings do NOT cache the table. In Sail 0.7 they cache the namespace's
        # table LISTING, consulted by list_tables only; every statement still calls loadTable
        # once per table it touches, which is the per-table WARN line in the log. Kept so the
        # constant applies the day Sail caches the loaded table. See config.CATALOG_CACHE_SECONDS
        # and lakehq/sail#2629.
        # Sail's `onelake` catalog hard-codes the public host, so any other endpoint (the
        # concurrency probes' local proxy) goes through its generic Iceberg REST catalog instead.
        def rest(endpoint: str) -> str:
            return (
                f'type="iceberg-rest", uri="{endpoint}", '
                f'warehouse="{self.cfg.warehouse}", bearer_access_token="{token}"'
            )

        if ICEBERG_ENDPOINT == "https://onelake.table.fabric.microsoft.com/iceberg":
            where = (
                f'type="onelake", url="{self.cfg.warehouse}", api="iceberg", bearer_token="{token}"'
            )
        else:
            where = rest(ICEBERG_ENDPOINT)
        cache = (
            f'table_cache_type="session", table_cache_ttl_secs={CATALOG_CACHE_SECONDS}, '
            f'database_cache_type="session", database_cache_ttl_secs={CATALOG_CACHE_SECONDS}'
        )
        catalogs = [f'{{{where}, name="onelake", {cache}}}']
        if race_endpoint:
            catalogs.append(f'{{{rest(race_endpoint)}, name="race", {cache}}}')
            # With two catalogs Sail will not pick one, and every session fails to start.
            os.environ["SAIL_CATALOG__DEFAULT_CATALOG"] = "onelake"
        os.environ["SAIL_CATALOG__LIST"] = f"[{', '.join(catalogs)}]"

        self._server = SparkConnectServer()
        self._server.start()
        _, port = self._server.listening_address
        self._spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
        # No `USE SCHEMA`: the probes' statements arrive fully qualified.
        scrub.safe_print(f"  pysail {self.version} listening on {port}")

    def close(self) -> None:
        """Stop the session and the server. Safe to call twice; never raises.

        A failure here must not mask the probe's own exception, and a half-stopped server is
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
