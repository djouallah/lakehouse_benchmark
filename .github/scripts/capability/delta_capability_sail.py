"""What LakeSail (Sail) can do writing Delta tables into OneLake, in SQL.

Sail speaks Spark SQL with its own Rust Delta code, so it is sent delta_probe.DeltaProbe's
statements byte for byte, the ones Spark is sent. The server is started with no catalog list:
Sail's default in-memory catalog holds each probe table, created with a LOCATION under
Tables/_bench_delta. Storage is the bearer in AZURE_STORAGE_TOKEN, as for the Iceberg grid
(bench/capability/engines/lakesail_iceberg.py says why that variable and not the client id).
"""

from __future__ import annotations

import os

from delta_probe import DeltaProbe, run

from bench import auth, scrub


class SailDelta(DeltaProbe):
    CLIENT, TITLE, PREFIX = "sail", "Sail", "sl"

    def start(self) -> str:
        from importlib.metadata import version

        from pysail.spark import SparkConnectServer
        from pyspark.sql import SparkSession

        os.environ["AZURE_STORAGE_TOKEN"] = auth.onelake_token()
        os.environ.pop("SAIL_CATALOG__LIST", None)
        self.server = SparkConnectServer()
        self.server.start()
        _, port = self.server.listening_address
        self.spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
        self.spark.conf.set("spark.sql.session.timeZone", "UTC")
        # The same as Spark's session: VACUUM ... RETAIN 0 HOURS is what the vacuum probe asks.
        self.spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        scrub.safe_print(f"  pysail listening on {port}")
        return version("pysail")

    def execute(self, statement: str) -> list[tuple]:
        return [tuple(r) for r in self.spark.sql(statement).collect()]

    def close(self) -> None:
        for attr in ("spark", "server"):
            handle = getattr(self, attr, None)
            if handle is not None:
                try:
                    handle.stop()
                except Exception as exc:  # noqa: BLE001 - teardown is best effort
                    scrub.safe_print(f"  warning: {attr}.stop() failed: {exc}")


if __name__ == "__main__":
    run(SailDelta)
