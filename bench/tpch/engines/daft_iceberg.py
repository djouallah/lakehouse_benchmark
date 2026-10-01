"""Daft against the OneLake Iceberg REST catalog, via pyiceberg.

The fifth engine, and the only one added after the port -- so unlike the other four there is no
notebook cell behind it.

IT TAKES THE SAME ROUTE POLARS DOES: pyiceberg resolves the catalog and the manifest, then Daft
opens the parquet itself with its own credential. Which means the two credential paths are
separate here, exactly as they are for Polars -- `auth.catalog()` signs the REST calls, and the
IOConfig below signs the blob reads.

THE CREDENTIAL IS THE EASY PART, for once. Daft's AzureConfig takes a plain `bearer_token`, and
`use_fabric_endpoint` is a literal OneLake flag -- no vending, no SAS, no service principal, no
env-var fallback chain to fight. Compare LakeSail, which does not implement vending, silently
fell back to building a credential from AZURE_CLIENT_ID, and died on "Identity not found".

`storage_account="onelake"` is not redundant with use_fabric_endpoint: the table's data files are
`abfss://<workspace-guid>@onelake.dfs.fabric.microsoft.com/<lakehouse-guid>/...`, so the ACCOUNT
is `onelake` and the CONTAINER is the workspace guid, which reads backwards until you have seen
it once.
"""

from __future__ import annotations

from bench import auth, scrub
from bench.config import Config
from bench.tpch.engines.base import restart_on_fresh_token


class DaftIceberg:
    name = "daft_iceberg"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._sess = None
        self._expires = float("inf")

    @property
    def version(self) -> str:
        from importlib.metadata import version

        return version("daft")

    def setup(self) -> None:
        import daft
        from daft import Session
        from daft.io import AzureConfig, IOConfig

        token = auth.onelake_token()
        self._expires = auth.token_expires_on()
        io_config = IOConfig(
            azure=AzureConfig(
                storage_account="onelake",
                bearer_token=token,
                use_fabric_endpoint=True,
            )
        )
        catalog = auth.catalog(self.cfg)

        # Registered under the FULL DOTTED NAME, so `daft_iceberg` is in the backticked group in
        # bench/tpch/queries.py alongside chDB and Polars. Same reasoning as Polars: a temp
        # table is a flat namespace, so `CH0010.lineitem` has to survive as one quoted identifier
        # rather than being parsed as schema + table.
        self._sess = Session()
        for table in self.cfg.TABLES:
            self._sess.create_temp_table(
                f"{self.cfg.schema}.{table}",
                daft.read_iceberg(
                    catalog.load_table(f"{self.cfg.schema}.{table}"),
                    io_config=io_config,
                ),
            )
        scrub.safe_print(f"  daft {self.version} registered {len(self.cfg.TABLES)} tables")

    def execute(self, sql: str) -> int:
        """Run and count.

        `.collect()` IS the measurement -- Daft is lazy, so without it the timer would measure
        plan construction. `count_rows()` on the collected frame then reads the materialized
        partitions rather than planning a second query.
        """
        return self._sess.sql(sql).collect().count_rows()

    def refresh(self) -> None:
        """Re-register on a fresh token: each read holds it in its IOConfig."""
        restart_on_fresh_token(self, self._expires)

    def close(self) -> None:
        self._sess = None
