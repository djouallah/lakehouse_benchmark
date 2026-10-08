"""Polars against the OneLake Iceberg REST catalog, via pyiceberg.

Port of cell 12's `polars_iceberg` branch, with the token bug fixed and the name resolution made
to work.

THE TOKEN BUG. Cell 12 read:

    storage_options={"bearer_token": '{token}'}

A plain single-quoted string inside a cell that is not an f-string, so Polars was handed the eight
literal characters `{token}` and never the credential.

THE NAME RESOLUTION. Polars has no catalog namespace, so `CH0010.lineitem` parses as a relation
named CH0010 and fails. The fix is to register each frame under its FULL dotted name and leave the
backticks in the SQL, so `` `CH0010.lineitem` `` is one quoted identifier matching one registered
key. That is why `polars_iceberg` is in NEEDS_BACKTICKS alongside chDB in
bench/tpch/queries.py -- different reason, same spelling.

An explicit `SQLContext` also replaces cell 12's `globals()[tbl] = ...`: same resolution, but the
frames are scoped to this object instead of depending on which module's globals the caller is
standing in.
"""

from __future__ import annotations

import os

from bench import auth, scrub
from bench.config import Config
from bench.tpch.engines.base import restart_on_fresh_token


class PolarsIceberg:
    name = "polars_iceberg"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._ctx = None
        self._expires = float("inf")

    @property
    def version(self) -> str:
        import polars as pl

        return pl.__version__

    def setup(self) -> None:
        # Polars sizes its thread pool at import time; pinning it keeps the number reproducible
        # across runner images rather than tracking whatever the host reports. So BEFORE the
        # import: set after it, as this line used to be, it was read by nothing.
        os.environ.setdefault("POLARS_MAX_THREADS", "4")
        import polars as pl

        catalog = auth.catalog(self.cfg)
        token = auth.onelake_token()
        self._expires = auth.token_expires_on()

        # Reading the data FILES is a separate credential path from reading the catalog: pyiceberg
        # resolves the manifest, then Polars opens the parquet itself and needs its own token.
        storage_options = {"bearer_token": token}

        # BY NAME, THROUGH THE CATALOG: `scan_iceberg("ns.table", catalog=...)` resolves the table
        # lazily at collect(), as DuckDB's ATTACH does, instead of a `catalog.load_table()` per
        # table up front (pola-rs/polars#27776, where a maintainer pointed at it). Polars reuses
        # the catalog it is given and caches REST scans and manifests (pola-rs/polars#29623,
        # #29790). The loop stays: SQLContext needs a frame per name. There is no data-file disk
        # cache to turn on -- Polars 2.0 removed it.
        self._ctx = pl.SQLContext()
        for table in self.cfg.TABLES:
            name = f"{self.cfg.schema}.{table}"
            self._ctx.register(
                name,
                pl.scan_iceberg(name, catalog=catalog, storage_options=storage_options),
            )
        scrub.safe_print(f"  polars {self.version} registered {len(self.cfg.TABLES)} tables")

    def execute(self, sql: str) -> int:
        """Run and count.

        `.collect()` IS THE MEASUREMENT. `ctx.execute()` returns a LazyFrame, so the notebook's
        `.show()` on it timed plan construction and nothing else.

        `engine='streaming'` is what gives Q18 and Q21 a chance at SF>=10 -- the in-memory engine
        builds the whole lineitem hash table and is OOM-killed (exit 137, no traceback) on a 16GB
        runner. Streaming spills for group-bys and joins, though its coverage is not total.
        """
        return self._ctx.execute(sql).collect(engine="streaming").height

    def refresh(self) -> None:
        """Re-register on a fresh token: each scan holds it in its storage_options."""
        restart_on_fresh_token(self, self._expires)

    def close(self) -> None:
        self._ctx = None
