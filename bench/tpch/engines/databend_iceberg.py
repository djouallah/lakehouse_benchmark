"""Databend: the all-in-one container, the OneLake REST catalog, queries over the MySQL protocol.

Everything about the container, the catalog and the credentials is bench/databend.py. Here: setup
times the cold start (container up, catalog attached) as the setup row, and `refresh` re-attaches
the catalog on a fresh bearer and SAS between statements when either runs low -- the catalog holds
both as fixed strings, so that is the only way to renew them.

The dialect: `catalog.database.table`, and `USE CATALOG` makes the suites' `CH0010.lineitem`
resolve (bench.tpch.queries.IDENT_STYLE, "dotted").
"""

from __future__ import annotations

from bench import auth, databend, scrub
from bench.config import Config


class DatabendIceberg:
    name = "databend_iceberg"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._conn = None
        self._version = "unknown"
        self._expires = float("inf")

    @property
    def version(self) -> str:
        return self._version

    def _attach(self, fresh: bool) -> None:
        token = auth.onelake_token(fresh=fresh)
        sas, sas_expires = databend.sas(self.cfg)
        self._expires = min(auth.token_expires_on(), sas_expires)
        databend.attach(self._conn, self.cfg, token, sas)
        with self._conn.cursor() as cur:
            cur.execute(f"USE {self.cfg.schema}")

    def setup(self) -> None:
        databend.start()
        self._conn = databend.connect()
        self._version = f"{databend.version(self._conn)} ({databend.IMAGE})"
        self._attach(fresh=False)
        scrub.safe_print(f"  databend {self._version} attached to {self.cfg.schema}")

    def refresh(self) -> None:
        """Outside the timer: a new bearer and SAS once either nears its expiry."""
        if self._conn is None or not databend.needs_refresh(self._expires):
            return
        self._attach(fresh=True)
        scrub.safe_print(
            "  bearer or SAS within 15 min of expiry: catalog re-attached on fresh ones"
        )

    def execute(self, sql: str) -> int:
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return len(cur.fetchall())

    def close(self) -> None:
        if self._conn is not None:
            try:
                scrub.safe_print(f"  databend data cache: {databend.cache_metrics(self._conn)}")
            except Exception as exc:  # noqa: BLE001 - a readout must never fail teardown
                scrub.safe_print(f"  warning: data cache readout failed: {exc}")
            try:
                self._conn.close()
            finally:
                self._conn = None
