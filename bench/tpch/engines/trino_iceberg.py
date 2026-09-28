"""Trino: the official image, the OneLake REST catalog, queries over Trino's HTTP protocol.

Everything about the container, the catalog and the credentials is bench/trino.py. Here: setup
times the cold start (container up, catalog created) as the setup row, and `refresh` re-creates
the catalog on a fresh bearer between statements when the one baked into it runs low -- the
catalog's `oauth2.token` is a fixed string. Storage renews itself (the assertion file).

The dialect: `catalog.schema.table`, and the connection's default catalog and schema make the
suites' `CH0010.lineitem` resolve (bench.tpch.queries.IDENT_STYLE, "dotted"). Trino is strict
where the other engines were lenient, which is why sql/tpch.sql now casts its date literals
(Q5, Q10, Q12) and groups by expressions rather than select aliases (Q8, Q9) for every engine.
"""

from __future__ import annotations

from bench import auth, scrub, trino
from bench.config import Config


class TrinoIceberg:
    name = "trino_iceberg"

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
        self._expires = auth.token_expires_on()
        trino.attach(self._conn, self.cfg, token)

    def setup(self) -> None:
        trino.start()
        self._conn = trino.connect(schema=self.cfg.schema.lower())
        self._version = f"{trino.version(self._conn)} ({trino.IMAGE})"
        self._attach(fresh=False)
        scrub.safe_print(f"  trino {self._version} attached to {self.cfg.schema}")

    def refresh(self) -> None:
        """Outside the timer: a new bearer once less than TOKEN_MIN_LIFETIME_SECONDS remains."""
        if self._conn is None or not trino.needs_refresh(self._expires):
            return
        self._attach(fresh=True)
        scrub.safe_print("  bearer within 15 min of expiry: catalog re-created on a fresh one")

    def execute(self, sql: str) -> int:
        return len(trino.sql(self._conn, sql))

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None
