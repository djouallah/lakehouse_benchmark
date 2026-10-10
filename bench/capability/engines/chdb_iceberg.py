"""chDB (ClickHouse in-process) against the OneLake Iceberg REST catalog.

The session is lakehouse_benchmark's read engine, without its cache and memory tuning:

  * `DataLakeCatalog(...) SETTINGS catalog_type='onelake', onelake_bearer_token=...` is the only
    route into OneLake that accepts an Entra bearer token. The same string signs the catalog's
    REST calls and the blob reads, so like DuckDB and Sail it never asks for vended credentials.
  * `onelake_bearer_token` does not exist below chdb-core 26.7, which chdb 4.4.0 is the first
    release to require. Anything older fails the attach with "Unknown setting", which reads like a
    OneLake permissions problem rather than a wheel problem.
  * The catalog's `<namespace>.<table>` is ONE table name inside the attached database, so a table
    is addressed as onelake.`ns.table`.
  * The token is captured once, in CREATE DATABASE, and never refreshed.
"""

from __future__ import annotations

import contextlib
import json
import tempfile

from bench import auth, scrub
from bench.chdb_version import chdb_version
from bench.config import ICEBERG_ENDPOINT, Config

# The attached catalog's name inside chDB.
DB = "onelake"

SESSION_SETTINGS = (
    # Beta gate for the DataLakeCatalog database engine. It must be a session-scoped SET, live when
    # CREATE DATABASE runs -- a SETTINGS clause there configures the DATABASE, not the statement.
    "SET allow_database_iceberg = 1",
    # Writes to Iceberg (INSERT, and the ALTERs that go with it) are behind their own gate.
    "SET allow_experimental_insert_into_iceberg = 1",
    # JSONCompact quotes 64-bit integers by default; the probes compare them as numbers.
    "SET output_format_json_quote_64bit_integers = 0",
)


def _last_document(text: str) -> dict:
    """The last JSON document in `text`: a failed statement's output can precede this one's."""
    decoder = json.JSONDecoder()
    position, last = 0, {}
    while position < len(text):
        while position < len(text) and text[position].isspace():
            position += 1
        if position == len(text):
            break
        last, position = decoder.raw_decode(text, position)
    return last


class ChdbIceberg:
    name = "chdb_iceberg"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._session = None

    @property
    def version(self) -> str:
        return chdb_version()

    def setup(self) -> None:
        from chdb import session

        self._session = session.Session(tempfile.mkdtemp(prefix="chdb_"))
        for statement in SESSION_SETTINGS:
            self._session.query(statement)

        self.attach(DB, ICEBERG_ENDPOINT)
        scrub.safe_print(f"  chdb {self.version} attached")

    def attach(self, name: str, endpoint: str) -> None:
        """The OneLake catalog as database `name`, reached at `endpoint`."""
        # NOT logged, at any verbosity: this statement contains the bearer token.
        token = auth.onelake_token()
        self._session.query(
            f"""
            CREATE DATABASE {name}
            ENGINE = DataLakeCatalog('{endpoint}')
            SETTINGS catalog_type = 'onelake',
                     warehouse = '{self.cfg.warehouse}',
                     onelake_bearer_token = '{token}'
            """
        )

    def query(self, sql: str) -> list[tuple]:
        """Run one statement; its rows, or [] for a statement that returns none.

        JSONCompact, never 'Pretty', which echoes the statement back on error.
        """
        try:
            result = self._session.query(sql, "JSONCompact")
        except Exception:
            # A FAILED STATEMENT LEAVES OUTPUT BEHIND, which the next statement would read as its
            # own. A throwaway statement takes the leftover.
            with contextlib.suppress(Exception):
                self._session.query("SELECT 1", "JSONCompact")
            raise
        return [tuple(row) for row in _last_document(str(result)).get("data", [])]

    def close(self) -> None:
        """Safe to call twice; never raises."""
        if self._session is None:
            return
        try:
            self._session.close()
        except Exception as exc:  # noqa: BLE001 - teardown is best-effort by design
            scrub.safe_print(f"  warning: chdb session close failed: {exc}")
        self._session = None
