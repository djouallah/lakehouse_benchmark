"""DuckDB's write-capable ATTACH of the OneLake Iceberg REST catalog.

TWO ATTACH FLAGS, both about writing:

* `STAGE_CREATE_TABLES false` -- OneLake refuses the commit that finishes a staged create, so the
  table is created in one request instead of created-then-committed.
* `SKIP_CREATE_TABLE_METADATA_UPDATES true` -- DuckDB follows a CREATE TABLE with a second commit
  that updates the fresh table's metadata. This flag fully initialises the metadata in the create
  itself.

Storage goes through the azure extension under an `access_token` secret, with
`ACCESS_DELEGATION_MODE 'none'`. See config.azure_transport for the transport rule.
"""

from __future__ import annotations

import re

from bench.config import ICEBERG_ENDPOINT, Config, azure_transport
from bench.duckdb_cli import DuckDBCli, Result
from bench.duckdb_cli import version as cli_version

CATALOG = "onelake"
_INT = re.compile(r"^-?\d+$")


def _value(cell: str):
    """A CSV cell as the probes compare it: NULL (empty) is None, an integer is an int."""
    if cell == "":
        return None
    return int(cell) if _INT.match(cell) else cell


class Connection:
    """The nightly CLI (bench.duckdb_cli says why not the wheel), answering like a
    `duckdb.connect()`: `execute()` and `sql()` run SQL and return rows, `close()` ends the
    process. A failed statement raises RuntimeError with DuckDB's own message."""

    def __init__(self):
        self._cli = DuckDBCli()

    def execute(self, sql: str) -> Result:
        rows = self._cli.sql(sql).fetchall()
        return Result([tuple(_value(cell) for cell in row) for row in rows])

    sql = execute

    def close(self) -> None:
        self._cli.close()


def connect() -> Connection:
    return Connection()


def version() -> str:
    """The CLI's library version, e.g. `v2.0.0-alpha44357`."""
    return cli_version()


def attach(conn, cfg: Config, token: str, endpoint: str = ICEBERG_ENDPOINT) -> None:
    """The write-capable ATTACH on one connection: transport, storage secret, the two flags."""
    conn.sql(f"""
        SET GLOBAL azure_transport_option_type = '{azure_transport()}';
        SET preserve_insertion_order = false;

        CREATE OR REPLACE SECRET onelake_storage (
            TYPE azure, PROVIDER access_token, ACCESS_TOKEN '{token}');

        ATTACH OR REPLACE '{cfg.warehouse}' AS {CATALOG} (
            TYPE ICEBERG,
            ENDPOINT '{endpoint}',
            TOKEN '{token}',
            ACCESS_DELEGATION_MODE 'none',
            STAGE_CREATE_TABLES false,
            SKIP_CREATE_TABLE_METADATA_UPDATES true);
    """)
