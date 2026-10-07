"""DuckDB: the iceberg extension attaches the catalog, the azure extension reads the files.

THE ENGINE IS THE NIGHTLY CLI, not the PyPI wheel: one `duckdb` process per run, statements in
on stdin (bench/duckdb_cli.py says how, and why the CLI).

THE EXTERNAL FILE CACHE. Since 1.3 DuckDB keeps the byte ranges it reads from remote files in
its buffer pool (`enable_external_file_cache`, on by default), so a statement that touches a
parquet file the session has already read does not go back to OneLake for it. It is the only
cache DuckDB turns on by itself -- `enable_object_cache`, `enable_http_metadata_cache` and
`parquet_metadata_cache` all default to off. Left at its default.

THE STORAGE SECRET AND THE CATALOG ATTACH ARE BOTH REPLACED WHEN THE TOKEN IS. Each holds a token
STRING, good for about an hour, and TPC-DS at SF=100 runs DuckDB longer than that: run 35862492772
read fine for 65 minutes, then failed Q88 onwards `Unauthorized` on store_sales. `refresh`, which
the runner calls before every statement and outside the timer, asks bench.auth for a token with at
least TOKEN_MIN_LIFETIME_SECONDS left and, only when the string changed, re-creates the secret and
re-attaches the catalog: one comparison per statement, one swap an hour. The catalog token used to
be left alone on the theory that table metadata is cached for CATALOG_CACHE_SECONDS -- but a table
the run has not loaded yet is a catalog call, and TPC-DS first touches web_page at Q77: run
37437748581 failed it `Access token validation failed` 70 minutes in. The re-attach drops the
catalog's metadata cache, the price every engine that restarts on a fresh token pays too; the
external file cache is the buffer pool's and survives it.

THE SPILL PROBE. What stops DuckDB at big scales is the disk, not memory: TPC-DS SF=100 Q64 died
at `90.6 GiB/90.6 GiB used`, and `max_temp_directory_size` defaults to 90% of the free disk under
`temp_directory`. So setup logs both, and a daemon thread sums the blocks allocated under the temp
directory once a second. It never touches the connection, only the filesystem. Each statement's
peak is printed by the NEXT `refresh` (and the last by `close`), outside the timer -- a `spill
peak` line in the log belongs to the query line just above it (TPC-DS runs its hard queries first,
so a statement counter would not be the query number).
"""

from __future__ import annotations

import os
import shutil
import threading

from bench import auth, scrub
from bench.config import (
    CATALOG_CACHE_SECONDS,
    ICEBERG_ENDPOINT,
    TOKEN_MIN_LIFETIME_SECONDS,
    Config,
    azure_transport,
)
from bench.duckdb_cli import DuckDBCli
from bench.duckdb_cli import version as cli_version


class DuckDBIceberg:
    name = "duckdb_iceberg"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._conn = None
        self._token = ""
        self._spill: _SpillProbe | None = None
        self._statement = 0

    @property
    def version(self) -> str:
        return cli_version()

    def setup(self) -> None:
        # curl transport: DuckDB's default one fails the OneLake TLS handshake on Linux, and the
        # ATTACH still succeeds (plain HTTPS) so every read fails instead, with an error that
        # reads like a bad credential. ACCESS_DELEGATION_MODE 'none' turns vending off -- it costs
        # ~7s per table cold, and the other three engines authenticate storage with one token too.
        token = auth.onelake_token()
        self._conn = DuckDBCli()
        self._conn.sql(f"""
            SET GLOBAL azure_transport_option_type = '{azure_transport() or "default"}';
        """)
        self._storage_secret(token)
        self._attach(token)
        scrub.safe_print(f"  duckdb {self.version} attached to {self.cfg.schema}")
        scrub.safe_print(f"  extensions: {self._conn.extensions()}")
        temp_dir, cap = self._conn.sql(
            "SELECT current_setting('temp_directory'), current_setting('max_temp_directory_size')"
        ).fetchone()
        temp_dir = os.path.abspath(temp_dir)
        free = shutil.disk_usage(os.path.dirname(temp_dir) or ".").free / 2**30
        scrub.safe_print(
            f"  spill: temp_directory {temp_dir}, max_temp_directory_size {cap}, "
            f"disk free {free:.1f} GiB"
        )
        self._spill = _SpillProbe(temp_dir)

    def _attach(self, token: str) -> None:
        """(Re-)attach the catalog with `token`. Off `onelake` first: the database in use cannot
        be replaced."""
        self._conn.sql(f"""
            USE memory;

            ATTACH OR REPLACE '{self.cfg.warehouse}' AS onelake (
                TYPE ICEBERG,
                URI '{ICEBERG_ENDPOINT}',
                TOKEN '{token}',
                ACCESS_DELEGATION_MODE 'none',
                MAX_TABLE_STALENESS '{CATALOG_CACHE_SECONDS // 60} minutes',
                DEFAULT_SCHEMA '{self.cfg.schema}');

            USE onelake;
        """)

    def _storage_secret(self, token: str) -> None:
        self._conn.sql(f"""
            CREATE OR REPLACE SECRET onelake_storage (
                TYPE azure, PROVIDER access_token, ACCESS_TOKEN '{token}');
        """)
        self._token = token

    def _report_spill(self) -> None:
        if self._spill is None:
            return
        peak = self._spill.take_peak() / 2**30
        if self._statement:
            scrub.safe_print(f"       spill peak {peak:.2f} GiB (query above)")

    def refresh(self) -> None:
        """Outside the timer: a new secret and attach once the token has under 15 minutes left."""
        self._report_spill()  # also starts the next statement's peak from zero
        self._statement += 1
        token = auth.onelake_token(skew=TOKEN_MIN_LIFETIME_SECONDS)
        if self._conn is not None and token != self._token:
            self._storage_secret(token)
            self._attach(token)
            scrub.safe_print("  token re-minted, storage secret and catalog attach replaced")

    def execute(self, sql: str) -> int:
        return len(self._conn.sql(sql).fetchall())

    def close(self) -> None:
        self._report_spill()
        self._statement = 0
        if self._spill is not None:
            self._spill.stop()
            self._spill = None
        if self._conn is not None:
            self._conn.close()
            self._conn = None


class _SpillProbe:
    """Peak bytes allocated under DuckDB's temp directory, sampled once a second."""

    def __init__(self, path: str, interval: float = 1.0):
        self.path = path
        self._peak = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        threading.Thread(target=self._loop, args=(interval,), daemon=True).start()

    def _allocated(self) -> int:
        total = 0
        for root, _, files in os.walk(self.path):
            for name in files:
                try:
                    st = os.stat(os.path.join(root, name))
                except OSError:  # deleted between the listing and the stat
                    continue
                # Blocks, not st_size: the size of a sparse or preallocated file is not disk used.
                total += getattr(st, "st_blocks", 0) * 512 or st.st_size
        return total

    def _loop(self, interval: float) -> None:
        while not self._stop.wait(interval):
            used = self._allocated()
            with self._lock:
                self._peak = max(self._peak, used)

    def take_peak(self) -> int:
        """The peak since the last call, and start a new one."""
        with self._lock:
            peak, self._peak = self._peak, 0
        return peak

    def stop(self) -> None:
        self._stop.set()
