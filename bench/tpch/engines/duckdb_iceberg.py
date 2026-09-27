"""DuckDB: the iceberg extension attaches the catalog, the azure extension reads the files.

THE EXTERNAL FILE CACHE. Since 1.3 DuckDB keeps the byte ranges it reads from remote files in
its buffer pool (`enable_external_file_cache`, on by default), so a statement that touches a
parquet file the session has already read does not go back to OneLake for it. It is the only
cache DuckDB turns on by itself -- `enable_object_cache`, `enable_http_metadata_cache` and
`parquet_metadata_cache` all default to off. Left at its default.

THE STORAGE SECRET IS REPLACED WHEN THE TOKEN IS. It holds a token STRING, good for about an hour,
and TPC-DS at SF=100 runs DuckDB longer than that: run 35862492772 read fine for 65 minutes, then
failed Q88 onwards `Unauthorized` on store_sales. `refresh`, which the runner calls before every
statement and outside the timer, asks bench.auth for a token with at least
TOKEN_MIN_LIFETIME_SECONDS left and re-creates the secret only when the string changed: one
comparison per statement, one CREATE SECRET an hour. The catalog token in ATTACH is left alone:
table metadata is cached for CATALOG_CACHE_SECONDS (six hours), so the REST catalog is not called
again inside a run. (Removed once, in 994c96a, while Q64 killed the SF=100 run before the hour;
back for the run where Q64 does not.)

THE SPILL PROBE. What stops DuckDB at big scales is the disk, not memory: TPC-DS SF=100 Q64 died
at `90.6 GiB/90.6 GiB used`, and `max_temp_directory_size` defaults to 90% of the free disk under
`temp_directory`. So setup logs both, and a daemon thread sums the blocks allocated under the temp
directory once a second. It never touches the connection, only the filesystem. Each statement's
peak is printed by the NEXT `refresh` (and the last by `close`), outside the timer -- `Q21 spill
peak` in the log is the query just before it.
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
        import duckdb

        return duckdb.__version__

    def setup(self) -> None:
        import duckdb

        # curl transport: DuckDB's default one fails the OneLake TLS handshake on Linux, and the
        # ATTACH still succeeds (plain HTTPS) so every read fails instead, with an error that
        # reads like a bad credential. ACCESS_DELEGATION_MODE 'none' turns vending off -- it costs
        # ~7s per table cold, and the other three engines authenticate storage with one token too.
        token = auth.onelake_token()
        self._conn = duckdb.connect()
        self._conn.sql(f"""
            SET GLOBAL azure_transport_option_type = '{azure_transport() or "default"}';
        """)
        self._storage_secret(token)
        self._conn.sql(f"""
            ATTACH OR REPLACE '{self.cfg.warehouse}' AS onelake (
                TYPE ICEBERG,
                ENDPOINT '{ICEBERG_ENDPOINT}',
                TOKEN '{token}',
                ACCESS_DELEGATION_MODE 'none',
                MAX_TABLE_STALENESS '{CATALOG_CACHE_SECONDS // 60} minutes',
                DEFAULT_SCHEMA '{self.cfg.schema}');

            USE onelake;
        """)
        scrub.safe_print(f"  duckdb {self.version} attached to {self.cfg.schema}")
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
            scrub.safe_print(f"  Q{self._statement:<2} spill peak {peak:.2f} GiB")

    def refresh(self) -> None:
        """Outside the timer: a new secret once the token has under 15 minutes left."""
        self._report_spill()  # also starts the next statement's peak from zero
        self._statement += 1
        token = auth.onelake_token(skew=TOKEN_MIN_LIFETIME_SECONDS)
        if self._conn is not None and token != self._token:
            self._storage_secret(token)
            scrub.safe_print("  storage token re-minted, secret replaced")

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
