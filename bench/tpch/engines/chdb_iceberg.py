"""chDB (ClickHouse in-process) against the OneLake Iceberg REST catalog.

Port of cell 12's `chdb_iceberg` branch, and the one engine whose OneLake support is narrow enough
to be worth writing down:

  * `DataLakeCatalog(...) SETTINGS catalog_type='onelake', onelake_bearer_token=...` is the ONLY
    route into OneLake that accepts an Entra bearer token. The same string signs the catalog's
    REST calls and the blob reads.
  * `azureBlobStorage()` / `icebergAzure()` / `deltaLakeAzure()` do NOT take a bearer token at
    all -- their credential surface is a connection string, account_name+key, a SAS, or a
    workload identity. Irrelevant here (everything is catalog-attached) but it is why there is no
    fallback path if the catalog attach fails.
  * `onelake_bearer_token` does not exist below chdb-core 26.7, which chdb 4.4.0 is the first
    release to require. Resolve anything older and the attach fails with "Unknown setting", which
    reads like a OneLake permissions problem rather than a wheel problem. Hence the >=4.4.0 floor
    in requirements/chdb_iceberg.txt.
"""

from __future__ import annotations

import contextlib
import json
import os
import pathlib

from bench import auth, scrub
from bench.config import CATALOG_CACHE_SECONDS, ICEBERG_ENDPOINT, Config
from bench.tpch.config import chdb_cache_gib

# The attached catalog's name inside chDB.
DB = "onelake"

# Beta gate. The datalake catalog database engine refuses to be created without it. It must be a
# session-scoped SET, live when CREATE DATABASE runs -- a SETTINGS clause there configures the
# DATABASE, not the statement.
GATE = "allow_database_iceberg"

# SETTINGS THAT CHANGE ANSWERS, kept apart from the ones that only change speed.
#
# .github/scripts/smoke_sql.py applies exactly this tuple to its local session, so the dialect
# check runs with the benchmark's semantics. That separation exists because the first version did
# NOT have it: the smoke adapter built a bare session, the join_use_nulls fix below was never in
# effect there, and the run came back 41 again as though the fix had done nothing.
SEMANTIC_SETTINGS = (
    # ClickHouse defaults join_use_nulls=0, which fills an unmatched outer-join cell with the
    # column's DEFAULT VALUE -- 0 for an integer key -- instead of NULL. Q13 counts exactly that:
    # `COUNT(o_orderkey)` over a LEFT OUTER JOIN, so every customer with no orders scored 1
    # instead of 0, collapsed into the c_count=1 bucket, and the c_count=0 group vanished. chDB
    # returned 41 rows where DuckDB, Polars and LakeSail all returned 42.
    #
    # Wrong in every published chDB result, at every scale factor, and invisible to a timing
    # chart. smoke_sql.py found it by making all four engines read identical local parquet.
    "SET join_use_nulls = 1",
    # A bare `UNION` is `UNION DISTINCT` in the SQL standard and in every other engine here.
    # ClickHouse refuses to guess ("Expected ALL or DISTINCT in SelectWithUnion query") unless
    # told, and four TPC-DS statements -- q36, q49, q75 among them -- write the bare form. This
    # tells it the standard's answer; no TPC-H statement has a UNION.
    "SET union_default_mode = 'DISTINCT'",
)


def _last_document(text: str) -> dict:
    """The last JSON document in `text`, which normally holds exactly one.

    Belt and braces for `execute`'s flush: if a failed statement's output still precedes this
    one's, this statement's result is the document that comes last.
    """
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
        import chdb

        return chdb.__version__

    def _write_config(self, scratch: pathlib.Path) -> pathlib.Path:
        """The ClickHouse server config: cache location and size, spill path, memory ceiling.

        The notebook asked for a 150GiB cache under /mnt/notebookfusetmp, a Fabric-only path. On a
        runner both halves are wrong: the directory does not exist, and 150GiB is ten times the
        whole disk. ClickHouse does NOT check free space before filling the cache, so an
        oversized max_size is an ENOSPC in the middle of a query rather than an eviction.
        """
        cache_dir = scratch / "cache"
        tmp_dir = scratch / "tmp"
        cache_dir.mkdir(parents=True, exist_ok=True)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        cfg_path = scratch / "config.xml"
        cfg_path.write_text(
            f"""<clickhouse>
    <tmp_path>{tmp_dir}/</tmp_path>
    <max_server_memory_usage>13000000000</max_server_memory_usage>
    <filesystem_caches>
        <onelake_cache>
            <path>{cache_dir}/onelake</path>
            <max_size>{chdb_cache_gib(self.cfg.estimated_gib)}Gi</max_size>
        </onelake_cache>
    </filesystem_caches>
</clickhouse>""",
            encoding="utf-8",
        )
        return cfg_path

    def setup(self) -> None:
        from chdb import session

        scratch = pathlib.Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "chdb"
        cfg_path = self._write_config(scratch)
        self._session = session.Session(f"{scratch}/bench?config-file={cfg_path}")

        for statement in (
            f"SET {GATE} = 1",
            "SET filesystem_cache_name = 'onelake_cache'",
            # 0 means "pick automatically". Left from the notebook: on a 4-core box an explicit
            # download thread count mostly fights the query threads.
            "SET max_download_threads = 0",
            f"SET iceberg_metadata_staleness_ms = {CATALOG_CACHE_SECONDS * 1000}",
            *SEMANTIC_SETTINGS,
            "SET max_threads = 4",
            # 12 GB A QUERY, 13 GB THE PROCESS -- DuckDB's default share of this runner (80% of RAM,
            # 12.5 GB) and under StarRocks' BE (13.5 GB). It was 10 GB / 11 GB, and TPC-H SF=60 Q21
            # was the one statement past it: "would use 10.41 GiB" against 9.31 (run 36287421972),
            # its IN and NOT IN subqueries each a hash set of ~100M order keys, which cannot spill.
            "SET max_memory_usage = 12000000000",
            # SPILL IS LEFT TO THE DEFAULTS, which since 25.x spill by themselves:
            # max_bytes_ratio_before_external_group_by / _sort / _join are all 0.5, and the join
            # one turns a hash join into a grace hash join only once memory runs short (it needs a
            # temporary path, which config.xml's <tmp_path> is). This used to force
            # join_algorithm='grace_hash,hash' and 5 GB absolute thresholds, from before those
            # defaults existed. Forcing grace hash ran every join through one-bucket grace hash
            # instead of the default parallel_hash, even the joins that fit in memory.
        ):
            self._session.query(statement)

        # NOT logged, at any verbosity: this statement contains the bearer token.
        token = auth.onelake_token()
        self._session.query(
            f"""
            CREATE DATABASE {DB}
            ENGINE = DataLakeCatalog('{ICEBERG_ENDPOINT}')
            SETTINGS catalog_type = 'onelake',
                     warehouse = '{self.cfg.warehouse}',
                     onelake_bearer_token = '{token}'
            """
        )
        self._session.query(f"USE {DB}")
        cache = chdb_cache_gib(self.cfg.estimated_gib)
        scrub.safe_print(f"  chdb {self.version} attached, cache {cache}Gi at {scratch}")

    def execute(self, sql: str) -> int:
        """Run and count.

        JSONCompact, not the notebook's 'Pretty': Pretty renders an ASCII table inside the timed
        window, and echoes the statement back on error -- which for the attach would put the
        token in the log.
        """
        try:
            result = self._session.query(sql, "JSONCompact")
        except Exception:
            # A FAILED STATEMENT LEAVES OUTPUT BEHIND. At SF=60 every query right after a
            # MEMORY_LIMIT_EXCEEDED -- Q4, Q8, Q10, Q22 -- came back as two JSON documents, the
            # failed one's and its own, and died "JSONDecodeError: Extra data" (run 36016149239).
            # A throwaway statement takes the leftover, so the next timed query starts clean.
            with contextlib.suppress(Exception):
                self._session.query("SELECT 1", "JSONCompact")
            raise
        return len(_last_document(str(result)).get("data", []))

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None
