"""DuckDB: read the CSVs over the azure extension, write Iceberg through the REST catalog itself.

Port of the notebook's `duckdb_clean_csv`, the one engine there that already wrote Iceberg. The
statement is the notebook's: `CREATE TABLE ... AS` over `read_csv(...)` with the 53-column
struct, the filter and the `COLUMNS(* EXCLUDE ...)` cast -- minus its `PARTITIONED BY (year)`,
because no engine here partitions (bench/etl/iceberg.py says why).

THE ATTACH IS bench/duckdb_onelake.py: the two write flags the TPC-H read engine does not carry
(`STAGE_CREATE_TABLES false`, `SKIP_CREATE_TABLE_METADATA_UPDATES true`) and the read benchmark's
storage path, explained there. It moved out of this module when the TPC-DS generator became its
second caller; `attach` and `CATALOG` are re-exported here so the concurrency benchmark, which
opens a fresh connection per writer through this name, keeps working.

THE ENGINE IS THE NIGHTLY CLI, as for the read benchmark: bench/duckdb_cli.py.
"""

from __future__ import annotations

from bench import auth, scrub
from bench.duckdb_cli import DuckDBCli
from bench.duckdb_cli import version as cli_version
from bench.duckdb_onelake import CATALOG, attach
from bench.etl.config import TABLE, EtlConfig
from bench.etl.schema import COLUMNS

__all__ = ["CATALOG", "DuckDBIceberg", "attach"]


class DuckDBIceberg:
    name = "duckdb_iceberg"

    def __init__(self, cfg: EtlConfig):
        self.cfg = cfg
        self._conn = None

    @property
    def version(self) -> str:
        return cli_version()

    @property
    def qualified(self) -> str:
        return f"{CATALOG}.{self.cfg.schema}.{TABLE[self.name]}"

    def setup(self) -> None:
        self._conn = DuckDBCli()
        attach(self._conn, self.cfg, auth.onelake_token())
        scrub.safe_print(f"  duckdb {self.version} attached")
        scrub.safe_print(f"  extensions: {self._conn.extensions()}")

    def load(self, files: list[str]) -> None:
        uris = [f"{self.cfg.csv_abfss}/{name}" for name in files]
        columns = ", ".join(f"'{c}': 'VARCHAR'" for c in COLUMNS)
        self._conn.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{self.cfg.schema}")
        self._conn.sql(f"DROP TABLE IF EXISTS {self.qualified}")
        self._conn.sql(f"""
            CREATE TABLE {self.qualified}
            AS
            WITH raw AS (
                SELECT * FROM read_csv(
                    {uris!r},
                    skip=1, header=0, all_varchar=1,
                    columns={{{columns}}},
                    filename=1, null_padding=true, ignore_errors=1, auto_detect=false
                )
                WHERE I = 'D' AND UNIT = 'DUNIT' AND VERSION = '3'
            )
            SELECT
                UNIT,
                DUID,
                filename,
                CAST(COLUMNS(* EXCLUDE (DUID, UNIT, SETTLEMENTDATE, I, XX, filename)) AS DOUBLE),
                CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
                year(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS year
            FROM raw
        """)

    def row_count(self) -> int:
        return int(self._conn.sql(f"SELECT count(*) FROM {self.qualified}").fetchone()[0])

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
