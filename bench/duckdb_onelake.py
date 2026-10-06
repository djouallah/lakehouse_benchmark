"""The write-capable DuckDB ATTACH of the OneLake Iceberg REST catalog.

Born in bench/etl/engines/duckdb_iceberg.py, where the ETL's DuckDB engine writes its table
through it; moved here for a second caller. (That caller, the TPC-DS generator, used it for one
day and went back to the TPC-H upload path -- bench/tpcds/generate.py says why.) The ETL engine
imports it from here, and the concurrency benchmark opens a fresh connection per writer -- every
connection (a `bench.duckdb_cli.DuckDBCli` process, or a `duckdb.connect()`) is its own database,
with its own secrets and its own catalog attach -- so the attach has to be byte-for-byte this one
wherever it happens.

TWO ATTACH FLAGS the TPC-H read engine does not carry, both about writing:

* `STAGE_CREATE_TABLES false` -- OneLake does not implement staged creates, so the table is
  created in one request instead of created-then-committed.
* `SKIP_CREATE_TABLE_METADATA_UPDATES true` -- DuckDB follows a CREATE TABLE with a second commit
  that updates the fresh table's metadata, and OneLake rejects that commit. This flag fully
  initialises the metadata in the create itself. Spark's create has no such follow-up, which is
  why the Spark engine did not need an equivalent.

STORAGE IS CREDENTIAL VENDING (2026-10-06, trying it): no azure secret and no
`ACCESS_DELEGATION_MODE 'none'`, so the catalog hands DuckDB the storage credential for each
table it loads, and the CTAS parquet goes out under that. The TPC-H read engine still uses its own
secret with vending off (bench/tpch/engines/duckdb_iceberg.py). Same curl transport --
see config.azure_transport for why that is not optional on Linux.
"""

from __future__ import annotations

from bench.config import ICEBERG_ENDPOINT, Config, azure_transport

# The attached catalog's name inside DuckDB.
CATALOG = "onelake"


def attach(conn, cfg: Config, token: str) -> None:
    """The write-capable ATTACH on one connection: transport, the two flags, vended storage."""
    conn.sql(f"""
        SET GLOBAL azure_transport_option_type = '{azure_transport() or "default"}';
        SET preserve_insertion_order = false;

        ATTACH OR REPLACE '{cfg.warehouse}' AS {CATALOG} (
            TYPE ICEBERG,
            URI '{ICEBERG_ENDPOINT}',
            TOKEN '{token}',
            STAGE_CREATE_TABLES false,
            SKIP_CREATE_TABLE_METADATA_UPDATES true);
    """)
