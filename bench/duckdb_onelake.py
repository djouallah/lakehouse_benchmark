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

THE STORAGE PATH IS THE READ BENCHMARK'S: `CREATE SECRET ... access_token` plus
`ACCESS_DELEGATION_MODE 'none'`. The parquet that CTAS writes goes out through the azure
extension under that secret, the same way the TPC-H scans come in. Same curl transport too --
see config.azure_transport for why that is not optional on Linux.
"""

from __future__ import annotations

from bench.config import ICEBERG_ENDPOINT, Config, azure_transport

# The attached catalog's name inside DuckDB.
CATALOG = "onelake"


def attach(conn, cfg: Config, token: str) -> None:
    """The write-capable ATTACH on one connection: transport, storage secret, the two flags."""
    conn.sql(f"""
        SET GLOBAL azure_transport_option_type = '{azure_transport() or "default"}';
        SET preserve_insertion_order = false;

        CREATE OR REPLACE SECRET onelake_storage (
            TYPE azure, PROVIDER access_token, ACCESS_TOKEN '{token}');

        ATTACH OR REPLACE '{cfg.warehouse}' AS {CATALOG} (
            TYPE ICEBERG,
            ENDPOINT '{ICEBERG_ENDPOINT}',
            TOKEN '{token}',
            ACCESS_DELEGATION_MODE 'none',
            STAGE_CREATE_TABLES false,
            SKIP_CREATE_TABLE_METADATA_UPDATES true);
    """)
