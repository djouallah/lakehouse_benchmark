"""chDB: read the CSVs over plain HTTPS, write the Iceberg table with one CTAS, both in ClickHouse.

Port of the notebook's `chdb_clean_csv`, which read `file('/lakehouse/default/Files/csv/{a,b}')`
off the Fabric mount and gave the Arrow stream to `write_deltalake`. Here chDB does all of it: the
read, the transform and the Iceberg write, through the catalog.

THE READ IS `url()`, NOT `file()`. There is no mount on a runner, and chDB's Azure table
functions (`azureBlobStorage`, `icebergAzure`) take a connection string, an account key or a
SAS -- none of which OneLake accepts -- and never a bearer token
(bench/tpch/engines/chdb_iceberg.py has the full list). `url()` takes arbitrary
headers, OneLake serves a file to a plain authenticated GET, and `url()` keeps the brace glob
and the `_file` virtual column the notebook's `file()` call used. So the statement is the
notebook's with the source swapped. It carries the token in its text: never printed, and
`scrub` masks it if an exception quotes it.

THE WRITE IS ClickHouse's own Iceberg writer: the OneLake catalog attached as a `DataLakeCatalog`
database (bench/capability/engines/chdb_iceberg.py's attach, plus the TPC-H engine's spill and
memory config), the previous run's table dropped through it, and one `CREATE TABLE ... AS
SELECT`: one scan, one statement, one commit. Until chdb-core 26.10 (the nightly after the
ClickHouse master sync, chdb-core#289) chDB could neither create an Iceberg table on OneLake
("MergeTree storages require data path") nor drop one (purgeRequested=False, ClickHouse#125410),
so it streamed Arrow into pyiceberg, which created the table and committed. Run 39179243597 of
the Iceberg support grid has CREATE TABLE, CTAS and DROP TABLE all working; that workaround is
gone. `_file` is CAST to String: it is LowCardinality(String), which the Iceberg writer
refuses ("Unsupported type for iceberg", run 39180855172), and toString() keeps that type.
"""

from __future__ import annotations

import os
import pathlib

from bench import auth, scrub
from bench.config import ICEBERG_ENDPOINT
from bench.etl.config import TABLE, EtlConfig
from bench.etl.schema import COLUMNS
from bench.tpch.engines.chdb_iceberg import ChdbIceberg as _TpchChdb

DB = "onelake"

SETTINGS = (
    # The notebook's three: infer every column as String, skip the `C` header line, accept the
    # ragged rows of a multi-table file.
    "SET input_format_csv_use_best_effort_in_schema_inference = 0",
    "SET input_format_csv_skip_first_lines = 1",
    "SET input_format_csv_allow_variable_number_of_columns = 1",
    # The DataLakeCatalog database engine and the Iceberg writer are behind their own gates; both
    # must be session SETs, live when CREATE DATABASE and the CTAS run.
    "SET allow_database_iceberg = 1",
    "SET allow_experimental_insert_into_iceberg = 1",
    "SET max_threads = 4",
    "SET max_memory_usage = 10000000000",
)


class ChdbIceberg:
    name = "chdb_iceberg"

    def __init__(self, cfg: EtlConfig):
        self.cfg = cfg
        self._session = None
        self._token = ""

    @property
    def version(self) -> str:
        import chdb

        return chdb.__version__

    @property
    def qualified(self) -> str:
        """The catalog's `ns.table` is one table name inside the attached database."""
        return f"{DB}.`{self.cfg.schema}.{TABLE[self.name]}`"

    def setup(self) -> None:
        from chdb import session

        scratch = pathlib.Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "chdb-etl"
        # The TPC-H engine's server config: spill path and the memory ceiling. Its OneLake cache
        # is configured too and simply never used: the CSVs are read once.
        cfg_path = _TpchChdb(self.cfg)._write_config(scratch)
        self._session = session.Session(f"{scratch}/etl?config-file={cfg_path}")
        for statement in SETTINGS:
            self._session.query(statement)
        self._token = auth.onelake_token()
        # NOT logged, at any verbosity: this statement contains the bearer token.
        self._session.query(
            f"""
            CREATE DATABASE {DB}
            ENGINE = DataLakeCatalog('{ICEBERG_ENDPOINT}')
            SETTINGS catalog_type = 'onelake',
                     warehouse = '{self.cfg.warehouse}',
                     onelake_bearer_token = '{self._token}'
            """
        )
        scrub.safe_print(f"  chdb {self.version} attached to the OneLake catalog")

    def _sql(self, names: list[str]) -> str:
        pattern = "{" + ",".join(names) + "}"
        source = (
            f"url('{self.cfg.csv_https}/{pattern}', 'CSV', 'auto', "
            f"headers('Authorization'='Bearer {self._token}'))"
        )
        # c8..c53 are the numeric columns; c1..c7 are handled by name below, as in the notebook.
        casts = ",\n        ".join(
            f"toFloat64OrNull(c{i}) AS {column}"
            for i, column in enumerate(COLUMNS, start=1)
            if i >= 8
        )
        return f"""
    WITH raw AS (
        SELECT *, _file
        FROM {source}
        WHERE c1 = 'D' AND c2 = 'DUNIT' AND c4 = '3'
    )
    SELECT
        c2 AS UNIT,
        toFloat64OrNull(c4) AS VERSION,
        parseDateTime64BestEffort(c5, 6) AS SETTLEMENTDATE,
        toFloat64OrNull(c6) AS RUNNO,
        c7 AS DUID,
        {casts},
        CAST(_file AS String) AS filename,
        toInt32(toYear(SETTLEMENTDATE)) AS year
    FROM raw
    """

    def load(self, files: list[str]) -> None:
        self._session.query(f"DROP TABLE IF EXISTS {self.qualified}")
        self._session.query(f"CREATE TABLE {self.qualified} AS {self._sql(files)}")
        scrub.safe_print(f"    {len(files)} files in one scan, one CTAS, one commit")

    def row_count(self) -> int:
        result = self._session.query(f"SELECT count() FROM {self.qualified}", "CSV")
        return int(str(result).strip())

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None
