"""chDB: read the CSVs over plain HTTPS, stream the Arrow result into pyiceberg.

Port of the notebook's `chdb_clean_csv`, which read `file('/lakehouse/default/Files/csv/{a,b}')`
off the Fabric mount and gave the Arrow stream to `write_deltalake`. Same shape here -- chDB
does the read and the transform, a Python writer commits the table, and the data goes through
as a stream of record batches -- with two substitutions:

THE READ IS `url()`, NOT `file()`. There is no mount on a runner, and chDB's Azure table
functions (`azureBlobStorage`, `icebergAzure`) take a connection string, an account key or a
SAS -- none of which OneLake accepts -- and never a bearer token
(bench/tpch/engines/chdb_iceberg.py has the full list). `url()` takes arbitrary
headers, OneLake serves a file to a plain authenticated GET, and `url()` keeps the brace glob
and the `_file` virtual column the notebook's `file()` call used. So the statement is the
notebook's with the source swapped. It carries the token in its text: never printed, and
`scrub` masks it if an exception quotes it.

THE WRITER IS pyiceberg, fed the `pa.RecordBatchReader` that `send_query(..., "Arrow")`
streams out of ClickHouse (see bench/etl/iceberg.py). Two spellings changed for the Arrow
boundary: `parseDateTime64BestEffort(c5, 6)` instead of `parseDateTimeBestEffort(c5)`, because
ClickHouse exports a second-precision `DateTime` to Arrow as `UINT32` and only `DateTime64`
becomes a timestamp; and `toInt32(toYear(...))`, because `toYear` is `UInt16`.
`output_format_arrow_string_as_string` likewise, or every text column lands as binary.
"""

from __future__ import annotations

import os
import pathlib

from bench import auth, scrub
from bench.chdb_version import chdb_version
from bench.etl import iceberg
from bench.etl.config import TABLE, EtlConfig
from bench.etl.schema import COLUMNS
from bench.tpch.engines.chdb_iceberg import ChdbIceberg as _TpchChdb

SETTINGS = (
    # The notebook's three: infer every column as String, skip the `C` header line, accept the
    # ragged rows of a multi-table file.
    "SET input_format_csv_use_best_effort_in_schema_inference = 0",
    "SET input_format_csv_skip_first_lines = 1",
    "SET input_format_csv_allow_variable_number_of_columns = 1",
    # Arrow output that pyiceberg can take: utf8 rather than binary, and no dictionary arrays
    # for the LowCardinality `_file` column.
    "SET output_format_arrow_string_as_string = 1",
    "SET output_format_arrow_low_cardinality_as_dictionary = 0",
    "SET max_threads = 4",
    "SET max_memory_usage = 10000000000",
)


class ChdbIceberg:
    name = "chdb_iceberg"

    def __init__(self, cfg: EtlConfig):
        self.cfg = cfg
        self._session = None
        self._catalog = None
        self._token = ""
        self._tbl = None

    @property
    def version(self) -> str:
        return chdb_version()

    def setup(self) -> None:
        from chdb import session

        scratch = pathlib.Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "chdb-etl"
        # The TPC-H engine's server config: spill path and the memory ceiling. Its OneLake cache
        # is configured too and simply never used -- nothing here reads through the catalog.
        cfg_path = _TpchChdb(self.cfg)._write_config(scratch)
        self._session = session.Session(f"{scratch}/etl?config-file={cfg_path}")
        for statement in SETTINGS:
            self._session.query(statement)
        self._token = auth.onelake_token()
        self._catalog = auth.catalog(self.cfg)
        scrub.safe_print(f"  chdb {self.version} ready, pyiceberg catalog loaded")

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
        _file AS filename,
        toInt32(toYear(SETTLEMENTDATE)) AS year
    FROM raw
    """

    def load(self, files: list[str]) -> None:
        reader = self._session.send_query(self._sql(files), "Arrow").record_batch()
        first, reader = iceberg.first_batch(reader)
        self._tbl = iceberg.recreate(self._catalog, self.cfg, TABLE[self.name], first.schema)
        rows = iceberg.append_stream(self._tbl, reader)
        scrub.safe_print(f"    {rows:,} rows streamed in one append")

    def row_count(self) -> int:
        return iceberg.total_records(self._tbl)

    def layout(self) -> str | None:
        return iceberg.layout(self._tbl)

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None
