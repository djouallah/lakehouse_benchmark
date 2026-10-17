"""Databend: read the CSVs over azblob, fill the Iceberg table with one INSERT, in its container.

The container, the catalog and the credentials are bench/databend.py, set up as for the query
benchmark, except that the SAS also carries create and write: Databend writes the data files and
manifests over Azdls with it. The statement is DuckDB's (engines/duckdb_iceberg.py) in Databend's
dialect: the 53-column DUNIT layout, short rows NULL-padded, the three-column filter, every measure
cast to DOUBLE, SETTLEMENTDATE parsed, `year` derived, the source file's name kept. The name is
`metadata$filename` in lower case: bench/databend.py makes unquoted names case-sensitive, and
`METADATA$FILENAME` is then an unknown column (run 39024820940).

THE CSVS, as candidate_engine.yml proved them (138,240 DUNIT v3 rows, the same sum as Python's csv
module, run 39015860524):

* AZBLOB WITH THE ENVIRONMENT CREDENTIAL. Databend's azblob reader takes the workload identity in
  AZURE_* (the assertion file bench/databend.py keeps fresh); its Azdls reader, the Iceberg one,
  does not, hence the SAS there. An https:// URL carrying a SAS is no way round it: Databend drops
  the query string and OneLake answers 401.
* POSITIONAL COLUMNS. A CSV file format with ERROR_ON_COLUMN_COUNT_MISMATCH = false reads ragged
  AEMO rows, short ones NULL-padded, as `$1`..`$n`.
* ONE SCAN OVER EXACTLY THE RUN'S FILES: `FILES => (...)` names them inside the folder, which can
  hold more than the run reads.

ONE INSERT INTO A TABLE THE CATALOG CREATED. The catalog refuses staged creates, so the empty table
is created through pyiceberg in one request (bench/etl/iceberg.py `recreate`, as for chDB and
Polars), then filled by one Databend INSERT: one scan, one statement, one commit. No partitioning,
as for every engine.
"""

from __future__ import annotations

from bench import auth, databend, scrub
from bench.config import ONELAKE_BLOB
from bench.etl import iceberg
from bench.etl.config import TABLE, EtlConfig
from bench.etl.schema import COLUMNS, FILTER, numeric_columns

FORMAT = "aemo_csv"
# Ragged AEMO rows: short ones NULL-padded, long ones cut, rather than an error.
CREATE_FORMAT = (
    f"CREATE OR REPLACE FILE FORMAT {FORMAT} TYPE = CSV SKIP_HEADER = 1 "
    "FIELD_DELIMITER = ',' QUOTE = '\"' ERROR_ON_COLUMN_COUNT_MISMATCH = false"
)
POSITION = {c: f"${i + 1}" for i, c in enumerate(COLUMNS)}


def arrow_schema():
    """The table's columns, in the order the INSERT's SELECT lists them."""
    import pyarrow as pa

    return pa.schema(
        [("UNIT", pa.string()), ("DUID", pa.string())]
        + [(c, pa.float64()) for c in numeric_columns()]
        + [
            ("SETTLEMENTDATE", pa.timestamp("us")),
            ("filename", pa.string()),
            ("year", pa.int32()),
        ]
    )


class DatabendIceberg:
    name = "databend_iceberg"

    def __init__(self, cfg: EtlConfig):
        self.cfg = cfg
        self._conn = None
        self._catalog = None
        self._version = "unknown"

    @property
    def version(self) -> str:
        return self._version

    @property
    def qualified(self) -> str:
        return f"{databend.CATALOG}.{self.cfg.schema}.{TABLE[self.name]}"

    def setup(self) -> None:
        databend.start()
        self._conn = databend.connect()
        self._version = f"{databend.version(self._conn)} ({databend.IMAGE})"
        sas, _ = databend.sas(self.cfg, write=True)
        databend.attach(self._conn, self.cfg, auth.onelake_token(), sas)
        self._sql(CREATE_FORMAT)
        self._catalog = auth.catalog(self.cfg)
        scrub.safe_print(f"  databend {self._version} attached")

    def _sql(self, statement: str) -> list[tuple]:
        with self._conn.cursor() as cur:
            cur.execute(statement)
            return list(cur.fetchall())

    def _select(self, files: list[str]) -> str:
        """The transform over one azblob scan of exactly `files`."""
        names = ", ".join("'" + name.replace("'", "''") + "'" for name in files)
        source = (
            f"'azblob://{self.cfg.workspace_id}/{self.cfg.csv_relative}/' "
            f"(CONNECTION => (ENDPOINT_URL = 'https://{ONELAKE_BLOB}'), "
            f"FILE_FORMAT => '{FORMAT}', FILES => ({names}))"
        )
        where = " AND ".join(f"{POSITION[c]} = '{v}'" for c, v in FILTER)
        settled = f"to_timestamp({POSITION['SETTLEMENTDATE']}, '%Y/%m/%d %H:%M:%S')"
        return (
            f"SELECT {POSITION['UNIT']}, {POSITION['DUID']}, "
            + ", ".join(f"TRY_CAST({POSITION[c]} AS DOUBLE)" for c in numeric_columns())
            + f", {settled}, metadata$filename, CAST(to_year({settled}) AS INT)"
            f" FROM {source} WHERE {where}"
        )

    def load(self, files: list[str]) -> None:
        iceberg.recreate(self._catalog, self.cfg, TABLE[self.name], arrow_schema())
        self._sql(f"INSERT INTO {self.qualified} {self._select(files)}")
        scrub.safe_print(f"    {len(files)} files in one scan, one INSERT, one commit")

    def row_count(self) -> int:
        return int(self._sql(f"SELECT count(*) FROM {self.qualified}")[0][0])

    def layout(self) -> str | None:
        return iceberg.layout(self._catalog.load_table(f"{self.cfg.schema}.{TABLE[self.name]}"))

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None
