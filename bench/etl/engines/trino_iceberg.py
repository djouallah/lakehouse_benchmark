"""Trino: the CSVs through a Hive table, the Iceberg table with one CTAS, in its container.

The container, the catalog and the credentials are bench/trino.py, set up exactly as for the query
benchmark. The statement is DuckDB's (engines/duckdb_iceberg.py) in Trino's dialect: the 53-column
DUNIT layout, short rows NULL-padded, the three-column filter, every measure cast to DOUBLE,
SETTLEMENTDATE parsed, `year` derived.

THE CSVS ARE A HIVE TABLE. Trino has no path-based file reader: a second catalog, `files`, is the
Hive connector with its file metastore inside the container, and one EXTERNAL table over
Files/csv declares 120 VARCHAR columns (the widest AEMO record; Trino's CSV format allows no other
type), so no row is longer than the schema and a short row reads NULL past its end. That layout
matched Python's csv module row for row in candidate_engine.yml (138,240 DUNIT v3 rows, same
sum). The folder can hold more files than the run reads, so the run's files are picked by name
from the hidden `"$path"` column: one scan, streaming into one Iceberg sink.

ONE CTAS. The table is dropped by Trino and made by one CREATE TABLE ... AS SELECT, one scan,
one statement. The catalog assigns its location (bench/trino.py `attach`), so Trino does not
stage the create -- the catalog refuses the commit that finishes a staged one. Trino's extended
statistics are off (setup), since the catalog also refuses a commit that carries them. Trino
writes column names in lower case. No partitioning, as for every engine.
"""

from __future__ import annotations

from bench import auth, scrub, trino
from bench.etl.config import TABLE, EtlConfig
from bench.etl.schema import COLUMNS, FILTER, numeric_columns

# The widest AEMO record in the landed files (candidate_engine.py CSV_MAX_WIDTH).
CSV_WIDTH = 120
FILES = "files"
CSV_TABLE = f"{FILES}.etl.aemo"


class TrinoIceberg:
    name = "trino_iceberg"

    def __init__(self, cfg: EtlConfig):
        self.cfg = cfg
        self._conn = None
        self._version = "unknown"

    @property
    def version(self) -> str:
        return self._version

    @property
    def qualified(self) -> str:
        return f"{trino.CATALOG}.{self.cfg.schema}.{TABLE[self.name]}"

    def setup(self) -> None:
        trino.start()
        self._conn = trino.connect()
        self._version = f"{trino.version(self._conn)} ({trino.IMAGE})"
        trino.attach(self._conn, self.cfg, auth.onelake_token())
        # NO EXTENDED STATISTICS ON WRITE. Trino's default puts Puffin statistics into the INSERT's
        # commit, and the catalog refuses that commit with a bare 400 "Malformed request"; without
        # them the commit is add-snapshot + set-snapshot-ref, which it accepts. No other engine
        # here writes Puffin statistics either.
        trino.sql(self._conn, "SET SESSION onelake.collect_extended_statistics_on_write = false")
        trino.create_catalog(
            self._conn,
            FILES,
            "hive",
            {
                "hive.metastore": "file",
                "hive.metastore.catalog.dir": "local:///trino-metastore",
                "fs.native-local.enabled": "true",
                "local.location": "/tmp",
                **trino.storage_properties(),
            },
        )
        declared = [c.lower() for c in COLUMNS] + [f"c{i}" for i in range(len(COLUMNS), CSV_WIDTH)]
        columns = ", ".join(f'"{c}" varchar' for c in declared)
        trino.sql(self._conn, f"CREATE SCHEMA IF NOT EXISTS {FILES}.etl")
        trino.sql(self._conn, f"DROP TABLE IF EXISTS {CSV_TABLE}")
        trino.sql(
            self._conn,
            f"CREATE TABLE {CSV_TABLE} ({columns}) "
            f"WITH (external_location = '{self.cfg.csv_abfss}', format = 'CSV', "
            "skip_header_line_count = 1)",
        )
        scrub.safe_print(f"  trino {self._version} attached; Files/csv as {CSV_TABLE}")

    def _select(self, files: list[str]) -> str:
        """The transform over the Hive table, restricted to exactly `files` by name."""
        names = ", ".join("'" + name.replace("'", "''") + "'" for name in files)
        where = " AND ".join(f"\"{c.lower()}\" = '{v}'" for c, v in FILTER)
        settled = "date_parse(\"settlementdate\", '%Y/%m/%d %H:%i:%s')"
        # NULLIF: Hive's CSV reader hands back an empty field as '', which CAST rejects; DuckDB's
        # reader, which this transform follows, reads it as NULL.
        return (
            'SELECT "unit", "duid", '
            + ", ".join(
                f'CAST(NULLIF("{c.lower()}", \'\') AS double) AS "{c.lower()}"'
                for c in numeric_columns()
            )
            + f", {settled} AS settlementdate, year({settled}) AS year"
            f" FROM {CSV_TABLE} WHERE {where}"
            f" AND element_at(split(\"$path\", '/'), -1) IN ({names})"
        )

    def load(self, files: list[str]) -> None:
        trino.sql(self._conn, f"DROP TABLE IF EXISTS {self.qualified}")
        trino.sql(self._conn, f"CREATE TABLE {self.qualified} AS {self._select(files)}")
        scrub.safe_print(f"    {len(files)} files in one scan, one CTAS")

    def row_count(self) -> int:
        return int(trino.sql(self._conn, f"SELECT count(*) FROM {self.qualified}")[0][0])

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None
