"""LakeSail: read the CSVs from OneLake, write Iceberg through Sail's own catalog integration.

Port of the notebook's `sail_clean_csv`, the other engine there that already wrote Iceberg --
`df.write.mode("append").format("iceberg").partitionBy("YEAR").saveAsTable("sail")` after a
`create schema if not exists` and a `use schema`. Kept, minus the `partitionBy` (no engine here
partitions; bench/etl/iceberg.py says why), plus a `DROP TABLE IF EXISTS` first so every run
creates and fills a fresh table rather than appending to last run's.

`filename` comes from `input_file_name()`, which Sail has from 0.7.3 (lakehq/sail#2571, sail#1210).

The session is the TPC-H engine's:
`bench.tpch.engines.lakesail_iceberg.LakesailIceberg.setup()` starts the in-process
Spark Connect server with the OneLake catalog in `SAIL_CATALOG__LIST` and the storage token
in `AZURE_STORAGE_TOKEN`, which is what signs both the CSV reads and the
parquet writes. Everything that docstring says about the token's lifetime and the server
outliving the script applies here unchanged.
"""

from __future__ import annotations

from bench import scrub
from bench.etl.config import TABLE, EtlConfig
from bench.etl.engines._spark_df import transform
from bench.tpch.engines.lakesail_iceberg import LakesailIceberg as _TpchSail


class LakesailIceberg:
    name = "lakesail_iceberg"

    def __init__(self, cfg: EtlConfig):
        self.cfg = cfg
        self._inner = _TpchSail(cfg)
        self._spark = None

    @property
    def version(self) -> str:
        return self._inner.version

    @property
    def table(self) -> str:
        return TABLE[self.name]

    def setup(self) -> None:
        self._inner.setup()
        self._spark = self._inner.session
        # Sail's session time zone defaults to the host's, which a GitHub runner spells
        # "Etc/UTC" -- and Sail's Iceberg writer accepts only "UTC" or "+00:00" for a
        # timestamptz column: `Unsupported timezone for Iceberg Timestamptz conversion: Etc/UTC`
        # on the first write. The Fabric notebook never saw this because its session was "UTC".
        self._spark.conf.set("spark.sql.session.timeZone", "UTC")

    def load(self, files: list[str]) -> None:
        uris = [f"{self.cfg.csv_abfss}/{name}" for name in files]
        self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.cfg.schema}")
        self._spark.sql(f"USE SCHEMA {self.cfg.schema}")
        self._spark.sql(f"DROP TABLE IF EXISTS {self.table}")
        df = transform(self._spark, uris, filename=True)
        df.write.mode("append").format("iceberg").saveAsTable(self.table)
        scrub.safe_print(f"    {self.cfg.schema}.{self.table} written in one append")

    def row_count(self) -> int:
        # count(filename) shows input_file_name() filled every row; outside the timed load.
        rows, named, sample = self._spark.sql(
            f"SELECT count(*), count(filename), min(filename) FROM {self.cfg.schema}.{self.table}"
        ).collect()[0]
        scrub.safe_print(f"    filename set on {named} of {rows} rows, e.g. {sample}")
        return rows

    def close(self) -> None:
        self._inner.close()
        self._spark = None
