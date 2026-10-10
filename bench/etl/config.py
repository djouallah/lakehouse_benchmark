"""Run configuration for the Light ETL benchmark.

REPLACES cells 2 and 4 of the ETL notebook: `engine` / `total_files`, the Fabric mount paths
(`/lakehouse/default/Files/{zip,csv}`), the workspace and lakehouse GUIDs, and
`schema = f'T{total_files}'`.

`EtlConfig` IS a `bench.config.Config` with two things redefined, so everything that takes a
Config -- `auth.catalog`, `onelake.table_root`, and the TPC-H Spark and Sail engines whose
`setup()` the ETL engines reuse -- works unchanged:

* `sf` is the NUMBER OF CSV FILES, not a TPC-H scale factor. It was the notebook's one input.
* `schema` is `T{n}`, unpadded, exactly as the notebook spelled it. A Fabric notebook pointed at
  the same lakehouse writes into the same namespace and reads the same `Files/csv`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from bench.config import ONELAKE_BLOB, Config, _env_int
from bench.tpch.config import estimated_gib as _tpch_estimated_gib

# The notebook's engines plus Spark and Gluten/Velox, minus DataFusion (not a public-facing
# engine the way the others are; dropped). Same identifiers as bench.tpch.config.ENGINES, so
# bench/charts.py's labels and colours apply to both benchmarks.
ETL_ENGINES = (
    "duckdb_iceberg",
    "chdb_iceberg",
    "polars_iceberg",
    "daft_iceberg",
    "lakesail_iceberg",
    "pyspark_iceberg",
    "pyspark_gluten_iceberg",
    "starrocks_iceberg",
    "trino_iceberg",
)

# The Iceberg table each engine writes inside namespace T{n}. The notebook's names, kept.
TABLE = {
    "duckdb_iceberg": "duckdb",
    "chdb_iceberg": "chdb",
    "polars_iceberg": "polars",
    "daft_iceberg": "daft",
    "lakesail_iceberg": "sail",
    "pyspark_iceberg": "spark",
    "pyspark_gluten_iceberg": "gluten",
    "starrocks_iceberg": "starrocks",
    "trino_iceberg": "trino",
}

# Where the landed CSVs live inside the lakehouse. The notebook's `/lakehouse/default/Files/csv/`.
CSV_DIR = "Files/csv"

DEFAULT_FILES = 100

# The file count the headline docs are built at. docs/etl/charts/totals.png is the 1000-file
# chart and the path never changes, so a publish at any other count records its run
# (results/etl/, the CSV) and leaves the charts and RESULTS.md alone. Before this guard a FILES=100
# check-run overwrote the 1000-file chart with a 100-file one under the same name (run
# 35591414961), and the blog that embeds it and the table under it disagreed.
HEADLINE_FILES = 1000


@dataclass(frozen=True)
class EtlConfig(Config):
    @property
    def files(self) -> int:
        """`sf`, under the name the notebook used."""
        return self.sf

    @property
    def schema(self) -> str:
        """Iceberg namespace holding this run's tables: T10, T100, T1000. Notebook parity."""
        return f"T{self.sf}"

    @property
    def estimated_gib(self) -> float:
        """What chDB's filesystem cache is sized from (bench/tpch/config.py: chdb_cache_gib).

        The ETL never had an estimate of its own: it passed its FILE COUNT through TPC-H's
        per-SF formula, which lands the cache on the 8 GiB clamp at 100 and 1000 files. Kept as
        that exact number so the ETL's chDB runs with the cache it always has.
        """
        return _tpch_estimated_gib(self.sf)

    @property
    def csv_relative(self) -> str:
        """`Files/csv` relative to the workspace filesystem client (below the lakehouse id)."""
        return f"{self.lakehouse_id}/{CSV_DIR}"

    @property
    def csv_abfss(self) -> str:
        """`abfss://` prefix of the landed CSVs, for every engine that speaks object storage."""
        return f"{self.base_path}/{CSV_DIR}"

    @property
    def csv_az(self) -> str:
        """`az://<workspace>/<lakehouse>/Files/csv`: the same files in the one Azure URI form Daft
        parses correctly on OneLake (see engines/daft_iceberg.py)."""
        return f"az://{self.workspace_id}/{self.lakehouse_id}/{CSV_DIR}"

    @property
    def csv_https(self) -> str:
        """Plain-HTTPS prefix of the same files, for chDB's `url()` (engines/chdb_iceberg.py)."""
        return f"https://{ONELAKE_BLOB}/{self.workspace_id}/{self.lakehouse_id}/{CSV_DIR}"

    @classmethod
    def from_env(cls) -> EtlConfig:
        return cls(
            workspace_id=os.environ["FABRIC_WORKSPACE_ID"],
            lakehouse_id=os.environ["FABRIC_LAKEHOUSE_ID"],
            sf=_env_int("ETL_FILES", DEFAULT_FILES),
            engine=os.environ.get("BENCH_ENGINE", ""),
            run_id=os.environ.get("GITHUB_RUN_ID", "local"),
            run_url=os.environ.get("BENCH_RUN_URL", ""),
            git_sha=os.environ.get("GITHUB_SHA", "")[:7],
        )
