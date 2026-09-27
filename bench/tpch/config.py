"""The TPC-H side of the run configuration: engines, tables, the part plan, chDB's cache size.

All of this was bench/config.py until the ETL benchmark arrived. What both benchmarks share --
the OneLake endpoints, the catalog-cache lifetime, the DuckDB transport rule and `Config` itself
-- stayed there; what only the query benchmark needs is here.

`TpchConfig` IS a `bench.config.Config` plus the description of the suite, as class constants:
which tables, which SQL file, how many statements, which table carries the generation marker,
where results and docs go. The runner, the engines, the charts and the CI scripts read those off
the config they are handed and never import a TPC-H constant directly -- which is what lets
bench/tpcds/config.py describe the second query suite and run it through the same runner, the
same seven engines and the same charts.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from bench.config import SQL_DIR, Config

ENGINES = (
    "duckdb_iceberg",
    "chdb_iceberg",
    "polars_iceberg",
    "lakesail_iceberg",
    "daft_iceberg",
    "pyspark_iceberg",
    # Spark with Gluten/Velox underneath.
    "pyspark_gluten_iceberg",
    # A server rather than a library: the allin1 container, driven over MySQL (bench/starrocks.py).
    "starrocks_iceberg",
)

TABLES = ("lineitem", "orders", "partsupp", "part", "customer", "nation", "region", "supplier")

# The scale the headline docs are built at: bench.yml's default, and what README's charts show.
# A publish at SF=1 or SF=30 records its run and leaves docs/charts and docs/RESULTS.md alone, for
# the reason bench/etl/config.py gives at HEADLINE_FILES.
HEADLINE_SF = 10

# Approximate parquet MiB per scale factor unit, measured from a real tpchgen-cli run. Used only
# to choose a part count; being off by 30% moves a file from 200MB to 260MB and changes nothing.
PARQUET_MB_PER_SF = {
    "lineitem": 180,
    "orders": 40,
    "partsupp": 35,
    "customer": 12,
    "part": 7,
    "supplier": 1,
    "nation": 0,
    "region": 0,
}

# Target size of one uploaded parquet file.
#
# NOT the notebook's rule. Cell 9 used `scaled(base, floor) = max(floor, int(base * sf / 1000))`,
# tuned for SF=1000 on an 8-vCore Fabric node. At SF=10 it yields TWO lineitem parts of ~900MB
# each: file-level parallelism in every engine here collapses to 2, and each part's
# generate-then-upload cycle becomes a multi-minute serial stall because the semaphore can only
# overlap whole parts. Targeting a size instead keeps files at a shape readers like at every SF.
TARGET_PART_MB = 200

# Floors, so the smoke path still exercises multi-file reading. At SF=1 a size rule alone would
# give one part for everything and the SF=1 run would not resemble the SF=10 run it is screening.
PART_FLOOR = {"lineitem": 2, "orders": 2}
# 192, RAISED FROM 64 for SF=300: 64 would make lineitem parts ~850MB there. The data already in
# OneLake at SF<=100 is not regenerated (prepare is idempotent), so its layout does not move.
MAX_PARTS = 192


def parts_for(table: str, sf: int) -> int:
    """How many parts tpchgen-cli should split `table` into at scale factor `sf`."""
    want = math.ceil(sf * PARQUET_MB_PER_SF[table] / TARGET_PART_MB)
    return max(PART_FLOOR.get(table, 1), min(want, MAX_PARTS))


def parts_plan(sf: int) -> dict[str, int]:
    """Part counts for every table, in generation order.

    ORDER MATTERS and is load-bearing: `supplier` is last because it carries the
    generation-complete marker (see generate.py). Do not sort this dict.
    """
    return {t: parts_for(t, sf) for t in TABLES}


def estimated_gib(sf: int) -> float:
    """Total parquet the dataset occupies in OneLake, GiB."""
    return sum(PARQUET_MB_PER_SF.values()) * sf / 1024


def chdb_cache_gib(dataset_gib: float) -> int:
    """Size for chDB's filesystem cache, from the dataset's size in OneLake.

    The notebook asked for 150Gi, which was fine on a Fabric node and is 10x the runner's entire
    disk. ClickHouse does NOT check free space before filling this cache, so a max_size larger
    than the disk is an ENOSPC in the middle of a query rather than an eviction. 1.5x the dataset
    gives the warm run somewhere to hit; the clamp keeps it inside a 14GB disk with room for
    spills and the OS. Takes the estimate rather than a scale factor because a TPC-DS SF is not a
    TPC-H SF: each suite's config knows its own bytes-per-SF (`Config.estimated_gib`).
    """
    return max(2, min(math.ceil(dataset_gib * 1.5), 8))


@dataclass(frozen=True)
class TpchConfig(Config):
    """The TPC-H suite: `Config` plus the constants that describe the suite.

    Uppercase on purpose. These are the same for every instance and the CI publish job reads
    them off the CLASS -- it has no Fabric secrets, so it cannot build an instance through
    `from_env`.
    """

    TEST = "tpch"  # the `test` column in every results row
    TITLE = "TPC-H"
    SF_ENV = "TPCH_SF"
    # ONE COLD PASS, as TPC-DS: past SF=10 the data outgrows a 16 GB runner and a warm pass
    # measures little but a second read of OneLake, while doubling a run that the one-hour token
    # already bounds. The totals chart compares every scale the suite runs at.
    PASSES = ("cold",)
    TOTALS_SFS = (10, 30, 60, 100, 200, 300)
    HEADLINE_SF = HEADLINE_SF
    ENGINES = ENGINES
    TABLES = TABLES
    SQL_PATH = SQL_DIR / "tpch.sql"
    N_QUERIES = 22
    # Carries the generation-complete marker; last in generation order (see generate.py).
    MARKER_TABLE = "supplier"
    # smoke_catalog.py's two reads: Q1 scans lineitem whole, Q6 scans it filtered.
    PROBE_QUERIES = (1, 6)
    RESULTS_DIR = "results"
    DOCS_DIR = "docs"
    CSV = "docs/data/tpch_results.csv"

    @property
    def estimated_gib(self) -> float:
        return estimated_gib(self.sf)
