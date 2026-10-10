"""The TPC-DS side of the run configuration: the 24 tables, the namespace, where results go.

`TpcdsConfig` IS a `bench.config.Config` with the suite constants that bench/tpch/config.py's
`TpchConfig` documents, plus two things redefined:

* `schema` is `DS{sf:04d}` -- DS0001, DS0010 -- next to TPC-H's CH0010 in the same lakehouse, so
  the two suites can never read each other's tables.
* `from_env` reads TPCDS_SF, so tpcds.yml and bench.yml can be dispatched at different scales.

The engine list is NOT TPC-H's. Identifiers and colours are shared with it -- an engine looks the
same in every picture -- but `ENGINES` below is a subset, because two of TPC-H's cannot
answer TPC-DS at all. Which two, and on what evidence, is written there.
"""

from __future__ import annotations

from dataclasses import dataclass

from bench.config import SQL_DIR, Config

# THE ENGINES THAT FINISH. Two of TPC-H's are left out, on measurement, not taste -- run
# 35732997698 (SF=10) and 35732283791 (SF=1) are the evidence:
#
#   chdb_iceberg      ABORTS IN GLIBC on its first query: `pthread_mutex_lock.c:94 assertion
#                     failed: mutex->__data.__owner == 0`, exit 134, no result rows at all. Both
#                     at SF=1 and SF=10, and against tables written by two different writers, so
#                     it is chDB 4.4.0, not the data. TPC-H is unaffected -- chDB still runs there.
#   daft_iceberg      never ran here: TPC-H already excludes it from the query benchmark
#                     (Eventual-Inc/Daft#7532).
#
# Spark-OSS is slow -- bench/tpch/engines/pyspark_iceberg.py's `refresh` exists because of it --
# but it answers all 99, so it stays.
ENGINES = (
    "duckdb_iceberg",
    "pyspark_iceberg",
    # Spark with Gluten/Velox underneath. Velox reads OneLake with a one-hour SAS (the engine
    # module says why), so a TPC-DS run has to finish inside that hour -- which is why
    # TpcdsConfig runs one pass.
    "pyspark_gluten_iceberg",
    # StarRocks: added 2026-09-26 after passing candidate_engine.yml (22/22 TPC-H, OneLake reads
    # and writes). Q49 fails on a StarRocks bug (StarRocks#79807); everything else runs.
    "starrocks_iceberg",
    # Trino: added 2026-09-28. 89/99 at SF=1 on its first smoke; the ten were query text, not
    # Trino, and read the same on every engine once written as standard SQL (bench/tpcds/
    # rewrite.py `portable`).
    "trino_iceberg",
    # LakeSail: back 2026-09-30. 0.7.2 answers 99/99 at SF=10 (run 36652684303) once its
    # double-quoted aliases are backticked (bench/tpch/queries.py BACKTICK_ALIASES).
    "lakesail_iceberg",
    # Polars: back 2026-10-10. 2.0.0 OOM-killed the runner at SF=10 (Q72 over scan_iceberg,
    # pola-rs/polars#29768; then Q4 on main, #29822). The main build pinned in
    # requirements/polars_iceberg.txt answers 99/99 at SF=10 in 256 s cold (run 37943815509).
    "polars_iceberg",
)

# The 24 tables of the spec (dsdgen also emits `dbgen_version`, which is not one), LARGEST FIRST
# so that a generator or a writer that is going to fail on size fails in the first minutes, not
# the last. `web_site` is last on purpose: it carries the generation-complete marker (see
# bench/tpch/generate.py), and the marker must be the last thing written.
TABLES = (
    "store_sales",
    "inventory",
    "catalog_sales",
    "web_sales",
    "store_returns",
    "catalog_returns",
    "web_returns",
    "customer_demographics",
    "customer",
    "customer_address",
    "item",
    "date_dim",
    "time_dim",
    "promotion",
    "household_demographics",
    "catalog_page",
    "store",
    "web_page",
    "call_center",
    "warehouse",
    "reason",
    "ship_mode",
    "income_band",
    "web_site",
)

# Approximate parquet MiB per scale factor unit, for chDB's cache sizing only. TPC-DS at SF=10 is
# ~3 GB as parquet -- a little over TPC-H at the same SF -- and the clamp in chdb_cache_gib
# makes a 30% error here change nothing.
PARQUET_MB_PER_SF = 300

# The scale docs/tpcds/RESULTS.md and the per-query chart are built at: SF=100, the scale the
# results page opens at. ~30 GB of parquet does not fit a 16 GB runner, so the engines are
# measured reading OneLake, not their caches. tpcds.yml still defaults to SF=60.
HEADLINE_SF = 100


@dataclass(frozen=True)
class TpcdsConfig(Config):
    TEST = "tpcds"
    TITLE = "TPC-DS"
    SF_ENV = "TPCDS_SF"
    # ONE PASS, COLD, AT EVERY SCALE. Run 35942277983 (SF=60) is why: Gluten's one-hour SAS
    # expired 57 minutes in, at Q61 of the warm pass, and the 39 statements after it failed with
    # 401 Unauthorized. And past SF=10 the warm pass barely measures a cache anyway -- 20 GiB
    # does not fit a 16 GB runner, so DuckDB's warm was only 8% under its cold (1,046s -> 962s).
    # One pass also halves a run that was already the longest in the repo.
    PASSES = ("cold",)
    # The totals chart shows every scale the suite has been run at, the per-query chart only SF=100.
    TOTALS_SFS = (10, 30, 60, 100)
    # HARD QUERIES FIRST, so a run that is going to die on one dies in its first minutes, not two
    # hours in after 98 easy ones. Q64 and Q72 lead because they are the ones that end runs
    # (DuckDB SF=100 Q64 out of spill; LakeSail SF=60 Q72 out of spill; Spark-OSS 341 s on Q72 at
    # SF=10). The rest is each query's mean share of an engine's total time across the latest
    # SF=60/100 runs of every engine (2026-10-07): 23 4.4%, 4 4.1%, 14 3.7%, 19 3.6%, 67 3.3%,
    # 9 3.2%, 95 2.9%, 78 2.7%, 5 2.3%, 11 1.9%, 75 1.9%, 80 1.8%, 28 1.6%. Same order for every
    # engine, so the cache state each query meets is the same everywhere.
    HARD_FIRST = (64, 72, 23, 4, 14, 19, 67, 9, 95, 78, 5, 11, 75, 80, 28)
    HEADLINE_SF = HEADLINE_SF
    ENGINES = ENGINES
    TABLES = TABLES
    SQL_PATH = SQL_DIR / "tpcds.sql"
    N_QUERIES = 99
    MARKER_TABLE = "web_site"
    # smoke_catalog.py's two reads: Q3 and Q42 both scan store_sales, the biggest fact table and
    # the one whose data files a credential problem hides behind.
    PROBE_QUERIES = (3, 42)
    RESULTS_DIR = "results/tpcds"
    DOCS_DIR = "docs/tpcds"
    CSV = "docs/data/tpcds_results.csv"

    @property
    def schema(self) -> str:
        """Iceberg namespace holding the TPC-DS tables: DS0001, DS0010."""
        return f"DS{self.sf:04d}"

    @property
    def estimated_gib(self) -> float:
        return PARQUET_MB_PER_SF * self.sf / 1024
