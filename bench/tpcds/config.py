"""The TPC-DS side of the run configuration: the 24 tables, the namespace, where results go.

`TpcdsConfig` IS a `bench.config.Config` with the suite constants that bench/tpch/config.py's
`TpchConfig` documents, plus two things redefined:

* `schema` is `DS{sf:04d}` -- DS0001, DS0010 -- next to TPC-H's CH0010 in the same lakehouse, so
  the two suites can never read each other's tables.
* `from_env` reads TPCDS_SF, so tpcds.yml and bench.yml can be dispatched at different scales.

The engine list is NOT TPC-H's. Identifiers and colours are shared with it -- an engine looks the
same in every picture -- but `ENGINES` below is a subset, because three of TPC-H's seven cannot
answer TPC-DS at the headline scale. Which three, and on what evidence, is written there.
"""

from __future__ import annotations

from dataclasses import dataclass

from bench.config import SQL_DIR, Config

# THE ENGINES THAT FINISH. TPC-H runs all seven; TPC-DS runs these three. Four of the rest were
# dropped on measurement, not taste -- run 35732997698 (SF=10) and 35732283791 (SF=1) are the
# evidence, and every number below is from the SF=10 run over identical OneLake tables.
#
#   chdb_iceberg      ABORTS IN GLIBC on its first query: `pthread_mutex_lock.c:94 assertion
#                     failed: mutex->__data.__owner == 0`, exit 134, no result rows at all. Both
#                     at SF=1 and SF=10, and against tables written by two different writers, so
#                     it is chDB 4.4.0, not the data. TPC-H is unaffected -- chDB still runs there.
#   polars_iceberg    KILLED THE RUNNER. 55 minutes in, the job died with "the hosted runner lost
#                     communication with the server", which on a 15.6 GB box after an hour of
#                     query memory is an OOM. (At SF=1 it finishes, 91/99, and loses q13/48/49/85
#                     to pola-rs/polars#29449 -- negative decimal bounds decoded as unsigned.)
#   lakesail_iceberg  38 MINUTES COLD for 90 of 99 statements, then 50 of 99 failed warm. Its
#                     parser rejects the spec's double-quoted aliases (`AS "order count"`), which
#                     is 8 statements at SF=1 already; the rest is scale. On 0.7.2 (run
#                     36648458503) cold is 24 minutes and Q71 passes (lakehq/sail#2642), and
#                     bench/tpch/queries.py now backticks those aliases for it (BACKTICK_ALIASES).
#   daft_iceberg      never ran here: TPC-H already excludes it from the query benchmark
#                     (Eventual-Inc/Daft#7532).
#
# What is left is DuckDB and Spark. Spark is slow --
# ~42 min cold, and bench/tpch/engines/pyspark_iceberg.py's `refresh` exists because of it -- but
# it is the only non-DuckDB engine that answers all 99, so dropping it would leave one engine
# measured against itself.
ENGINES = (
    "duckdb_iceberg",
    "pyspark_iceberg",
    # Spark with Gluten/Velox underneath. Velox reads OneLake with a one-hour SAS (the engine
    # module says why), so a TPC-DS run has to finish inside that hour -- which is why
    # TpcdsConfig runs one pass.
    "pyspark_gluten_iceberg",
    # StarRocks: added 2026-09-26 after passing candidate_engine.yml (22/22 TPC-H, OneLake reads
    # and writes). Its TPC-DS coverage is whatever its first runs show.
    "starrocks_iceberg",
    # Trino: added 2026-09-28. 89/99 at SF=1 on its first smoke; the ten were query text, not
    # Trino, and read the same on every engine once written as standard SQL (bench/tpcds/
    # rewrite.py `portable`).
    "trino_iceberg",
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

# The scale the headline docs are built at, and tpcds.yml's default. SF=60, the largest a 16 GB
# runner gets through: 20 GiB of parquet that does not fit in memory, so the engines are measured
# reading OneLake, not their caches. SF=10, 30 and 100 are charted too, in the totals chart only.
HEADLINE_SF = 60


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
    # The totals chart shows every scale the suite has been run at, the per-query chart only SF=60.
    TOTALS_SFS = (10, 30, 60, 100)
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
