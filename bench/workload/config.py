"""Run configuration for the dbt workload test.

`WorkloadConfig` IS a `bench.config.Config`, so the Gluten session from bench/tpch
(`PysparkGlutenIceberg.setup()`) takes it unchanged. `sf` is unused by dbt; it stays at the
Config default and only sizes Spark's initial shuffle partitions there.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from bench.config import ICEBERG_ENDPOINT, Config

# The dbt targets, in run order. Same names as workload/profiles.yml and the engine folders under
# workload/models/aemo/.
ENGINES = ("duckdb", "polars", "gluten", "trino")

# bench's engine id for each target, so labels and colours match the rest of the page once it is
# shown there.
BENCH_ENGINE = {
    "duckdb": "duckdb_iceberg",
    "polars": "polars_iceberg",
    "gluten": "pyspark_gluten_iceberg",
    "trino": "trino_iceberg",
    "sail": "lakesail_iceberg",
}

# Targets workload_run_engine.py also runs, but only from their own probe workflow
# (workload_sail_probe.yml): never in workload.yml's matrix, never published.
PROBE_ENGINES = ("sail",)


# The landing zone, inside bench's lakehouse. Its own folder: the ETL's Files/csv holds only the
# daily CSVs, flat, while this project reads csv_raw/{daily,scada_today,price_today,duid} plus the
# archive-log parquet download_aemo.py keeps beside them.
FILES_DIR = "Files/dbt"

DEFAULT_PROCESS_LIMIT = 50


@dataclass(frozen=True)
class WorkloadConfig(Config):
    @property
    def files_relative(self) -> str:
        """The landing root relative to the workspace filesystem client."""
        return f"{self.lakehouse_id}/{FILES_DIR}"

    @property
    def files_abfss(self) -> str:
        """The landing root as every engine reads it: dbt's FILES_PATH."""
        return f"{self.base_path}/{FILES_DIR}"

    @property
    def schema(self) -> str:
        """Not one namespace: workload/macros/generate_schema_name.sql names dbt_<engine>_*."""
        return f"dbt_{self.engine or 'duckdb'}_mart"

    def dbt_env(self, token: str) -> dict[str, str]:
        """What workload/profiles.yml and the models read, besides process_limit."""
        return {
            "ONELAKE_TOKEN": token,
            "ONELAKE_ENDPOINT": ICEBERG_ENDPOINT,
            "WAREHOUSE_PATH": self.warehouse,
            "FILES_PATH": self.files_abfss,
            # dbt-polars' storage catalog over the same root (profiles.yml `landing`).
            "FILES_CONTAINER": self.workspace_id,
            "FILES_PREFIX": self.files_relative,
        }

    @classmethod
    def from_env(cls) -> WorkloadConfig:
        return cls(
            workspace_id=os.environ["FABRIC_WORKSPACE_ID"],
            lakehouse_id=os.environ["FABRIC_LAKEHOUSE_ID"],
            sf=10,
            engine=os.environ.get("BENCH_ENGINE", ""),
            run_id=os.environ.get("GITHUB_RUN_ID", "local"),
            run_url=os.environ.get("BENCH_RUN_URL", ""),
            git_sha=os.environ.get("GITHUB_SHA", "")[:7],
        )
