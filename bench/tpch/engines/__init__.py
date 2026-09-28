"""Engine registry.

Imports are deferred into the factory on purpose: each engine's job installs ONLY its own
requirements file, so `import chdb` raises in the duckdb job and vice versa. Importing the four
modules eagerly here would make the package unimportable in every job.
"""

from __future__ import annotations

from bench.config import Config
from bench.tpch.config import ENGINES

__all__ = ["ENGINES", "get_engine"]


def get_engine(name: str, cfg: Config):
    """Construct the engine called `name`. The package it needs is imported only now."""
    if name == "duckdb_iceberg":
        from bench.tpch.engines.duckdb_iceberg import DuckDBIceberg

        return DuckDBIceberg(cfg)
    if name == "chdb_iceberg":
        from bench.tpch.engines.chdb_iceberg import ChdbIceberg

        return ChdbIceberg(cfg)
    if name == "polars_iceberg":
        from bench.tpch.engines.polars_iceberg import PolarsIceberg

        return PolarsIceberg(cfg)
    if name == "lakesail_iceberg":
        from bench.tpch.engines.lakesail_iceberg import LakesailIceberg

        return LakesailIceberg(cfg)
    if name == "pyspark_alluxio_iceberg":
        from bench.tpch.engines.pyspark_alluxio_iceberg import PysparkAlluxioIceberg

        return PysparkAlluxioIceberg(cfg)
    if name == "pyspark_gluten_iceberg":
        from bench.tpch.engines.pyspark_gluten_iceberg import PysparkGlutenIceberg

        return PysparkGlutenIceberg(cfg)
    if name == "daft_iceberg":
        from bench.tpch.engines.daft_iceberg import DaftIceberg

        return DaftIceberg(cfg)
    if name == "pyspark_iceberg":
        from bench.tpch.engines.pyspark_iceberg import PysparkIceberg

        return PysparkIceberg(cfg)
    if name == "starrocks_iceberg":
        from bench.tpch.engines.starrocks_iceberg import StarrocksIceberg

        return StarrocksIceberg(cfg)
    if name == "trino_iceberg":
        from bench.tpch.engines.trino_iceberg import TrinoIceberg

        return TrinoIceberg(cfg)
    raise ValueError(f"unknown engine {name!r}; expected one of {list(ENGINES)}")
