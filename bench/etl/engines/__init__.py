"""ETL engine registry.

Imports are deferred into the factory, as in bench/tpch/engines: each job installs
only its own requirements file, so an eager import of any engine module would make the
package unimportable in every other job.
"""

from __future__ import annotations

from bench.etl.config import ETL_ENGINES, EtlConfig

__all__ = ["ETL_ENGINES", "get_engine"]


def get_engine(name: str, cfg: EtlConfig):
    """Construct the ETL engine called `name`. The package it needs is imported only now."""
    if name == "duckdb_iceberg":
        from bench.etl.engines.duckdb_iceberg import DuckDBIceberg

        return DuckDBIceberg(cfg)
    if name == "chdb_iceberg":
        from bench.etl.engines.chdb_iceberg import ChdbIceberg

        return ChdbIceberg(cfg)
    if name == "polars_iceberg":
        from bench.etl.engines.polars_iceberg import PolarsIceberg

        return PolarsIceberg(cfg)
    if name == "daft_iceberg":
        from bench.etl.engines.daft_iceberg import DaftIceberg

        return DaftIceberg(cfg)
    if name == "lakesail_iceberg":
        from bench.etl.engines.lakesail_iceberg import LakesailIceberg

        return LakesailIceberg(cfg)
    if name == "pyspark_iceberg":
        from bench.etl.engines.pyspark_iceberg import PysparkIceberg

        return PysparkIceberg(cfg)
    if name == "pyspark_gluten_iceberg":
        from bench.etl.engines.pyspark_gluten_iceberg import PysparkGlutenIceberg

        return PysparkGlutenIceberg(cfg)
    if name == "starrocks_iceberg":
        from bench.etl.engines.starrocks_iceberg import StarrocksIceberg

        return StarrocksIceberg(cfg)
    if name == "trino_iceberg":
        from bench.etl.engines.trino_iceberg import TrinoIceberg

        return TrinoIceberg(cfg)
    raise ValueError(f"unknown engine {name!r}; expected one of {list(ETL_ENGINES)}")
