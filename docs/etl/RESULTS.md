# Light ETL results

1000 AEMO daily CSV files read from OneLake, filtered, cast and written as one Iceberg table per engine, on 4 vCPU / 15.6 GB (linux-6.17.0-1022-azure, Python 3.12.14).

Last run: `2026-09-28T01:02:00Z` · commit `e0912e0` · [Actions run](https://github.com/djouallah/lakehouse_benchmark/actions/runs/36364362101)

## Per engine

| Engine | Version | Load | Attach | Runs | Rows | Error |
|---|---|---:|---:|---:|---:|---|
| Polars | `2.0.0-rc.2` | 470.8s | 1.3s | 3 | 149,146,763 | — |
| DuckDB | `2.0.0.dev2609121639` | 497.4s | 4.6s | 3 | 149,146,763 | — |
| LakeSail | `0.7.1` | 684.5s | 0.7s | 3 | 149,146,763 | — |
| Gluten/Velox | `4.1.1 + iceberg Apache Iceberg 1.11.0 (commit 6976e020b894f6a6777704df2b8c4458cb291ae9)` | 728.8s | 57.0s | 1 | 149,146,763 | — |
| Daft | `0.7.25` | 757.9s | 1.6s | 3 | 149,146,763 | — |
| StarRocks | `4.1.4-4a9848e (starrocks/allin1-ubuntu:4.1-latest)` | 845.7s | 20.8s | 1 | 149,146,763 | — |
| chDB | `4.4.0` | 878.1s | 1.7s | 3 | 149,146,763 | — |
| Trino | `483 (trinodb/trino:483)` | 892.3s | 18.5s | 1 | 149,146,763 | — |
| Spark-OSS | `4.1.3 + iceberg Apache Iceberg 1.11.0 (commit 6976e020b894f6a6777704df2b8c4458cb291ae9)` | 982.2s | 15.4s | 3 | 149,146,763 | — |

Each engine is the mean of its own last 3 runs at this file count, or of as many as it has (Runs). Load = drop and create the table, read the CSVs, transform, write, commit. Attach = session start and catalog attach, timed separately and excluded from Load. Rows is the count read back from the table after the latest run; every engine applies the same filter, so they agree.

Gluten/Velox: Velox does not read the CSVs. Open-source Gluten has no CSV reader on Spark 4.x, so plain Spark reads and parses them, and Velox runs only the filter, casts and Parquet write after that.

## History

72 timed rows across 11 runs.
Raw data: one immutable JSON per run under [`results/etl/`](../../results/etl/), flattened to [`data/etl_results.csv`](../data/etl_results.csv).

