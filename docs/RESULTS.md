# Results

TPC-H-like, scale factor 100, 22 queries, on 4 vCPU / 15.6 GB (linux-6.17.0-1022-azure, Python 3.12.15).

Last run: `2026-10-09T01:56:21Z` · commit `e55b56f` · [Actions run](https://github.com/djouallah/lakehouse_benchmark/actions/runs/37872115807)

## Latest run

Each engine's most recent run at this scale; the newest run may not include every engine.

| Engine | Version | Cold total | Attach | Failed queries |
|---|---|---:|---:|---|
| DuckDB | `2.0.0.dev2609222040` | 499.9s | 6.4s | — |
| Polars | `2.0.0` | 588.7s | 2.3s | — |
| StarRocks | `4.1.4-4a9848e (starrocks/allin1-ubuntu:4.1-latest)` | 688.2s | 23.2s | — |
| Gluten/Velox | `4.1.1 + iceberg Apache Iceberg 1.11.0 (commit 6976e020b894f6a6777704df2b8c4458cb291ae9)` | 1,007.5s | 18.9s | — |
| LakeSail | `0.7.2` | 2,662.7s | 1.6s | — |
| Spark-OSS | `4.1.3 + iceberg Apache Iceberg 1.11.0 (commit 6976e020b894f6a6777704df2b8c4458cb291ae9)` | — | 17.6s | Q21 |
| Trino | `483 (trinodb/trino:483)` | — | 12.2s | Q18 |

One cold pass, the first after attaching the catalog. Attach is timed separately and excluded from the total.

## Failures

| Engine | Pass | Query | Error |
|---|---|---|---|
| Trino | cold | Q18 | `TrinoQueryError: TrinoQueryError(type=INSUFFICIENT_RESOURCES, name=EXCEEDED_LOCAL_MEMORY_LIMIT, message="Query exceeded per-node memory limit of 9GB [Allocated: 6.76GB, Delta: 3.40GB, Top Consumers: {HashBuilderOperator=6.76GB, MergeSortedPages=2.94MB, MergingHashAggregationBuilder=1.69MB}]", query_` |
| Spark-OSS | cold | Q21 | `Py4JJavaError: An error occurred while calling o103.collectToPython. : org.apache.spark.SparkException: [STAGE_MATERIALIZATION_MULTIPLE_FAILURES] Multiple failures (2) in stage materialization:    1. SparkException: Not enough memory to build and broadcast the table to all worker nodes. As a workaro` |

## Per query, latest run

Seconds, cold pass. `—` means the query failed or did not run; see Failures above.

| Query | DuckDB | Polars | StarRocks | Gluten/Velox | LakeSail | Spark-OSS | Trino |
|---|---|---|---|---|---|---|---|
| Q1 | 30.22 | 27.01 | 78.26 | 46.84 | 33.79 | 253.18 | 55.44 |
| Q2 | 7.28 | 12.21 | 104.91 | 24.51 | 46.11 | 62.04 | 26.93 |
| Q3 | 26.50 | 17.48 | 26.61 | 31.72 | 81.76 | 129.08 | 76.76 |
| Q4 | 10.25 | 8.63 | 16.38 | 33.03 | 28.05 | 93.04 | 35.30 |
| Q5 | 12.98 | 15.06 | 27.07 | 66.92 | 285.64 | 212.59 | 67.24 |
| Q6 | 2.77 | 6.86 | 4.80 | 9.18 | 20.29 | 39.68 | 26.52 |
| Q7 | 6.65 | 14.77 | 39.61 | 87.12 | 780.13 | 213.21 | 234.38 |
| Q8 | 10.61 | 16.46 | 17.65 | 75.26 | 254.91 | 203.78 | 90.00 |
| Q9 | 35.86 | 27.61 | 45.07 | 118.86 | 305.59 | 303.02 | 113.26 |
| Q10 | 22.53 | 18.30 | 27.58 | 31.64 | 52.24 | 91.50 | 37.63 |
| Q11 | 2.80 | 2.31 | 5.13 | 13.35 | 28.40 | 37.15 | 24.97 |
| Q12 | 16.15 | 11.25 | 14.08 | 23.64 | 32.13 | 83.56 | 26.04 |
| Q13 | 19.80 | 19.06 | 24.33 | 24.31 | 20.26 | 81.48 | 48.76 |
| Q14 | 17.51 | 24.82 | 11.88 | 16.83 | 22.99 | 54.11 | 16.84 |
| Q15 | 9.65 | 18.22 | 9.47 | 25.20 | 21.99 | 113.07 | 14.78 |
| Q16 | 3.02 | 2.80 | 4.41 | 9.49 | 10.33 | 25.87 | 10.87 |
| Q17 | 108.16 | 70.29 | 19.89 | 97.89 | 164.49 | 365.33 | 81.27 |
| Q18 | 19.81 | 102.42 | 104.72 | 87.75 | 169.32 | 270.16 | — |
| Q19 | 9.98 | 23.72 | 10.22 | 14.64 | 36.37 | 71.73 | 26.91 |
| Q20 | 19.97 | 28.33 | 9.54 | 23.07 | 57.19 | 78.18 | 59.70 |
| Q21 | 98.86 | 115.60 | 80.07 | 134.17 | 199.26 | — | 185.71 |
| Q22 | 8.53 | 5.49 | 6.54 | 12.07 | 11.43 | 47.68 | 23.26 |

## History

1,541 timed statements across 49 runs.
Raw data: one immutable JSON per run under [`results/`](../results/), flattened to [`data/tpch_results.csv`](data/tpch_results.csv).

