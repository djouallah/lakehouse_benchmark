# Results

TPC-H-like, scale factor 10, 22 queries, on 4 vCPU / 15.6 GB (linux-6.17.0-1022-azure, Python 3.12.14).

Last run: `2026-09-28T01:20:57Z` · commit `e0912e0` · [Actions run](https://github.com/djouallah/lakehouse_benchmark/actions/runs/36364367814)

## Latest run

Each engine's most recent run at this scale; the newest run may not include every engine.

| Engine | Version | Cold total | Attach | Failed queries |
|---|---|---:|---:|---|
| DuckDB | `2.0.0.dev2609121639` | 44.3s | 7.1s | — |
| StarRocks | `4.1.4-4a9848e (starrocks/allin1-ubuntu:4.1-latest)` | 77.2s | 24.9s | — |
| Polars | `2.0.0-rc.2` | 100.4s | 7.5s | — |
| chDB | `4.4.0` | 147.8s | 2.5s | — |
| Gluten/Velox | `4.1.1 + iceberg Apache Iceberg 1.11.0 (commit 6976e020b894f6a6777704df2b8c4458cb291ae9)` | 160.4s | 24.7s | — |
| LakeSail | `0.7.1` | 222.6s | 2.4s | — |
| Trino | `483 (trinodb/trino:483)` | 249.6s | 12.9s | — |
| Spark-OSS | `4.1.3 + iceberg Apache Iceberg 1.11.0 (commit 6976e020b894f6a6777704df2b8c4458cb291ae9)` | 429.8s | 18.9s | — |

One cold pass, the first after attaching the catalog. Attach is timed separately and excluded from the total.

## Per query, latest run

Seconds, cold pass. `—` means the query failed; see Failures above.

| Query | DuckDB | StarRocks | Polars | chDB | Gluten/Velox | LakeSail | Trino | Spark-OSS |
|---|---|---|---|---|---|---|---|---|
| Q1 | 8.68 | 15.21 | 11.28 | 11.34 | 20.91 | 13.60 | 15.36 | 42.51 |
| Q2 | 4.08 | 5.32 | 5.80 | 11.65 | 9.10 | 11.90 | 6.23 | 10.18 |
| Q3 | 5.50 | 6.74 | 3.78 | 9.13 | 13.28 | 9.68 | 8.97 | 37.80 |
| Q4 | 2.01 | 2.48 | 2.57 | 4.45 | 6.92 | 6.09 | 5.75 | 11.32 |
| Q5 | 2.45 | 4.96 | 3.92 | 6.51 | 9.77 | 11.92 | 6.69 | 26.32 |
| Q6 | 0.50 | 1.10 | 1.99 | 1.53 | 2.15 | 5.56 | 3.67 | 7.42 |
| Q7 | 0.96 | 2.38 | 10.42 | 17.65 | 5.43 | 11.22 | 20.44 | 40.10 |
| Q8 | 1.62 | 4.99 | 7.11 | 8.05 | 10.50 | 14.12 | 6.33 | 23.74 |
| Q9 | 2.13 | 3.45 | 7.70 | 7.18 | 10.10 | 13.18 | 17.86 | 31.40 |
| Q10 | 1.86 | 3.11 | 5.38 | 4.70 | 5.42 | 10.55 | 27.54 | 13.34 |
| Q11 | 0.15 | 1.06 | 3.13 | 3.81 | 2.22 | 6.05 | 1.50 | 2.92 |
| Q12 | 0.78 | 4.22 | 1.61 | 2.58 | 5.00 | 7.45 | 29.48 | 9.74 |
| Q13 | 1.85 | 2.98 | 4.17 | 3.49 | 3.19 | 4.20 | 27.73 | 11.42 |
| Q14 | 0.83 | 1.17 | 2.10 | 1.99 | 2.50 | 6.80 | 4.37 | 7.43 |
| Q15 | 0.62 | 1.73 | 1.43 | 2.83 | 4.70 | 12.12 | 6.45 | 19.78 |
| Q16 | 0.26 | 0.70 | 0.81 | 2.00 | 1.88 | 3.66 | 2.63 | 5.05 |
| Q17 | 2.35 | 2.94 | 5.23 | 4.24 | 11.22 | 12.87 | 11.21 | 36.76 |
| Q18 | 1.48 | 4.95 | 7.71 | 25.79 | 10.78 | 16.38 | 12.73 | 31.93 |
| Q19 | 0.92 | 1.33 | 2.12 | 2.60 | 4.23 | 9.18 | 8.13 | 9.18 |
| Q20 | 1.12 | 1.22 | 3.43 | 4.04 | 3.66 | 11.23 | 5.34 | 9.04 |
| Q21 | 3.55 | 4.45 | 7.67 | 9.32 | 15.53 | 19.56 | 19.23 | 37.19 |
| Q22 | 0.55 | 0.68 | 1.07 | 2.89 | 1.93 | 5.27 | 1.94 | 5.27 |

## History

1,173 timed statements across 33 runs.
Raw data: one immutable JSON per run under [`results/`](../results/), flattened to [`data/tpch_results.csv`](data/tpch_results.csv).

