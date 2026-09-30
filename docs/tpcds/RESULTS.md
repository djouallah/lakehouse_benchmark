# Results

TPC-DS-like, scale factor 60, 99 queries, on 4 vCPU / 15.6 GB (linux-6.17.0-1022-azure, Python 3.12.14).

Last run: `2026-09-30T02:13:18Z` · commit `93b065f` · [Actions run](https://github.com/djouallah/lakehouse_benchmark/actions/runs/36658601681)

## Latest run

Each engine's most recent run at this scale; the newest run may not include every engine.

| Engine | Version | Cold total | Attach | Failed queries |
|---|---|---:|---:|---|
| DuckDB | `2.0.0.dev2609222040` | 1,367.0s | 6.7s | — |
| Gluten/Velox | `4.1.1 + iceberg Apache Iceberg 1.11.0 (commit 6976e020b894f6a6777704df2b8c4458cb291ae9)` | 2,117.1s | 39.3s | — |
| Trino | `483 (trinodb/trino:483)` | 2,470.8s | 12.9s | — |
| LakeSail | `0.7.2` | 3,688.5s | 1.4s | Q32, Q33, Q34, Q35, Q36, Q37, Q38, Q39, Q40, Q41, Q42, Q43, Q44, Q45, Q46, Q47, Q48, Q49, Q50, Q51, Q52, Q53, Q54, Q55, Q56, Q57, Q58, Q59, Q60, Q61, Q62, Q63, Q64, Q65, Q66, Q67, Q68, Q69, Q70, Q71, Q72, Q73, Q74, Q75, Q76, Q77, Q78, Q79, Q80, Q81, Q82, Q83, Q84, Q85, Q86, Q87, Q88, Q89, Q90, Q91, Q92, Q93, Q94, Q95, Q96, Q97, Q98, Q99 |
| Spark-OSS | `4.1.3 + iceberg Apache Iceberg 1.11.0 (commit 6976e020b894f6a6777704df2b8c4458cb291ae9)` | 9,303.4s | 8.7s | — |

One cold pass, the first after attaching the catalog. Attach is timed separately and excluded from the total.

## Failures

| Engine | Pass | Query | Error |
|---|---|---|---|
| LakeSail | cold | Q32 | `AnalysisException: Failed to load table DS0060.catalog_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q33 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q34 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q35 | `AnalysisException: Failed to load table DS0060.customer: response error: status code 400 Bad Request` |
| LakeSail | cold | Q36 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q37 | `AnalysisException: Failed to load table DS0060.item: response error: status code 400 Bad Request` |
| LakeSail | cold | Q38 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q39 | `AnalysisException: Failed to load table DS0060.inventory: response error: status code 400 Bad Request` |
| LakeSail | cold | Q40 | `AnalysisException: Failed to load table DS0060.catalog_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q41 | `AnalysisException: Failed to load table DS0060.item: response error: status code 400 Bad Request` |
| LakeSail | cold | Q42 | `AnalysisException: Failed to load table DS0060.date_dim: response error: status code 400 Bad Request` |
| LakeSail | cold | Q43 | `AnalysisException: Failed to load table DS0060.date_dim: response error: status code 400 Bad Request` |
| LakeSail | cold | Q44 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q45 | `AnalysisException: Failed to load table DS0060.web_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q46 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q47 | `AnalysisException: Failed to load table DS0060.item: response error: status code 400 Bad Request` |
| LakeSail | cold | Q48 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q49 | `AnalysisException: Failed to load table DS0060.web_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q50 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q51 | `AnalysisException: Failed to load table DS0060.web_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q52 | `AnalysisException: Failed to load table DS0060.date_dim: response error: status code 400 Bad Request` |
| LakeSail | cold | Q53 | `AnalysisException: Failed to load table DS0060.item: response error: status code 400 Bad Request` |
| LakeSail | cold | Q54 | `AnalysisException: Failed to load table DS0060.catalog_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q55 | `AnalysisException: Failed to load table DS0060.date_dim: response error: status code 400 Bad Request` |
| LakeSail | cold | Q56 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q57 | `AnalysisException: Failed to load table DS0060.item: response error: status code 400 Bad Request` |
| LakeSail | cold | Q58 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q59 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q60 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q61 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q62 | `AnalysisException: Failed to load table DS0060.web_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q63 | `AnalysisException: Failed to load table DS0060.item: response error: status code 400 Bad Request` |
| LakeSail | cold | Q64 | `AnalysisException: Failed to load table DS0060.catalog_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q65 | `AnalysisException: Failed to load table DS0060.store: response error: status code 400 Bad Request` |
| LakeSail | cold | Q66 | `AnalysisException: Failed to load table DS0060.web_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q67 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q68 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q69 | `AnalysisException: Failed to load table DS0060.customer: response error: status code 400 Bad Request` |
| LakeSail | cold | Q70 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q71 | `AnalysisException: Failed to load table DS0060.item: response error: status code 400 Bad Request` |
| LakeSail | cold | Q72 | `AnalysisException: Failed to load table DS0060.catalog_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q73 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q74 | `AnalysisException: Failed to load table DS0060.customer: response error: status code 400 Bad Request` |
| LakeSail | cold | Q75 | `AnalysisException: Failed to load table DS0060.catalog_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q76 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q77 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q78 | `AnalysisException: Failed to load table DS0060.web_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q79 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q80 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q81 | `AnalysisException: Failed to load table DS0060.catalog_returns: response error: status code 400 Bad Request` |
| LakeSail | cold | Q82 | `AnalysisException: Failed to load table DS0060.item: response error: status code 400 Bad Request` |
| LakeSail | cold | Q83 | `AnalysisException: Failed to load table DS0060.store_returns: response error: status code 400 Bad Request` |
| LakeSail | cold | Q84 | `AnalysisException: Failed to load table DS0060.customer: response error: status code 400 Bad Request` |
| LakeSail | cold | Q85 | `AnalysisException: Failed to load table DS0060.web_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q86 | `AnalysisException: Failed to load table DS0060.web_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q87 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q88 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q89 | `AnalysisException: Failed to load table DS0060.item: response error: status code 400 Bad Request` |
| LakeSail | cold | Q90 | `AnalysisException: Failed to load table DS0060.web_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q91 | `AnalysisException: Failed to load table DS0060.call_center: response error: status code 400 Bad Request` |
| LakeSail | cold | Q92 | `AnalysisException: Failed to load table DS0060.web_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q93 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q94 | `AnalysisException: Failed to load table DS0060.web_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q95 | `AnalysisException: Failed to load table DS0060.web_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q96 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q97 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q98 | `AnalysisException: Failed to load table DS0060.store_sales: response error: status code 400 Bad Request` |
| LakeSail | cold | Q99 | `AnalysisException: Failed to load table DS0060.catalog_sales: response error: status code 400 Bad Request` |

## Per query, latest run

Seconds, cold pass. `—` means the query failed; see Failures above.

| Query | DuckDB | Gluten/Velox | Trino | LakeSail | Spark-OSS |
|---|---|---|---|---|---|
| Q1 | 11.26 | 21.01 | 20.58 | 20.42 | 29.48 |
| Q2 | 15.93 | 31.22 | 43.31 | 44.41 | 34.46 |
| Q3 | 30.28 | 36.28 | 58.73 | 39.03 | 66.95 |
| Q4 | 20.00 | 93.60 | 112.03 | 656.55 | 236.89 |
| Q5 | 18.00 | 73.70 | 53.20 | 137.18 | 125.34 |
| Q6 | 5.87 | 7.70 | 6.91 | 54.58 | 56.32 |
| Q7 | 9.62 | 36.04 | 23.73 | 194.34 | 83.49 |
| Q8 | 6.06 | 3.66 | 7.81 | 39.80 | 33.41 |
| Q9 | 77.04 | 40.72 | 46.44 | 36.10 | 468.02 |
| Q10 | 7.69 | 10.28 | 6.89 | 63.01 | 49.52 |
| Q11 | 16.00 | 30.63 | 64.87 | 378.63 | 134.89 |
| Q12 | 2.01 | 2.70 | 6.45 | 19.03 | 17.27 |
| Q13 | 17.32 | 31.93 | 23.35 | 43.53 | 84.89 |
| Q14 | 39.09 | 60.94 | 167.23 | 482.37 | 282.76 |
| Q15 | 1.22 | 12.63 | 4.77 | 32.18 | 55.46 |
| Q16 | 10.12 | 42.01 | 15.43 | 48.49 | 88.04 |
| Q17 | 6.40 | 27.43 | 13.87 | 100.27 | 170.18 |
| Q18 | 3.74 | 15.61 | 31.11 | 38.45 | 61.60 |
| Q19 | 125.78 | 15.16 | 9.67 | 43.02 | 52.81 |
| Q20 | 3.51 | 3.29 | 1.51 | 33.86 | 32.02 |
| Q21 | 2.52 | 11.97 | 4.98 | 79.23 | 49.58 |
| Q22 | 3.65 | 15.36 | 59.03 | 48.48 | 51.24 |
| Q23 | 52.78 | 53.12 | 145.83 | 285.35 | 380.31 |
| Q24 | 13.36 | 15.61 | 33.06 | 117.35 | 107.54 |
| Q25 | 4.14 | 23.08 | 23.31 | 101.59 | 186.13 |
| Q26 | 4.19 | 13.25 | 6.47 | 32.95 | 54.68 |
| Q27 | 3.03 | 20.98 | 48.74 | 226.61 | 81.35 |
| Q28 | 7.48 | 36.19 | 33.63 | 34.16 | 319.31 |
| Q29 | 8.21 | 9.55 | 69.71 | 98.95 | 171.31 |
| Q30 | 1.43 | 3.40 | 7.69 | 8.93 | 12.26 |
| Q31 | 5.76 | 26.28 | 21.47 | 149.64 | 154.68 |
| Q32 | 0.67 | 10.62 | 1.42 | — | 44.66 |
| Q33 | 6.64 | 11.40 | 6.44 | — | 89.58 |
| Q34 | 2.43 | 12.98 | 5.83 | — | 47.68 |
| Q35 | 8.43 | 8.40 | 7.29 | — | 53.97 |
| Q36 | 8.77 | 10.93 | 23.70 | — | 54.79 |
| Q37 | 4.84 | 8.55 | 8.82 | — | 43.99 |
| Q38 | 9.28 | 9.44 | 14.51 | — | 66.90 |
| Q39 | 1.57 | 20.68 | 27.68 | — | 51.02 |
| Q40 | 3.11 | 9.50 | 4.08 | — | 47.36 |
| Q41 | 0.11 | 0.51 | 0.29 | — | 1.27 |
| Q42 | 7.27 | 2.60 | 3.94 | — | 43.04 |
| Q43 | 6.07 | 6.72 | 7.72 | — | 40.67 |
| Q44 | 44.36 | 6.28 | 10.28 | — | 100.99 |
| Q45 | 2.38 | 6.08 | 4.31 | — | 29.14 |
| Q46 | 19.42 | 11.93 | 12.41 | — | 60.37 |
| Q47 | 11.84 | 11.88 | 67.08 | — | 61.03 |
| Q48 | 11.19 | 13.23 | 13.36 | — | 43.66 |
| Q49 | 18.73 | 65.21 | 18.53 | — | 134.03 |
| Q50 | 9.76 | 20.83 | 21.31 | — | 81.97 |
| Q51 | 20.57 | 23.65 | 37.66 | — | 95.40 |
| Q52 | 10.06 | 14.36 | 5.97 | — | 43.64 |
| Q53 | 4.97 | 5.97 | 5.56 | — | 51.67 |
| Q54 | 14.18 | 21.32 | 6.64 | — | 93.36 |
| Q55 | 5.76 | 3.51 | 3.84 | — | 40.75 |
| Q56 | 10.33 | 12.13 | 6.60 | — | 89.41 |
| Q57 | 2.27 | 5.35 | 16.82 | — | 32.11 |
| Q58 | 12.73 | 6.12 | 26.74 | — | 78.23 |
| Q59 | 11.40 | 9.04 | 21.18 | — | 50.09 |
| Q60 | 17.11 | 10.59 | 6.43 | — | 92.34 |
| Q61 | 17.56 | 22.67 | 13.43 | — | 115.74 |
| Q62 | 3.80 | 12.67 | 6.42 | — | 15.26 |
| Q63 | 9.39 | 4.24 | 4.91 | — | 53.68 |
| Q64 | 31.93 | 69.68 | 33.38 | — | 211.58 |
| Q65 | 29.02 | 18.97 | 27.44 | — | 125.90 |
| Q66 | 14.18 | 18.79 | 8.03 | — | 56.88 |
| Q67 | 46.41 | 49.34 | 121.64 | — | 183.75 |
| Q68 | 47.34 | 26.68 | 18.82 | — | 76.91 |
| Q69 | 9.49 | 9.15 | 9.06 | — | 53.21 |
| Q70 | 2.09 | 23.18 | 16.82 | — | 68.10 |
| Q71 | 9.47 | 28.36 | 32.32 | — | 75.43 |
| Q72 | 23.69 | 71.33 | 180.80 | — | 489.41 |
| Q73 | 7.84 | 14.13 | 8.62 | — | 55.12 |
| Q74 | 8.03 | 42.80 | 46.87 | — | 121.06 |
| Q75 | 29.39 | 65.42 | 34.32 | — | 245.67 |
| Q76 | 16.77 | 29.86 | 14.00 | — | 101.47 |
| Q77 | 20.40 | 31.66 | 9.04 | — | 98.92 |
| Q78 | 29.13 | 60.05 | 50.30 | — | 237.75 |
| Q79 | 21.57 | 37.62 | 21.77 | — | 64.92 |
| Q80 | 15.16 | 53.34 | 39.99 | — | 263.00 |
| Q81 | 0.97 | 4.72 | 4.49 | — | 23.70 |
| Q82 | 4.02 | 10.95 | 12.11 | — | 62.37 |
| Q83 | 0.94 | 3.45 | 2.91 | — | 13.97 |
| Q84 | 0.62 | 3.33 | 3.29 | — | 7.08 |
| Q85 | 2.55 | 5.11 | 10.56 | — | 28.20 |
| Q86 | 2.05 | 2.85 | 3.36 | — | 13.85 |
| Q87 | 7.77 | 12.61 | 15.70 | — | 71.89 |
| Q88 | 16.77 | 35.12 | 31.94 | — | 282.02 |
| Q89 | 3.03 | 7.18 | 33.00 | — | 73.75 |
| Q90 | 3.64 | 8.65 | 4.87 | — | 19.46 |
| Q91 | 0.79 | 2.00 | 2.34 | — | 5.81 |
| Q92 | 1.14 | 7.54 | 2.63 | — | 23.78 |
| Q93 | 10.19 | 22.25 | 19.24 | — | 129.97 |
| Q94 | 3.77 | 18.16 | 4.91 | — | 46.19 |
| Q95 | 58.79 | 62.35 | 15.24 | — | 86.60 |
| Q96 | 20.71 | 10.11 | 6.01 | — | 35.36 |
| Q97 | 6.42 | 13.23 | 16.88 | — | 80.33 |
| Q98 | 8.57 | 6.08 | 8.15 | — | 63.34 |
| Q99 | 1.88 | 6.36 | 6.85 | — | 26.66 |

## History

2,500 timed statements across 21 runs.
Raw data: one immutable JSON per run under [`results/tpcds/`](../../results/tpcds/), flattened to [`data/tpcds_results.csv`](../data/tpcds_results.csv).

