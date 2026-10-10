## Small Data Benchmark
Most benchmarks are big data using big compute. Here we are testing small to medium data with small compute. The purpose is to stress test: spill to disk, bad join orders, being hard on engines. Adding more compute just hides the issues.



**[Interactive results →](https://djouallah.github.io/lakehouse_benchmark/)**

## Adding an engine

A candidate engine must pass all three; the `candidate engine` workflow checks them:

1. **SQL**: it runs the TPC-H suite as SQL.
2. **Read from Azure**: it reads the OneLake Iceberg tables through the REST catalog, and raw files in the lakehouse Files section.
3. **Write Iceberg**: it creates and fills an Iceberg table through the same catalog.

Bonus:

- It finishes TPC-DS.
- It supports more Iceberg operations (the Iceberg support tab on the results page).

In the bench, with gaps:

- **Daft**: ETL only; 16/22 TPC-H queries, the rest fail on decimal precision, cross join and `SUBSTRING` ([Daft#7532](https://github.com/Eventual-Inc/Daft/issues/7532), OneLake paths [Daft#7533](https://github.com/Eventual-Inc/Daft/pull/7533)).
- **StarRocks**: no complete TPC-DS run; Q49 fails on a SQL bug ([StarRocks#79807](https://github.com/StarRocks/starrocks/issues/79807)).
- **chDB**: not in TPC-DS; aborts on the first query (chDB 4.4.0).

Tried and not added:

- **Apache Doris**: reads OneLake only with a client secret; with a workload-identity or SAS token the backend crashes (condition 2).
- **Firebolt Core**: attaches the catalog, but only reads data from `s3://`, `gs://` or `file://`. It rejects OneLake's `abfss://` paths, and its `azure://` location takes no Azure token (condition 2; [firebolt-core#90](https://github.com/firebolt-db/firebolt-core/issues/90)).
- **Databend**: the release build has no Azure storage for Iceberg ("azdls not supported now"); the fix is in the nightly builds, not yet in a stable release (condition 2; [databend#20590](https://github.com/databendlabs/databend/pull/20590), decimal bug [databend#20588](https://github.com/databendlabs/databend/issues/20588)).
- **DataFusion Comet**: its native Iceberg scan has no `abfss://`, so OneLake scans fall back to the JVM ([apache/datafusion-comet#6058](https://github.com/apache/datafusion-comet/issues/6058)) (condition 2).
- **DataFusion**: its Python package has no Iceberg support: no catalog, no scan, no write ([apache/datafusion-python#1097](https://github.com/apache/datafusion-python/issues/1097)). The only route, pyiceberg-core's DataFusion table, was read-only and has been removed upstream ([apache/iceberg-rust#3036](https://github.com/apache/iceberg-rust/issues/3036)) (conditions 2 and 3).
- **pg_lake** (3.5.3): runs TPC-H 22/22, but has no catalog cache for read-only tables; every statement reloads the metadata, so the 25-row `nation` takes 4-5s.

## Gluten/Velox vs Spark-OSS

<!-- speedup:start -->
| Test | Scale | Spark-OSS | Gluten/Velox | Speedup |
|---|---:|---:|---:|---:|
| Light ETL | 10 files | 54.2s | 29.2s | 1.9x |
| Light ETL | 100 files | 136.5s | 81.8s | 1.7x |
| Light ETL | 1,000 files | 982.2s | 728.8s | 1.3x |
| TPC-H | SF=10 | 513.8s | 139.7s | 3.7x |
| TPC-H | SF=30 | 1,439.5s | 385.3s | 3.7x |
| TPC-H | SF=60 | 2,318.3s | 677.7s | 3.4x |
| TPC-H | SF=100 | failed | 1,007.5s | — |
| TPC-DS | SF=10 | 2,170.3s | 675.2s | 3.2x |
| TPC-DS | SF=30 | 9,280.4s | 951.5s | 9.8x |
| TPC-DS | SF=60 | 9,303.4s | 2,117.1s | 4.4x |
| TPC-DS | SF=100 | — | 5,762.4s | — |

Mean of each engine's last 3 runs at each scale; the query suites count only runs that completed every query, and `failed` means none did. `—` = not run. Regenerated on every publish.
<!-- speedup:end -->
