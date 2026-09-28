## Small Data Benchmark
Most benchmarks are big data using big compute. Here we are testing small to medium data with small compute. The purpose is to stress test: spill to disk, bad join orders, being hard on engines. Adding more compute just hides the issues.



<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/etl/charts/totals-dark.png">
  <img alt="Seconds to read 1000 CSVs and write one Iceberg table, per engine, fastest first" src="docs/etl/charts/totals.png">
</picture>


<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/charts/totals-dark.png">
  <img alt="Total seconds for all 22 queries, per engine" src="docs/charts/totals.png">
</picture>


<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/tpcds/charts/totals-dark.png">
  <img alt="TPC-DS total seconds for all 99 queries, per engine" src="docs/tpcds/charts/totals.png">
</picture>

## Adding an engine

A candidate engine must pass all three, checked by the `candidate engine` workflow:

1. **SQL**: it runs the TPC-H suite as SQL.
2. **Read from Azure**: it reads the OneLake Iceberg tables through the REST catalog, and raw files in the lakehouse Files section.
3. **Write Iceberg**: it creates and fills an Iceberg table through the same catalog.

## Gluten/Velox vs Spark-OSS

| Test | Spark-OSS | Gluten/Velox | Speedup |
|---|---:|---:|---:|
| Light ETL, 1000 CSVs | 982.2s | 728.8s | 1.3x |
| TPC-H SF=10 | 429.8s | 160.4s | 2.7x |
| TPC-DS SF=60 | 9,303.4s | 2,117.1s | 4.4x |
