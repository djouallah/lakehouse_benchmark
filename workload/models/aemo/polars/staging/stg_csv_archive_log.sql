{#-- The archive log download_aemo.py writes: one row per landed source file. Every engine
     has its own copy of this model reading the SAME parquet file, which is what makes the
     fact models' file-selection logic identical across engines.

     A TABLE here, rebuilt every run, where the DuckDB engines have a session view:
     dbt-polars has no session to hold one -- every model is its own Polars SQLContext. A
     full rebuild, never an incremental, so it cannot keep rows csv_raw_archive_log.parquet
     no longer has (the trap iceberg's insert-only table fell into; see that copy). --#}
{{ config(
    materialized='table',
    schema='landing'
) }}

SELECT
  source_type,
  source_filename,
  archive_path,
  archived_at,
  row_count,
  source_url,
  etag,
  csv_filename
FROM read_parquet('{{ get_archive_log_path() }}')
WHERE csv_filename IS NOT NULL
