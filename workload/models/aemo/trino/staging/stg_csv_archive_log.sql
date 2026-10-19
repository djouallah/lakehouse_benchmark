-- The archive log, through the Hive EXTERNAL parquet table the runner declares over Files/dbt
-- (Trino has no path-based reader). A TABLE rebuilt every run, like the polars and gluten trees:
-- the OneLake Iceberg REST catalog has no CREATE VIEW.
{{ config(materialized='table', schema='landing') }}

SELECT
  source_type,
  source_filename,
  archive_path,
  archived_at,
  row_count,
  source_url,
  etag,
  csv_filename
FROM files.dbt.archive_log
WHERE csv_filename IS NOT NULL
