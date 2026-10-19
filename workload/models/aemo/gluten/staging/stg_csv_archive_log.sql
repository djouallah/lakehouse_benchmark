-- The archive log, read with the parquet path datasource. A TABLE rebuilt every run, where the
-- Delta spark leg it is ported from has a view: the OneLake Iceberg REST catalog has no CREATE
-- VIEW (the duckdb leg works around the same gap with a view in DuckDB's in-memory catalog), and
-- a full rebuild cannot keep rows csv_raw_archive_log.parquet no longer has.
{{ config(materialized='table', file_format='iceberg', schema='landing') }}

SELECT
    source_type,
    source_filename,
    archive_path,
    archived_at,
    row_count,
    source_url,
    etag,
    csv_filename
FROM parquet.`{{ get_root_path() }}/csv_raw_archive_log.parquet`
