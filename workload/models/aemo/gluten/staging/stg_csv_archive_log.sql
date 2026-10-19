-- The archive log, read through a parquet temp view. A TABLE rebuilt every run, where the
-- Delta spark leg it is ported from has a view: the OneLake Iceberg REST catalog has no CREATE
-- VIEW (the duckdb leg works around the same gap with a view in DuckDB's in-memory catalog), and
-- a full rebuild cannot keep rows csv_raw_archive_log.parquet no longer has.
--
-- NOT `parquet.`<path>``, the source's spelling. With the Iceberg REST catalog as the default,
-- Spark first asks it for a table named after the path in namespace `parquet`; OneLake answers
-- that lookup with 400 Malformed request instead of "no such table", so Spark never falls back
-- to reading the file (workload run 39338017378). A temp view makes no catalog lookup.
{{ config(
    materialized='table',
    file_format='iceberg',
    schema='landing',
    pre_hook="CREATE OR REPLACE TEMPORARY VIEW csv_raw_archive_log USING parquet OPTIONS (path '" ~ get_archive_log_path() ~ "')"
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
FROM csv_raw_archive_log
