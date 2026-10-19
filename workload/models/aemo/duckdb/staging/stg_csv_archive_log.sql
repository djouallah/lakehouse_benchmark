{#-- The archive log download_aemo.py writes: one row per landed source file. Every engine
     has its own copy of this model reading the SAME parquet file, which is what makes the
     fact models' file-selection logic identical across engines.

     A plain view, like the other DuckDB engines -- but in the IN-MEMORY catalog, not the
     Iceberg one, which is what `database='memory'` is for. The profile's default database is
     `onelake` (the attached Iceberg catalog) and that catalog has no CREATE VIEW, so without
     the override this model cannot be a view at all. dbt-duckdb hands every model a cursor on
     ONE in-process connection, so a named view in `memory` is visible to every later model in
     the run and nothing survives it -- exactly the session view duckrun and ducklake get for
     free. (A literal CREATE TEMPORARY VIEW would be per-cursor and invisible to the fact
     models' pre-hooks.)

     It used to be an insert-only TABLE in the catalog, which made iceberg the one engine
     whose log could hold rows csv_raw_archive_log.parquet no longer has -- download_aemo.py
     rewrites that file delete-then-copy, and starts an EMPTY log if it cannot read the old
     one, so iceberg would fold a CSV no other engine can see. --#}
{{ config(
    materialized='view',
    database='memory',
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
