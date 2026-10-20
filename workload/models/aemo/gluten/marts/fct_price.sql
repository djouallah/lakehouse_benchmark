-- Regional spot prices from the daily AEMO files (Spark): AEMO's DREGION record, all 130
-- source columns, selected by name.
{#-- Column layout comes from macros/aemo_columns.sql, the single source of truth shared
     by every engine. --#}
{%- set cast_cols = aemo_cast_columns('price') -%}
{#-- The CSV read happens in the two pre_hooks and lands in the <model>__stage table; this body only
     casts and parses. macros/spark_read_csv.sql explains the stage. Same shape on the first build and on every incremental run;
     the hooks resolve this run's files (oldest first, process_limit) at run time.

     THE HOOKS STAY ONE STRING LITERAL EACH, WITH THE MACRO CALL UNEVALUATED. dbt renders
     hooks at run time, and only then are `this` and is_incremental() bound. --#}
{#-- Merge on the natural key, not append. dbt-spark has no skip_matched_step (that was
     dbt-fabricspark's), so a matched key UPDATEs -- and it only ever matches when two runs
     fold the same file, where the update rewrites identical values.

     Why not append: the file list excludes files already in {{ this }}, but that list is
     computed BEFORE the write. Two overlapping runs both see a file as new and both append
     it. The key match is the write-time guard underneath the file list. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='iceberg',
    unique_key=['file', 'REGIONID', 'SETTLEMENTDATE', 'INTERVENTION'],
    pre_hook=["{{ spark_stage_view('price') }}", "{{ spark_stage_table('price') }}"],
    post_hook="{{ spark_drop_stage() }}"
) }}

-- depends_on: {{ archive_log() }}

SELECT
  UNIT,
  REGIONID,
  {%- for name in cast_cols %}
  CAST({{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  {{ parse_filename('_fname') }} AS file,
  -- AEMO ships SETTLEMENTDATE as 'yyyy/MM/dd HH:mm:ss'. Spark's CAST(string AS TIMESTAMP)
  -- accepts only yyyy-MM-dd and returns NULL for slashes instead of erroring (non-ANSI mode),
  -- which silently nulled the whole column here. DuckDB and T-SQL both parse slashes, so only
  -- this leg was affected. Parse the format explicitly.
  to_timestamp(SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss') AS SETTLEMENTDATE,
  to_date(SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss') AS DATE,
  CAST(YEAR(to_timestamp(SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss')) AS INT) AS YEAR,
  {#-- YYYYMM partition key. Emitted by every engine so the fact schemas match. --#}
  CAST(YEAR(to_timestamp(SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss')) AS INT) * 100 + CAST(MONTH(to_timestamp(SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss')) AS INT) AS month_key
FROM {{ spark_stage_relation() }}
-- The stage was already filtered to this record; repeated here as the correctness check.
WHERE I = 'D' AND UNIT = 'DREGION' AND VERSION = '3'
