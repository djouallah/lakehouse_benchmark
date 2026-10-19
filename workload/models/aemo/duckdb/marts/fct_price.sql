{#-- AEMO DREGION record from the daily archive files: all 130 source columns.

     Insert-only merge (WHEN MATCHED DO NOTHING): every commit stays a single append
     snapshot — the OneLake catalog rejects multi-snapshot commits, see the fct_summary.sql
     header — while re-processed files dedupe on the unique_key instead of double-inserting.
     That only holds ACROSS batches: MERGE inserts every not-matched source row, so the
     pre-hook's DISTINCT is what stops one file being read twice WITHIN a batch (the log is
     append-only and lists a file once per run it waited). --#}
{%- set spec = aemo_spec('price') -%}
{#-- THE PRE-HOOK MUST STAY ONE STRING LITERAL, WITH ITS {{ }} AND {% %} UNEVALUATED.
     dbt renders hooks at RUN time, and only then are `ref()`, `this` and `is_incremental()`
     bound properly. Moving this into a `{%- set -%}` block instead renders it during dbt's
     CONFIG pass, where `ref()` is a stub that returns the MODEL'S OWN relation — the hook
     then silently reads from {{ this }} instead of the archive log, and the run fails with
     "Table with name <this model> does not exist". Verified the hard way. Only
     `source_type`, a parse-time constant, is concatenated in. --#}
{#-- ORDER BY archive_path DESC inside the pre-hook, i.e. NEWEST FIRST, and it is not
     cosmetic. archive_path is '/<subfolder>/PUBLIC_*_YYYYMMDD*.CSV', so lexicographic DESC is
     chronological. Two reasons, both learned:
       * A bare `LIMIT process_limit` with no ORDER BY -- which is what stood here -- lets
         DuckDB return ANY n of the backlog, and a different n per engine. spark ordered
         theirs (spark_new_files.sql, and the since-dropped T-SQL leg), so on a run with a small
         process_limit the five engines folded DIFFERENT FILES and parity.py graded them on
         different inputs, reporting that as a logic difference.
       * Newest first means a partial load is RECENT data. The backlog is then old data still
         queued, which converges over runs -- and the assert_all_*_files_processed_* tests say
         so as a WARNING rather than failing the build. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    merge_clauses={'when_matched': [{'action': 'do_nothing'}]},
    unique_key=spec['unique_key'],
    pre_hook="SET VARIABLE price_daily_paths = (SELECT COALESCE(NULLIF(list('{{ get_csv_archive_path() }}' || archive_path), []), ['']) FROM (SELECT DISTINCT archive_path FROM {{ ref('stg_csv_archive_log') }} WHERE source_type = '" ~ spec['source_type'] ~ "'{% if is_incremental() %} AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this }}){% endif %} ORDER BY archive_path DESC LIMIT {{ env_var('process_limit', '1000') }}))"
) }}

{%- set check_files_query -%}
SELECT COUNT(*) AS cnt FROM {{ ref('stg_csv_archive_log') }}
WHERE source_type = '{{ spec['source_type'] }}'
{%- if is_incremental() %}
AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this }})
{%- endif -%}
{%- endset -%}

{%- if execute and flags.WHICH in ('run', 'build', 'retry') -%}
  {%- set files_result = run_query(check_files_query) -%}
  {%- set has_files = files_result and files_result.rows[0][0] > 0 -%}
{%- else -%}
  {%- set has_files = true -%}
{%- endif -%}

{% if has_files %}
WITH price_staging AS (
  SELECT *
  FROM {{ duckdb_read_csv("getvariable('price_daily_paths')", 'price') }}
  WHERE {{ duckdb_record_filter('price') }}
)

SELECT
  UNIT,
  REGIONID,
  {%- for name in aemo_cast_columns('price') %}
  CAST({{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  {{ parse_filename('filename') }} AS file,
  CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
  CAST(SETTLEMENTDATE AS DATE) AS DATE,
  CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS YEAR,
  {#-- Monthly partition key (YYYYMM). Low-cardinality, deterministic, and a useful filter on
       every engine — it is emitted on both daily facts by all five so the fact schemas match. --#}
  CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) * 100
    + CAST(MONTH(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS month_key
FROM price_staging
{% else %}
-- No unprocessed files: an empty result keeps the existing data untouched.
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
