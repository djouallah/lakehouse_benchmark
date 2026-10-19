-- Regional spot prices from the daily AEMO files (Trino): AEMO's DREGION record.
{#-- Column layout from macros/aemo_columns.sql, the read from macros/trino_read_csv.sql. Merge on
     the natural key, as in the Spark tree: a key only matches when two runs fold the same file. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['file', 'REGIONID', 'SETTLEMENTDATE', 'INTERVENTION'],
) }}

-- depends_on: {{ ref('stg_csv_archive_log') }}
{%- set files = polars_pending_files('daily') %}

SELECT
  UNIT,
  REGIONID,
  {%- for name in aemo_cast_columns('price') %}
  CAST({{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  {{ parse_filename('_fname') }} AS file,
  {{ trino_ts('SETTLEMENTDATE') }} AS SETTLEMENTDATE,
  CAST({{ trino_ts('SETTLEMENTDATE') }} AS DATE) AS DATE,
  CAST(year({{ trino_ts('SETTLEMENTDATE') }}) AS INTEGER) AS YEAR,
  CAST(year({{ trino_ts('SETTLEMENTDATE') }}) AS INTEGER) * 100
    + CAST(month({{ trino_ts('SETTLEMENTDATE') }}) AS INTEGER) AS month_key
FROM ({{ trino_staging('price', 'daily', files) }}) staged
