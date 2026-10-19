-- Intraday regional prices (Trino), from the landed PUBLIC_DISPATCHIS files.
{#-- Column layout from macros/aemo_columns.sql, the read from macros/trino_read_csv.sql. Merge on
     the natural key, as in the Spark tree. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['file', 'REGIONID', 'SETTLEMENTDATE', 'INTERVENTION'],
) }}

-- depends_on: {{ ref('stg_csv_archive_log') }}
{%- set files = polars_pending_files('price_today') %}

SELECT
  REGIONID,
  {%- for name in aemo_cast_columns('price_today') %}
  CAST({{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  {{ trino_ts('SETTLEMENTDATE') }} AS SETTLEMENTDATE,
  CAST({{ trino_ts('SETTLEMENTDATE') }} AS DATE) AS DATE,
  {{ parse_filename('_fname') }} AS file,
  CAST(year({{ trino_ts('SETTLEMENTDATE') }}) AS INTEGER) AS YEAR
FROM ({{ trino_staging('price_today', 'price_today', files) }}) staged
