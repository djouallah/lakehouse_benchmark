-- Intraday SCADA (Trino), from the landed PUBLIC_DISPATCHSCADA files.
{#-- Column layout from macros/aemo_columns.sql, the read from macros/trino_read_csv.sql (whose
     record filter casts SCADAVALUE before `<> 0`, as every leg must). Merge on the natural key,
     as in the Spark tree; this feed has no INTERVENTION. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['file', 'DUID', 'SETTLEMENTDATE'],
) }}

-- depends_on: {{ archive_log() }}
{%- set files = polars_pending_files('scada_today') %}

SELECT
  DUID,
  CAST(SCADAVALUE AS DOUBLE) AS INITIALMW,
  {{ parse_filename('_fname') }} AS file,
  {{ trino_ts('SETTLEMENTDATE') }} AS SETTLEMENTDATE,
  {{ trino_ts('LASTCHANGED') }} AS LASTCHANGED,
  CAST({{ trino_ts('SETTLEMENTDATE') }} AS DATE) AS DATE,
  CAST(year({{ trino_ts('SETTLEMENTDATE') }}) AS INTEGER) AS YEAR
FROM ({{ trino_staging('scada_today', 'scada_today', files) }}) staged
