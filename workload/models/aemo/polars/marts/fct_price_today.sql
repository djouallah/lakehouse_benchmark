{#-- Intraday DISPATCHIS price record.

     A merge on the natural key, as on every engine: dbt-polars overwrites the matched keys
     and appends, in one commit checked against the snapshot it read, so two runs cannot
     both insert a file. polars_pending_files() still picks only unprocessed files, so a
     normal run matches nothing and the merge is an append.

     The files are chosen at parse time -- dbt-polars has no SQL hooks -- by the same query
     the DuckDB pre-hooks run, NEWEST FIRST, capped at process_limit. See
     macros/polars_read_csv.sql. --#}
{%- set spec = aemo_spec('price_today') -%}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=spec['unique_key']
) }}

-- depends_on: {{ archive_log() }}
{%- set files = polars_pending_files(spec['source_type']) %}

{% if files is none or files | length > 0 %}
WITH staging AS (
  {{ polars_staging('price_today', 'price_today', files) }}
)

SELECT
  REGIONID,
  {%- for name in aemo_cast_columns('price_today') %}
  CAST({{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
  CAST(CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS DATE) AS DATE,
  {{ parse_filename('filename') }} AS file,
  CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) AS INT) AS YEAR
FROM staging
{% else %}
-- No unprocessed files: an empty result keeps the existing data untouched.
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
