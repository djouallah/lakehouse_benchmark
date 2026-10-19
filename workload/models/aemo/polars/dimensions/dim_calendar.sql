{#-- Same calendar as every engine, one row per day, 2018-04-01 .. 2026-12-31, by the DuckDB
     legs' generate_series. Insert-only by `append` plus the NOT IN filter, which is what the
     other engines' delete+insert on `date` amounts to when the filter already drops every
     existing date. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}
SELECT
  CAST(date AS DATE) AS date,
  CAST(EXTRACT(year FROM date) AS INT) AS year,
  CAST(EXTRACT(month FROM date) AS INT) AS month
FROM (
  SELECT unnest(generate_series(
    CAST('2018-04-01' AS DATE),
    CAST('2026-12-31' AS DATE),
    INTERVAL 1 DAY
  )) AS date
)
{% if is_incremental() %}
WHERE date NOT IN (SELECT date FROM {{ this }})
{% endif %}
