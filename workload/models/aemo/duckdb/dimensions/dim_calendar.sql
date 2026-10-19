{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='date'
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
