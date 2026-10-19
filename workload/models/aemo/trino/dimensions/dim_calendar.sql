-- Calendar dimension (Trino): sequence() + UNNEST, the generate_series() equivalent. Merge on
-- date, with the NOT IN filter keeping the merge source empty on a steady-state run.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='date',
) }}

SELECT
  d AS "date",
  CAST(year(d) AS INTEGER) AS "year",
  CAST(month(d) AS INTEGER) AS "month"
FROM UNNEST(sequence(DATE '2018-04-01', DATE '2026-12-31', INTERVAL '1' DAY)) AS t(d)
{% if is_incremental() %}
WHERE d NOT IN (SELECT "date" FROM {{ this }})
{% endif %}
