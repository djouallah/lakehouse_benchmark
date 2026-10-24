-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

-- Power BI-facing summary at (date, time, DUID). The Spark tree's logic (see that copy and the
-- duckdb one for the determinism contract and the dispatch_duids gate), in Trino's dialect:
-- date_format -> format_datetime, date_sub -> date_add, GROUP BY ALL -> GROUP BY 1, 2, 3 (Trino's
-- GROUP BY ALL is a set quantifier, not "every non-aggregate"), and `date` / `time` quoted.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date', 'time', 'DUID'],
    schema='mart'
) }}

{%- set scoped = is_incremental() %}

WITH
dispatch_duids AS (
  SELECT DISTINCT DUID FROM {{ ref('fct_scada') }}
),
{% if scoped %}
rebuild_dates AS (
  SELECT DISTINCT s.DATE AS "date" FROM {{ ref('fct_scada') }} s
  WHERE s.INTERVENTION = 0
    AND s.DATE NOT IN (SELECT DISTINCT "date" FROM {{ this }})
  UNION
  SELECT DISTINCT s.DATE FROM {{ ref('fct_scada') }} s
  WHERE s.DATE >= (SELECT date_add('day', -6, MAX(DATE)) FROM {{ ref('fct_scada') }})
  UNION
  SELECT DISTINCT s.DATE FROM {{ ref('fct_scada_today') }} s
  UNION
  SELECT "date" FROM {{ this }} GROUP BY "date" HAVING COUNT(DISTINCT "time") < 280
),
{% endif %}

daily_summary AS (
  SELECT
    s.DATE AS "date",
    CAST(format_datetime(s.SETTLEMENTDATE, 'HHmm') AS INTEGER) AS "time",
    s.DUID,
    MAX(s.INITIALMW) AS mw,
    MAX(p.RRP) AS price
  FROM {{ ref('fct_scada') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    {% if scoped %}
    AND s.DATE IN (SELECT "date" FROM rebuild_dates)
    {% endif %}
  GROUP BY 1, 2, 3

  UNION ALL

  SELECT
    s.DATE AS "date",
    CAST(format_datetime(s.SETTLEMENTDATE, 'HHmm') AS INTEGER) AS "time",
    s.DUID,
    MAX(s.INITIALMW) AS mw,
    MAX(p.RRP) AS price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    AND s.DUID IN (SELECT DUID FROM dispatch_duids)
    AND s.SETTLEMENTDATE > (SELECT MAX(SETTLEMENTDATE) FROM {{ ref('fct_scada') }})
  GROUP BY 1, 2, 3
),

batch AS (
SELECT
  "date",
  "time",
  DUID,
  -- (18,5): AEMO publishes five decimals, so the cast is exact and every engine stores the
  -- published value. (18,4) forced a rounding that DuckDB and Polars break differently.
  CAST(mw AS DECIMAL(18, 5)) AS mw,
  CAST(price AS DECIMAL(18, 5)) AS price,
  greatest(
    (SELECT MAX(SETTLEMENTDATE) FROM {{ ref('fct_scada') }}),
    COALESCE((SELECT MAX(SETTLEMENTDATE) FROM {{ ref('fct_scada_today') }}), TIMESTAMP '1900-01-01 00:00:00')
  ) AS cutoff
FROM daily_summary
)

-- Only the rows that differ from what is stored (see the duckdb copy).
SELECT b."date", b."time", b.DUID, b.mw, b.price, b.cutoff
FROM batch b
{% if scoped %}
LEFT JOIN (
  SELECT "date", "time", DUID, mw, price FROM {{ this }}
  WHERE "date" IN (SELECT "date" FROM rebuild_dates)
) t
  ON b."date" = t."date" AND b."time" = t."time" AND b.DUID = t.DUID
 AND b.mw = t.mw AND b.price = t.price
WHERE t.DUID IS NULL
{% endif %}
