-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

-- Power BI-facing summary at (date, time, DUID). Same logic as the DuckDB and Polars versions,
-- in Spark SQL (Gluten and Sail both run this file): strftime -> date_format, TIMESTAMPTZ -> TIMESTAMP.
--
-- Determinism contract (see the duckdb version for the full story): the defect was in what
-- the SOURCE emitted — only wholly-missing dates, so an incomplete date could never be
-- repaired. Now every run emits the COMPLETE recomputation for exactly the dates whose
-- stored content could still be stale, and the merge reconciles that batch key by key.
-- No cutoff watermark, no dependence on this engine's run history.
--
-- The intraday branch is gated on dispatch_duids because the two branches read AEMO tables
-- with DIFFERENT UNIT UNIVERSES: 26 non-scheduled units publish SCADA telemetry but have
-- zero rows in fct_scada ever, so rows written for them became permanent orphans once the
-- date settled and merge could not delete them. See the duckdb version for the full story.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='iceberg',
    unique_key=['date', 'time', 'DUID'],
    schema='mart'
) }}

{# Full-history reset is plain `--full-refresh` or dropping the table (there is no CI input
   for it) — never a var that makes this branch emit all history, which would hand the merge
   the whole table as a source. #}
{# Closes with `%}`, NOT `-%}`: a right-strip swallows the newlines after this tag and
   glues WITH onto the `-- depends_on` comment line above, commenting the keyword out. #}
{%- set scoped = is_incremental() %}
{#- For Sail, valid on Gluten too: GROUP BY ordinals, because Sail reads
    GROUP BY ALL as a column named ALL (lakehq/sail#2836); and the interval joined on its epoch
    seconds, because DataFusion's sort-merge join cannot compare a UTC timestamp key
    (apache/datafusion#26003). AEMO's intervals are whole seconds, so the match is the same. #}

WITH
-- The unit universe the DAILY branch can reproduce. Deliberately UNBOUNDED, not a trailing
-- window: fct_scada is append-only, so this set only ever GROWS and can never orphan a row
-- it previously admitted. Outside the `scoped` block on purpose — a --full-refresh runs the
-- intraday branch too and must apply the identical filter.
dispatch_duids AS (
  SELECT DISTINCT DUID FROM {{ ref('fct_scada') }}
),
{% if scoped %}
-- Dates whose stored content could differ from a clean recomputation; everything older
-- is settled. Shrinking this window silently reduces what can be repaired.
-- Run first and inlined as DATE literals (macros/date_literals.sql), so Iceberg skips every
-- fct_scada and fct_price data file outside these dates instead of reading the whole history.
{% set rebuild_q %}
  -- Never seen before: archive backfill, or a first build catching up. Distinct dates first, then
  -- EXCEPT: `s.DATE NOT IN (...)` was a hash anti join over every fct_scada row, which cannot spill
  -- and ran out of the 10 GB pool (HashJoinInput 4.8 GB, run 39865545715). date is never null.
  SELECT date FROM (
    SELECT DISTINCT s.DATE AS date FROM {{ ref('fct_scada') }} s WHERE s.INTERVENTION = 0
    EXCEPT
    SELECT date FROM {{ this }}
  ) AS new_dates
  UNION
  -- Recently settled: incomplete until the daily file lands, which is several days later
  -- if the pipeline missed a run — so a window, not just the newest daily date.
  SELECT DISTINCT s.DATE FROM {{ ref('fct_scada') }} s
  WHERE s.DATE >= (SELECT date_sub(MAX(DATE), 6) FROM {{ ref('fct_scada') }})
  UNION
  -- Still in flux until their daily file lands.
  SELECT DISTINCT s.DATE FROM {{ ref('fct_scada_today') }} s
  UNION
  -- Partially written and never completed. A calendar date straddles TWO PUBLIC_DAILY files
  -- (they roll at 04:00), so a date first computed when only one had landed holds ~48 or
  -- ~240 intervals; when the second file lands in a LATER run, a 60-file backfill batch has
  -- moved MAX(DATE) two months past the 6-day window above and the date is never revisited.
  -- Every batch boundary of a backfill left one (measured 2026-09-17: spark short ~30k rows
  -- on each of 2019-01-27, 2019-11-23, 2020-01-23 after three incremental runs; the reference
  -- repo never saw it because it loads the whole archive at once). 280 matches
  -- assert_fct_summary_no_partial_dates. A date the SOURCE itself still lacks stays in this
  -- set and recomputes each run until its file lands -- a few dates' scan, nothing inserts.
  SELECT date FROM {{ this }} GROUP BY date HAVING COUNT(DISTINCT time) < 280
{% endset %}
{% set rebuild_dates = date_literals(rebuild_q) %}
{% endif %}

daily_summary AS (
  SELECT
    s.DATE as date,
    CAST(date_format(s.SETTLEMENTDATE, 'HHmm') AS INT) as time,
    s.DUID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM {{ ref('fct_scada') }} s
  -- INNER joins: `WHERE p.INTERVENTION = 0` always discarded null-price rows anyway.
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price') }} p
    ON CAST(s.SETTLEMENTDATE AS BIGINT) = CAST(p.SETTLEMENTDATE AS BIGINT) AND d.Region = p.REGIONID
  WHERE
    s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    {% if scoped %}
    AND s.DATE IN ({{ rebuild_dates }})
    -- The same dates on the price side: the join is on SETTLEMENTDATE and both facts derive DATE
    -- from it with the same SQL, so this drops no matching row, and fct_price is pruned too.
    AND p.DATE IN ({{ rebuild_dates }})
    {% endif %}
  GROUP BY 1, 2, 3

  UNION ALL

  -- Intraday tail: intervals beyond the daily horizon. Every date here is in
  -- rebuild_dates by construction.
  SELECT
    s.DATE as date,
    CAST(date_format(s.SETTLEMENTDATE, 'HHmm') AS INT) as time,
    s.DUID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON CAST(s.SETTLEMENTDATE AS BIGINT) = CAST(p.SETTLEMENTDATE AS BIGINT) AND d.Region = p.REGIONID
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    -- Only units the daily branch will be able to reproduce once this date settles.
    AND s.DUID IN (SELECT DUID FROM dispatch_duids)
    AND s.SETTLEMENTDATE > (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMP)) FROM {{ ref('fct_scada') }})
  GROUP BY 1, 2, 3
),

batch AS (
SELECT
  date,
  time,
  DUID,
  -- (18,5): AEMO publishes five decimals, so the cast is exact and every engine stores the
  -- published value. (18,4) forced a rounding that DuckDB and Polars break differently.
  CAST(mw AS DECIMAL(18, 5)) AS mw,
  CAST(price AS DECIMAL(18, 5)) AS price,
  -- Provenance column only — no read path depends on it anymore. Kept to match the
  -- other engines' schema.
  greatest(
    (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMP)) FROM {{ ref('fct_scada') }}),
    COALESCE((SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMP)) FROM {{ ref('fct_scada_today') }}), CAST('1900-01-01' AS TIMESTAMP))
  ) AS cutoff
FROM daily_summary
)

-- Only the rows that differ from what is stored (see the duckdb copy).
SELECT b.date, b.time, b.DUID, b.mw, b.price, b.cutoff
FROM batch b
{% if scoped %}
LEFT JOIN (
  SELECT date, time, DUID, mw, price FROM {{ this }} WHERE date IN ({{ rebuild_dates }})
) t
  ON b.date = t.date AND b.time = t.time AND b.DUID = t.DUID
 AND b.mw = t.mw AND b.price = t.price
WHERE t.DUID IS NULL
{% endif %}
-- No ORDER BY: this SQL is a merge source, and a sort here reaches nothing stored.
