-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

-- Determinism contract, rebuild window and dispatch_duids: the iceberg copy's logic -- read
-- that header for why each piece is there. The SQL is shaped for Polars' streaming engine
-- (filters before joins), each difference marked at its site:
--   * dbt-polars' `merge` with no merge_clauses (it takes none): a matched key is UPDATED and
--     a new one inserted, which is the merge every tree runs.
--   * fct_scada and fct_price are cut to the rebuild dates BEFORE the joins (`scada`, `price`).
--     Streaming equi-joins do not spill, and with the window applied only as a semi-join after
--     them the join grew with the whole history: 14-15 GB peak, then the runner died
--     (workload run 39703181069). Same rows: DATE is CAST(SETTLEMENTDATE AS DATE) in both
--     tables and the join is on SETTLEMENTDATE. Polars' optimizer does not do this itself:
--     the semi join stays above the joins (pola-rs/polars#30186), and join reordering starts
--     with dim x price on Region, a near-cross join (#30185). Run 39717027666: fct_summary
--     269 s -> 68 s, the build's peak RSS 15.0 -> 12.1 GB.
--   * No trailing ORDER BY: this is a merge source, the order never reaches the table.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date', 'time', 'DUID'],
    schema='mart'
) }}

{# No full-history lever inside the model, deliberately: a var that makes the incremental
   branch emit all history would hand the merge the whole table as a source. Reset = drop. #}
{# Closes with `%}`, NOT `-%}`: a right-strip swallows the newlines after this tag and
   glues WITH onto the `-- depends_on` comment line above, commenting the keyword out
   (the compiled SQL then starts at `daily_summary AS (` and the parser errors there). #}
{%- set scoped = is_incremental() %}

WITH
-- The unit universe the DAILY branch can reproduce. Gates the intraday branch so it never
-- emits a unit that will be unreproducible once the date settles (see the header).
-- Deliberately UNBOUNDED, not a trailing window: fct_scada is append-only, so this set only
-- ever GROWS and can never orphan a row it previously admitted. A rolling window would
-- reintroduce the same bug from the other side — a unit ageing out of the window turns its
-- already-written intraday rows into orphans, which merge still cannot delete.
-- Outside the `scoped` block on purpose: a --full-refresh runs the intraday branch too and
-- must apply the identical filter.
dispatch_duids AS (
  SELECT DISTINCT DUID FROM {{ ref('fct_scada') }}
),
{% if scoped %}
-- Dates whose stored content could differ from a clean recomputation. Everything older
-- is settled: its daily file has landed and been folded in, so recomputing it would
-- reproduce it exactly. Shrinking this window silently reduces what can be repaired.
rebuild_dates AS (
  -- Never seen before: archive backfill, or a first build catching up. Distinct dates first,
  -- then the anti-join: a few thousand dates, not every fct_scada row.
  SELECT d.date FROM (
    SELECT DISTINCT s.DATE AS date FROM {{ ref('fct_scada') }} s WHERE s.INTERVENTION = 0
  ) d
  WHERE d.date NOT IN (SELECT DISTINCT date FROM {{ this }})
  UNION
  -- Recently settled: a date first written from the intraday feed is incomplete until
  -- its daily file lands, which is several days later if the pipeline missed a run — so
  -- a window, not just the newest daily date.
  SELECT DISTINCT s.DATE FROM {{ ref('fct_scada') }} s
  WHERE s.DATE >= (SELECT MAX(DATE) - INTERVAL 6 DAY FROM {{ ref('fct_scada') }})
  UNION
  -- Still in flux: the intraday feed keeps extending these until their daily file lands.
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
),
{% endif %}

-- The daily branch's inputs, cut to the rebuild dates before any join (see the header). The
-- range on DATE lets scan_iceberg prune files; the IN keeps exactly the rebuild dates.
scada AS (
  SELECT s.DATE, s.SETTLEMENTDATE, s.DUID, s.INITIALMW
  FROM {{ ref('fct_scada') }} s
  WHERE s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    {% if scoped %}
    AND s.DATE >= (SELECT MIN(date) FROM rebuild_dates)
    AND s.DATE IN (SELECT date FROM rebuild_dates)
    {% endif %}
),

price AS (
  SELECT p.SETTLEMENTDATE, p.REGIONID, p.RRP
  FROM {{ ref('fct_price') }} p
  WHERE p.INTERVENTION = 0
    {% if scoped %}
    AND p.DATE >= (SELECT MIN(date) FROM rebuild_dates)
    AND p.DATE IN (SELECT date FROM rebuild_dates)
    {% endif %}
),

daily_summary AS (
  SELECT
    s.DATE as date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as time,
    s.DUID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM scada s
  -- INNER joins: `WHERE p.INTERVENTION = 0` always discarded null-price rows anyway,
  -- so the old LEFT JOINs were inner joins in disguise — say what we do.
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN price p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  GROUP BY ALL

  UNION ALL

  -- Intraday tail: intervals beyond the daily horizon. Every date here is in
  -- rebuild_dates by construction, so no extra scoping predicate is needed.
  SELECT
    s.DATE as date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as time,
    s.DUID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    -- Only units the daily branch will be able to reproduce once this date settles.
    AND s.DUID IN (SELECT DUID FROM dispatch_duids)
    AND s.SETTLEMENTDATE > (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada') }})
  GROUP BY ALL
),

batch AS (
SELECT
  date,
  time,
  DUID,
  -- Not a plain CAST: Polars rounds a fourth-decimal tie to even and lands 0.0001 off Spark on
  -- some rows (polars#30074). macros/date_literals.sql, decimal_18_4; checked on main 4426a548fd
  -- against Spark's rounding (run 39948740444).
  {{ decimal_18_4('mw') }} AS mw,
  {{ decimal_18_4('price') }} AS price,
  -- Provenance column only (see the iceberg copy).
  (SELECT GREATEST(
    (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada') }}),
    COALESCE((SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada_today') }}), CAST('1900-01-01' AS TIMESTAMPTZ))
  )) AS cutoff
FROM daily_summary
)

SELECT b.date, b.time, b.DUID, b.mw, b.price, b.cutoff
FROM batch b
