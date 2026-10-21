-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

-- Power BI-facing summary at (date, time, DUID). Same logic as the DuckDB and Polars versions,
-- in Spark SQL: strftime -> date_format, TIMESTAMPTZ -> TIMESTAMP.
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
{#- The sail probe target: Sail reads `GROUP BY ALL` as a column named ALL ("attribute
    ObjectName([Identifier(\"ALL\")]) is missing from the schema", run 39688464508; lakehq/sail#2836); the ordinals
    are what the Trino tree uses. #}
{%- set group_by = '1, 2, 3' if target.name == 'sail' else 'ALL' %}
{#- The sail probe target joins the interval on its epoch seconds: DataFusion's sort-merge join
    cannot compare a UTC timestamp key ("Unsupported data type in sort merge join comparator:
    Timestamp(us, \"UTC\")", run 39690013877; apache/datafusion#26003). AEMO's intervals are whole
    seconds, so the match is the same. #}
{%- set interval_key = 'CAST({}.SETTLEMENTDATE AS BIGINT)' if target.name == 'sail' else '{}.SETTLEMENTDATE' %}

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
rebuild_dates AS (
  -- Never seen before: archive backfill, or a first build catching up.
  SELECT DISTINCT s.DATE AS date FROM {{ ref('fct_scada') }} s
  WHERE s.INTERVENTION = 0
    AND s.DATE NOT IN (SELECT DISTINCT date FROM {{ this }})
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
),
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
    ON {{ interval_key.format('s') }} = {{ interval_key.format('p') }} AND d.Region = p.REGIONID
  WHERE
    s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    {% if scoped %}
    AND s.DATE IN (SELECT date FROM rebuild_dates)
    {% endif %}
  GROUP BY {{ group_by }}

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
    ON {{ interval_key.format('s') }} = {{ interval_key.format('p') }} AND d.Region = p.REGIONID
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    -- Only units the daily branch will be able to reproduce once this date settles.
    AND s.DUID IN (SELECT DUID FROM dispatch_duids)
    AND s.SETTLEMENTDATE > (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMP)) FROM {{ ref('fct_scada') }})
  GROUP BY {{ group_by }}
)

SELECT
  date,
  time,
  DUID,
  CAST(mw AS DECIMAL(18, 4)) AS mw,
  CAST(price AS DECIMAL(18, 4)) AS price,
  -- Provenance column only — no read path depends on it anymore. Kept to match the
  -- other engines' schema.
  greatest(
    (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMP)) FROM {{ ref('fct_scada') }}),
    COALESCE((SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMP)) FROM {{ ref('fct_scada_today') }}), CAST('1900-01-01' AS TIMESTAMP))
  ) AS cutoff
FROM daily_summary
ORDER BY date, time
