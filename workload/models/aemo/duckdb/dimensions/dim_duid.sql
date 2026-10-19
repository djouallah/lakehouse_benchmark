{% set csv_archive_path = get_csv_archive_path() %}

{#-- Parse-time probe: is there anything new to insert? Guarded on execute + flags.WHICH so
     `dbt compile` / `ls` / `docs generate` do not fire a real query (the ducklake repo's copy
     was unguarded and did). The else-branch assumes there IS work, which is the safe default:
     a needless scan, never a skipped insert. --#}
{%- set check_new_duids_query -%}
  SELECT count(*) AS cnt FROM (
    SELECT DUID FROM read_csv('{{ csv_archive_path }}/duid/duid_data.csv') WHERE length(DUID) > 2
    UNION
    SELECT "Facility Code" AS DUID FROM read_csv_auto('{{ csv_archive_path }}/duid/facilities.csv')
  ) source_duids
  WHERE DUID NOT IN (SELECT DUID FROM {{ this }})
{%- endset -%}

{%- if execute and is_incremental() and flags.WHICH in ('run', 'build', 'retry') -%}
  {%- set result = run_query(check_new_duids_query) -%}
  {%- set has_new_duids = result and result.rows[0][0] > 0 -%}
{%- else -%}
  {%- set has_new_duids = true -%}
{%- endif -%}

{#-- Insert-only merge on DUID, the same pattern as the facts: new DUIDs insert, existing ones
     are never touched — so a run that sees a stale or empty view of the table can at worst
     re-insert nothing that survives the merge, instead of the old wipe-and-reload appending a
     full duplicate copy. Consequence: attribute changes (region / fuel / geo) never update in
     place; `dbt run --full-refresh -s dim_duid` is the reconciliation lever. --#}
{#-- sync_all_columns, the same value as the other DuckDB engines and as the source repo
     (djouallah/dbt_fabric_python_iceberg), which is the configuration this leg is known to
     work on. It is not free on this catalog: a TYPE change is performed as add-copy-rename
     (<col>__dbt_alter), which is not atomic there -- it once left latitude__dbt_alter behind
     in iceberg_mart.dim_duid and every later run died on "Column with name
     latitude__dbt_alter already exists!" until the table was dropped. Adding a column is
     fine; if a type ever changes, expect to drop the table by hand. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['DUID'],
    merge_clauses={'when_matched': [{'action': 'do_nothing'}]},
    on_schema_change='sync_all_columns'
) }}

-- The reference CSVs are landed by download_aemo.py, which the log model stands for in the DAG.
-- depends_on: {{ ref('stg_csv_archive_log') }}

{% if has_new_duids %}
{# Plain, non-trimming comment tags below on purpose. The trimming form eats the
   newline after WITH and compiles to `WITHstates AS (`. Note also that Jinja comments
   do NOT nest, so a comment must never quote comment delimiters. Both met in the wild. #}
WITH
  {# Inline CTE, not a dbt seed. Six static rows are not worth a materialized table plus a
       `dbt seed` step in every runner — and on the duckrun target a seed is materialized into
       in-memory DuckDB rather than Delta, so it would be invisible to Power BI Direct Lake. #}
  states AS (
    SELECT 'WA1' AS RegionID, 'Western Australia' AS State
    UNION ALL SELECT 'QLD1', 'Queensland'
    UNION ALL SELECT 'NSW1', 'New South Wales'
    UNION ALL SELECT 'TAS1', 'Tasmania'
    UNION ALL SELECT 'SA1', 'South Australia'
    UNION ALL SELECT 'VIC1', 'Victoria'
  ),

  duid_aemo AS (
    SELECT
      DUID AS DUID,
      first(Region) AS Region,
      first("Fuel Source - Descriptor") AS FuelSourceDescriptor,
      first(Participant) AS Participant
    FROM read_csv('{{ csv_archive_path }}/duid/duid_data.csv')
    WHERE length(DUID) > 2
    GROUP BY DUID
  ),

  wa_facilities AS (
    SELECT
      'WA1' AS Region,
      "Facility Code" AS DUID,
      "Participant Name" AS Participant
    FROM read_csv_auto('{{ csv_archive_path }}/duid/facilities.csv')
  ),

  wa_energy AS (
    SELECT * FROM read_csv_auto('{{ csv_archive_path }}/duid/WA_ENERGY.csv', header = 1)
  ),

  duid_wa AS (
    SELECT
      wa_facilities.DUID,
      wa_facilities.Region,
      wa_energy.Technology AS FuelSourceDescriptor,
      wa_facilities.Participant
    FROM wa_facilities
    LEFT JOIN wa_energy ON wa_facilities.DUID = wa_energy.DUID
  ),

  duid_all AS (
    SELECT * FROM duid_aemo
    UNION ALL
    SELECT * FROM duid_wa
  ),

  geo AS (
    SELECT
      duid,
      max(latitude) AS latitude,
      max(longitude) AS longitude
    FROM read_csv('{{ csv_archive_path }}/duid/geo_data.csv')
    WHERE latitude IS NOT NULL
    GROUP BY duid
  )

SELECT
  a.DUID,
  first(a.Region) AS Region,
  first(UPPER(LEFT(TRIM(FuelSourceDescriptor), 1)) || LOWER(SUBSTR(TRIM(FuelSourceDescriptor), 2))) AS FuelSourceDescriptor,
  first(a.Participant) AS Participant,
  first(states.State) AS State,
  first(geo.latitude) AS latitude,
  first(geo.longitude) AS longitude
FROM duid_all a
JOIN states ON a.Region = states.RegionID
LEFT JOIN geo ON a.duid = geo.duid
GROUP BY a.DUID
{% else %}
-- No new DUIDs: an empty result keeps the existing rows untouched.
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
