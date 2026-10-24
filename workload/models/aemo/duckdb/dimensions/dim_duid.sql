{% set csv_archive_path = get_csv_archive_path() %}

{#-- The same materialization on every engine (the rule in workload/dbt_project.yml): an incremental merge on
     the natural key, update on match. A merge commits against the snapshot it read, so two
     runs folding the same file cannot both insert it -- the loser's commit fails; an append
     would keep both. A key only matches when a file is folded twice, and the update then
     rewrites identical values. --#}
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
    on_schema_change='sync_all_columns'
) }}

-- The reference CSVs are landed by download_aemo.py, which the log model stands for in the DAG.
-- depends_on: {{ source('landing', 'csv_raw_archive_log') }}

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
{% if is_incremental() %}
-- Only new or changed units. Every unit used to go to the merge each run, all 690 matched and
-- UPDATED to the values they already had (Trino: MERGE (690 rows) on every run). EXCEPT, not a
-- join: State, latitude and longitude can be NULL, and EXCEPT compares NULLs as equal. With
-- nothing changed the batch is empty and the merge is skipped (macros/merge_prune.sql).
EXCEPT
SELECT DUID, Region, FuelSourceDescriptor, Participant, State, latitude, longitude FROM {{ this }}
{% endif %}
