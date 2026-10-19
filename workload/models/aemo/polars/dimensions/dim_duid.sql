{#-- Insert-only on DUID, the same pattern as every engine: new DUIDs insert, existing ones
     are never touched. `append` plus an anti-join on {{ this }} is that operation here --
     dbt-polars ignores merge_clauses, so the do-nothing merge the DuckDB legs ask for would
     UPDATE matched rows instead (polars_smoke probe 4).

     No parse-time "any new DUIDs?" probe, which the DuckDB legs use to skip the scan: it is
     an optimisation, the anti-join keeps the result identical without it, and a run_query on
     a SOURCE before any model has materialised is not something dbt-polars resolves. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns'
) }}

-- The reference CSVs are landed by download_aemo.py, which the log model stands for in the DAG.
-- depends_on: {{ ref('stg_csv_archive_log') }}

WITH
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
    FROM read_csv('{{ get_csv_archive_path() }}/duid/duid_data.csv')
    WHERE length(DUID) > 2
    GROUP BY DUID
  ),

  wa_facilities AS (
    SELECT
      'WA1' AS Region,
      "Facility Code" AS DUID,
      "Participant Name" AS Participant
    FROM read_csv('{{ get_csv_archive_path() }}/duid/facilities.csv')
  ),

  wa_energy AS (
    SELECT * FROM read_csv('{{ get_csv_archive_path() }}/duid/WA_ENERGY.csv')
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

  -- geo_data.csv's header says DUID. The DuckDB copies write `duid`, which DuckDB resolves
  -- case-insensitively; Polars SQL matches column names exactly.
  geo AS (
    SELECT
      DUID AS duid,
      max(latitude) AS latitude,
      max(longitude) AS longitude
    FROM read_csv('{{ get_csv_archive_path() }}/duid/geo_data.csv')
    WHERE latitude IS NOT NULL
    GROUP BY DUID
  ),

  batch AS (
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
    LEFT JOIN geo ON a.DUID = geo.duid
    GROUP BY a.DUID
  )

SELECT * FROM batch
{% if is_incremental() %}
WHERE DUID NOT IN (SELECT DUID FROM {{ this }})
{% endif %}
