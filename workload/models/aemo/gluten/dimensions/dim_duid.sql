-- DUID dimension (Spark). The reference CSVs are headered, so register them as temp views
-- (USING csv, header+inferSchema) in pre_hooks, then join. Full table replace every run --
-- atomic, so no concurrent-writer duplicate risk, and attribute changes flow through. It
-- previously also carried incremental_strategy='merge' and unique_key, which materialized
-- ='table' silently ignores; they are gone rather than left to describe a merge that never ran.
{{ config(
    materialized='table',
    pre_hook=[
      "CREATE OR REPLACE TEMPORARY VIEW duid_data USING csv OPTIONS (path '" ~ get_csv_archive_path() ~ "/duid/duid_data.csv', header 'true', inferSchema 'true')",
      "CREATE OR REPLACE TEMPORARY VIEW facilities USING csv OPTIONS (path '" ~ get_csv_archive_path() ~ "/duid/facilities.csv', header 'true', inferSchema 'true')",
      "CREATE OR REPLACE TEMPORARY VIEW wa_energy_raw USING csv OPTIONS (path '" ~ get_csv_archive_path() ~ "/duid/WA_ENERGY.csv', header 'true', inferSchema 'true')",
      "CREATE OR REPLACE TEMPORARY VIEW geo_data USING csv OPTIONS (path '" ~ get_csv_archive_path() ~ "/duid/geo_data.csv', header 'true', inferSchema 'true')"
    ]
) }}

-- depends_on: {{ ref('stg_csv_archive_log') }}

WITH states AS (
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
        first(`Fuel Source - Descriptor`) AS FuelSourceDescriptor,
        first(Participant) AS Participant
    FROM duid_data
    WHERE length(DUID) > 2
    GROUP BY DUID
),

wa_facilities AS (
    SELECT 'WA1' AS Region, `Facility Code` AS DUID, `Participant Name` AS Participant
    FROM facilities
),

duid_wa AS (
    SELECT
        wa_facilities.DUID,
        wa_facilities.Region,
        wa_energy_raw.Technology AS FuelSourceDescriptor,
        wa_facilities.Participant
    FROM wa_facilities
    LEFT JOIN wa_energy_raw ON wa_facilities.DUID = wa_energy_raw.DUID
),

duid_all AS (
    SELECT * FROM duid_aemo
    UNION ALL
    SELECT * FROM duid_wa
),

geo AS (
    SELECT duid, max(latitude) AS latitude, max(longitude) AS longitude
    FROM geo_data
    WHERE latitude IS NOT NULL
    GROUP BY duid
)

SELECT
    a.DUID,
    first(a.Region) AS Region,
    first(concat(upper(substring(trim(FuelSourceDescriptor), 1, 1)), lower(substring(trim(FuelSourceDescriptor), 2)))) AS FuelSourceDescriptor,
    first(a.Participant) AS Participant,
    first(states.State) AS State,
    first(geo.latitude) AS latitude,
    first(geo.longitude) AS longitude
FROM duid_all a
JOIN states ON a.Region = states.RegionID
LEFT JOIN geo ON a.duid = geo.duid
GROUP BY a.DUID
