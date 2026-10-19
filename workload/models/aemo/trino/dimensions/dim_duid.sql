-- DUID dimension (Trino). The four reference CSVs share one landing folder and so one Hive
-- table, files.dbt.duid (c0, c1, ... by position, header skipped); each is picked by name on
-- "$path". Positions are the files' header order (download_aemo.py lands them as published):
--   duid_data.csv   Participant c0, Region c2, Fuel Source - Descriptor c7, DUID c12
--   facilities.csv  Participant Name c1, Facility Code c2
--   WA_ENERGY.csv   DUID c0, Technology c3
--   geo_data.csv    DUID c3, latitude c5, longitude c6
-- Full table replace every run, as in the Spark tree. arbitrary() is Spark's and DuckDB's first().
{{ config(materialized='table') }}

-- depends_on: {{ ref('stg_csv_archive_log') }}

WITH duid_csv AS (
  SELECT element_at(split("$path", '/'), -1) AS f, * FROM files.dbt.duid
),

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
        NULLIF(c12, '') AS DUID,
        arbitrary(NULLIF(c2, '')) AS Region,
        arbitrary(NULLIF(c7, '')) AS FuelSourceDescriptor,
        arbitrary(NULLIF(c0, '')) AS Participant
    FROM duid_csv
    WHERE f = 'duid_data.csv' AND length(c12) > 2
    GROUP BY NULLIF(c12, '')
),

wa_facilities AS (
    SELECT 'WA1' AS Region, NULLIF(c2, '') AS DUID, NULLIF(c1, '') AS Participant
    FROM duid_csv WHERE f = 'facilities.csv'
),

wa_energy_raw AS (
    SELECT NULLIF(c0, '') AS DUID, NULLIF(c3, '') AS Technology
    FROM duid_csv WHERE f = 'WA_ENERGY.csv'
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
    SELECT
        NULLIF(c3, '') AS duid,
        max(CAST(NULLIF(c5, '') AS DOUBLE)) AS latitude,
        max(CAST(NULLIF(c6, '') AS DOUBLE)) AS longitude
    FROM duid_csv
    WHERE f = 'geo_data.csv' AND NULLIF(c5, '') IS NOT NULL
    GROUP BY NULLIF(c3, '')
)

SELECT
    a.DUID,
    arbitrary(a.Region) AS Region,
    arbitrary(concat(upper(substr(trim(FuelSourceDescriptor), 1, 1)), lower(substr(trim(FuelSourceDescriptor), 2)))) AS FuelSourceDescriptor,
    arbitrary(a.Participant) AS Participant,
    arbitrary(states.State) AS State,
    arbitrary(geo.latitude) AS latitude,
    arbitrary(geo.longitude) AS longitude
FROM duid_all a
JOIN states ON a.Region = states.RegionID
LEFT JOIN geo ON a.DUID = geo.duid
GROUP BY a.DUID
