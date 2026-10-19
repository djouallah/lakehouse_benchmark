{{ config(severity='warn') }}
-- Rows returned = landed scada_today files the model has not folded into fct_scada_today YET.
-- A WARNING, not a failure: with a process_limit smaller than the backlog an initial load
-- cannot fold everything, and what it did fold is correct -- just incomplete. Files are
-- selected NEWEST first on every engine, so a row here is older data still queued, and the
-- count falls run by run until it reaches zero. Investigate a count that STOPS falling, not
-- a count that is non-zero.
SELECT
  csv_filename
FROM {{ ref('stg_csv_archive_log') }}
WHERE source_type = 'scada_today'
  AND csv_filename NOT IN (
    SELECT DISTINCT `file`
    FROM {{ ref('fct_scada_today') }}
  )
