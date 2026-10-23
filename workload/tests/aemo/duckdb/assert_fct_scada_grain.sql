-- Grain check: the merge key of fct_scada must be unique. This is the tripwire for a
-- residual concurrent-writer race, and for an incremental strategy that stopped deduping.
--
-- Two steps, still exact over the whole table. A GROUP BY on the key itself held every key of
-- 386M rows (168 s on DuckDB, run 39913866873); on one 64-bit hash of the key it took 23 s
-- (run 39952876729). Tried on Spark and Trino too: no gain there, Trino got slower (705 -> 971 s).
-- A duplicate key always shares its hash, so no duplicate is missed; a hash collision only
-- sends rows to the exact GROUP BY below, which then finds no duplicate among them.
WITH dup_hashes AS (
  SELECT hash(file, DUID, SETTLEMENTDATE, INTERVENTION) AS h
  FROM {{ ref('fct_scada') }}
  GROUP BY 1
  HAVING COUNT(*) > 1
)
SELECT file, DUID, SETTLEMENTDATE, INTERVENTION, COUNT(*) AS n
FROM {{ ref('fct_scada') }}
WHERE hash(file, DUID, SETTLEMENTDATE, INTERVENTION) IN (SELECT h FROM dup_hashes)
GROUP BY ALL
HAVING COUNT(*) > 1
