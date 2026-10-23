-- Grain check: the merge key of fct_scada must be unique. This is the tripwire for a
-- residual concurrent-writer race, and for an incremental strategy that stopped deduping.
--
-- Two steps, still exact over the whole table. A GROUP BY on the key itself held every key of
-- 386M rows (80 s on Gluten, 110 s on Sail, run 39913866873); a GROUP BY on one 64-bit hash of the key is far
-- smaller. A duplicate key always shares its hash, so no duplicate is missed; a hash collision
-- only sends rows to the exact GROUP BY below, which then finds no duplicate among them.
WITH dup_hashes AS (
  SELECT xxhash64(`file`, `DUID`, `SETTLEMENTDATE`, `INTERVENTION`) AS h
  FROM {{ ref('fct_scada') }}
  GROUP BY 1
  HAVING COUNT(*) > 1
)
SELECT `file`, `DUID`, `SETTLEMENTDATE`, `INTERVENTION`, COUNT(*) AS n
FROM {{ ref('fct_scada') }}
WHERE xxhash64(`file`, `DUID`, `SETTLEMENTDATE`, `INTERVENTION`) IN (SELECT h FROM dup_hashes)
-- Ordinals, not GROUP BY ALL, which Sail reads as a column named ALL (lakehq/sail#2836).
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1
