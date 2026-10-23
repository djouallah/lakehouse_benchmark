-- Grain check: the merge key of fct_summary must be unique. This is the tripwire for a
-- residual concurrent-writer race, and for an incremental strategy that stopped deduping.
--
-- Two steps, still exact over the whole table. A GROUP BY on the key itself held every key of
-- 150M rows (25 s on Gluten, 26 s on Sail, run 39913866873); a GROUP BY on one 64-bit hash of the key is far
-- smaller. A duplicate key always shares its hash, so no duplicate is missed; a hash collision
-- only sends rows to the exact GROUP BY below, which then finds no duplicate among them.
WITH dup_hashes AS (
  SELECT xxhash64(`date`, `time`, `DUID`) AS h
  FROM {{ ref('fct_summary') }}
  GROUP BY 1
  HAVING COUNT(*) > 1
)
SELECT `date`, `time`, `DUID`, COUNT(*) AS n
FROM {{ ref('fct_summary') }}
WHERE xxhash64(`date`, `time`, `DUID`) IN (SELECT h FROM dup_hashes)
-- Ordinals, not GROUP BY ALL, which Sail reads as a column named ALL (lakehq/sail#2836).
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
