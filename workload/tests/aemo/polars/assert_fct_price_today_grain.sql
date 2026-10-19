-- Grain check: the merge key of fct_price_today must be unique. This is the tripwire for a
-- residual concurrent-writer race, and for an incremental strategy that stopped deduping.
SELECT file, REGIONID, SETTLEMENTDATE, INTERVENTION, COUNT(*) AS n
FROM {{ ref('fct_price_today') }}
GROUP BY ALL
HAVING COUNT(*) > 1
