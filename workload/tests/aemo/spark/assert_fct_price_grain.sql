-- Grain check: the merge key of fct_price must be unique. This is the tripwire for a
-- residual concurrent-writer race, and for an incremental strategy that stopped deduping.
SELECT `file`, `REGIONID`, `SETTLEMENTDATE`, `INTERVENTION`, COUNT(*) AS n
FROM {{ ref('fct_price') }}
GROUP BY ALL
HAVING COUNT(*) > 1
