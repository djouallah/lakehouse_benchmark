-- Grain check: the merge key of fct_price_today must be unique. This is the tripwire for a
-- residual concurrent-writer race, and for an incremental strategy that stopped deduping.
SELECT `file`, `REGIONID`, `SETTLEMENTDATE`, `INTERVENTION`, COUNT(*) AS n
FROM {{ ref('fct_price_today') }}
-- Ordinals, not GROUP BY ALL, which Sail reads as a column named ALL (lakehq/sail#2836).
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1
