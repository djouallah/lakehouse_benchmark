-- Grain check: the merge key of fct_price_today must be unique. This is the tripwire for a
-- residual concurrent-writer race, and for an incremental strategy that stopped deduping.
SELECT `file`, `REGIONID`, `SETTLEMENTDATE`, `INTERVENTION`, COUNT(*) AS n
FROM {{ ref('fct_price_today') }}
-- Ordinals on the sail probe target: Sail reads GROUP BY ALL as a column named ALL.
GROUP BY {{ '1, 2, 3, 4' if target.name == 'sail' else 'ALL' }}
HAVING COUNT(*) > 1
