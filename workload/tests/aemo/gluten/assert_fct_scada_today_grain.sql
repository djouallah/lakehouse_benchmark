-- Grain check: the merge key of fct_scada_today must be unique. This is the tripwire for a
-- residual concurrent-writer race, and for an incremental strategy that stopped deduping.
SELECT `file`, `DUID`, `SETTLEMENTDATE`, COUNT(*) AS n
FROM {{ ref('fct_scada_today') }}
-- Ordinals on the sail probe target: Sail reads GROUP BY ALL as a column named ALL.
GROUP BY {{ '1, 2, 3' if target.name == 'sail' else 'ALL' }}
HAVING COUNT(*) > 1
