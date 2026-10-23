"""Polars SQL on the workload's main build: does fct_summary's DOUBLE -> DECIMAL(18,4) land on
Spark's last digit with the nudged cast, and which hash function does Polars SQL take?

Spark rounds a tie half away from zero (BigDecimal(Double.toString), HALF_UP). Polars rounds
DOUBLE -> DECIMAL as (x * 10^s).round_ties_even() and DECIMAL rescale to nearest even
(polars-compute decimal.rs). The nudge: DECIMAL(38,10) holds AEMO's <= 5-decimal values
exactly; moving it 1e-10 away from zero leaves no tie, so any nearest rounding gives HALF_UP.
"""

from decimal import Decimal

import polars as pl

xs = [96.07385, -38.11185, 0.08545, 71.47265, 203.99725, 0.0005, 1e-7, 15500.12345,
      -999.99995, -0.00005, 0.0, 1234.5, 71.47265000000000157, 203.99725000000000819]
# Spark's answer: HALF_UP on the double's shortest text.
spark = [Decimal(repr(x)).quantize(Decimal("0.0001"), rounding="ROUND_HALF_UP") for x in xs]

NUDGED = """CAST(CASE WHEN x < 0 THEN CAST(x AS DECIMAL(38,10)) - CAST('0.0000000001' AS DECIMAL(38,10))
                   ELSE CAST(x AS DECIMAL(38,10)) + CAST('0.0000000001' AS DECIMAL(38,10)) END AS DECIMAL(18,4))"""
ctx = pl.SQLContext(v=pl.LazyFrame({"x": xs}))
out = ctx.execute(
    f"SELECT x, CAST(x AS DECIMAL(18,4)) AS plain, {NUDGED} AS nudged FROM v", eager=True
)
print(out)
bad = 0
for x, plain, nudged, want in zip(xs, out["plain"], out["nudged"], spark):
    flag = "" if nudged == want else "  <-- NUDGED != SPARK"
    bad += nudged != want
    print(f"{x!r:>24}  plain {plain}  nudged {nudged}  spark {want}{flag}")
print("nudged matches Spark on every value" if bad == 0 else f"{bad} values differ")

# Hash candidates for the grain tests' pre-check.
k = pl.SQLContext(t=pl.LazyFrame({"a": ["f1", "f1"], "b": [1, 1]}))
for fn in ["HASH(a, b)", "HASH(a)", "XXHASH64(a, b)", "MD5(a)", "CONCAT_WS('|', a, CAST(b AS VARCHAR))"]:
    try:
        r = k.execute(f"SELECT {fn} AS h FROM t", eager=True)
        print(f"OK   {fn}: {r['h'].dtype} {r['h'].to_list()}")
    except Exception as e:  # noqa: BLE001
        print(f"FAIL {fn}: {type(e).__name__}: {str(e).splitlines()[0]}")
