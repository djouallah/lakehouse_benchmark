"""TEMPORARY: minimal repro of TPC-H Q11 failing on Polars 2.0.0 at SF=30.

`search_sorted operation not supported for dtypes decimal[38,2] and decimal[38,12]`. Q11 is a
CROSS JOIN filtered by `value > threshold`, which Polars turns into an inequality join.
"""

from decimal import Decimal

import polars as pl

print("polars", pl.__version__)
pv = pl.DataFrame(
    {"ps_partkey": [1, 2, 3], "value": [Decimal("10.50"), Decimal("2.25"), Decimal("7.00")]},
    schema={"ps_partkey": pl.Int64, "value": pl.Decimal(38, 2)},
)
src = pl.DataFrame({"v": [Decimal("100.00")]}, schema={"v": pl.Decimal(38, 2)})
for sf in (1, 10, 30):
    sql = f"""
        WITH gv AS (SELECT SUM(v) * (0.0001 / {sf}) AS threshold FROM src)
        SELECT pv.ps_partkey, pv.value FROM pv CROSS JOIN gv
        WHERE pv.value > gv.threshold ORDER BY pv.value DESC
    """
    ctx = pl.SQLContext(pv=pv, src=src)
    for engine in ("auto", "streaming"):
        try:
            out = ctx.execute(sql, eager=False).collect(engine=engine)
            print(f"sf={sf} {engine}: ok {out.height} rows, schema {out.schema}")
        except Exception as exc:  # noqa: BLE001
            print(f"sf={sf} {engine}: {type(exc).__name__}: {exc}")
    gv = ctx.execute(f"SELECT SUM(v) * (0.0001 / {sf}) AS threshold FROM src").schema
    print(f"  threshold dtype at sf={sf}: {gv}")
