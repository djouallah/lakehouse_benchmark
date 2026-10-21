"""The engine palette and display labels, mirrored by the dashboard (docs/index.html).

The dashboard draws every chart; tests/test_results_page.py holds its CSS colour variables and
labels to the values here, so an engine looks the same on every tab.

COLOR. One categorical slot per engine, in a FIXED order -- assigned to the ENGINE, never to its
rank, so a run that omits an engine never repaints the survivors. Daft holds slot 5 even though it
is not in bench.yml's engine list, which is the rule working as intended: adding it later must not
recolour Spark.

Slot 7, brown, is Gluten/Velox, validated in that position in both modes -- nothing passes the
normal-vision floor between blue and orange.

Slot 8, plum, is StarRocks. Bars sort fastest-first, so a new slot can land beside ANY other, and
it was chosen to pass CVD and the normal-vision floor against every one of the seven, pairwise, in
both modes (a scan of OKLCH for the hue that clears them all; only plum does in both). The dark
step carries a contrast WARN (2.61:1), under the same relief rule as light aqua and yellow.

Slot 9, indigo, is Trino, chosen the same way against all eight -- Daft included, since the ETL
charts still draw it -- by a scan of OKLCH with the dataviz validator's own Delta E: worst pair
19.0 normal / 10.0 CVD in light, 15.2 / 8.5 in dark, contrast 9.0:1 and 3.07:1. An olive cleared
light mode and failed dark (CVD 3.5). Checked on ALL pairs, which the older slots are not: brown
sits 12.6 from orange in light and 4.7 from yellow in dark, both under the 15 normal-vision floor,
which is what the per-bar value labels are for.

Slot 10, rust, is pandas (Light ETL only), found by the same scan against all nine. No hue clears
the 15 normal-vision floor against every slot in BOTH modes any more; rust comes closest. Light
#932601: worst pair 14.2 normal (StarRocks) / 11.7 CVD, contrast 8.1:1. Dark #8d4e10: worst pair
15.2 normal (Gluten/Velox) / 9.3 CVD (chDB), contrast WARN 2.68:1. The light pairs under 15 are
again what the per-bar value labels are for.

Slot 11, periwinkle, is Databend, by the same scan against all ten (validate_palette.js per
pair). One hue in both modes, a light and a dark step. Light #99a7f9: worst pair 18.6 normal / 15.9
CVD (DuckDB), contrast WARN 2.21:1. Dark #4f58b1: worst pair 12.8 normal (Trino) / 7.9 CVD
(StarRocks), contrast WARN 2.79:1. No dark step of any hue in the band clears 15 against all ten;
brick reached 14.3, but a hue that changes between modes is a different engine to the eye. Value
labels again carry the relief.

The palette is validated, not eyeballed (dataviz `scripts/validate_palette.js`), and re-validated
whenever a slot is added -- every subset that can actually render has to pass, not just the full
set. Light passes every gate with a contrast WARN on aqua and yellow, which obligates the relief
rule -- hence the value labels on the dashboard's bars. Dark passes outright, though amber-green
sits in the 6-8 CVD floor band, which the legend and the bar gaps satisfy as secondary encoding.
"""

from __future__ import annotations

# Categorical slots, assigned to engines in a FIXED order.
LIGHT = {
    "duckdb_iceberg": "#2a78d6",  # slot 1 blue
    "chdb_iceberg": "#eb6834",  # slot 2 orange
    "polars_iceberg": "#1baf7a",  # slot 3 aqua
    "lakesail_iceberg": "#eda100",  # slot 4 yellow
    "daft_iceberg": "#8a5cd1",  # slot 5 purple
    "pyspark_iceberg": "#d6468f",  # slot 6 magenta
    "pyspark_gluten_iceberg": "#a8642a",  # slot 7 brown
    "starrocks_iceberg": "#980067",  # slot 8 plum
    "trino_iceberg": "#3c0fd9",  # slot 9 indigo
    "pandas_iceberg": "#932601",  # slot 10 rust
    "databend_iceberg": "#99a7f9",  # slot 11 periwinkle
}
DARK = {
    "duckdb_iceberg": "#3987e5",
    "chdb_iceberg": "#d95926",
    "polars_iceberg": "#199e70",
    "lakesail_iceberg": "#c98500",
    "daft_iceberg": "#9b6ee0",
    "pyspark_iceberg": "#e05a9c",
    "pyspark_gluten_iceberg": "#c07a3a",
    "starrocks_iceberg": "#a0268b",
    "trino_iceberg": "#5c43ff",
    "pandas_iceberg": "#8d4e10",
    "databend_iceberg": "#4f58b1",
}

LABEL = {
    "duckdb_iceberg": "DuckDB",
    "chdb_iceberg": "chDB",
    "polars_iceberg": "Polars",
    "lakesail_iceberg": "LakeSail",
    "daft_iceberg": "Daft",
    "pyspark_iceberg": "Spark-OSS",
    "pyspark_gluten_iceberg": "Gluten/Velox",
    "starrocks_iceberg": "StarRocks",
    "trino_iceberg": "Trino",
    "pandas_iceberg": "Pandas",
    "databend_iceberg": "Databend",
}
