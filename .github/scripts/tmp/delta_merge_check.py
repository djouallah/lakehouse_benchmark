"""TEMPORARY, read-only: checks the polars_delta leg's fct_scada after the dbt-polars#68 fork
build's appends. Prints the row count against the distinct count of the unique key, and the
stats of the files the append route committed (dbt-polars-<uuid>/ paths). Writes nothing.
Deleted once the fix is upstream."""

import json
import os

import polars as pl
from deltalake import DeltaTable

from bench import auth
from bench.workload.config import WorkloadConfig

KEYS = ["file", "DUID", "SETTLEMENTDATE", "INTERVENTION"]

cfg = WorkloadConfig.from_env()
uri = f"{cfg.tables_abfss}/dbt_polars_delta_landing/fct_scada"
opts = {"bearer_token": auth.onelake_token(), "use_fabric_endpoint": "true"}

dt = DeltaTable(uri, storage_options=opts)
print(f"version {dt.version()}")

adds = pl.DataFrame(dt.get_add_actions(flatten=True))
appended = adds.filter(pl.col("path").str.starts_with("dbt-polars-"))
print(f"files: {adds.height} live, {appended.height} written by the append route")
if appended.height:
    stat_cols = [c for c in appended.columns if c.startswith(("min.", "max.", "null_count."))]
    filled = {c: appended[c].is_not_null().sum() for c in stat_cols}
    print("stats on appended files (non-null count per column):")
    print(json.dumps({c: n for c, n in filled.items() if n}, indent=1))
    print(appended.select("path", "size_bytes", "num_records").head(3))
    for key in KEYS:
        if f"min.{key}" in appended.columns:
            lo, hi = appended[f"min.{key}"][0], appended[f"max.{key}"][0]
            print(key, "min/max of first file:", lo, hi)

lf = pl.scan_delta(uri, storage_options=opts)
rows = lf.select(pl.len()).collect(engine="streaming").item()
dupes = (
    lf.group_by(KEYS)
    .len()
    .filter(pl.col("len") > 1)
    .select(pl.len().alias("keys"), (pl.col("len") - 1).sum().alias("extra_rows"))
    .collect(engine="streaming")
)
extra = dupes["extra_rows"][0] or 0
print(f"rows {rows:,}; duplicated keys {dupes['keys'][0]:,}, extra rows {extra:,}")
per_file = lf.group_by("file").len().collect(engine="streaming")
print(
    f"distinct files {per_file.height:,}; rows per file min/median/max "
    f"{per_file['len'].min():,}/{int(per_file['len'].median()):,}/{per_file['len'].max():,}"
)
if os.environ.get("GITHUB_STEP_SUMMARY"):
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
        f.write(
            f"## polars_delta fct_scada check\n\nversion {dt.version()}, {rows:,} rows, "
            f"{dupes['keys'][0]:,} duplicated keys, {appended.height} files from the append route\n"
        )
