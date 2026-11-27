"""dbt-polars: an incremental merge into a Delta table collects the whole batch in memory.

A local Delta catalog and one Python incremental model (merge on `id`, ROWS rows x 20 Float64
columns). Three phases, each in its own process, peak RSS sampled (the process and its children):

  A  dbt run, first build: the table is written with sink_delta (streams)
  B  dbt run, incremental merge of the same ROWS rows (half of them match): new_data.collect(),
     then dt.merge(df.to_arrow(), ...)
  C  the same merge with deltalake directly, the source a stream of the same LazyFrame
     (collect_batches -> pyarrow.RecordBatchReader), streamed_exec=True

    python dbt_polars_merge_collect_repro.py [rows]
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from importlib import metadata
from pathlib import Path

import psutil

ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 20_000_000
COLS = 20

FRAME = textwrap.dedent("""
    import polars as pl

    def frame(offset: int) -> pl.LazyFrame:
        # ROWS rows x COLS Float64 columns, ids [offset, offset + ROWS), built lazily.
        ids = pl.int_range(offset, offset + ROWS, dtype=pl.Int64).alias("id")
        return pl.LazyFrame().select(ids).with_columns(
            [(pl.col("id") * k % 1_000_003).cast(pl.Float64).alias(f"c{k}") for k in range(COLS)]
        )
""").replace("ROWS", str(ROWS)).replace("COLS", str(COLS))

MODEL = textwrap.dedent("""
    import os
    from frame import frame

    def model(dbt, session):
        dbt.config(materialized="incremental", incremental_strategy="merge", unique_key="id")
        return frame(int(os.environ["OFFSET"]))
""")

PROFILES = textwrap.dedent("""
    repro:
      target: dev
      outputs:
        dev:
          type: polars
          catalog_type: local
          schema: dev
          root: ./data
          threads: 1
""")

PROJECT = "name: repro\nversion: '1.0'\nprofile: repro\nmodel-paths: ['models']\n"

DIRECT = textwrap.dedent("""
    import sys
    import pyarrow as pa
    from deltalake import DeltaTable
    from frame import frame

    path, offset = sys.argv[1], int(sys.argv[2])
    lf = frame(offset)
    schema = lf.limit(0).collect().to_arrow().schema
    batches = (b for df in lf.collect_batches(lazy=True) for b in df.to_arrow().to_batches())
    source = pa.RecordBatchReader.from_batches(schema, batches)
    (DeltaTable(path).merge(source, "s.id = t.id", source_alias="s", target_alias="t",
                            streamed_exec=True)
        .when_matched_update_all().when_not_matched_insert_all().execute())
""")


def peak_rss(cmd: list[str], cwd: Path, env: dict) -> tuple[int, float, int]:
    """Run cmd; return (exit code, seconds, peak RSS in bytes of it and its children)."""
    t0 = time.perf_counter()
    proc = subprocess.Popen(cmd, cwd=cwd, env=env)
    ps, peak, done = psutil.Process(proc.pid), 0, threading.Event()

    def sample() -> None:
        nonlocal peak
        while not done.wait(0.2):
            try:
                procs = [ps, *ps.children(recursive=True)]
                peak = max(peak, sum(p.memory_info().rss for p in procs))
            except psutil.Error:
                pass

    t = threading.Thread(target=sample, daemon=True)
    t.start()
    rc = proc.wait()
    done.set()
    t.join()
    return rc, time.perf_counter() - t0, peak


def main() -> int:
    root = Path(tempfile.mkdtemp(prefix="dbt_polars_repro_"))
    (root / "models").mkdir()
    (root / "frame.py").write_text(FRAME)
    (root / "models" / "wide.py").write_text(MODEL)
    (root / "profiles.yml").write_text(PROFILES)
    (root / "dbt_project.yml").write_text(PROJECT)
    (root / "direct.py").write_text(DIRECT)
    env = {**os.environ, "PYTHONPATH": str(root)}
    dbt = ["dbt", "run", "--profiles-dir", "."]

    versions = {p: metadata.version(p) for p in ("dbt-polars", "dbt-core", "polars", "deltalake")}
    print(f"versions: {versions}", flush=True)
    gb = ROWS * (COLS + 1) * 8 / 1e9
    print(f"batch: {ROWS:,} rows x {COLS + 1} columns, {gb:.1f} GB as Arrow", flush=True)

    rows = []
    rc, s, a = peak_rss(dbt, root, {**env, "OFFSET": "0"})
    rows.append(("A  first build (sink_delta)", rc, s, a))
    rc, s, b = peak_rss(dbt, root, {**env, "OFFSET": str(ROWS // 2)})
    rows.append(("B  incremental merge (dbt-polars)", rc, s, b))
    table = next(p for p in (root / "data").rglob("_delta_log")).parent
    rc, s, c = peak_rss([sys.executable, "direct.py", str(table), str(ROWS)], root, env)
    rows.append(("C  same merge, streamed source (deltalake)", rc, s, c))

    print("\n| phase | exit | seconds | peak RSS |\n|---|---|---|---|")
    for name, rc, s, peak in rows:
        print(f"| {name} | {rc} | {s:.0f} | {peak / 2**30:.2f} GiB |")
    return 0 if all(r[1] == 0 for r in rows) else 1


if __name__ == "__main__":
    sys.exit(main())
