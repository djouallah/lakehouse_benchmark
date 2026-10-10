"""README's Gluten/Velox vs Spark-OSS table, regenerated from the committed CSVs on every publish.

Every publish script calls `write_readme` after it has written its own CSV, so the table always
covers all three benchmarks at every scale either engine has a number for, whichever one just ran.
It reads only docs/data/*.csv -- files the publish commits anyway -- so a publish that resets onto
origin/main and regenerates (bench.yml says why) rebuilds the table from the same inputs.

THE SAME NUMBERS AS THE CHARTS. Queries: the mean of each engine's last RECENT_RUNS runs that
completed every statement, as `totals_by_sf`. ETL: the mean successful load over each engine's own
last RECENT_RUNS runs, as `recent_summary`. A scale where only one engine has a number still gets
a row: the other reads `failed` if it ran there and never completed, `—` if it never ran there.

Only the block between the two markers is rewritten; the rest of README is hand-written.
"""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

from bench.charts import RECENT_RUNS
from bench.tpcds.config import TpcdsConfig
from bench.tpch.config import TpchConfig

SPARK, GLUTEN = "pyspark_iceberg", "pyspark_gluten_iceberg"
START, END = "<!-- speedup:start -->", "<!-- speedup:end -->"
ETL_CSV = "docs/data/etl_results.csv"


def _read(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [r for r in csv.DictReader(handle) if r["engine"] in (SPARK, GLUTEN)]


def _mean_recent(runs: dict[tuple[str, int], list[tuple[str, float | None]]]) -> dict:
    """{(engine, sf): mean} over each key's last RECENT_RUNS (started, seconds) runs.

    A None in the window (a failed run) is skipped, not counted as zero; a key whose window has
    no number at all maps to None, which the table prints as `failed`.
    """
    out = {}
    for key, values in runs.items():
        window = [d for _, d in sorted(values, reverse=True)[:RECENT_RUNS] if d is not None]
        out[key] = sum(window) / len(window) if window else None
    return out


def query_totals(rows: list[dict], test: str, n_queries: int) -> dict:
    """{(engine, sf): mean cold total} over the last complete runs, as `totals_by_sf`."""
    runs: dict[tuple, dict] = defaultdict(lambda: {"started": "", "dur": 0.0, "ok": 0})
    for r in rows:
        if r["test"] != test or r["run_type"] != "cold" or r["phase"] != "query":
            continue
        run = runs[(r["engine"], int(r["sf"]), r["run_id"])]
        run["started"] = max(run["started"], r["run_started_at"])
        run["dur"] += float(r["dur"] or 0)
        run["ok"] += r["status"] == "ok"
    # An incomplete run is dropped before the window, as `totals_by_sf` does; it only marks the
    # engine as having run at that scale.
    complete: dict[tuple[str, int], list] = {}
    for (engine, sf, _), run in runs.items():
        kept = complete.setdefault((engine, sf), [])
        if run["ok"] == n_queries:
            kept.append((run["started"], run["dur"]))
    return _mean_recent(complete)


def etl_loads(rows: list[dict]) -> dict:
    """{(engine, files): mean load} over each engine's own last runs, as `recent_summary`."""
    runs: dict[tuple, dict] = defaultdict(lambda: {"started": "", "dur": None})
    for r in rows:
        if r["test"] != "etl":
            continue
        run = runs[(r["engine"], int(r["sf"]), r["run_id"])]
        run["started"] = max(run["started"], r["run_started_at"])
        if r["phase"] == "load" and r["status"] == "ok" and r["dur"]:
            run["dur"] = float(r["dur"])
    by_key: dict[tuple[str, int], list] = defaultdict(list)
    for (engine, sf, _), run in runs.items():
        by_key[(engine, sf)].append((run["started"], run["dur"]))
    return _mean_recent(by_key)


def table_lines(root: Path = Path(".")) -> list[str]:
    sections = [
        ("Light ETL", "{:,} files", etl_loads(_read(root / ETL_CSV))),
        *(
            (s.TITLE, "SF={}", query_totals(_read(root / s.CSV), s.TEST, s.N_QUERIES))
            for s in (TpchConfig, TpcdsConfig)
        ),
    ]
    lines = [
        "| Test | Scale | Spark-OSS | Gluten/Velox | Speedup |",
        "|---|---:|---:|---:|---:|",
    ]
    for title, scale, data in sections:
        for sf in sorted({sf for _, sf in data}):
            spark, gluten = data.get((SPARK, sf)), data.get((GLUTEN, sf))
            cells = [
                f"{v:,.1f}s" if v else "failed" if (e, sf) in data else "—"
                for e, v in ((SPARK, spark), (GLUTEN, gluten))
            ]
            speedup = f"{spark / gluten:.1f}x" if spark and gluten else "—"
            lines.append(f"| {title} | {scale.format(sf)} | {' | '.join(cells)} | {speedup} |")
    return lines


def write_readme(path: Path = Path("README.md"), root: Path = Path(".")) -> None:
    """Replace the block between the markers; leave README alone if they are missing."""
    text = path.read_text(encoding="utf-8")
    if START not in text or END not in text:
        print(f"::warning::{path} has no {START} block; the speedup table was not written")
        return
    head, rest = text.split(START, 1)
    _, tail = rest.split(END, 1)
    note = (
        f"Mean of each engine's last {RECENT_RUNS} runs at each scale; the query suites count "
        "only runs that completed every query, and `failed` means none did. `—` = not run. "
        "Regenerated on every publish."
    )
    body = "\n".join([START, *table_lines(root), "", note, END])
    path.write_text(head + body + tail, encoding="utf-8")
