"""What every publish script shares: merge the matrix artifacts, flatten to CSV, refuse to leak.

Lifted out of .github/scripts/publish.py so that publish.py and etl_publish.py share one copy.
Moving the three functions was the alternative to copying the leak check, which is the one piece
of this repo that must exist exactly once.

THE LEAK CHECK IS NOT OPTIONAL. Everything a publish script writes is about to be committed to a
public repo, and git history is forever. `find_token_shaped` scans for JWT-shaped strings that
this process never saw -- one an engine logged, or a subprocess minted -- and refuses to publish
rather than leaking.
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

from bench import scrub
from bench.store import Run, now_iso, read_engine_part


def merge(parts_dir: Path, cfg_sf: int) -> Run:
    """One Run from the per-engine parts in `parts_dir`, stamped with the bench runners' facts."""
    run = Run(
        run_id="{}-{}".format(
            os.environ.get("GITHUB_RUN_ID", "local"),
            os.environ.get("GITHUB_RUN_ATTEMPT", "1"),
        ),
        run_started_at=os.environ.get("BENCH_RUN_STARTED_AT") or now_iso(),
        sf=cfg_sf,
        run_url=os.environ.get("BENCH_RUN_URL", ""),
        git_sha=os.environ.get("GITHUB_SHA", "")[:7],
    )
    hosts = []
    for path in sorted(parts_dir.rglob("*.json")):
        engine, result = read_engine_part(path)
        run.engines[engine] = result
        if result.host:
            hosts.append((engine, result.host))
        print(f"  merged {engine}: {len(result.rows)} rows, status={result.status}")
    if not run.engines:
        raise SystemExit(f"no engine parts found under {parts_dir}; nothing to publish")

    # The hardware comes from the BENCH runners, never from this one. If the matrix somehow ran
    # on mixed hardware the numbers are not comparable, so say so loudly rather than silently
    # stamping the run with whichever came first.
    if hosts:
        distinct = {tuple(sorted(h.items())) for _, h in hosts}
        if len(distinct) > 1:
            print(f"::warning::engines ran on {len(distinct)} different runner specs: {hosts}")
        for key, value in hosts[0][1].items():
            setattr(run, key, value)
    return run


def write_csv(table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(table.column_names)
        columns = [table.column(name).to_pylist() for name in table.column_names]
        writer.writerows(zip(*columns, strict=True))


def leak_check(paths: list[Path]) -> None:
    """Refuse to publish anything that looks like it contains a bearer token."""
    for path in paths:
        if not path.exists() or path.suffix not in {".json", ".md", ".csv"}:
            continue
        hits = scrub.find_token_shaped(path.read_text(encoding="utf-8", errors="replace"))
        if hits:
            raise SystemExit(
                f"::error::refusing to publish {path}: it contains {len(hits)} JWT-shaped "
                f"string(s). Rotate the credential and fix the code path that logged it."
            )
