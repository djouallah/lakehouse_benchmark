"""Publish the capability grid: what each engine can do against the OneLake Iceberg REST catalog.

The probes run in djouallah/iceberg-probe-native, which writes the grid as docs/capability.json
(its render_readme.py, from its results/). This copies that file to docs/data/capability.json,
where the Capability tab of docs/index.html reads it, after checking that it is the shape the
page expects and that every engine in it is one this site knows. Rows the catalog itself blocks
arrive already set apart under `blocked`.

    python .github/scripts/capability_publish.py [url-or-path]
"""

from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

from bench.charts import LABEL
from bench.report import leak_check

SOURCE = (
    "https://raw.githubusercontent.com/djouallah/iceberg-probe-native/main/docs/capability.json"
)
TARGET = Path("docs/data/capability.json")
OUTCOMES = {"supported", "no", "no-op", "skipped", "broken", "na"}


def fetch(source: str) -> dict:
    if source.startswith("https://"):
        with urllib.request.urlopen(source, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))
    return json.loads(Path(source).read_text(encoding="utf-8"))


def check(grid: dict) -> dict:
    """The grid, if it is one the page can draw; SystemExit naming the first thing wrong."""
    unknown = set(grid["engines"]) - set(LABEL)
    if unknown:
        raise SystemExit(f"::error::engines with no label or colour here: {sorted(unknown)}")
    for row in grid["rows"]:
        if row["group"] not in grid["groups"]:
            raise SystemExit(f"::error::{row['label']!r} is in unknown group {row['group']!r}")
        if set(row["cells"]) != set(grid["engines"]):
            raise SystemExit(f"::error::{row['label']!r} does not have one cell per engine")
        bad = {c["o"] for c in row["cells"].values()} - OUTCOMES
        if bad:
            raise SystemExit(f"::error::{row['label']!r} has unknown outcomes {sorted(bad)}")
    return grid


def main() -> int:
    grid = check(fetch(sys.argv[1] if len(sys.argv) > 1 else SOURCE))
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    TARGET.write_text(
        json.dumps(grid, ensure_ascii=False, indent=1) + "\n", encoding="utf-8", newline="\n"
    )
    leak_check([TARGET])
    print(f"wrote {TARGET}: {len(grid['rows'])} rows, engines {', '.join(grid['engines'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
