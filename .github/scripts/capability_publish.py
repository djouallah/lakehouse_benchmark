"""Check the Iceberg support grid before it is published.

.github/scripts/capability/render_readme.py writes docs/data/capability.json from
results/capability/; this checks it is the shape the Iceberg support tab of docs/index.html draws,
that every engine in it has a label and colour here, and that it carries no token.

    python .github/scripts/capability_publish.py [path]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from bench.charts import LABEL
from bench.report import leak_check

TARGET = Path("docs/data/capability.json")
OUTCOMES = {"supported", "no", "no-op", "skipped", "broken", "na"}


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


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
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else TARGET
    grid = check(load(path))
    leak_check([path])
    print(f"checked {path}: {len(grid['rows'])} rows, engines {', '.join(grid['engines'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
