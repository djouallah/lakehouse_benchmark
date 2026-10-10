"""Raw plans (plans.yml's artifact) -> docs/data/plans/*.json, the files the Bad joins tab reads.

    python .github/scripts/plans_normalize.py <raw dir> [<out dir>]

Every `*.json` under the raw directory is one capture (.github/scripts/capture_plan.py); each
becomes one file of the same name in the out directory (default docs/data/plans), in the shape
bench/plans.py documents. Pure file work: no engine, no network.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from bench.plans import normalize


def main() -> int:
    raw_dir = Path(sys.argv[1])
    out_dir = Path(sys.argv[2] if len(sys.argv) > 2 else "docs/data/plans")
    out_dir.mkdir(parents=True, exist_ok=True)
    for path in sorted(raw_dir.rglob("*.json")):
        plan = normalize(json.loads(path.read_text(encoding="utf-8")))
        target = out_dir / path.name
        target.write_text(json.dumps(plan, indent=1) + "\n", encoding="utf-8")
        print(f"{path.name}: {plan['status']} {plan['seconds']}s -> {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
