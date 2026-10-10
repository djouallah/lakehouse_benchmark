"""Keep the catalogs that worked, check them, and write them for the page.

testing-iceberg-rest-catalog's storage-token workflow writes results/storage_token.json there;
.github/workflows/storage_token.yml downloads it and runs this. Only the catalogs where DuckDB wrote
a row and read it back are kept: the Catalogs tab of docs/index.html shows what works.

    python .github/scripts/storage_token_publish.py SOURCE [TARGET]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from bench.report import leak_check

TARGET = Path("docs/data/storage_token.json")
FIELDS = {"catalog", "label", "storage"}


def keep(result: dict) -> dict:
    """The result with only the catalogs that worked; SystemExit if it is not one the page draws."""
    if not {"run", "date", "catalogs"} <= set(result):
        raise SystemExit(f"::error::missing run, date or catalogs: {sorted(result)}")
    ok = [{k: c[k] for k in sorted(FIELDS)} for c in result["catalogs"] if c.get("ok") is True]
    if not ok:
        raise SystemExit("::error::no catalog worked")
    return {"run": result["run"], "date": result["date"], "catalogs": ok}


def main() -> int:
    source = Path(sys.argv[1])
    target = Path(sys.argv[2]) if len(sys.argv) > 2 else TARGET
    result = keep(json.loads(source.read_text(encoding="utf-8")))
    target.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    leak_check([target])
    print(f"wrote {target}: {', '.join(c['catalog'] for c in result['catalogs'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
