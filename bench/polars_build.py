"""The Polars a run used, as results and the grid should name it.

A main wheel reports the last release's version (pl.__version__ said 2.1.0 on main c8c0e7a2c4). The
one thing that tells them apart is where pip got the wheel: tmp_polars_main.yml attaches it to a
polars-main-<sha> prerelease of this repo, and requirements/*polars*.txt pin that URL.
"""

from __future__ import annotations

import re
from importlib import metadata

_MAIN_WHEEL = re.compile(r"/polars-main-([0-9a-f]+)/")


def polars_version() -> str:
    """`main <sha>` for a main wheel, otherwise the release's version."""
    dist = metadata.distribution("polars")
    found = _MAIN_WHEEL.search(dist.read_text("direct_url.json") or "")
    return f"main {found.group(1)}" if found else dist.version
