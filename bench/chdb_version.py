"""Version label shared by every chDB benchmark surface."""

from __future__ import annotations

import os
from importlib.metadata import version


def chdb_version() -> str:
    label = f"{version('chdb')} / core {version('chdb-core')}"
    source = os.environ.get("CHDB_CORE_SOURCE", "").strip()
    return f"{label} ({source})" if source else label
