"""A runner that dies leaves only its job log: the version comes from there too."""

import importlib.util
from pathlib import Path

from bench.tpch.config import TpchConfig

_SCRIPT = Path(__file__).parents[1] / ".github" / "scripts" / "killed_parts.py"
_spec = importlib.util.spec_from_file_location("killed_parts", _SCRIPT)
killed_parts = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(killed_parts)

T = "2026-10-31T10:39:50.0Z "
LOG = (
    f"{T}polars==2.1.0\n"
    f"{T}polars_iceberg | polars 2.1.0 | SF=300 | namespace CH0300\n"
    f"{T}  setup 2.5s\n"
)
MAIN = f"{T}Collecting polars @ https://x/download/polars-main-c8c0e7a2c4/polars-2.1.0-py3-none-any.whl\n"


def test_a_polars_main_wheel_is_named_main():
    _, result = killed_parts.part_from_log(MAIN + LOG, TpchConfig)
    assert result.version == "main c8c0e7a2c4"


def test_a_polars_release_keeps_its_version():
    _, result = killed_parts.part_from_log(LOG, TpchConfig)
    assert result.version == "2.1.0"
