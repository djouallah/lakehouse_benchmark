"""The TPC-DS generator's pure pieces. tpcgen-cli itself is not run here; what is here is what a
refactor could quietly break without a real run noticing."""

from __future__ import annotations

from pathlib import Path

import pytest

from bench.tpcds.config import TABLES, parts_for
from bench.tpcds.generate import plan_table, tpcgen_command


@pytest.mark.parametrize(
    ("exists", "has_files", "force", "want"),
    [
        (False, False, False, "write"),  # nothing there: create and write
        (True, False, False, "write"),  # a husk from a crashed run: write into it
        (True, True, False, "skip"),  # done: a resumed run must not register files twice
        (True, True, True, "purge"),  # regenerate: throw the table away first
        (True, False, True, "purge"),
        (False, False, True, "write"),
    ],
)
def test_resume_decision(exists, has_files, force, want):
    assert plan_table(exists, has_files, force) == want


def test_fact_tables_split_and_dimensions_do_not():
    assert parts_for("store_sales", 100) == 60
    assert parts_for("store_sales", 1) == 1
    assert parts_for("date_dim", 100) == 1
    assert parts_for("customer", 100) == 1
    assert all(parts_for(table, 10_000) <= 192 for table in TABLES)


def test_tpcgen_command_is_one_table_zstd_with_parts_only_when_split():
    one = tpcgen_command(10, "web_site", 1, Path("out"))
    assert one[:3] == ["tpcgen-cli", "tpcds", "parquet"]
    assert "ZSTD(1)" in one and "--parts" not in one
    split = tpcgen_command(10, "store_sales", 6, Path("out"))
    assert split[split.index("--tables") + 1] == "store_sales"
    assert split[split.index("--parts") + 1] == "6"


def test_generator_imports_without_pyiceberg_at_module_level():
    """Heavy imports stay inside the functions, so the module loads without the landing env."""
    import sys

    assert "bench.tpcds.generate" in sys.modules
    source = Path(sys.modules["bench.tpcds.generate"].__file__).read_text(encoding="utf-8")
    head = source.split("def _log")[0]
    assert "import pyarrow" not in head and "import pyiceberg" not in head
