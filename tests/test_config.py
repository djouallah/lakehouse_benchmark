"""Part counts, cache sizing, the suite configs, and the naming that ties this repo to the notebook.

The part plan is pinned to exact numbers on purpose. It is the thing that keeps peak local disk
proportional to PART size rather than dataset size, and a refactor that quietly returns 2 parts
for lineitem again would not fail anything else -- it would just make SF=10 slow and SF=30 run out
of disk, twenty minutes in.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from bench.config import Config
from bench.tpcds.config import TpcdsConfig
from bench.tpch.config import (
    PART_FLOOR,
    TABLES,
    TpchConfig,
    chdb_cache_gib,
    parts_for,
    parts_plan,
)


def test_lineitem_is_split_into_readable_files():
    """The regression the notebook's `scaled()` rule had: two 900MB parts at SF=10."""
    assert parts_for("lineitem", 10) == 9
    assert parts_for("lineitem", 1) == 2
    assert parts_for("lineitem", 30) == 27


def test_small_tables_are_one_part():
    for table in ("nation", "region", "supplier"):
        assert parts_for(table, 100) == 1


def test_floors_keep_the_smoke_path_multi_file():
    """At SF=1 a pure size rule gives one part for everything, so the smoke run would not
    resemble the run it is screening."""
    for table, floor in PART_FLOOR.items():
        assert parts_for(table, 1) >= floor


def test_plan_covers_every_table_in_generation_order():
    plan = parts_plan(10)
    assert list(plan) == list(TABLES)
    # supplier LAST is load-bearing: it carries the generation-complete marker.
    assert list(plan)[-1] == "supplier"


@pytest.mark.parametrize("gib", [0.3, 2.7, 8.0, 27.0, 100.0])
def test_chdb_cache_always_fits_the_runner_disk(gib):
    """ClickHouse does not check free space before filling the cache, so a max_size larger than
    the disk is an ENOSPC mid-query rather than an eviction."""
    assert 2 <= chdb_cache_gib(gib) <= 8


def test_chdb_cache_is_sized_from_each_suites_own_estimate():
    """A TPC-DS SF is not a TPC-H SF; both land on the same cache at SF=10 because both datasets
    are ~3 GB there, not because the number was passed through."""
    tpch = TpchConfig(workspace_id="w", lakehouse_id="l", sf=10)
    tpcds = TpcdsConfig(workspace_id="w", lakehouse_id="l", sf=10)
    assert 2 < tpch.estimated_gib < 4 and 2 < tpcds.estimated_gib < 4
    assert chdb_cache_gib(tpch.estimated_gib) == chdb_cache_gib(tpcds.estimated_gib) == 5


def test_schema_name_matches_the_fabric_notebook():
    """Identical naming is what lets the notebook and CI share generated data in one lakehouse."""
    assert Config(workspace_id="w", lakehouse_id="l", sf=1).schema == "CH0001"
    assert Config(workspace_id="w", lakehouse_id="l", sf=10).schema == "CH0010"
    assert Config(workspace_id="w", lakehouse_id="l", sf=100).schema == "CH0100"


def test_tpcds_namespace_never_collides_with_tpch():
    """DS0010 beside CH0010 in one lakehouse: neither suite can read the other's tables."""
    assert TpcdsConfig(workspace_id="w", lakehouse_id="l", sf=1).schema == "DS0001"
    assert TpcdsConfig(workspace_id="w", lakehouse_id="l", sf=10).schema == "DS0010"
    assert TpchConfig(workspace_id="w", lakehouse_id="l", sf=10).schema == "CH0010"


def test_each_suite_reads_its_own_scale(monkeypatch):
    """bench.yml sets TPCH_SF and tpcds.yml sets TPCDS_SF; neither may read the other's."""
    monkeypatch.setenv("FABRIC_WORKSPACE_ID", "w")
    monkeypatch.setenv("FABRIC_LAKEHOUSE_ID", "l")
    monkeypatch.setenv("TPCH_SF", "30")
    monkeypatch.setenv("TPCDS_SF", "1")
    assert TpchConfig.from_env().sf == 30
    assert TpcdsConfig.from_env().sf == 1


@pytest.mark.parametrize("suite", [TpchConfig, TpcdsConfig])
def test_marker_table_is_last_in_generation_order(suite):
    """The completion marker is written on the last table, so it must BE the last table."""
    assert suite.TABLES[-1] == suite.MARKER_TABLE


def test_tpcds_has_the_twenty_four_spec_tables():
    assert len(TpcdsConfig.TABLES) == 24
    assert len(set(TpcdsConfig.TABLES)) == 24
    assert "dbgen_version" not in TpcdsConfig.TABLES  # dsdgen emits it; the spec does not have it
    assert TpcdsConfig.TABLES[0] == "store_sales"  # largest first


def test_tpcds_runs_a_subset_of_the_tpch_engines():
    """Fewer engines, but only ones TPC-H also knows -- the charts key off shared identifiers.

    The exclusions are measured (bench/tpcds/config.py names the runs); what this guards is that
    dropping one never invents a NEW engine id, which would reach bench/charts.py with no label
    and no colour.
    """
    assert set(TpcdsConfig.ENGINES) < set(TpchConfig.ENGINES)


def test_workflow_defaults_dispatch_only_engines_their_suite_lists():
    """Each workflow hardcodes its engine list twice; nothing in it may be off the suite's roll.

    An engine the suite does not list writes a part file publish.py never reads, so the run
    spends a job on it and then drops it from the results. A workflow may list FEWER than the
    suite -- bench.yml leaves Daft out over Eventual-Inc/Daft#7532 while TPC-H's ENGINES keeps it
    for the ETL charts -- and tpcds.yml lists its seven exactly.
    """
    root = Path(__file__).resolve().parent.parent
    for workflow, suite in (("tpcds.yml", TpcdsConfig), ("bench.yml", TpchConfig)):
        text = (root / ".github" / "workflows" / workflow).read_text(encoding="utf-8")
        listed = re.findall(r"(?:default: |inputs\.engines \|\| ')([a-z_,]*iceberg)", text)
        assert len(listed) == 2, workflow
        assert listed[0] == listed[1], workflow  # the input default and the plan step's fallback
        for value in listed:
            assert set(value.split(",")) <= set(suite.ENGINES), (workflow, value)
    text = (root / ".github" / "workflows" / "tpcds.yml").read_text(encoding="utf-8")
    listed = re.findall(r"(?:default: |inputs\.engines \|\| ')([a-z_,]*iceberg)", text)
    assert sorted(listed[0].split(",")) == sorted(TpcdsConfig.ENGINES)


def test_suite_class_follows_bench_suite(monkeypatch):
    from bench.suite import suite_class

    monkeypatch.delenv("BENCH_SUITE", raising=False)
    assert suite_class() is TpchConfig
    monkeypatch.setenv("BENCH_SUITE", "tpcds")
    assert suite_class() is TpcdsConfig
    assert suite_class("tpch") is TpchConfig
    with pytest.raises(ValueError, match="unknown suite"):
        suite_class("etl")


def test_warehouse_is_guid_over_guid():
    """The Iceberg REST catalog resolves ids, not names."""
    cfg = Config(workspace_id="ws-guid", lakehouse_id="lh-guid", sf=10)
    assert cfg.warehouse == "ws-guid/lh-guid"
    assert cfg.base_path == "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/lh-guid"


class _FakeTable:
    def __init__(self, files, properties=None):
        self._files = files
        self.properties = properties or {}

    def scan(self):
        return self

    def plan_files(self):
        return iter(self._files)


class _FakeCatalog:
    def __init__(self, tables):
        self._tables = tables

    def table_exists(self, identifier):
        return identifier in self._tables

    def load_table(self, identifier):
        return self._tables[identifier]


def test_existing_data_without_a_marker_counts_as_complete():
    """The Fabric notebook generated CH0100 before this repo existed, so it has data and no
    marker. Regenerating over it would re-register every file through
    add_files(check_duplicate_files=False) and silently double every row count."""
    from bench.tpch.generate import is_complete

    cfg = TpchConfig(workspace_id="w", lakehouse_id="l", sf=100)
    catalog = _FakeCatalog({"CH0100.supplier": _FakeTable(files=["a.parquet"])})
    assert is_complete(catalog, cfg) is True


def test_an_empty_supplier_is_not_complete():
    """The husk a crash leaves between create_table_if_not_exists and add_files. The notebook's
    bare table_exists() check read this as done and then benchmarked nothing."""
    from bench.tpch.generate import is_complete

    cfg = TpchConfig(workspace_id="w", lakehouse_id="l", sf=10)
    catalog = _FakeCatalog({"CH0010.supplier": _FakeTable(files=[])})
    assert is_complete(catalog, cfg) is False


def test_the_marker_alone_is_enough():
    from bench.tpch.generate import COMPLETE_PROPERTY, is_complete

    cfg = TpchConfig(workspace_id="w", lakehouse_id="l", sf=10)
    catalog = _FakeCatalog(
        {
            "CH0010.supplier": _FakeTable(
                files=[], properties={COMPLETE_PROPERTY: "10|2026-09-20T03:00:00Z"}
            )
        }
    )
    assert is_complete(catalog, cfg) is True


def test_a_marker_from_a_different_sf_does_not_count():
    from bench.tpch.generate import COMPLETE_PROPERTY, is_complete

    cfg = TpchConfig(workspace_id="w", lakehouse_id="l", sf=10)
    catalog = _FakeCatalog(
        {
            "CH0010.supplier": _FakeTable(
                files=[], properties={COMPLETE_PROPERTY: "100|2026-09-20T03:00:00Z"}
            )
        }
    )
    assert is_complete(catalog, cfg) is False


def test_the_tpcds_marker_lives_on_web_site():
    """Same two functions, different last table: the TPC-DS generator reuses the TPC-H marker."""
    from bench.tpch.generate import COMPLETE_PROPERTY, is_complete

    cfg = TpcdsConfig(workspace_id="w", lakehouse_id="l", sf=10)
    catalog = _FakeCatalog(
        {
            "DS0010.web_site": _FakeTable(
                files=[], properties={COMPLETE_PROPERTY: "10|2026-09-22T03:00:00Z"}
            )
        }
    )
    assert is_complete(catalog, cfg) is True
    assert is_complete(_FakeCatalog({"DS0010.web_site": _FakeTable(files=[])}), cfg) is False


def test_azure_transport_prefers_curl_off_windows(monkeypatch):
    """The setting that cost two failed benchmark runs.

    DuckDB's azure extension has its own HTTP stack; its default transport fails the OneLake TLS
    handshake on Linux. The Iceberg ATTACH still succeeds (different extension, plain HTTPS), so
    the symptom is 44 identical "AzureStorageFileSystem could not open file" errors that read
    like a credential problem and are not one.
    """
    from bench.config import azure_transport

    monkeypatch.delenv("AZURE_TRANSPORT_OPTION_TYPE", raising=False)
    monkeypatch.setattr("platform.system", lambda: "Linux")
    assert azure_transport() == "curl"


def test_azure_transport_leaves_duckdb_default_on_windows(monkeypatch):
    """Backwards from Linux, and not an oversight: DuckDB's bundled libcurl has no CA bundle on
    Windows, so `curl` fails every handshake there while the default (WinHTTP) trusts the system
    cert store. Only matters for running an engine by hand on a laptop."""
    from bench.config import azure_transport

    monkeypatch.delenv("AZURE_TRANSPORT_OPTION_TYPE", raising=False)
    monkeypatch.setattr("platform.system", lambda: "Windows")
    assert azure_transport() is None


def test_explicit_transport_always_wins(monkeypatch):
    from bench.config import azure_transport

    monkeypatch.setenv("AZURE_TRANSPORT_OPTION_TYPE", "default")
    monkeypatch.setattr("platform.system", lambda: "Linux")
    assert azure_transport() == "default"


def test_catalog_cache_is_one_number_every_engine_derives_from():
    """No engine may carry its own literal.

    The defaults are all different -- Iceberg's Spark catalog expires after 30 seconds, Sail after
    5 minutes, DuckDB and chDB had 10 written by hand -- so an engine left on its own default is
    timed on REST round-trips while the others answer from cache. Nothing in a timing chart shows
    that.
    """
    from pathlib import Path

    from bench.config import CATALOG_CACHE_SECONDS

    # DuckDB spells it in MINUTES (`MAX_TABLE_STALENESS '10 minutes'`), so a value that is not a
    # whole number of minutes would be silently truncated by the `// 60`.
    assert CATALOG_CACHE_SECONDS % 60 == 0, "must be a whole number of minutes for DuckDB"

    engines = Path(__file__).resolve().parent.parent / "bench" / "tpch" / "engines"
    for name in ("duckdb_iceberg", "chdb_iceberg", "lakesail_iceberg", "pyspark_iceberg"):
        source = (engines / f"{name}.py").read_text(encoding="utf-8")
        assert "CATALOG_CACHE_SECONDS" in source, f"{name} does not derive its catalog cache"
