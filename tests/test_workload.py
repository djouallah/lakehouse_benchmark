"""The dbt workload test (workload/, bench/workload/, workload.yml): offline checks.

What would otherwise fail only minutes into a real run, or -- worse -- pass green having built
nothing: a target with no model tree, a leg missing from the workflow. And the page reads exactly
the file the publish step writes, including an engine whose runner died.
"""

from __future__ import annotations

import re
from pathlib import Path

from bench.workload.config import BENCH_ENGINE, ENGINES

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "workload"


def test_every_engine_has_a_target_a_model_tree_and_a_gate():
    profiles = (PROJECT / "profiles.yml").read_text(encoding="utf-8")
    project = (PROJECT / "dbt_project.yml").read_text(encoding="utf-8")
    for engine in ENGINES:
        assert re.search(rf"^    {engine}:\n", profiles, re.M), engine
        assert f"target.name == '{engine}'" in project, engine
        models = sorted(p.name for p in (PROJECT / "models" / "aemo" / engine).rglob("*.sql"))
        assert len(models) == 6, (engine, models)
        assert (PROJECT / "tests" / "aemo" / engine).is_dir(), engine
        assert (ROOT / "requirements" / f"workload_{engine}.txt").exists(), engine
        assert engine in BENCH_ENGINE


def test_the_same_models_in_every_tree():
    trees = {
        e: sorted(p.name for p in (PROJECT / "models" / "aemo" / e).rglob("*.sql")) for e in ENGINES
    }
    assert len({tuple(v) for v in trees.values()}) == 1, trees


def test_gluten_tree_writes_iceberg_not_delta():
    for path in (PROJECT / "models" / "aemo" / "gluten").rglob("*.sql"):
        text = path.read_text(encoding="utf-8")
        assert "file_format='delta'" not in text, path
        assert "USING DELTA" not in text.upper(), path


def test_workflow_default_engines_are_the_engines():
    text = (ROOT / ".github" / "workflows" / "workload.yml").read_text(encoding="utf-8")
    default = re.search(r"engines:\n(?:.*\n){2}\s+default: (\S+)", text).group(1)
    assert tuple(default.split(",")) == ENGINES


def test_the_page_reads_what_publish_writes():
    page = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
    assert 'json: "data/workload.json"' in page
    publish = (ROOT / ".github" / "scripts" / "workload_publish.py").read_text(encoding="utf-8")
    assert 'PAGE_DATA = REPO / "docs" / "data" / "workload.json"' in publish
    workflow = (ROOT / ".github" / "workflows" / "workload.yml").read_text(encoding="utf-8")
    assert "git add results/workload docs/data/workload.json" in workflow
    pages = (ROOT / ".github" / "workflows" / "pages.yml").read_text(encoding="utf-8")
    assert "dbt workload" in pages


def test_runner_script_is_not_named_like_a_dbt_plugin():
    # dbt imports every top-level dbt_* module as a plugin; the script dir is sys.path[0].
    scripts = (ROOT / ".github" / "scripts").glob("*.py")
    assert not [p.name for p in scripts if p.name.startswith("dbt_")]


def _publish():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "workload_publish", ROOT / ".github" / "scripts" / "workload_publish.py"
    )
    publish = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(publish)
    return publish


def test_page_data_carries_the_dag_and_each_legs_models():
    publish = _publish()

    def part(engine, adapter, dag):
        return {
            "engine": engine,
            "run_id": "1",
            "status": "success",
            "build_seconds": 10.0,
            "finished": "2026-10-20T00:00:00Z",
            "versions": {"dbt-core": "1.10.0", adapter: "1.10.1"},
            "nodes": [
                {"node": "model.aemo_electricity.stg", "status": "success", "seconds": 1.5},
                {"node": "model.aemo_electricity.fct", "status": "success", "seconds": 4.0},
                {"node": "test.aemo_electricity.t", "status": "pass", "seconds": 0.1},
                {
                    "node": "test.aemo_electricity.assert_all_daily_files_processed_scada",
                    "status": "warn",
                    "seconds": 0.2,
                    "message": "Got 809 results, configured to warn if != 0",
                },
            ],
            "dag": dag,
        }

    legs = {
        "gluten": part("gluten", "dbt-spark", {"stg": {"materialized": "table", "parents": []}}),
        "duckdb": part(
            "duckdb",
            "dbt-duckdb",
            {
                "stg": {"materialized": "view", "parents": []},
                "fct": {
                    "materialized": "incremental",
                    "parents": ["stg"],
                    "strategy": "merge",
                    "merge_clauses": True,
                    "path": "models/aemo/duckdb/marts/fct.sql",
                    "sql_lines": 40,
                    "description": "A fact.",
                    "differences": ["`GROUP BY ALL` here."],
                },
            },
        ),
    }
    out = publish.page_data(legs)
    assert out["dag"]["fct"]["parents"] == ["stg"]
    assert set(out["dag"]) == {"stg", "fct"}
    assert out["legs"]["duckdb_iceberg"]["adapter"] == "dbt-duckdb 1.10.1"
    assert out["legs"]["duckdb_iceberg"]["model_seconds"] == {"stg": 1.5, "fct": 4.0}
    assert out["legs"]["duckdb_iceberg"]["models"] == 2  # the count, not the seconds
    assert "parity_ok" not in out
    assert out["legs"]["duckdb_iceberg"]["files_behind"] == 809
    assert out["legs"]["duckdb_iceberg"]["last_ok"]["run_id"] == "1"
    assert out["dag"]["fct"]["differences"] == ["`GROUP BY ALL` here."]
    assert out["legs"]["duckdb_iceberg"]["model_config"]["fct"]["strategy"] == "merge"
    assert out["legs"]["duckdb_iceberg"]["model_config"]["fct"]["sql_lines"] == 40


def test_every_model_says_where_the_engines_sql_differs():
    """The Real workload panel reads each model's meta.differences from the shared patch files."""
    import yaml

    names = set()
    for patch in ("_dimensions", "_marts"):
        doc = yaml.safe_load((PROJECT / "models" / "aemo" / f"{patch}.yml").read_text("utf-8"))
        for model in doc["models"]:
            names.add(model["name"])
            assert model["config"]["meta"]["differences"], model["name"]
    trees = {p.stem for p in (PROJECT / "models" / "aemo" / "duckdb").rglob("*.sql")}
    assert names == trees


def test_every_engine_builds_every_model_the_same_way():
    """One materialization per model on every engine: an incremental merge on the natural key.

    A merge commits against the snapshot it read, so two concurrent runs cannot insert a file
    twice; an append (or an insert-only merge, a plain append commit) would keep both. The landed
    files are dbt sources, not models, so nothing here copies them.
    """

    def config(path: Path) -> dict:
        text = re.sub(r"\{#.*?#\}", "", path.read_text(encoding="utf-8"), flags=re.S)
        block = text[text.index("config(") : text.index(")", text.index("config("))]
        return {
            "materialized": re.search(r"materialized='(\w+)'", block).group(1),
            "strategy": (re.search(r"incremental_strategy='([\w+]+)'", block) or [None, ""])[1],
            "merge_clauses": "merge_clauses" in block,
        }

    by_model: dict[str, dict] = {}
    for engine in ENGINES:
        for path in (PROJECT / "models" / "aemo" / engine).rglob("*.sql"):
            by_model.setdefault(path.stem, {})[engine] = config(path)
    for model, engines in by_model.items():
        for engine, c in engines.items():
            assert c == {
                "materialized": "incremental",
                "strategy": "merge",
                "merge_clauses": False,
            }, (
                model,
                engine,
                c,
            )


def test_the_archive_log_is_a_source_not_a_copy():
    """Nothing builds the landing log: it is a dbt source on every engine, and every model and
    test reads it through source()."""
    for engine in ENGINES:
        tree = PROJECT / "models" / "aemo" / engine
        assert not list(tree.rglob("stg_*.sql")), engine
        for path in tree.rglob("*.sql"):
            assert "ref('stg_csv_archive_log')" not in path.read_text(encoding="utf-8"), path
    sources = (PROJECT / "models" / "sources.yml").read_text(encoding="utf-8")
    assert "name: csv_raw_archive_log" in sources
    assert 'external_location: "read_parquet(' in sources
    assert "location: csv_raw_archive_log.parquet" in sources  # dbt-polars' external.location
    assert not (PROJECT / "macros" / "archive_log.sql").exists()
    project = (PROJECT / "dbt_project.yml").read_text(encoding="utf-8")
    assert "\nsources:" not in project  # no engine has the source switched off


def test_merges_prune_the_target_on_a_key_column():
    """macros/merge_prune.sql bounds every fact merge by its batch's `file` (fct_summary: `date`)
    values; a fact whose key lost both would merge against the whole table again."""
    macros = (PROJECT / "macros" / "merge_prune.sql").read_text(encoding="utf-8")
    for hook in ("duckdb__get_incremental_merge_sql", "macro get_merge_sql("):
        assert hook in macros, hook
    for engine in ENGINES:
        for path in (PROJECT / "models" / "aemo" / engine / "marts").glob("fct_*.sql"):
            sql = path.read_text(encoding="utf-8")
            assert "'file'" in sql or "spec['unique_key']" in sql or "'date'" in sql, path


def test_an_engine_whose_runner_died_stays_on_the_page():
    """A runner killed out of memory uploads no part: the engine is carried forward as "died",
    with its last good run, not dropped from the page. An engine the run did not ask for is
    carried forward unchanged."""
    publish = _publish()
    previous = {
        "legs": {
            "polars_iceberg": {"status": "success", "last_ok": {"run_id": "7", "finished": "x"}},
            "trino_iceberg": {"status": "success", "last_ok": {"run_id": "7", "finished": "x"}},
        }
    }
    legs = {
        "duckdb": {"engine": "duckdb", "run_id": "8", "status": "success", "nodes": [], "dag": {}}
    }
    out = publish.page_data(legs, previous, asked=["duckdb", "polars"])
    assert out["legs"]["polars_iceberg"]["status"] == "died"
    assert out["legs"]["polars_iceberg"]["last_ok"]["run_id"] == "7"
    assert out["legs"]["trino_iceberg"]["status"] == "success"
    assert out["legs"]["duckdb_iceberg"]["last_ok"]["run_id"] == "8"


def test_the_workload_runs_on_a_schedule_and_publishes():
    import yaml

    wf = yaml.safe_load((ROOT / ".github" / "workflows" / "workload.yml").read_text("utf-8"))
    assert wf[True]["schedule"], "no schedule"
    assert wf["concurrency"]["group"] != "onelake"
    grade = wf["jobs"]["grade"]
    assert any("schedule" in str(step.get("if", "")) for step in grade["steps"])
    assert "decimals" not in wf["jobs"]
