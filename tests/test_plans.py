"""bench/plans.py and the Query plans tab: engines' plans in one shape, and the page's files."""

from __future__ import annotations

import json
import re
from pathlib import Path

from bench.plans import joins, normalize
from bench.scrub import find_token_shaped

ROOT = Path(__file__).parent.parent
PAGE = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
PLAN_DIR = ROOT / "docs" / "data" / "plans"
KEYS = {
    "kind",
    "op",
    "label",
    "detail",
    "tables",
    "rows",
    "est",
    "read",
    "seconds",
    "cross",
    "children",
}


def record(engine: str, raw, **kw) -> dict:
    return {
        "engine": engine,
        "version": "v",
        "suite": "tpcds",
        "query": 19,
        "sf": 10,
        "seconds": 1.5,
        "error": None,
        "raw": raw,
        **kw,
    }


def scan(columns, rows, est, **extra) -> dict:
    info = {"Function": "ICEBERG_SCAN", "Projections": columns, "Estimated Cardinality": str(est)}
    return {
        "type": "TABLE_SCAN",
        "extra_info": {**info, **extra},
        "intermediate_rows": rows,
        "timing": 0.1,
        "children": [],
    }


DUCKDB = {
    "profile": {
        "operator": [
            {
                "type": "RESULT_COLLECTOR",
                "extra_info": {},
                "intermediate_rows": 0,
                "timing": 0,
                "children": [
                    {
                        "type": "HASH_JOIN",
                        "intermediate_rows": 5000,
                        "timing": 2.0,
                        "extra_info": {
                            "Join Type": "INNER",
                            "Conditions": "ss_customer_sk = c_customer_sk",
                            "Estimated Cardinality": "50",
                        },
                        "children": [
                            scan(
                                ["ss_customer_sk"],
                                1000,
                                1000,
                                **{
                                    "Filename(s)": "abfss://***/Tables/DS0010/store_sales/0.parquet"
                                },
                            ),
                            {
                                "type": "NESTED_LOOP_JOIN",
                                "intermediate_rows": 4000,
                                "timing": 1.0,
                                "extra_info": {
                                    "Join Type": "INNER",
                                    "Conditions": "c_zip != s_zip",
                                    "Estimated Cardinality": "40",
                                },
                                "children": [
                                    scan(["c_customer_sk"], 100, 100),
                                    scan(["s_store_sk"], 40, 40),
                                ],
                            },
                        ],
                    }
                ],
            }
        ],
        "system": {"peak_buffer_memory": 7, "peak_temp_dir_size": 9},
    },
    "explain": None,
}


def test_duckdb_profile_keeps_joins_and_scans_and_flags_the_cross_product():
    plan = normalize(record("duckdb_iceberg", DUCKDB))
    root = plan["root"]
    assert root["kind"] == "join" and root["rows"] == 5000 and root["est"] == 50
    assert root["tables"] == ["customer", "store", "store_sales"]
    nested = root["children"][1]
    assert nested["cross"] and nested["tables"] == ["customer", "store"]
    assert not root["cross"]
    assert root["children"][0]["label"] == "store_sales"  # from the file path
    assert plan["spill_bytes"] == 9 and plan["peak_memory_bytes"] == 7


def test_a_failed_duckdb_query_keeps_the_estimates_and_no_rows():
    explain = [
        {
            "name": "HASH_JOIN",
            "extra_info": {"Conditions": "a = b", "Estimated Cardinality": "7"},
            "children": [
                {"name": "TABLE_SCAN", "extra_info": {"Projections": ["i_item_sk"]}, "children": []}
            ],
        }
    ]
    plan = normalize(record("duckdb_iceberg", {"profile": None, "explain": explain}, error="OOM"))
    assert plan["status"] == "error" and plan["root"]["rows"] is None and plan["root"]["est"] == 7
    assert plan["root"]["tables"] == ["item"]


def trino_fragment(fragment_id, tree, stats, ops) -> dict:
    return {
        "plan": {
            "id": fragment_id,
            "jsonRepresentation": json.dumps(tree),
            "statsAndCosts": {"stats": stats},
        },
        "stageStats": {"operatorSummaries": ops},
    }


def test_trino_follows_remote_sources_and_reads_rows_from_the_operators():
    top = {
        "id": "1",
        "name": "InnerJoin",
        "descriptor": {"criteria": "(ss_item_sk = i_item_sk)"},
        "children": [
            {
                "id": "2",
                "name": "ScanFilter",
                "descriptor": {"table": "onelake:ds0010.store_sales$data@1"},
                "children": [],
            },
            {
                "id": "3",
                "name": "RemoteSource",
                "descriptor": {"sourceFragmentIds": "[1]"},
                "children": [],
            },
        ],
    }
    leaf = {
        "id": "4",
        "name": "ScanFilterProject",
        "descriptor": {
            "table": "onelake:ds0010.item$data@2",
            "filterPredicate": "(i_manager_id = 8)",
        },
        "children": [],
    }

    def op(node, kind, out, inp=0):
        return {
            "planNodeId": node,
            "operatorType": kind,
            "outputPositions": out,
            "inputPositions": inp,
            "addInputWall": "1.00s",
        }

    raw = {
        "stages": {
            "outputStageId": "q.0",
            "stages": [
                trino_fragment(
                    "0",
                    top,
                    {"1": {"outputRowCount": "NaN"}},
                    [
                        op("1", "LookupJoinOperator", 15, 99),
                        op("1", "HashBuilderOperator", 0, 3),
                        op("2", "ScanFilterAndProjectOperator", 99, 1000),
                    ],
                ),
                trino_fragment(
                    "1",
                    leaf,
                    {"4": {"outputRowCount": 3.0}},
                    [op("4", "ScanFilterAndProjectOperator", 3, 50)],
                ),
            ],
        },
        "queryStats": {"peakUserMemoryReservation": "1.50MB", "spilledDataSize": "0B"},
    }
    plan = normalize(record("trino_iceberg", raw))
    root = plan["root"]
    assert root["kind"] == "join" and root["rows"] == 15 and root["est"] is None  # NaN: unknown
    assert root["tables"] == ["item", "store_sales"]
    item = root["children"][1]
    assert item["label"] == "item" and item["rows"] == 3 and item["read"] == 50 and item["est"] == 3
    assert plan["peak_memory_bytes"] == 1_500_000 and plan["spill_bytes"] == 0


def test_spark_folds_filters_into_scans_and_drops_partial_aggregates():
    def n(cls, string, rows=None, children=(), **metrics):
        m = {"numOutputRows": rows} if rows is not None else {}
        return {
            "name": cls,
            "class": cls,
            "string": string,
            "metrics": {**m, **metrics},
            "children": list(children),
        }

    tree = n(
        "HashAggregateExec",
        "HashAggregate(keys=[i_brand#1], functions=[sum(x)])",
        5,
        [
            n(
                "HashAggregateExec",
                "HashAggregate(keys=[i_brand#1], functions=[partial_sum(x)])",
                9,
                [
                    n(
                        "SortMergeJoinExec",
                        "SortMergeJoin [cs_item_sk#18L], [inv_item_sk#3L]",
                        1_000_000,
                        [
                            n(
                                "FilterExec",
                                "Filter isnotnull(cs#1L)",
                                10,
                                [n("BatchScanExec", "BatchScan x.DS1.catalog_sales[cs#1L]", 12)],
                            ),
                            n(
                                "BatchScanExec",
                                "BatchScan x.DS1.inventory[inv#2L]",
                                20,
                                spillSize=64,
                            ),
                        ],
                    )
                ],
            )
        ],
    )
    plan = normalize(record("pyspark_iceberg", tree))
    root = plan["root"]
    assert root["kind"] == "agg" and root["rows"] == 5
    join = root["children"][0]
    assert (
        join["label"] == "Sort-merge join"
        and join["rows"] == 1_000_000
        and "#" not in join["detail"]
    )
    sales = join["children"][0]
    assert sales["label"] == "catalog_sales" and sales["rows"] == 10 and sales["read"] == 12
    assert plan["spill_bytes"] == 64
    assert [j["rows"] for j in joins(root)] == [1_000_000]


def test_polars_walks_the_physical_plan_and_reads_rows_sent_by_node_id():
    def n(i, kind, inputs=(), **props):
        return {"id": i, "input_ids": list(inputs), "properties": {"type": kind, **props}}

    raw = {
        "physical": [
            n(1, "TopK", [2], by_exprs=["col(\"x\")"]),
            n(2, "GroupBy", [3], key_per_input=[["d_year"]]),
            n(
                3, "EquiJoin", [4, 6], how="Inner",
                left_on=["ws_sold_date_sk"], right_on=["d_date_sk"],
            ),
            n(4, "Filter", [5], predicate="col(\"ws_quantity\") > 0"),
            n(5, "MultiScan", first_source="abfss://***/Tables/DS1/web_sales/data/0.parquet"),
            n(6, "MultiScan", projected_file_columns=["d_date_sk", "d_year"]),
        ],
        "metrics": {"nodes": [
            {"phys_node_key": 1, "rows_sent": 100}, {"phys_node_key": 2, "rows_sent": 7},
            {"phys_node_key": 3, "rows_sent": 900}, {"phys_node_key": 4, "rows_sent": 950},
            {"phys_node_key": 5, "rows_sent": 1000}, {"phys_node_key": 6, "rows_sent": 73},
        ]},
    }  # fmt: skip
    root = normalize(record("polars_iceberg", raw))["root"]
    assert root["kind"] == "sort" and root["rows"] == 100
    join = root["children"][0]["children"][0]
    assert join["label"] == "Hash join" and join["rows"] == 900 and not join["cross"]
    assert join["detail"] == "INNER ws_sold_date_sk = d_date_sk"
    sales, dates = join["children"]
    assert sales["label"] == "web_sales" and sales["rows"] == 950 and sales["read"] == 1000
    assert dates["label"] == "date_dim" and dates["est"] is None
    assert join["tables"] == ["date_dim", "web_sales"]


# --- the files the page reads ---------------------------------------------------------------------


def story_files() -> set[str]:
    start = PAGE.index("const STORIES = [")
    block = PAGE[start : PAGE.index("\n];", start)]
    return set(re.findall(r'"((?:tpch|tpcds)_q\d+_\w+?_sf\d+)"', block))


def test_the_query_plans_tab_is_in_the_main_bar_before_performance():
    tab = '{ id: "work", title: "Less work", subs: [PRUNING, PLANS] }'
    assert tab in PAGE
    performance = '{ id: "performance", title: "Benchmarks", subs: [...SUITES] }'
    assert PAGE.index(tab) < PAGE.index(performance)
    assert '<section class="pl" id="plans" hidden></section>' in PAGE


def test_every_plan_a_story_names_is_committed():
    for name in story_files():
        assert (PLAN_DIR / f"{name}.json").exists(), name


def test_committed_plans_have_the_shape_and_no_lakehouse_ids_or_tokens():
    for path in PLAN_DIR.glob("*.json"):
        text = path.read_text(encoding="utf-8")
        assert "onelake.dfs" not in text and not find_token_shaped(text), path.name
        plan = json.loads(text)
        assert plan["status"] in ("ok", "error") and plan["root"], path.name
        stack = [plan["root"]]
        while stack:
            node = stack.pop()
            assert set(node) == KEYS, path.name
            stack += node["children"]
