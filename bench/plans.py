"""Turn an engine's raw plan (.github/scripts/capture_plan.py) into the one shape the page draws.

The "Bad joins" tab (docs/index.html #plans) puts two engines' plans for the same query side by
side. Each engine reports its plan its own way; this keeps only what the picture needs, in one
shape for all of them:

    {engine, version, suite, query, sf, run_url, seconds, status, error,
     spill_bytes, peak_memory_bytes, root: node}

    node = {kind, op, label, detail, tables, rows, est, read, seconds, cross, children}

* kind     scan | join | agg | sort | other. Projections, exchanges, partial aggregates and the
           other plumbing between them are dropped and their children moved up: the picture is
           the tables and the joins.
* tables   every table under the node, sorted -- how a join on one side finds the join over the
           same tables on the other.
* rows     rows the node produced (None when the query failed before it finished);
  est      the optimizer's guess (None when the engine gives none);
  read     for a scan, rows read before its filter.
* cross    a join with no join condition: every row on one side with every row on the other.

A FILTER right above a scan is folded into the scan (DuckDB, Spark): the scan's `rows` becomes
the filter's and `read` keeps what was read.
"""

from __future__ import annotations

import re

# TPC-DS and TPC-H column prefixes: every column starts with its table's. DuckDB's EXPLAIN names
# a scan only by its columns.
PREFIX = {
    "ss": "store_sales", "sr": "store_returns", "cs": "catalog_sales", "cr": "catalog_returns",
    "ws": "web_sales", "wr": "web_returns", "inv": "inventory", "s": "store", "cc": "call_center",
    "cp": "catalog_page", "web": "web_site", "wp": "web_page", "w": "warehouse", "c": "customer",
    "ca": "customer_address", "cd": "customer_demographics", "d": "date_dim",
    "hd": "household_demographics", "i": "item", "ib": "income_band", "p": "promotion",
    "r": "reason", "sm": "ship_mode", "t": "time_dim",
    "l": "lineitem", "o": "orders", "ps": "partsupp", "n": "nation",
}  # fmt: skip
# TPC-H's single-letter prefixes clash with TPC-DS's (s_, c_, p_, r_): by suite.
PREFIX_TPCH = {"s": "supplier", "c": "customer", "p": "part", "r": "region"}


def _num(value) -> float | None:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    return None if n != n else n  # NaN: "the engine does not know"


def _table_from_columns(columns: list[str], suite: str) -> str | None:
    for col in columns:
        head = str(col).split("_", 1)[0].lstrip("#").lower()
        if suite == "tpch" and head in PREFIX_TPCH:
            return PREFIX_TPCH[head]
        if head in PREFIX:
            return PREFIX[head]
    return None


def node(kind: str, op: str, label: str, **kw) -> dict:
    out = {
        "kind": kind,
        "op": op,
        "label": label,
        "detail": kw.get("detail") or "",
        "tables": [],
        "rows": kw.get("rows"),
        "est": kw.get("est"),
        "read": kw.get("read"),
        "seconds": kw.get("seconds"),
        "cross": bool(kw.get("cross")),
        "children": kw.get("children") or [],
    }
    if kind == "scan" and kw.get("table"):
        out["tables"] = [kw["table"]]
    return out


def _finish(n: dict) -> dict:
    """Fill `tables` bottom-up."""
    for child in n["children"]:
        _finish(child)
    if n["kind"] != "scan":
        n["tables"] = sorted({t for c in n["children"] for t in c["tables"]})
    return n


def _flatten(kids: list) -> list[dict]:
    out = []
    for kid in kids:
        if kid is None:
            continue
        out.extend(_flatten(kid) if isinstance(kid, list) else [kid])
    return out


def _has_equality(cond) -> bool:
    """A join condition with at least one `a = b` -- not `!=`, `<>`, `<=` or `>=`."""
    text = " AND ".join(map(str, cond)) if isinstance(cond, list) else str(cond or "")
    return bool(re.search(r"(?<![!<>=])=(?!=)", text))


def _short(text, limit: int = 160) -> str:
    if isinstance(text, list):
        text = " AND ".join(map(str, text))
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    return text if len(text) <= limit else text[: limit - 1] + "…"


# --- DuckDB ---------------------------------------------------------------------------------------

_DUCK_JOINS = (
    "HASH_JOIN",
    "NESTED_LOOP_JOIN",
    "PIECEWISE_MERGE_JOIN",
    "CROSS_PRODUCT",
    "BLOCKWISE_NL_JOIN",
    "IE_JOIN",
    "ASOF_JOIN",
    "LEFT_DELIM_JOIN",
    "RIGHT_DELIM_JOIN",
    "DELIM_JOIN",
    "POSITIONAL_JOIN",
)
_DUCK_AGGS = (
    "HASH_GROUP_BY",
    "PERFECT_HASH_GROUP_BY",
    "UNGROUPED_AGGREGATE",
    "WINDOW",
    "STREAMING_WINDOW",
)
_DUCK_SORTS = ("ORDER_BY", "TOP_N")


def _duck_table(extra: dict, suite: str) -> str | None:
    if extra.get("Table"):
        return str(extra["Table"]).split(".")[-1]
    files = extra.get("Filename(s)") or extra.get("File Path") or ""
    m = re.search(r"/Tables/[^/]+/([^/]+)/", str(files))
    if m:
        return m.group(1)
    columns = extra.get("Projections") or []
    return _table_from_columns([columns] if isinstance(columns, str) else list(columns), suite)


def _duckdb_node(raw: dict, suite: str, actual: bool, ctes: dict) -> list[dict] | dict | None:
    op = raw.get("type") or raw.get("name") or "?"
    extra = raw.get("extra_info") or {}
    if op == "CTE":  # its children: the CTE's definition, then the query that reads it
        ctes[str(extra.get("Table Index"))] = extra.get("CTE Name") or "CTE"
    kids = _flatten([_duckdb_node(c, suite, actual, ctes) for c in raw.get("children", [])])
    rows = raw.get("intermediate_rows", raw.get("operator_cardinality")) if actual else None
    common = {
        "rows": rows,
        "est": _num(extra.get("Estimated Cardinality")),
        "seconds": raw.get("timing", raw.get("operator_timing")) if actual else None,
    }
    if op == "COLUMN_DATA_SCAN":  # the values of an IN list
        return []
    if op == "CTE_SCAN":
        name = ctes.get(str(extra.get("CTE Index")), "CTE")
        return node("scan", op, f"{name} (CTE)", detail="", **common)
    if op in ("TABLE_SCAN", "ICEBERG_SCAN", "SEQ_SCAN", "READ_PARQUET") or "SCAN" in op:
        table = _duck_table(extra, suite)
        detail = _short(extra.get("Filters") or "")
        return node("scan", op, table or op.lower(), table=table, detail=detail, **common)
    if op == "FILTER" and len(kids) == 1 and kids[0]["kind"] == "scan" and not kids[0]["detail"]:
        scan = kids[0]
        scan["read"], scan["rows"] = scan["rows"], rows
        scan["detail"] = _short(extra.get("Expression") or extra.get("Filters") or "")
        scan["est"] = common["est"] if common["est"] is not None else scan["est"]
        return scan
    if op in _DUCK_JOINS:
        cond = extra.get("Conditions") or ""
        # No equality to join on: every row of one side meets every row of the other, whatever
        # inequality then throws most of the pairs away (Q19's zip codes).
        cross = op == "CROSS_PRODUCT" or not _has_equality(cond)
        label = "Cross product" if op == "CROSS_PRODUCT" else op.replace("_", " ").capitalize()
        kind_detail = _short(f"{extra.get('Join Type', '')} {_short(cond)}".strip())
        return node("join", op, label, detail=kind_detail, cross=cross, children=kids, **common)
    if op in _DUCK_AGGS:
        return node(
            "agg",
            op,
            "Group by" if "GROUP" in op else op.replace("_", " ").capitalize(),
            detail=_short(extra.get("Groups") or ""),
            children=kids,
            **common,
        )
    if op in _DUCK_SORTS:
        return node("sort", op, "Top N" if op == "TOP_N" else "Sort", children=kids, **common)
    return kids  # projection, result collector, filter elsewhere...: plumbing


def _root(kids, top: dict | None = None) -> dict:
    kids = _flatten([kids])
    if len(kids) == 1:
        return kids[0]
    return node("other", "RESULT", "Result", children=kids, **(top or {}))


def duckdb(raw: dict, suite: str) -> tuple[dict | None, dict]:
    profile, explain = raw.get("profile"), raw.get("explain")
    extra, ctes = {}, {}
    if profile:
        tree = _root([_duckdb_node(op, suite, True, ctes) for op in profile["operator"]])
        system = profile.get("system", {})
        extra = {
            "peak_memory_bytes": system.get("peak_buffer_memory"),
            "spill_bytes": system.get("peak_temp_dir_size"),
        }
    elif explain:
        tree = _root([_duckdb_node(op, suite, False, ctes) for op in explain])
    else:
        tree = None
    return tree, extra


# --- Trino ----------------------------------------------------------------------------------------

_DUR = {"ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}
_SIZE = {"B": 1, "kB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12, "PB": 1e15}


def _duration(text) -> float:
    m = re.fullmatch(r"([\d.]+)\s*([a-z]+)", str(text or "").strip())
    return float(m.group(1)) * _DUR.get(m.group(2), 0) if m else 0.0


def size_bytes(text) -> int | None:
    """Trino's `1.23GB` (decimal units, as Trino's DataSize prints them) in bytes."""
    if text is None:
        return None
    if isinstance(text, (int, float)):
        return int(text)
    m = re.fullmatch(r"([\d.]+)\s*([kMGTP]?B)", str(text).strip())
    return int(float(m.group(1)) * _SIZE[m.group(2)]) if m else None


_TRINO_JOINS = (
    "InnerJoin",
    "LeftJoin",
    "RightJoin",
    "FullJoin",
    "SemiJoin",
    "CrossJoin",
    "SpatialJoin",
    "IndexJoin",
)


def trino(raw: dict, suite: str) -> tuple[dict | None, dict]:
    import json

    stages = raw.get("stages") or {}
    by_fragment, est, ops = {}, {}, {}
    for stage in stages.get("stages", []):
        plan = stage["plan"]
        by_fragment[plan["id"]] = json.loads(plan["jsonRepresentation"])
        for node_id, stat in ((plan.get("statsAndCosts") or {}).get("stats") or {}).items():
            est[node_id] = _num(stat.get("outputRowCount"))
        for o in stage["stageStats"]["operatorSummaries"]:
            ops.setdefault(o["planNodeId"], []).append(o)
    if not by_fragment:
        return None, {}

    def rows_of(node_id: str, kind: str):
        summaries = ops.get(node_id, [])
        if not summaries:
            return None, None, None
        if kind == "join":
            probe = [o for o in summaries if "Join" in o["operatorType"]] or summaries
            out = sum(o["outputPositions"] for o in probe)
        else:
            out = max(o["outputPositions"] for o in summaries)
        read = max((o["inputPositions"] for o in summaries), default=None)
        wall = sum(
            _duration(o.get(k))
            for o in summaries
            for k in ("addInputWall", "getOutputWall", "finishWall")
        )
        return out, read, wall

    def walk(n: dict):
        name, desc = n["name"], n.get("descriptor", {})
        kids = _flatten([walk(c) for c in n.get("children", [])])
        if (
            "sourceFragmentIds" in desc
        ):  # RemoteSource, RemoteMerge: the plan goes on in other fragments
            for fid in desc.get("sourceFragmentIds", "").strip("[]").split(","):
                if fid.strip() in by_fragment:
                    kids += _flatten([walk(by_fragment[fid.strip()])])
            return kids
        if name in ("TableScan", "ScanFilter", "ScanFilterProject", "ScanProject"):
            m = re.search(r"\.([A-Za-z0-9_]+)(?:\$|$)", desc.get("table", ""))
            table = m.group(1) if m else None
            out, read, wall = rows_of(n["id"], "scan")
            return node(
                "scan",
                name,
                table or name,
                table=table,
                rows=out,
                read=read,
                est=est.get(n["id"]),
                seconds=wall,
                detail=_short(desc.get("filterPredicate") or ""),
            )
        if name in _TRINO_JOINS:
            out, _, wall = rows_of(n["id"], "join")
            criteria = desc.get("criteria", "")
            cross = name == "CrossJoin" or not _has_equality(criteria)
            label = "Cross join" if cross else "Hash join"
            detail = _short(f"{criteria} {desc.get('filter', '')}".strip())
            return node(
                "join",
                name,
                label,
                rows=out,
                est=est.get(n["id"]),
                seconds=wall,
                detail=detail,
                cross=cross,
                children=kids,
            )
        if name in ("Aggregate", "Aggregation") and desc.get("type", "FINAL") != "PARTIAL":
            out, _, wall = rows_of(n["id"], "agg")
            return node(
                "agg",
                name,
                "Group by",
                rows=out,
                est=est.get(n["id"]),
                seconds=wall,
                detail=_short(desc.get("keys", "")),
                children=kids,
            )
        if name in ("TopN", "Sort"):
            out, _, wall = rows_of(n["id"], "sort")
            return node(
                "sort",
                name,
                "Top N" if name == "TopN" else "Sort",
                rows=out,
                seconds=wall,
                children=kids,
            )
        return kids

    out_stage = stages.get("outputStageId", "")
    root_fragment = out_stage.rsplit(".", 1)[-1] if out_stage else min(by_fragment, key=int)
    tree = _root(walk(by_fragment[root_fragment]))
    stats = raw.get("queryStats") or {}
    return tree, {
        "peak_memory_bytes": size_bytes(stats.get("peakUserMemoryReservation")),
        "spill_bytes": size_bytes(stats.get("spilledDataSize")),
    }


# --- Spark ----------------------------------------------------------------------------------------

_SPARK_SCANS = ("BatchScanExec", "FileSourceScanExec", "InMemoryTableScanExec")
_SPARK_JOINS = (
    "SortMergeJoinExec",
    "BroadcastHashJoinExec",
    "ShuffledHashJoinExec",
    "BroadcastNestedLoopJoinExec",
    "CartesianProductExec",
)


def spark(raw: dict, suite: str) -> tuple[dict | None, dict]:
    totals = {"spill": 0, "peak": 0}

    def walk(n: dict):
        # `cs_item_sk#18L` -> `cs_item_sk`: Spark's expression ids mean nothing on the page.
        cls, metrics = n["class"], n.get("metrics", {})
        text = re.sub(r"#\d+L?", "", n.get("string", ""))
        totals["spill"] += int(metrics.get("spillSize") or 0)
        totals["peak"] = max(totals["peak"], int(metrics.get("peakMemory") or 0))
        kids = _flatten([walk(c) for c in n.get("children", [])])
        rows = metrics.get("numOutputRows")
        if cls in _SPARK_SCANS:
            m = re.search(r"\.([A-Za-z0-9_]+)\[", text) or re.search(r"\.([A-Za-z0-9_]+)\s", text)
            table = m.group(1) if m else None
            return node("scan", cls.removesuffix("Exec"), table or cls, table=table, rows=rows)
        if cls == "FilterExec" and len(kids) == 1 and kids[0]["kind"] == "scan":
            scan = kids[0]
            scan["read"], scan["rows"] = scan["rows"], rows
            scan["detail"] = _short(text.removeprefix("Filter "))
            return scan
        if cls in _SPARK_JOINS:
            cross = cls in ("CartesianProductExec",) or (
                cls == "BroadcastNestedLoopJoinExec" and "None" in text.split(",")[-1]
            )
            label = {
                "SortMergeJoinExec": "Sort-merge join",
                "BroadcastHashJoinExec": "Broadcast hash join",
                "ShuffledHashJoinExec": "Shuffled hash join",
            }.get(cls, "Nested loop join" if not cross else "Cross product")
            return node(
                "join",
                cls.removesuffix("Exec"),
                label,
                rows=rows,
                detail=_short(re.sub(r"^\w+\s*", "", text)),
                cross=cross,
                children=kids,
            )
        if cls in ("HashAggregateExec", "SortAggregateExec", "ObjectHashAggregateExec"):
            if "partial_" in text and kids:
                return kids
            return node("agg", cls.removesuffix("Exec"), "Group by", rows=rows, children=kids)
        if cls in ("TakeOrderedAndProjectExec", "SortExec") and not kids_are_join_inputs(n):
            return node(
                "sort",
                cls.removesuffix("Exec"),
                "Top N" if "Take" in cls else "Sort",
                rows=rows,
                children=kids,
            )
        return kids

    def kids_are_join_inputs(n: dict) -> bool:
        return n["class"] == "SortExec"  # the sorts inside a sort-merge join are plumbing

    tree = _root(walk(raw))
    return tree, {
        "spill_bytes": totals["spill"] or None,
        "peak_memory_bytes": totals["peak"] or None,
    }


PARSERS = {
    "duckdb_iceberg": duckdb,
    "trino_iceberg": trino,
    "pyspark_iceberg": spark,
    "pyspark_gluten_iceberg": spark,
}


def normalize(record: dict) -> dict:
    """One raw capture -> the page's shape."""
    tree, extra = (None, {})
    if record.get("raw") is not None:
        tree, extra = PARSERS[record["engine"]](record["raw"], record.get("suite", "tpcds"))
    spill = extra.get("spill_bytes")
    if spill is None:
        spill = size_bytes(record.get("spill_bytes"))
    return {
        "engine": record["engine"],
        "version": record.get("version"),
        "suite": record.get("suite"),
        "query": record["query"],
        "sf": record["sf"],
        "run_url": record.get("run_url"),
        "warmup": bool(record.get("warmup")),
        "seconds": round(record["seconds"], 2) if record.get("seconds") is not None else None,
        "status": "error" if record.get("error") else "ok",
        "error": record.get("error"),
        "spill_bytes": spill,
        "peak_memory_bytes": extra.get("peak_memory_bytes"),
        "root": _cte_tables(_finish(tree)) if tree else None,
    }


def _cte_tables(root: dict) -> dict:
    """A CTE scan reads what the CTE's definition read: every table outside the CTE scans (TPC-DS
    Q64's `cross_sales` is the whole query). Then the tables up the tree again."""
    stack, scans, tables = [root], [], set()
    while stack:
        n = stack.pop()
        stack += n["children"]
        if n["op"] == "CTE_SCAN":
            scans.append(n)
        elif n["kind"] == "scan":
            tables.update(n["tables"])
    for scan in scans:
        scan["tables"] = sorted(tables)
    return _finish(root) if scans else root


def joins(tree: dict) -> list[dict]:
    """Every join in the tree, bottom-up."""
    out = []
    for child in tree["children"]:
        out += joins(child)
    if tree["kind"] == "join":
        out.append(tree)
    return out
