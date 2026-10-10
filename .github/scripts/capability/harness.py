"""What every capability probe shares: the outcomes, the report, and the fixtures and checks.

Each engine's probe script asks one engine what it can do against the OneLake Iceberg REST catalog
and checks the effect through pyiceberg, which reads what the CATALOG now says rather than what
the engine cached. A probe returns a detail string for `supported`, or raises `Refused`, `NoOp`,
`Skip` or `Broken`; anything else is classified by the status code in its message.

Every reading lands in results/capability/<client>.json (save_results), which render_readme.py
turns into docs/capability/RESULTS.md and the site's docs/data/capability.json.
"""

from __future__ import annotations

import json
import os

from bench import scrub

NAMESPACE = "_bench_capability"

# Probe outcomes. Three of them are answers about the endpoint:
#
#   supported  the request was taken and had the effect it describes
#   no         the endpoint declined it, with a status code and a message
#   no-op      the request was accepted and the effect was not applied
#
# A no-op is tracked apart from a `no` because the response does not distinguish the two: both
# come back without an error in the client's hands, so only a probe that CHECKS THE EFFECT can
# tell them apart. That is why create_staged asks whether the table is there. `broken` is the
# only outcome that says nothing about the endpoint, because the probe could not ask its question.
SUPPORTED, REFUSED, NOOP, SKIPPED, BROKEN = (
    "supported",
    "no",
    "no-op",
    "skipped",
    "broken",
)

MARK = {SUPPORTED: "yes", REFUSED: "no", NOOP: "no-op", SKIPPED: "—", BROKEN: "?"}


class Skip(Exception):
    """A probe with nothing to ask this run, because a prerequisite did not land."""


class Refused(Exception):
    """The endpoint answered, and the answer was no. A finding, not a failure."""


class NoOp(Exception):
    """The endpoint took the request, returned success, and did not do it."""


class Broken(Exception):
    """The probe could not ask its question, whatever the message says."""


# --------------------------------------------------------------------------------------------
# the probe harness
# --------------------------------------------------------------------------------------------


def _is_server_refusal(message: str) -> bool:
    """Did the endpoint answer no, as opposed to the client or the network falling over?"""
    lowered = message.lower()
    marks = (
        "400",
        "403",
        "404",
        "405",
        "409",
        "415",
        "422",
        "501",
        "badrequest",
        "bad request",
        "not implemented",
        "unsupported",
        "malformed",
        "already exists",
        "notfound",
        "not found",
        "forbidden",
        "commitfailed",
        "does not support endpoint",
    )
    return any(mark in lowered for mark in marks)


def save_results(payload: dict) -> None:
    """Write a probe's readings to $RESULTS_FILE as JSON, for render_readme.py. No-op without it."""
    import datetime as dt

    path = os.environ.get("RESULTS_FILE")
    if not path:
        return
    payload = {
        **payload,
        "run": os.environ.get("GITHUB_RUN_ID", "local"),
        "url": "{}/{}/actions/runs/{}".format(
            os.environ.get("GITHUB_SERVER_URL", "https://github.com"),
            os.environ.get("GITHUB_REPOSITORY", "local"),
            os.environ.get("GITHUB_RUN_ID", "local"),
        ),
        "date": dt.datetime.now(dt.UTC).strftime("%Y-%m-%d"),
    }
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=1, default=str)


class Report:
    def __init__(self, reason=None) -> None:
        self.rows: list[dict] = []
        # How an unexpected exception is turned into one line. The default reads the exception
        # itself, which is right for pyiceberg, where the endpoint's message is in the text. The
        # engine probes pass their own, so each engine's error reaches the log in its own words.
        self._reason = reason or (lambda exc: scrub.scrub_exc(exc, 600))

    def record(
        self, group: str, question: str, outcome: str, detail: object, key: str = ""
    ) -> None:
        self.rows.append(
            {
                "key": key,
                "group": group,
                "question": question,
                "outcome": outcome,
                "detail": scrub.scrub(detail),
            }
        )

    def save(self, engine: str, version: str) -> None:
        """The rows, for render_readme.py: see save_results."""
        save_results({"engine": engine, "version": version, "rows": self.rows})

    def run(self, group: str, question: str, fn) -> bool:
        print(f"\n[{len(self.rows) + 1:>2}] {group}: {question}", flush=True)
        key = getattr(fn, "__name__", "")
        try:
            detail = fn() or ""
        except Skip as skip:
            self.record(group, question, SKIPPED, skip, key)
            print(f"     skip       {scrub.scrub(skip)}", flush=True)
            return False
        except Refused as refused:
            self.record(group, question, REFUSED, refused, key)
            print(f"     no         {scrub.scrub(refused)}", flush=True)
            return False
        except NoOp as noop:
            self.record(group, question, NOOP, noop, key)
            print(f"     no-op      {scrub.scrub(noop)}", flush=True)
            return False
        except Broken as broken:
            self.record(group, question, BROKEN, broken, key)
            print(f"     broken     {scrub.scrub(broken)}", flush=True)
            return False
        except Exception as exc:  # noqa: BLE001 - reporting the failure is the job
            message = self._reason(exc)
            # A client-side exception is usually still the endpoint talking back through the
            # client, so the status code in the message decides which it was.
            outcome = REFUSED if _is_server_refusal(message) else BROKEN
            self.record(group, question, outcome, message, key)
            print(f"     {outcome:<10} {message}", flush=True)
            return False
        self.record(group, question, SUPPORTED, detail, key)
        print(f"     supported  {scrub.scrub(detail)}", flush=True)
        return True

    def markdown(self) -> list[str]:
        lines = ["| Area | Question | Supported | What came back |", "|---|---|---|---|"]
        for row in self.rows:
            detail = row["detail"].replace("|", "\\|").replace("\n", " ")[:240]
            lines.append(
                f"| {row['group']} | {row['question']} | {MARK[row['outcome']]} | "
                f"{'`' + detail + '`' if detail else ''} |"
            )
        return lines

    def tally(self) -> dict[str, int]:
        counts = dict.fromkeys(MARK, 0)
        for row in self.rows:
            counts[row["outcome"]] += 1
        return counts


# --------------------------------------------------------------------------------------------
# what the probes write
# --------------------------------------------------------------------------------------------


def iceberg_schema():
    """Two optional columns with field ids from 1 -- pyiceberg's numbering, which this endpoint
    accepts. Declared explicitly rather than inferred from Arrow, so the partition and sort
    probes have real source ids to point at."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    return Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "v", LongType(), required=False),
    )


def rows(pairs):
    import pyarrow as pa

    return pa.table(
        {
            "id": pa.array([i for i, _ in pairs], pa.int64()),
            "v": pa.array([v for _, v in pairs], pa.int64()),
        }
    )


# --------------------------------------------------------------------------------------------
# partition transforms, column types, type promotion: the same cases for every engine
# --------------------------------------------------------------------------------------------

TEMPORAL = ("year", "month", "day", "hour")
# Rows every engine writes for each transform: (id, value). The two timestamps differ in year,
# month, day and hour, so every temporal transform splits them; truncate(2) splits ap / ba.
TRANSFORM_ROWS = {
    "bucket": [(i, i * 10) for i in range(1, 9)],
    "truncate": [(1, "apple"), (2, "apricot"), (3, "banana")],
    "temporal": [(1, "2025-01-01 10:00:00"), (2, "2026-02-02 11:00:00")],
}

SCALAR_TYPES = ("decimal", "date", "timestamp", "timestamptz", "uuid", "binary")
NESTED_TYPES = ("struct", "list", "map")
# What column x reads back as, normalised by `normalize`, after each engine writes its literal.
TYPE_EXPECTED = {
    "decimal": "12.34",
    "date": "2026-01-02",
    "timestamp": "2026-01-02T03:04:05.123456",
    "timestamptz": "2026-01-02T03:04:05.123456+00:00",
    "uuid": "6f1c2d3e-4b5a-4c6d-8e7f-0123456789ab",
    "binary": "0102",
    "struct": '{"a": 1, "b": "x"}',
    "list": "[1, 2, 3]",
    "map": '{"k": 1}',
}


def _family(kind: str) -> str:
    return "temporal" if kind in TEMPORAL else kind


def transform_case(kind: str):
    """(schema, spec, expected partition count) for one transform: bucket, truncate, or one of
    TEMPORAL. Column 2 holds the value; bucket partitions column 1, the others column 2."""
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import (
        BucketTransform,
        DayTransform,
        HourTransform,
        MonthTransform,
        TruncateTransform,
        YearTransform,
    )
    from pyiceberg.types import LongType, NestedField, StringType, TimestampType

    value_type = {"bucket": LongType(), "truncate": StringType(), "temporal": TimestampType()}
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "x", value_type[_family(kind)], required=False),
    )
    transform = {
        "bucket": BucketTransform(4),
        "truncate": TruncateTransform(2),
        "year": YearTransform(),
        "month": MonthTransform(),
        "day": DayTransform(),
        "hour": HourTransform(),
    }[kind]
    source = 1 if kind == "bucket" else 2
    spec = PartitionSpec(PartitionField(source, 1000, transform, f"{kind}_p"))
    rows_ = TRANSFORM_ROWS[_family(kind)]
    if kind == "bucket":
        bucket = BucketTransform(4).transform(LongType())
        parts = len({bucket(i) for i, _ in rows_})
    else:
        parts = 2
    return schema, spec, parts


def transform_arrow(kind: str, schema):
    """TRANSFORM_ROWS for `kind` as an Arrow table matching `schema`, for pyiceberg's append."""
    import datetime as dt

    import pyarrow as pa
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    rows_ = TRANSFORM_ROWS[_family(kind)]
    if kind in TEMPORAL:
        rows_ = [(i, dt.datetime.fromisoformat(v)) for i, v in rows_]
    return pa.Table.from_pylist(
        [{"id": i, "x": v} for i, v in rows_], schema=schema_to_pyarrow(schema)
    )


def type_schema(name: str):
    """(id long, x <the type>) for one TYPE_EXPECTED case."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        BinaryType,
        DateType,
        DecimalType,
        ListType,
        LongType,
        MapType,
        NestedField,
        StringType,
        StructType,
        TimestampType,
        TimestamptzType,
        UUIDType,
    )

    x = {
        "decimal": DecimalType(10, 2),
        "date": DateType(),
        "timestamp": TimestampType(),
        "timestamptz": TimestamptzType(),
        "uuid": UUIDType(),
        "binary": BinaryType(),
        "struct": StructType(
            NestedField(3, "a", LongType(), required=False),
            NestedField(4, "b", StringType(), required=False),
        ),
        "list": ListType(element_id=3, element_type=LongType(), element_required=False),
        "map": MapType(
            key_id=3, key_type=StringType(), value_id=4, value_type=LongType(), value_required=False
        ),
    }[name]
    return Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "x", x, required=False),
    )


def type_arrow(name: str, schema):
    """The one row (1, <TYPE_EXPECTED value>) as an Arrow table matching `schema`."""
    import datetime as dt
    import decimal
    import uuid

    import pyarrow as pa
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    ts = dt.datetime(2026, 1, 2, 3, 4, 5, 123456)
    value = {
        "decimal": decimal.Decimal("12.34"),
        "date": dt.date(2026, 1, 2),
        "timestamp": ts,
        "timestamptz": ts.replace(tzinfo=dt.UTC),
        "uuid": uuid.UUID(TYPE_EXPECTED["uuid"]).bytes,
        "binary": b"\x01\x02",
        "struct": {"a": 1, "b": "x"},
        "list": [1, 2, 3],
        "map": [("k", 1)],
    }[name]
    return pa.Table.from_pylist([{"id": 1, "x": value}], schema=schema_to_pyarrow(schema))


def normalize(name: str, value) -> str:
    """One read-back value as the string TYPE_EXPECTED holds for it."""
    import datetime as dt
    import uuid

    if value is None:
        return "null"
    if name == "uuid":
        if isinstance(value, bytes | bytearray):
            value = uuid.UUID(bytes=bytes(value))
        return str(value)
    if name == "binary":
        return bytes(value).hex()
    if name == "timestamptz" and isinstance(value, dt.datetime):
        return value.astimezone(dt.UTC).isoformat()
    if isinstance(value, dt.date | dt.datetime):
        return value.isoformat()
    if name == "map" and isinstance(value, list):
        value = dict(value)
    if name in NESTED_TYPES:
        return json.dumps(value, sort_keys=True, default=str)
    return str(value)


def type_result(tbl, name: str) -> tuple[str, str, str]:
    """Column x of the table's one row, read through pyiceberg, against TYPE_EXPECTED."""
    try:
        found = tbl.scan(selected_fields=("x",)).to_arrow().to_pylist()
    except Exception as exc:  # noqa: BLE001 - the read-back failing is the answer
        return name, NOOP, f"written; pyiceberg cannot read it: {scrub.scrub_exc(exc, 200)}"
    got = normalize(name, found[0]["x"]) if len(found) == 1 else f"{len(found)} rows"
    if got != TYPE_EXPECTED[name]:
        return name, NOOP, f"expected {TYPE_EXPECTED[name]}, reads {got}"
    return name, SUPPORTED, got


def partition_result(tbl, kind: str, parts: int) -> tuple[str, str, str]:
    """The spec carries the transform and the data files split the rows into `parts`."""
    spec = [str(f.transform) for f in tbl.spec().fields]
    if not any(s.startswith(kind) for s in spec):
        return kind, NOOP, f"the spec is {spec}"
    files = list(tbl.scan().plan_files())
    found = {str(task.file.partition) for task in files}
    count = sum(task.file.record_count for task in files)
    expected = len(TRANSFORM_ROWS[_family(kind)])
    if len(found) != parts or count != expected:
        return (
            kind,
            NOOP,
            f"{len(found)} partition(s) holding {count} row(s), expected {parts} and {expected}",
        )
    return kind, SUPPORTED, f"{spec[0]} over {parts} partitions"


def verdict(results: list[tuple[str, str, str]]) -> str:
    """One probe asking several things: yes only if every one of them is."""
    ok = [label for label, outcome, _ in results if outcome == SUPPORTED]
    refused = [f"{label}: {d}" for label, outcome, d in results if outcome == REFUSED]
    ignored = [f"{label}: {d}" for label, outcome, d in results if outcome == NOOP]
    if refused:
        raise Refused(f"works: {ok or 'none'}; refused: " + "; ".join(refused + ignored))
    if ignored:
        raise NoOp(f"works: {ok or 'none'}; not applied: " + "; ".join(ignored))
    return "all of " + ", ".join(ok)


# Write after partition evolution: these rows land unpartitioned, the spec gains bucket(4, id),
# then the second set is written under the new spec.
EVOLVE_FIRST = [(1, 10), (2, 20)]
EVOLVE_THEN = [(i, i * 10) for i in range(3, 9)]


def evolve_spec(tbl) -> None:
    """Add bucket(4, id) to the table's spec, through pyiceberg."""
    from pyiceberg.transforms import BucketTransform

    with tbl.update_spec() as update:
        update.add_field("id", BucketTransform(4), "id_bucket")


def evolution_check(tbl, engine_count: int | None = None) -> str:
    """Data files sit on both the first spec and the evolved one, and every row reads back."""
    total = len(EVOLVE_FIRST) + len(EVOLVE_THEN)
    current = tbl.spec().spec_id
    if current == 0:
        raise NoOp("the spec never changed")
    on = sorted({task.file.spec_id for task in tbl.scan().plan_files()})
    count = tbl.scan().to_arrow().num_rows
    if on != sorted({0, current}):
        raise NoOp(f"data files on spec(s) {on}, expected 0 and {current}; pyiceberg reads {count}")
    if count != total:
        raise NoOp(f"pyiceberg reads {count} rows, expected {total}")
    if engine_count is not None and engine_count != total:
        raise NoOp(f"pyiceberg reads {total} rows, the engine reads {engine_count}")
    both = ", by pyiceberg and the engine" if engine_count is not None else ""
    return f"data files on specs {on}; all {total} rows read{both}"


def promotion_schema():
    """(id long, c int), for the int -> long promotion."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import IntegerType, LongType, NestedField

    return Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "c", IntegerType(), required=False),
    )


def promotion_check(tbl) -> str:
    """After the promotion: c is a long, and the row written as an int still reads 7."""
    kind = str(tbl.schema().find_field("c").field_type)
    if kind != "long":
        raise NoOp(f"returned success and c is still {kind}")
    found = tbl.scan(selected_fields=("c",)).to_arrow().to_pylist()
    if [r["c"] for r in found] != [7]:
        raise NoOp(f"c is long, and the old row reads {found}")
    return "c is long, and the row written as int reads 7"


# --------------------------------------------------------------------------------------------
# reading from the metadata: file pruning, and MAX without the data files
# --------------------------------------------------------------------------------------------

# Four appends by pyiceberg: four data files with ids 1-3, 11-13, 21-23 and 31-33 (v = id * 10).
# Their min/max bounds in the manifests do not overlap. pyiceberg writes the files for every
# engine, so every engine reads the same bounds and only its reader is asked.
STATS_FILES = [[(i, i * 10) for i in range(base + 1, base + 4)] for base in (0, 10, 20, 30)]
PRUNE_ID = 22
PRUNE_EXPECTED = [(220,)]
MAX_EXPECTED = [(33,)]


def write_stats_files(tbl) -> None:
    """STATS_FILES into an empty (id, v) table, one append and one data file each."""
    for pairs in STATS_FILES:
        tbl.append(rows(pairs))


def remove_data_files(tbl, keep_id: int | None = None) -> str:
    """Delete the data files from storage and leave the metadata as it is. With keep_id, the one
    file whose bounds hold that id stays. An engine that answers after this never opened them."""
    from pyiceberg.expressions import EqualTo

    files = [task.file.file_path for task in tbl.scan().plan_files()]
    keep = set()
    if keep_id is not None:
        keep = {
            task.file.file_path for task in tbl.scan(row_filter=EqualTo("id", keep_id)).plan_files()
        }
    if len(files) != len(STATS_FILES) or (keep_id is not None and len(keep) != 1):
        raise Broken(
            f"expected {len(STATS_FILES)} data files, one of them holding id {keep_id}; "
            f"found {len(files)} and {len(keep)}"
        )
    gone = [path for path in files if path not in keep]
    for path in gone:
        tbl.io.delete(path)
    left = [path for path in gone if tbl.io.new_input(path).exists()]
    if left:
        raise Broken(f"{len(left)} data file(s) still in storage after the delete")
    return f"{len(gone)} of {len(files)} data files deleted"


def read_without_files(label: str, query, expected: list[tuple], control, gone: str) -> str:
    """The engine's read (`query`), after remove_data_files. The right answer means the engine
    never opened the deleted files. `control` reads every row, so it must fail. If it works, the
    engine skips missing files, and the right answer proves nothing."""
    try:
        got = query()
    except Exception as exc:  # noqa: BLE001 - the engine's error is the answer
        message = str(exc) if isinstance(exc, Refused) else scrub.scrub_exc(exc, 400)
        raise Refused(f"{label} fails with {gone}, so it opens them: {message}") from None
    if got != expected:
        raise Refused(f"{label} with {gone} returns {got}, expected {expected}")
    try:
        found = control()
    except Exception:  # noqa: BLE001 - failing is what the control is for
        return f"{label} returns {got} with {gone}; a full scan fails, as it should"
    raise Broken(
        f"{label} returns {got}, and a full scan also works with {gone} ({found}): "
        "the engine skips missing files, so this proves nothing"
    )
