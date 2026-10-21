"""The pyiceberg write path, for the engines that have no Iceberg writer of their own.

Polars and pandas produce a STREAM of Arrow record batches; in the notebook a Python library
(deltalake) turned that stream into a table. Here that library is pyiceberg 0.12, whose `append`
takes a `pa.RecordBatchReader` and microbatches it into files by `write.target-file-size-bytes`
-- so the engines stream, and nothing on this side holds more than one batch. DuckDB, Sail, Spark,
Trino and, since chdb-core 26.10, chDB never come here. Databend creates its table here
(`recreate`) and writes it itself.

UNPARTITIONED, ON PURPOSE, EVERY ENGINE. The notebook partitioned each table by `year`.
pyiceberg's streaming append is unpartitioned-only: a partitioned table takes a materialised
`pa.Table` (`NotImplementedError` on main as of 2026-09-18; the partitioned follow-up to
apache/iceberg-python#2152, PR #3336, was closed unmerged). A first version of this module
batched files to fit that, and 100 files of it took a 16 GB runner down (run 35548585688). So no
engine partitions -- the native writers dropped their `PARTITIONED BY` / `partitionBy` too, so
the seven tables are the same shape -- and `year` stays as a plain column, which is what every
transform produces anyway.

RECREATE, NOT OVERWRITE. Every run measures a fresh table: drop what the last run left, create,
append. pyiceberg's `overwrite` would keep the old snapshots and schema, and the schema is exactly
what may differ from run to run as an engine version changes what it infers.
"""

from __future__ import annotations

import contextlib

from bench import onelake, scrub
from bench.etl.config import EtlConfig
from bench.tpch.generate import _all_optional


def normalise_schema(schema):
    """`schema` with every timestamp at microsecond precision.

    Iceberg timestamps are microseconds. pyiceberg accepts seconds and milliseconds on create and
    then rejects the same Arrow data on append as a type mismatch, and refuses nanoseconds
    outright. chDB's `DateTime64(6)` and Polars' `Datetime("us")` are fine, but nothing here
    should depend on which engine is writing.
    """
    import pyarrow as pa

    fields = []
    for field in schema:
        if pa.types.is_timestamp(field.type) and field.type.unit != "us":
            field = field.with_type(pa.timestamp("us", tz=field.type.tz))
        fields.append(field)
    return pa.schema(fields, metadata=schema.metadata)


def first_batch(reader):
    """Peek one batch -- the table is created from its schema -- and hand back a reader that
    still starts with it. Raises StopIteration on an empty stream, which is a result too."""
    import itertools

    import pyarrow as pa

    first = reader.read_next_batch()
    return first, pa.RecordBatchReader.from_batches(reader.schema, itertools.chain([first], reader))


def recreate(catalog, cfg: EtlConfig, table: str, arrow_schema):
    """Drop `T{n}.<table>` if it exists, then create it from `arrow_schema`. Unpartitioned."""
    identifier = f"{cfg.schema}.{table}"
    catalog.create_namespace_if_not_exists(cfg.schema)
    if catalog.table_exists(identifier):
        try:
            catalog.purge_table(identifier)
        except Exception as exc:  # noqa: BLE001 - purge needs more permission than drop
            scrub.safe_print(
                f"  purge refused ({scrub.scrub_exc(exc, 160)}); dropping metadata only"
            )
            catalog.drop_table(identifier)
    return catalog.create_table(
        identifier,
        schema=_all_optional(normalise_schema(arrow_schema)),
        location=onelake.table_root(cfg, table),
    )


def append_stream(tbl, reader) -> int:
    """Stream `reader` into `tbl` as ONE append; returns the rows that went through.

    Each batch is cast to the table's own Arrow schema on the way past: string views and large
    strings to string, second- or nanosecond timestamps to microseconds, small integers up to
    what the table declared. That is the same cast pyiceberg would apply to a materialised
    table, done per batch so the stream stays a stream.
    """
    import pyarrow as pa

    target = tbl.schema().as_arrow()
    seen = {"rows": 0}

    def cast_batches():
        for batch in reader:
            seen["rows"] += batch.num_rows
            yield batch.cast(target)

    tbl.append(pa.RecordBatchReader.from_batches(target, cast_batches()))
    return seen["rows"]


def summary(tbl) -> dict:
    """The current snapshot's summary as a plain dict -- a metadata read, no scan.

    pyiceberg models a summary as a `Summary` object whose standard keys are attributes and
    whose everything-else is `additional_properties`, and which key lands where has moved between
    versions. Reading both, `additional_properties` first, is what survives an upgrade.
    """
    tbl.refresh()
    snapshot = tbl.current_snapshot()
    if snapshot is None:
        return {}
    raw = snapshot.summary
    props = dict(getattr(raw, "additional_properties", None) or {})
    for key in ("total-records", "total-data-files", "total-files-size"):
        if props.get(key) is None:
            # Absent in this pyiceberg's Summary -> absent from the dict, and the caller decides.
            with contextlib.suppress(Exception):
                props[key] = raw[key]
    return props


def total_records(tbl) -> int:
    """Rows in the table per its current snapshot summary."""
    value = summary(tbl).get("total-records")
    return 0 if value is None else int(value)


def layout(tbl) -> str | None:
    """`N files, avg X MB, Y GB` for the table as written, or None if the snapshot omits it.

    THE OTHER HALF OF THE MEASUREMENT. An engine that writes this table in 60 seconds as 400
    fragments has not done the same job as one that takes 90 and writes 40 whole files -- the
    next reader pays the difference, and a load time alone cannot show it. Metadata only: the
    snapshot summary already carries both numbers, so this costs one REST call, not a scan.
    """
    props = summary(tbl)
    return describe(props.get("total-data-files"), props.get("total-files-size"))


def describe(files, size) -> str | None:
    """Format a file count and a byte total; None if either is missing or there are no files."""
    if files is None or size is None or int(files) == 0:
        return None
    files, size = int(files), int(size)
    return f"{files} files, {size / files / 1048576:.1f} MB avg, {size / 1073741824:.2f} GB"
