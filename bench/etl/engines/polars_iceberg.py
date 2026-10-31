"""Polars: scan the CSVs from OneLake, stream the result into pyiceberg.

Port of the notebook's `polars_clean_csv`, which ended in `sink_delta(...)` -- a streaming sink.
Polars has no `sink_iceberg`, and its `write_iceberg` is `tbl.append(df.to_arrow())` over a
materialised frame. What it does have is `sink_batches`: the streaming engine runs the query
and hands each finished batch to a Python callback. That callback feeds a bounded queue on a
worker thread, the queue is exposed as a `pa.RecordBatchReader`, and pyiceberg appends it (see
bench/etl/iceberg.py). Polars streams, pyiceberg streams, nothing holds the frame.
`sink_batches` is marked unstable by Polars and slower than a native sink; it is still the only
streaming exit to Python it offers.

The scan is the notebook's: `skip_rows=1` for the `C` header line, a 53-column String schema,
`truncate_ragged_lines` for the multi-table file, `include_file_paths="filename"`. The storage
credential is the same `storage_options={"bearer_token": ...}` the TPC-H engine gives
`scan_iceberg`; object_store recognises the `dfs.fabric.microsoft.com` host on its own.

One departure: the notebook cast "every column not in a set" and so wrote the numeric columns in
whatever order the set iterated. Here they are cast in file order.
"""

from __future__ import annotations

import itertools
import os
import queue
import threading

from bench import auth, scrub
from bench.etl import iceberg
from bench.etl.config import TABLE, EtlConfig
from bench.etl.schema import COLUMNS, TIMESTAMP_FORMAT, numeric_columns
from bench.polars_build import polars_version

# Batches waiting between Polars' worker and pyiceberg's writer. Small on purpose: back-pressure
# is what keeps a 52 GB scan from turning into a 52 GB queue.
QUEUE_DEPTH = 8


class PolarsIceberg:
    name = "polars_iceberg"

    def __init__(self, cfg: EtlConfig):
        self.cfg = cfg
        self._catalog = None
        self._storage: dict[str, str] = {}
        self._tbl = None

    @property
    def version(self) -> str:
        return polars_version()

    def setup(self) -> None:
        os.environ.setdefault("POLARS_MAX_THREADS", "4")
        self._storage = {"bearer_token": auth.onelake_token()}
        self._catalog = auth.catalog(self.cfg)
        scrub.safe_print(f"  polars {self.version} ready, pyiceberg catalog loaded")

    def _frame(self, names: list[str]):
        import polars as pl

        raw = pl.scan_csv(
            [f"{self.cfg.csv_abfss}/{name}" for name in names],
            skip_rows=1,
            schema={column: pl.String for column in COLUMNS},
            has_header=False,
            truncate_ragged_lines=True,
            # A DISPATCHLOAD file's first data rows are OTHER record types with fewer fields, so
            # the row Polars sizes the file from has ~25 columns and the 53-name schema is "not
            # found in CSV file" without this. Polars 2.0's spelling of the notebook's
            # truncate_ragged_lines intent; `insert` fills the absent tail with nulls.
            missing_columns="insert",
            include_file_paths="filename",
            ignore_errors=True,
            storage_options=self._storage,
        )
        transform = raw.filter(
            (pl.col("I") == "D") & (pl.col("UNIT") == "DUNIT") & (pl.col("VERSION") == "3")
        ).drop("XX", "I", "VERSION")
        settlement = pl.col("SETTLEMENTDATE").str.to_datetime(TIMESTAMP_FORMAT)
        return transform.with_columns(
            [
                settlement,
                *[pl.col(c).cast(pl.Float64) for c in numeric_columns() if c != "VERSION"],
                settlement.dt.year().alias("year"),
            ]
        )

    @staticmethod
    def _stream(lf):
        """`lf` as a `pa.RecordBatchReader`, produced by `sink_batches` on a worker thread.

        The worker converts each Polars batch to Arrow and blocks on the queue when the writer
        is behind. A worker exception travels through the queue and is raised to the consumer.
        The reader's schema is the first real batch's, so what Polars actually emits (string
        views, `Datetime("us")`) is what pyiceberg is asked to cast.
        """
        import pyarrow as pa

        pending: queue.Queue = queue.Queue(maxsize=QUEUE_DEPTH)
        done = object()

        def produce() -> None:
            try:
                lf.sink_batches(
                    lambda df: pending.put(df.to_arrow()),
                    maintain_order=False,
                    engine="streaming",
                )
                pending.put(done)
            except BaseException as exc:  # noqa: BLE001 - re-raised on the consumer side
                pending.put(exc)

        threading.Thread(target=produce, name="polars-sink", daemon=True).start()

        def tables():
            while True:
                item = pending.get()
                if item is done:
                    return
                if isinstance(item, BaseException):
                    raise item
                yield item

        stream = tables()
        first = next(stream, None)
        if first is None:
            empty = lf.head(0).collect().to_arrow()
            return pa.RecordBatchReader.from_batches(empty.schema, [])
        batches = itertools.chain.from_iterable(
            table.to_batches() for table in itertools.chain([first], stream)
        )
        return pa.RecordBatchReader.from_batches(first.schema, batches)

    def load(self, files: list[str]) -> None:
        reader = self._stream(self._frame(files))
        first, reader = iceberg.first_batch(reader)
        self._tbl = iceberg.recreate(self._catalog, self.cfg, TABLE[self.name], first.schema)
        rows = iceberg.append_stream(self._tbl, reader)
        scrub.safe_print(f"    {rows:,} rows streamed in one append")

    def row_count(self) -> int:
        return iceberg.total_records(self._tbl)

    def layout(self) -> str | None:
        return iceberg.layout(self._tbl)

    def close(self) -> None:
        self._catalog = None
