"""pandas: read the CSVs from OneLake chunk by chunk, stream the chunks into pyiceberg.

pandas has no SQL and no Iceberg writer, so it is in the Light ETL only. The shape is Polars'
(engines/polars_iceberg.py): the DataFrame library reads and transforms, each piece becomes an
Arrow batch, and pyiceberg appends the whole stream in ONE commit (bench/etl/iceberg.py).

THE READ is pandas' own: `read_csv` on the `abfss://` path, which pandas hands to fsspec, which
resolves it to adlfs. adlfs needs OneLake's blob host and a credential, given through
`storage_options` -- the same two things pyiceberg gives adlfs for every manifest. `chunksize`
makes `read_csv` an iterator, so one file is never held whole.

A THREAD POOL, AND ONLY FOR PANDAS. Every other engine parallelises its own read. pandas' C
parser is one thread per file, and its multithreaded option (`engine="pyarrow"`) fails on these
files (pandas-dev/pandas#71034). So the bench reads WORKERS files at once on a thread pool -- the
C tokenizer releases the GIL. Each worker filters, types and converts its chunks to Arrow, then
puts them on a bounded queue; pyiceberg drains the queue in one append. Chunk order across files
does not matter: it is one commit either way.

The parse is the notebook's intent spelled for pandas: skip the `C` header line, 53 names, every
column as text. A DISPATCHLOAD file mixes record types of different widths. Shorter lines are
filled with NaN. Longer lines (another record type) are skipped, which the filter would drop
anyway; `index_col=False` keeps pandas from turning the extra leading fields into an index.

pandas has no file path column. `filename` is set per file, in Python, to the path that was read.
"""

from __future__ import annotations

import itertools
import queue
import threading
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait

from bench import auth, scrub
from bench.config import ONELAKE_BLOB
from bench.etl import iceberg
from bench.etl.config import TABLE, EtlConfig
from bench.etl.schema import COLUMNS, TIMESTAMP_FORMAT, numeric_columns

# Rows per read_csv chunk. Bounds memory: one chunk of 53 text columns at a time.
CHUNK_ROWS = 500_000

# Files read at once. The runner has 4 vCores, and every other engine is told it has 4 threads.
WORKERS = 4

# Arrow batches waiting between the workers and pyiceberg's writer. Back-pressure bounds memory
# to about QUEUE_DEPTH + WORKERS chunks.
QUEUE_DEPTH = 8


class PandasIceberg:
    name = "pandas_iceberg"

    def __init__(self, cfg: EtlConfig):
        self.cfg = cfg
        self._catalog = None
        self._storage: dict = {}
        self._tbl = None

    @property
    def version(self) -> str:
        import pandas as pd

        return pd.__version__

    def setup(self) -> None:
        self._storage = {
            "account_name": "onelake",
            "account_host": ONELAKE_BLOB,
            "credential": auth.credential(),
        }
        self._catalog = auth.catalog(self.cfg)
        scrub.safe_print(f"  pandas {self.version} ready, pyiceberg catalog loaded")

    def _chunks(self, name: str):
        """One file's rows, filtered and typed, as DataFrames of at most CHUNK_ROWS input rows."""
        import pandas as pd

        numeric = [c for c in numeric_columns() if c != "VERSION"]
        path = f"{self.cfg.csv_abfss}/{name}"
        reader = pd.read_csv(
            path,
            header=None,
            skiprows=1,
            names=list(COLUMNS),
            index_col=False,
            dtype=str,
            on_bad_lines="skip",
            chunksize=CHUNK_ROWS,
            storage_options=self._storage,
        )
        with reader:
            for raw in reader:
                df = raw[
                    (raw["I"] == "D") & (raw["UNIT"] == "DUNIT") & (raw["VERSION"] == "3")
                ].drop(columns=["XX", "I", "VERSION"])
                if df.empty:
                    continue
                df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"], format=TIMESTAMP_FORMAT)
                df[numeric] = df[numeric].astype("float64")
                df["filename"] = path
                df["year"] = df["SETTLEMENTDATE"].dt.year
                yield df

    def _batches(self, names: list[str]):
        """Every file's chunks as Arrow batches, read WORKERS files at a time on a thread pool.

        A worker exception travels through the queue and is raised to the consumer; files not
        started yet are cancelled.
        """
        import pyarrow as pa

        pending: queue.Queue = queue.Queue(maxsize=QUEUE_DEPTH)
        done = object()

        def read(name: str) -> None:
            for df in self._chunks(name):
                pending.put(pa.RecordBatch.from_pandas(df, preserve_index=False))

        def produce() -> None:
            try:
                with ThreadPoolExecutor(WORKERS, thread_name_prefix="pandas-read") as pool:
                    futures = [pool.submit(read, name) for name in names]
                    finished, _ = wait(futures, return_when=FIRST_EXCEPTION)
                    failed = next((f for f in finished if f.exception() is not None), None)
                    if failed is not None:
                        pool.shutdown(wait=False, cancel_futures=True)
                        failed.result()
                pending.put(done)
            except BaseException as exc:  # noqa: BLE001 - re-raised on the consumer side
                pending.put(exc)

        threading.Thread(target=produce, name="pandas-pool", daemon=True).start()
        while True:
            item = pending.get()
            if item is done:
                return
            if isinstance(item, BaseException):
                raise item
            yield item

    def _stream(self, names: list[str]):
        """The chunks as a `pa.RecordBatchReader`, schema from the first chunk."""
        import pyarrow as pa

        batches = self._batches(names)
        first = next(batches, None)
        if first is None:
            raise RuntimeError("no DUNIT rows in the files read")
        return pa.RecordBatchReader.from_batches(first.schema, itertools.chain([first], batches))

    def load(self, files: list[str]) -> None:
        reader = self._stream(files)
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
