"""pandas: read the CSVs from OneLake file by file, stream the frames into pyiceberg.

pandas has no SQL and no Iceberg writer, so it is in the Light ETL only. The shape is Polars'
(engines/polars_iceberg.py): the DataFrame library reads and transforms, each piece becomes an
Arrow batch, and pyiceberg appends the whole stream in ONE commit (bench/etl/iceberg.py).

THE READ is pandas' own: `read_csv` on the `abfss://` path, which pandas hands to fsspec, which
resolves it to adlfs. adlfs needs OneLake's blob host and a credential, given through
`storage_options` -- the same two things pyiceberg gives adlfs for every manifest. pandas takes one
file per call (no glob, no list), so files are read one after the other, each one whole.

`engine="pyarrow"`: pandas' own option to parse with Arrow's multithreaded CSV reader. The default C
parser is one thread; the filter and casts after the parse still are. pyarrow takes no `chunksize`,
hence one frame per file (a daily file is tens of MB).

The parse is the notebook's intent spelled for pandas: skip the `C` header line, 53 names, every
column as text. A DISPATCHLOAD file mixes record types of different widths; a line with the wrong
number of fields is another record type, which the filter would drop anyway, so it is skipped.

pandas has no file path column. `filename` is set per file, in Python, to the path that was read.
"""

from __future__ import annotations

from bench import auth, scrub
from bench.config import ONELAKE_BLOB
from bench.etl import iceberg
from bench.etl.config import TABLE, EtlConfig
from bench.etl.schema import COLUMNS, TIMESTAMP_FORMAT, numeric_columns


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

    def _frames(self, names: list[str]):
        """Each file's rows, filtered and typed, one DataFrame per file."""
        import pandas as pd

        numeric = [c for c in numeric_columns() if c != "VERSION"]
        for name in names:
            path = f"{self.cfg.csv_abfss}/{name}"
            raw = pd.read_csv(
                path,
                engine="pyarrow",
                header=None,
                skiprows=1,
                names=list(COLUMNS),
                dtype=str,
                on_bad_lines="skip",
                storage_options=self._storage,
            )
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

    def _stream(self, names: list[str]):
        """The frames as a `pa.RecordBatchReader`, schema from the first frame."""
        import itertools

        import pyarrow as pa

        batches = (
            pa.RecordBatch.from_pandas(df, preserve_index=False) for df in self._frames(names)
        )
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
