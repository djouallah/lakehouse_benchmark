"""Daft: read the CSVs from OneLake, write Iceberg with Daft's own `write_iceberg`.

Port of the notebook's `daft_clean_csv`, which ended in `write_deltalake(...)`. Daft has a native
Iceberg writer that takes a pyiceberg `Table`, executes the lazy plan itself and commits the
files it wrote, so this is the one pyiceberg-catalogued engine that does the whole list in one
write with no batching: Daft's runner streams the plan's output through the writer.

The credential is the TPC-H engine's IOConfig -- `AzureConfig(storage_account="onelake",
bearer_token=..., use_fabric_endpoint=True)` -- for the read AND the write; `auth.catalog()`
signs the REST calls that create the table and commit the snapshot.

ONELAKE'S abfss:// PATHS PARSE SINCE Eventual-Inc/Daft#7533 (the nightly in
requirements/etl_daft_iceberg.txt). Before it, `parse_azure_uri` took the host as the container
(`400 FriendlyNameSupportDisabled`), so the CSVs were read and the data files written as
`az://<workspace>/<lakehouse>/...`.

DAFT'S AZURE WRITE IS PYTHON, not the Rust client: pyarrow's ParquetWriter over fsspec/adlfs
(see `_register_onelake_filesystem` for what that costs on OneLake and how it is made to work).
The timed window therefore includes adlfs's block uploads, which is Daft's write path as shipped.

The read and the transform are the notebook's: a 53-column string schema, `infer_schema=False`,
`has_headers=False`, `allow_variable_columns=True`, `file_path_column="filename"`; the filter;
every non-text column cast to float64 (VERSION included, as the notebook had it); `DATE` and
`year` from the parsed timestamp. `Expression.to_datetime(format)` and `Expression.year()` are
direct methods in Daft 0.7 -- the notebook's spelling; the `.str` / `.dt` namespaces of older
releases are gone (`'Expression' object has no attribute 'str'`).
"""

from __future__ import annotations

from bench import auth, scrub
from bench.config import ONELAKE_BLOB
from bench.etl import iceberg
from bench.etl.config import TABLE, EtlConfig
from bench.etl.schema import COLUMNS, TIMESTAMP_FORMAT, numeric_columns


class DaftIceberg:
    name = "daft_iceberg"

    def __init__(self, cfg: EtlConfig):
        self.cfg = cfg
        self._io = None
        self._catalog = None
        self._tbl = None

    @property
    def version(self) -> str:
        from importlib.metadata import version

        return version("daft")

    def setup(self) -> None:
        from daft.io import AzureConfig, IOConfig

        self._io = IOConfig(
            azure=AzureConfig(
                storage_account="onelake",
                bearer_token=auth.onelake_token(),
                use_fabric_endpoint=True,
            )
        )
        self._catalog = auth.catalog(self.cfg)
        self._register_onelake_filesystem()
        scrub.safe_print(f"  daft {self.version} ready, pyiceberg catalog loaded")

    @staticmethod
    def _register_onelake_filesystem() -> None:
        """Point the fsspec `abfs` implementation Daft writes through at OneLake.

        Daft's Azure WRITE is not its Rust client: `daft/filesystem.py` resolves an Azure path to
        `pyarrow.fs.PyFileSystem(FSSpecHandler(adlfs.AzureBlobFileSystem(...)))` and hands it
        exactly six kwargs from the IOConfig -- account name, account key, SAS token, tenant,
        client id, client secret -- never the bearer token and never a host. On OneLake that
        means adlfs dials `onelake.blob.core.windows.net` with DefaultAzureCredential, which on a
        runner ends in the az CLI and `AADSTS700016` (run 35547908735). adlfs itself is fine with
        OneLake: pyiceberg drives it here with `account_host` and a token for every manifest.

        So the class fsspec returns for `abfs` becomes a subclass that fills in what Daft omits:
        OneLake's blob host and this repo's refreshing OIDC credential. Daft's kwargs still win
        where it passes one. pyiceberg imports adlfs directly and is unaffected.
        """
        import fsspec
        from adlfs import AzureBlobFileSystem

        credential = auth.credential()

        class OneLakeBlobFileSystem(AzureBlobFileSystem):
            def __init__(self, *args, **kwargs):
                kwargs.setdefault("account_host", ONELAKE_BLOB)
                kwargs.setdefault("credential", credential)
                super().__init__(*args, **kwargs)

        fsspec.register_implementation("abfs", OneLakeBlobFileSystem, clobber=True)

    def _frame(self, names: list[str]):
        import daft
        from daft import DataType, col

        df = daft.read_csv(
            [f"{self.cfg.csv_abfss}/{name}" for name in names],
            schema={column: DataType.string() for column in COLUMNS},
            infer_schema=False,
            has_headers=False,
            allow_variable_columns=True,
            file_path_column="filename",
            io_config=self._io,
        )
        df = df.where((df["UNIT"] == "DUNIT") & (df["VERSION"] == "3") & (df["I"] == "D"))
        df = df.exclude("I", "XX")
        for column in numeric_columns():
            df = df.with_column(column, col(column).cast(DataType.float64()))
        df = df.with_column("SETTLEMENTDATE", col("SETTLEMENTDATE").to_datetime(TIMESTAMP_FORMAT))
        df = df.with_column("DATE", col("SETTLEMENTDATE").cast(DataType.date()))
        return df.with_column("year", col("SETTLEMENTDATE").year())

    def load(self, files: list[str]) -> None:
        df = self._frame(files)
        self._tbl = iceberg.recreate(
            self._catalog, self.cfg, TABLE[self.name], df.schema().to_pyarrow_schema()
        )
        written = df.write_iceberg(self._tbl, mode="append", io_config=self._io)
        rows = sum(int(n or 0) for n in written.to_pydict().get("rows", []))
        scrub.safe_print(f"    {rows:,} rows written in one append")

    def row_count(self) -> int:
        return iceberg.total_records(self._tbl)

    def layout(self) -> str | None:
        return iceberg.layout(self._tbl)

    def close(self) -> None:
        self._catalog = None
        self._io = None
