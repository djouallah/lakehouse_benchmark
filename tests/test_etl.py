"""The ETL port: naming parity with the notebook, the landing's selection rules, the runner's rows.

Everything here is offline. The lakehouse is a fake filesystem object with the three ADLS calls
the code makes; nemweb and the mirror are a dict of canned responses.
"""

from __future__ import annotations

import io
import zipfile

import pytest

from bench.etl import landing
from bench.etl.config import ETL_ENGINES, TABLE, EtlConfig
from bench.etl.data import csv_names, list_csvs
from bench.etl.iceberg import append_stream, first_batch, normalise_schema
from bench.etl.runner import benchmark, load_row


def _cfg(files: int = 100, **kwargs) -> EtlConfig:
    return EtlConfig(workspace_id="ws", lakehouse_id="lh", sf=files, **kwargs)


# -- config -----------------------------------------------------------------------------------


def test_namespace_matches_the_notebook():
    """`f'T{total_files}'`, unpadded: the notebook and the workflow share a lakehouse."""
    assert _cfg(10).schema == "T10"
    assert _cfg(100).schema == "T100"
    assert _cfg(1000).schema == "T1000"
    assert _cfg(100).files == 100


def test_csv_locations():
    cfg = _cfg()
    assert cfg.csv_relative == "lh/Files/csv"
    assert cfg.csv_abfss == "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Files/csv"
    assert cfg.csv_https == "https://onelake.blob.fabric.microsoft.com/ws/lh/Files/csv"
    assert cfg.csv_az == "az://ws/lh/Files/csv"
    assert cfg.warehouse == "ws/lh"


def test_from_env(monkeypatch):
    monkeypatch.setenv("FABRIC_WORKSPACE_ID", "ws")
    monkeypatch.setenv("FABRIC_LAKEHOUSE_ID", "lh")
    monkeypatch.setenv("ETL_FILES", "10")
    monkeypatch.setenv("BENCH_ENGINE", "chdb_iceberg")
    cfg = EtlConfig.from_env()
    assert (cfg.files, cfg.engine, cfg.schema) == (10, "chdb_iceberg", "T10")


def test_every_etl_engine_has_a_label_a_colour_and_a_table():
    from bench.charts import DARK, LABEL, LIGHT
    from bench.etl.engines import get_engine

    for engine in ETL_ENGINES:
        assert engine in LABEL and engine in LIGHT and engine in DARK and engine in TABLE
        # Constructing never imports the engine's package; only setup() does.
        assert get_engine(engine, _cfg()).name == engine
    with pytest.raises(ValueError):
        get_engine("nope", _cfg())


# -- landing ------------------------------------------------------------------------------


def _nemweb_line(stem: str, size: int) -> str:
    return (
        f" Saturday, September 20, 2026  4:05 AM   {size} "
        f'<A HREF="/Reports/Current/Daily_Reports/{stem}.zip">{stem}.zip</A><br>'
    )


NEMWEB = chr(10).join(
    [
        '<html><body><pre><A HREF="/Reports/Current/Daily_Reports/">[To Parent Directory]</A><br>',
        _nemweb_line("PUBLIC_DAILY_202609170000_20260918040502", 5988304),
        _nemweb_line("PUBLIC_DAILY_202609180000_20260919040504", 6001122),
        _nemweb_line("PUBLIC_DAILY_202609190000_20260920040503", 6012331),
        ' Saturday, September 20, 2026  4:06 AM   1200 <A HREF="/Reports/Current/Daily_Reports/'
        'PUBLIC_DAILY_OTHER_README.txt">README</A><br>',
        "</pre></body></html>",
    ]
)

MIRROR_2018 = (
    '[{"name": "PUBLIC_DAILY_201804010000_20180402040501.zip", "type": "file",'
    ' "download_url": "https://x/2018/PUBLIC_DAILY_201804010000_20180402040501.zip"},'
    ' {"name": "PUBLIC_DAILY_201804020000_20180403040501.zip", "type": "file",'
    ' "download_url": "https://x/2018/PUBLIC_DAILY_201804020000_20180403040501.zip"},'
    ' {"name": "README.md", "type": "file", "download_url": "https://x/README.md"}]'
)


def test_parse_nemweb_keeps_only_daily_zips():
    found = landing.parse_nemweb(NEMWEB)
    assert sorted(found) == [
        "PUBLIC_DAILY_202609170000_20260918040502",
        "PUBLIC_DAILY_202609180000_20260919040504",
        "PUBLIC_DAILY_202609190000_20260920040503",
    ]
    assert found["PUBLIC_DAILY_202609190000_20260920040503"] == (
        "https://nemweb.com.au/Reports/Current/Daily_Reports/"
        "PUBLIC_DAILY_202609190000_20260920040503.zip"
    )


def test_parse_mirror_keeps_only_daily_zips():
    found = landing.parse_mirror(MIRROR_2018)
    assert sorted(found) == [
        "PUBLIC_DAILY_201804010000_20180402040501",
        "PUBLIC_DAILY_201804020000_20180403040501",
    ]


def test_select_takes_the_newest_missing_files_first():
    """The notebook's rule: name-descending, skip what is present, take `want - present`."""
    available = {f"PUBLIC_DAILY_2026090{d}0000": f"u{d}" for d in range(1, 6)}
    present = ["PUBLIC_DAILY_202609030000.CSV"]  # CSV names, zip stems: same file
    chosen = landing.select(available, present, want=3)
    assert chosen == [("PUBLIC_DAILY_202609050000", "u5"), ("PUBLIC_DAILY_202609040000", "u4")]
    assert landing.select(available, present, want=1) == []


def test_listing_backfills_from_the_mirror_only_when_nemweb_is_short():
    calls: list[str] = []

    def fetch(url: str) -> bytes:
        calls.append(url)
        if url == landing.NEMWEB_DAILY:
            return NEMWEB.encode()
        if url.endswith("/2018"):
            return MIRROR_2018.encode()
        return b"[]"

    found = landing.listing(present=[], want=2, fetch=fetch)
    assert len(found) == 3 and calls == [landing.NEMWEB_DAILY]

    calls.clear()
    found = landing.listing(present=[], want=5, fetch=fetch)
    assert len(found) == 5
    assert calls[0] == landing.NEMWEB_DAILY and len(calls) == 1 + len(landing.MIRROR_YEARS)


# -- a fake lakehouse ------------------------------------------------------------------------


class _Path:
    def __init__(self, name: str, size: int, is_directory: bool = False):
        self.name, self.content_length, self.is_directory = name, size, is_directory


class _Directory:
    def __init__(self, fs, path):
        self.fs, self.path = fs, path

    def exists(self) -> bool:
        return self.fs.exists

    def create_directory(self) -> None:
        self.fs.exists = True


class _File:
    def __init__(self, fs, path):
        self.fs, self.path = fs, path

    def upload_data(self, payload: bytes, overwrite: bool = False) -> None:
        assert overwrite
        self.fs.files[self.path.rsplit("/", 1)[-1]] = len(payload)
        self.fs.uploads.append(self.path)


class _FS:
    """The three ADLS calls the ETL makes: exists(), get_paths(), upload_data()."""

    def __init__(self, files: dict[str, int] | None = None):
        self.files = dict(files or {})
        self.exists = bool(self.files)
        self.uploads: list[str] = []

    def get_directory_client(self, path):
        return _Directory(self, path)

    def get_file_client(self, path):
        return _File(self, path)

    def get_paths(self, path, recursive=False):
        assert path == "lh/Files/csv" and not recursive
        yield _Path("lh/Files/csv/sub", 0, is_directory=True)
        yield _Path("lh/Files/csv/notes.txt", 5)
        for name, size in self.files.items():
            yield _Path(f"lh/Files/csv/{name}", size)


def test_list_csvs_ignores_directories_and_other_files():
    fs = _FS({"B.CSV": 2, "a.csv": 1})
    assert list_csvs(fs, _cfg()) == {"B.CSV": 2, "a.csv": 1}
    assert list_csvs(_FS(), _cfg()) == {}


def test_csv_names_is_ascending_first_n_with_size():
    fs = _FS({f"PUBLIC_DAILY_2026090{d}.CSV": 2**30 for d in (3, 1, 2)})
    names, gb = csv_names(_cfg(), 2, fs=fs)
    assert names == ["PUBLIC_DAILY_20260901.CSV", "PUBLIC_DAILY_20260902.CSV"]
    assert gb == 2.0
    with pytest.raises(RuntimeError, match="holds 3"):
        csv_names(_cfg(), 4, fs=fs)


def _zip(stem: str, payload: bytes) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(f"{stem}.CSV", payload)
    return buffer.getvalue()


def test_land_skips_when_the_folder_already_has_enough():
    fs = _FS({"PUBLIC_DAILY_202609170000_20260918040502.CSV": 10})

    def fetch(url: str) -> bytes:
        raise AssertionError("nothing should be fetched")

    summary = landing.land(_cfg(1), fetch=fetch, fs=fs)
    assert summary["skipped"] is True and fs.uploads == []


def test_land_downloads_only_the_shortfall_newest_first():
    fs = _FS({"PUBLIC_DAILY_202609170000_20260918040502.CSV": 10})
    fetched: list[str] = []

    def fetch(url: str) -> bytes:
        fetched.append(url)
        if url == landing.NEMWEB_DAILY:
            return NEMWEB.encode()
        stem = url.rsplit("/", 1)[-1].removesuffix(".zip")
        return _zip(stem, b"C,DISPATCH\nD,DUNIT\n")

    summary = landing.land(_cfg(3), fetch=fetch, fs=fs)
    assert summary["landed"] == 2 and summary["present"] == 3 and summary["skipped"] is False
    assert fs.uploads == [
        "lh/Files/csv/PUBLIC_DAILY_202609190000_20260920040503.CSV",
        "lh/Files/csv/PUBLIC_DAILY_202609180000_20260919040504.CSV",
    ] or sorted(fs.uploads) == [
        "lh/Files/csv/PUBLIC_DAILY_202609180000_20260919040504.CSV",
        "lh/Files/csv/PUBLIC_DAILY_202609190000_20260920040503.CSV",
    ]
    assert fetched[0] == landing.NEMWEB_DAILY and len(fetched) == 3


def test_land_refuses_a_listing_that_cannot_cover_the_request():
    def fetch(url: str) -> bytes:
        return NEMWEB.encode() if url == landing.NEMWEB_DAILY else b"[]"

    with pytest.raises(RuntimeError, match="available to land"):
        landing.land(_cfg(10), fetch=fetch, fs=_FS())


# -- iceberg helper --------------------------------------------------------------------------


def test_normalise_schema_puts_every_timestamp_at_microseconds():
    import pyarrow as pa

    schema = pa.schema(
        [
            ("s", pa.timestamp("s")),
            ("ns_tz", pa.timestamp("ns", tz="UTC")),
            ("us", pa.timestamp("us")),
            ("x", pa.float64()),
        ]
    )
    out = normalise_schema(schema)
    assert out.field("s").type == pa.timestamp("us")
    assert out.field("ns_tz").type == pa.timestamp("us", tz="UTC")
    assert out.field("us").type == pa.timestamp("us")
    assert out.field("x").type == pa.float64()
    assert normalise_schema(out).equals(out)


def _reader(rows_per_batch=(2, 3)):
    import pyarrow as pa

    schema = pa.schema([("year", pa.uint16()), ("t", pa.timestamp("s")), ("s", pa.large_string())])
    batches = [
        pa.record_batch(
            [pa.array([2026] * n, pa.uint16()), pa.array([1] * n, pa.timestamp("s")), ["a"] * n],
            schema=schema,
        )
        for n in rows_per_batch
    ]
    return pa.RecordBatchReader.from_batches(schema, batches)


class _IcebergSchema:
    def __init__(self, arrow):
        self._arrow = arrow

    def as_arrow(self):
        return self._arrow


class _FakeTable:
    """Records the reader it is handed and drains it, like pyiceberg's streaming append."""

    def __init__(self, arrow_schema):
        self._schema = _IcebergSchema(arrow_schema)
        self.appended = []

    def schema(self):
        return self._schema

    def append(self, reader):
        self.appended.append(reader.read_all())


def test_first_batch_peeks_without_losing_it():
    first, reader = first_batch(_reader())
    assert first.num_rows == 2
    assert reader.read_all().num_rows == 5


def test_append_stream_casts_each_batch_to_the_table_schema_and_counts():
    import pyarrow as pa

    target = pa.schema([("year", pa.int32()), ("t", pa.timestamp("us")), ("s", pa.string())])
    tbl = _FakeTable(target)
    assert append_stream(tbl, _reader()) == 5
    (table,) = tbl.appended
    assert table.schema.equals(target)
    assert table.num_rows == 5 and table.column("year").to_pylist() == [2026] * 5


# -- runner --------------------------------------------------------------------------------------


class _Engine:
    name = "duckdb_iceberg"
    version = "0.0-fake"

    def __init__(self):
        self.closed = False
        self.loaded: list[str] | None = None

    def setup(self):
        pass

    def load(self, files):
        self.loaded = files

    def row_count(self):
        return 42

    def close(self):
        self.closed = True


class _LoadFails(_Engine):
    def load(self, files):
        raise RuntimeError("catalog refused the commit")


class _SetupFails(_Engine):
    def setup(self):
        raise RuntimeError("no token")


def test_runner_records_setup_and_load_as_two_rows():
    engine = _Engine()
    result = benchmark(engine, _cfg(2), ["a.CSV", "b.CSV"])
    assert result.status == "ok" and result.version == "0.0-fake"
    assert [(r.phase, r.query) for r in result.rows] == [("setup", 0), ("load", 1)]
    row = load_row(result)
    assert row.status == "ok" and row.rows == 42 and row.dur is not None
    assert engine.loaded == ["a.CSV", "b.CSV"] and engine.closed


def test_runner_records_a_load_failure_and_still_closes():
    engine = _LoadFails()
    result = benchmark(engine, _cfg(), ["a.CSV"])
    row = load_row(result)
    assert result.status == "ok" and row.status == "error"
    assert "catalog refused the commit" in row.error and row.dur is None
    assert engine.closed


def test_runner_records_a_setup_failure():
    engine = _SetupFails()
    result = benchmark(engine, _cfg(), ["a.CSV"])
    assert result.status == "setup_failed" and len(result.rows) == 1
    assert "no token" in result.rows[0].error and load_row(result) is None
    assert engine.closed
