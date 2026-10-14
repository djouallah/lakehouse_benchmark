"""The DuckDB nightly CLI: which nightly gets picked, and how a CLI session reads its answers."""

from __future__ import annotations

import importlib.util
import io
import queue
import re
import tarfile
import zipfile
from pathlib import Path

import pytest

from bench import duckdb_cli

_SCRIPT = Path(__file__).resolve().parents[1] / ".github" / "scripts" / "duckdb_nightly.py"
_spec = importlib.util.spec_from_file_location("duckdb_nightly", _SCRIPT)
nightly = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(nightly)


# --- which nightly ------------------------------------------------------------------------------


def _run(run_id, name):
    return {"id": run_id, "name": name}


def test_alpha_parses_only_nightly_run_names():
    assert nightly.alpha("Main (alpha44357)") == 44357
    assert nightly.alpha("Main") is None
    assert nightly.alpha("NightlyTests") is None


def test_extension_url_is_the_versioned_core_repository():
    assert nightly.extension_url(44357, "iceberg") == (
        "https://extensions.duckdb.org/v2.0.0-alpha44357/linux_amd64/iceberg.duckdb_extension.gz"
    )


def _none(rid):
    return False


def test_pick_skips_nightlies_whose_extensions_were_not_deployed():
    """The 2026-10-06 shape: the newest nightly built its CLI but published no extensions."""
    runs = [
        _run(3, "Main (alpha44578)"),
        _run(9, "NightlyTests"),
        _run(2, "Main (alpha44357)"),
        _run(1, "Main (alpha44297)"),
    ]
    missing = {44578: ["iceberg", "azure"], 44357: [], 44297: []}
    log = []
    run, number, source = nightly.pick(
        runs, missing.__getitem__, lambda rid: True, _none, log.append
    )
    assert (run["id"], number, source) == (2, 44357, "published")
    assert log == ["  alpha44578 (run 3): not published: iceberg, azure; no extension artifacts"]


def test_pick_takes_a_newer_nightly_from_its_artifacts():
    """The 2026-10-14 shape: alpha48043 stuck on osx_amd64, never deployed, Linux builds in its
    artifacts; alpha47600 deployed. The newer build wins."""
    runs = [_run(38578625719, "Main (alpha48043)"), _run(38440753121, "Main (alpha47600)")]
    missing = {48043: list(nightly.EXTENSIONS), 47600: []}
    log = []
    run, number, source = nightly.pick(
        runs, missing.__getitem__, lambda rid: True, lambda rid: rid == 38578625719, log.append
    )
    assert (number, source) == (48043, "artifacts")
    assert log == ["  alpha48043 (run 38578625719): not published, taking its artifacts"]


def test_pick_prefers_published_extensions_within_a_run():
    run, _, source = nightly.pick(
        [_run(1, "Main (alpha1)")], lambda n: [], lambda rid: True, lambda rid: True, print
    )
    assert source == "published"


def test_pick_skips_a_run_whose_cli_artifact_expired():
    runs = [_run(2, "Main (alpha2)"), _run(1, "Main (alpha1)")]
    run, _, _ = nightly.pick(
        runs, lambda n: [], lambda rid: rid == 1, lambda rid: True, lambda line: None
    )
    assert run["id"] == 1


def test_pick_fails_loudly_when_nothing_is_usable():
    with pytest.raises(RuntimeError, match="iceberg"):
        nightly.pick(
            [_run(1, "Main (alpha1)")], lambda n: ["iceberg"], lambda r: True, _none, print
        )


def _extension_zip(names, number=48043):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        for name in names:
            archive.writestr(f"v2.0.0-alpha{number}/linux_amd64/{name}.duckdb_extension", name)
    return buf.getvalue()


def test_extensions_come_out_of_their_artifacts_in_repository_layout(tmp_path):
    blobs = {
        "cloud-extensions-linux_amd64": _extension_zip(["azure", "iceberg", "aws"]),
        "core-extensions-linux_amd64": _extension_zip(["avro", "excel"]),
        "release-core-extensions-linux-amd64": _extension_zip(["httpfs", "tpcds", "json"]),
    }
    written = nightly.extract_extensions(blobs, 48043, tmp_path)
    assert sorted(p.name for p in written) == sorted(
        f"{e}.duckdb_extension" for e in nightly.EXTENSIONS
    )
    iceberg = tmp_path / "v2.0.0-alpha48043" / "linux_amd64" / "iceberg.duckdb_extension"
    assert iceberg.read_text() == "iceberg"
    assert not (tmp_path / "v2.0.0-alpha48043" / "linux_amd64" / "aws.duckdb_extension").exists()


def test_a_missing_extension_in_its_artifact_fails(tmp_path):
    blobs = {
        "cloud-extensions-linux_amd64": _extension_zip(["azure"]),
        "core-extensions-linux_amd64": _extension_zip(["avro"]),
        "release-core-extensions-linux-amd64": _extension_zip(["httpfs", "tpcds"]),
    }
    with pytest.raises(RuntimeError, match="iceberg"):
        nightly.extract_extensions(blobs, 48043, tmp_path)


def test_check_script_points_install_at_the_local_repository():
    script = nightly.check_script("/tmp/ext")
    assert "SET custom_extension_repository = '/tmp/ext';" in script
    assert "SET autoinstall_extension_repository = '/tmp/ext';" in script
    assert all(f"INSTALL {e}; LOAD {e};" in script for e in nightly.EXTENSIONS)
    assert "repository" not in nightly.check_script(None)


def _tar_gz(tmp_path):
    binary = tmp_path / "src" / "duckdb"
    binary.parent.mkdir()
    binary.write_bytes(b"\x7fELF")
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as archive:
        archive.add(binary, arcname="duckdb")
    return buf.getvalue()


def test_unpack_reads_a_raw_tar_gz_artifact(tmp_path):
    binary = nightly._unpack(_tar_gz(tmp_path), tmp_path / "out")
    assert binary.name == "duckdb" and binary.read_bytes() == b"\x7fELF"


def test_unpack_reads_a_zipped_tar_gz_artifact(tmp_path):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr("duckdb-cli-linux-amd64.tar.gz", _tar_gz(tmp_path))
    binary = nightly._unpack(buf.getvalue(), tmp_path / "out")
    assert binary.read_bytes() == b"\x7fELF"


# --- the CLI session ----------------------------------------------------------------------------


class _Stream:
    def __init__(self):
        self.lines: queue.Queue = queue.Queue()

    def readline(self):
        return self.lines.get()


class _Stdin:
    def __init__(self, proc):
        self.proc, self.buffer = proc, ""

    def write(self, text):
        self.buffer += text

    def flush(self):
        script, self.buffer = self.buffer, ""
        self.proc.scripts.append(script)
        self.proc.respond(script)

    def close(self):
        self.proc.stdout.lines.put("")
        self.proc.stderr.lines.put("")


class _FakeCli:
    """Answers each script the way the duckdb shell would: rows on stdout, errors on stderr,
    then the sentinel on each stream."""

    answers: dict[str, tuple[list[str], list[str]]] = {}
    # What the shell writes, unterminated, ahead of the next line on (stdout, stderr).
    noise = ("", "")

    def __init__(self, cmd, **kwargs):
        self.cmd, self.scripts = cmd, []
        self.stdout, self.stderr, self.stdin = _Stream(), _Stream(), _Stdin(self)
        self.returncode = None

    def respond(self, script):
        sentinel = re.search(r"\.print (__bench_end_\w+__)", script).group(1)
        out, err = [], []
        for key, (rows, errors) in self.answers.items():
            if key in script:
                out, err = rows, errors
        for stream, lines, noise in (
            (self.stdout, out + [sentinel], self.noise[0]),
            (self.stderr, err + [sentinel], self.noise[1]),
        ):
            lines[0] = noise + lines[0]
            for line in lines:
                stream.lines.put(line + "\n")

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        self.returncode = 0
        return 0


@pytest.fixture
def cli(monkeypatch):
    monkeypatch.setenv(duckdb_cli.BINARY_ENV, "/opt/duckdb")
    monkeypatch.delenv(duckdb_cli.EXTENSION_REPO_ENV, raising=False)
    monkeypatch.setattr(duckdb_cli.shutil, "which", lambda name: None)
    monkeypatch.setattr(duckdb_cli.subprocess, "Popen", _FakeCli)
    _FakeCli.answers = {}
    _FakeCli.noise = ("", "")
    return _FakeCli


def test_the_session_is_one_process_set_up_for_csv(cli):
    conn = duckdb_cli.DuckDBCli("/tmp/sf10.duckdb")
    assert conn._proc.cmd == ["/opt/duckdb", "-batch", "/tmp/sf10.duckdb"]
    assert ".mode csv" in conn._proc.scripts[0] and ".headers off" in conn._proc.scripts[0]
    assert "extension_repository" not in conn._proc.scripts[0]


def test_artifact_extensions_run_unsigned_from_the_local_repository(cli, monkeypatch):
    monkeypatch.setenv(duckdb_cli.EXTENSION_REPO_ENV, "/runner/_temp/duckdb-extensions")
    conn = duckdb_cli.DuckDBCli()
    assert conn._proc.cmd == ["/opt/duckdb", "-batch", "-unsigned"]
    init = conn._proc.scripts[0]
    assert "SET custom_extension_repository = '/runner/_temp/duckdb-extensions';" in init
    assert "SET autoinstall_extension_repository = '/runner/_temp/duckdb-extensions';" in init


def test_rows_come_back_as_csv_tuples(cli):
    cli.answers = {"FROM lineitem": (['1,"a,b"', "2,c"], [])}
    conn = duckdb_cli.DuckDBCli()
    result = conn.sql("SELECT * FROM lineitem")
    assert result.fetchall() == [("1", "a,b"), ("2", "c")]
    assert result.fetchone() == ("1", "a,b")


def test_a_statement_without_rows_has_none(cli):
    assert duckdb_cli.DuckDBCli().sql("CREATE SECRET s (TYPE azure)").fetchone() is None


def test_stderr_is_the_error(cli):
    cli.answers = {"FROM nope": ([], ["Catalog Error: Table with name nope does not exist!"])}
    conn = duckdb_cli.DuckDBCli()
    with pytest.raises(RuntimeError, match="nope does not exist"):
        conn.sql("SELECT * FROM nope")
    # The session survives a failed statement, as a connection did.
    cli.answers = {"SELECT 1": (["1"], [])}
    assert conn.sql("SELECT 1").fetchone() == ("1",)


def test_the_statement_is_terminated_on_its_own_line():
    """A trailing `-- comment` must not swallow the `;` the shell waits for."""
    assert duckdb_cli._statement("SELECT 1;\n") == "SELECT 1;"
    assert duckdb_cli._statement("SELECT 1 -- q1") == "SELECT 1 -- q1\n;"


def test_extensions_lists_name_and_build(cli):
    cli.answers = {"duckdb_extensions()": (["azure,abc", "iceberg,def"], [])}
    assert duckdb_cli.DuckDBCli().extensions() == "azure=abc, iceberg=def"


def test_terminal_noise_before_the_sentinel_does_not_hang(cli):
    """A slow ATTACH makes the shell write `ESC[00m` with no newline, so it lands on the
    sentinel's line. Exact matching hung every OneLake setup (tmp run 37426460630)."""
    cli.noise = ("\x1b[00m", "\x1b[00m")
    conn = duckdb_cli.DuckDBCli()
    assert conn.sql("ATTACH 'w' AS onelake (TYPE ICEBERG)").fetchall() == []


def test_terminal_noise_is_stripped_from_rows(cli):
    cli.noise = ("\x1b[00m", "")
    cli.answers = {"count(*)": (["2880404"], [])}
    conn = duckdb_cli.DuckDBCli()
    assert conn.sql("SELECT count(*) FROM store_sales").fetchone() == ("2880404",)


def test_a_carriage_return_overwrites_what_came_before():
    assert duckdb_cli._clean("spinner\r42\n") == "42"
