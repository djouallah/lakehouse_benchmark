"""The DuckDB CLI session: how one `duckdb` process reads its answers."""

from __future__ import annotations

import queue
import re

import pytest

from bench import duckdb_cli

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
