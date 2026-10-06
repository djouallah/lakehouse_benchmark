"""DuckDB as its CLI binary: one long-lived `duckdb` process per session, SQL in on stdin.

WHY THE CLI AND NOT THE WHEEL. The PyPI 2.0 dev wheel moves to a new DuckDB core only when
duckdb-python merges a submodule bump, which is a manual PR -- it ran about a week behind the
nightly in October 2026. The nightly CLI is built every night from v2.0-cyanoptera, so the bench
runs that instead. Which nightly is .github/scripts/duckdb_nightly.py's job: the newest one whose
iceberg/azure extensions were actually published, which most nights they are not.

ONE PROCESS, NOT ONE PER STATEMENT. Secrets, the catalog ATTACH, the buffer pool and the external
file cache all live in the process, exactly as they lived in one `duckdb.connect()`. A process per
query would re-attach the catalog every time and measure a cold cache on every warm pass.

HOW A STATEMENT'S END IS FOUND. After the SQL, `sql()` writes a sentinel to BOTH streams:
`.print S` on stdout, and `.output /dev/stderr` / `.print S` / `.output` on stderr. Everything on
stdout before S is the result (CSV, no header); anything on stderr before S is the error. Resetting
`.output` closes the stderr handle, which flushes it, and the process runs under `stdbuf -oL` so
stdout's sentinel is not stuck in a block buffer. The pipe round trip is inside the runner's timer,
the same way Trino's and StarRocks' HTTP round trips are inside theirs.

TERMINAL NOISE. A statement that runs for more than a moment makes the CLI write an ANSI reset
(`ESC[00m`) with no newline, even with no terminal attached, so it lands at the start of whatever
line comes next -- the sentinel included. Matching the sentinel exactly hung every OneLake ATTACH
(smoke 37425114956, ETL 37425193969; tmp run 37426460630 found it). So escape sequences and
anything before a carriage return are stripped from every line, and a line that ENDS with the
sentinel ends the statement.

Values come back as strings (CSV). The callers that need a number say `int(...)`.
"""

from __future__ import annotations

import csv
import functools
import os
import queue
import re
import shutil
import subprocess
import threading
import uuid

# Where the binary is: $DUCKDB_CLI if set, else `duckdb` on PATH (the duckdb-nightly action puts
# it there).
BINARY_ENV = "DUCKDB_CLI"

# Run before the first statement. The progress bar is off for the same reason a non-interactive
# Python connection has it off: it would write to stderr and read as an error.
_INIT = (".bail off", ".mode csv", ".headers off", "SET enable_progress_bar = false;")


def binary() -> str:
    path = os.environ.get(BINARY_ENV) or shutil.which("duckdb")
    if not path:
        raise RuntimeError(
            "no duckdb CLI on PATH -- the duckdb-nightly action "
            "(.github/actions/duckdb-nightly) installs it"
        )
    return path


@functools.cache
def version() -> str:
    """The CLI's `library_version`, e.g. `v2.0.0-alpha44357`. Recorded with every result."""
    out = subprocess.run(
        [binary(), "-csv", "-noheader", "-c", "SELECT library_version FROM pragma_version()"],
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout.strip()


def _statement(text: str) -> str:
    """The SQL with a terminating `;` the shell needs before it will run it.

    On its own line, so a trailing `-- comment` cannot swallow it.
    """
    body = text.rstrip()
    return body if body.endswith(";") else body + "\n;"


class Result:
    """The rows of one statement, as tuples of strings."""

    def __init__(self, rows: list[tuple[str, ...]]):
        self._rows = rows

    def fetchall(self) -> list[tuple[str, ...]]:
        return list(self._rows)

    def fetchone(self) -> tuple[str, ...] | None:
        return self._rows[0] if self._rows else None


_ESCAPE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")


def _clean(line: str) -> str:
    """The line as text: no escape sequences, nothing a carriage return wrote over."""
    return _ESCAPE.sub("", line.rstrip("\r\n")).rsplit("\r", 1)[-1]


def _pump(stream, sink: queue.Queue) -> None:
    for line in iter(stream.readline, ""):
        sink.put(_clean(line))
    sink.put(None)  # EOF: the process is gone


class DuckDBCli:
    """One `duckdb` process. `database` is a file path, or None for in-memory."""

    def __init__(self, database: str | None = None):
        cmd = [binary(), "-batch"]
        if database:
            cmd.append(database)
        if shutil.which("stdbuf"):
            cmd = ["stdbuf", "-oL", *cmd]
        self._proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )
        self._out: queue.Queue = queue.Queue()
        self._err: queue.Queue = queue.Queue()
        for stream, sink in ((self._proc.stdout, self._out), (self._proc.stderr, self._err)):
            threading.Thread(target=_pump, args=(stream, sink), daemon=True).start()
        self._send("\n".join(_INIT))

    def _read_until(self, sink: queue.Queue, sentinel: str) -> list[str]:
        lines = []
        while True:
            line = sink.get()
            if line is None:
                raise RuntimeError(
                    f"duckdb CLI exited (code {self._proc.poll()}): " + "\n".join(lines)
                )
            if line.endswith(sentinel):
                before = line[: -len(sentinel)]
                if before.strip():
                    lines.append(before)
                return lines
            lines.append(line)

    def _send(self, script: str) -> list[str]:
        sentinel = f"__bench_end_{uuid.uuid4().hex}__"
        self._proc.stdin.write(
            f"{script}\n.output /dev/stderr\n.print {sentinel}\n.output\n.print {sentinel}\n"
        )
        self._proc.stdin.flush()
        out = self._read_until(self._out, sentinel)
        err = [line for line in self._read_until(self._err, sentinel) if line.strip()]
        if err:
            raise RuntimeError("\n".join(err))
        return out

    def sql(self, text: str) -> Result:
        """Run one statement (or several, `;`-separated) to completion; its rows, if any."""
        lines = self._send(_statement(text))
        return Result([tuple(row) for row in csv.reader(lines)])

    def extensions(self) -> str:
        """The loaded extensions and their builds, `iceberg=abc123, azure=def456`."""
        rows = self.sql(
            "SELECT extension_name, extension_version FROM duckdb_extensions() "
            "WHERE loaded ORDER BY 1"
        ).fetchall()
        return ", ".join(f"{name}={ver}" for name, ver in rows)

    def close(self) -> None:
        if self._proc.poll() is None:
            try:
                self._proc.stdin.write(".quit\n")
                self._proc.stdin.close()
                self._proc.wait(timeout=60)
            except (OSError, subprocess.TimeoutExpired):
                self._proc.kill()
                self._proc.wait()
