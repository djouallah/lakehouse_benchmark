"""A REST proxy that puts another writer's commit between an engine's read and its commit.

WHY A PROXY. Most engines have no multi-statement transaction: one statement loads the table,
plans the write and commits it, and nothing outside can land a commit inside that window on
purpose. The window does show on the wire, though. The engine's commit is a POST to
`.../namespaces/{ns}/tables/{table}` built on the snapshot it read. Hold that POST, commit
something else to the same table first, and then forward it. Whatever the engine does next is
what it does when it loses a race: give up, refresh and retry, or rebase blindly. The race is
deterministic, and it is the same race for every engine.

ONLY THE CATALOG GOES THROUGH HERE. Data and manifest files still go straight to OneLake storage.
The proxy speaks plain HTTP on 127.0.0.1 and forwards to the real endpoint over HTTPS, passing
the engine's own bearer token through untouched.
"""

from __future__ import annotations

import contextlib
import json
import re
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from bench.config import ICEBERG_ENDPOINT

_TABLE_COMMIT = re.compile(r"/namespaces/(?P<ns>[^/]+)/tables/(?P<table>[^/]+)$")
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "transfer-encoding",
    "content-length",
    "proxy-connection",
    "te",
    "upgrade",
}


def commit_target(method: str, path: str, body: bytes) -> list[tuple[str, str]]:
    """The (namespace, table) pairs a request commits to; empty for anything that is not a
    commit. A table commit is a POST to the table's own path; a multi-table one is a POST to
    transactions/commit naming each table."""
    if method != "POST":
        return []
    path = urllib.parse.urlsplit(path).path
    match = _TABLE_COMMIT.search(path)
    if match:
        ns = urllib.parse.unquote(match["ns"]).replace("\x1f", ".")
        return [(ns, urllib.parse.unquote(match["table"]))]
    if path.endswith("/transactions/commit"):
        try:
            changes = json.loads(body or b"{}").get("table-changes", [])
        except ValueError:
            return []
        found = []
        for change in changes:
            ident = change.get("identifier") or {}
            found.append((".".join(ident.get("namespace", [])), ident.get("name", "")))
        return found
    return []


class RaceProxy:
    """`arm(ns, table, inject)` makes the next commit to that table wait for `inject()` first."""

    def __init__(self, upstream: str = ICEBERG_ENDPOINT):
        parts = urllib.parse.urlsplit(upstream)
        self._origin = f"{parts.scheme}://{parts.netloc}"
        self._lock = threading.Lock()
        self._armed: dict[tuple[str, str], object] = {}
        # Every commit seen per table: (HTTP status, first line of the error body or "").
        self.commits: dict[tuple[str, str], list[tuple[int, str]]] = {}
        self.injected: dict[tuple[str, str], str] = {}
        # Every request: (method, path without the query, status, the current-snapshot-id a
        # loadTable answered with), for tracing an engine.
        self.log: list[tuple[str, str, int, int | None]] = []
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self._server.daemon_threads = True
        self.endpoint = f"http://127.0.0.1:{self._server.server_address[1]}{parts.path}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def start(self) -> RaceProxy:
        self._thread.start()
        return self

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def arm(self, ns: str, table: str, inject) -> None:
        with self._lock:
            self._armed[(ns, table)] = inject
            self.commits[(ns, table)] = []

    def statuses(self, ns: str, table: str) -> list[int]:
        return [status for status, _ in self.commits.get((ns, table), [])]

    # -- the request path ----------------------------------------------------------------------

    def _take(self, key: tuple[str, str]):
        with self._lock:
            return self._armed.pop(key, None)

    def _inject(self, key: tuple[str, str]) -> None:
        inject = self._take(key)
        if inject is None:
            return
        try:
            inject()
            self.injected[key] = "landed"
        except Exception as exc:  # noqa: BLE001 - recorded; the probe reads it
            self.injected[key] = f"failed: {type(exc).__name__}: {exc}"

    def _forward(self, method: str, path: str, headers: dict, body: bytes | None):
        req = urllib.request.Request(self._origin + path, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=300) as response:
                return response.status, list(response.headers.items()), response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, list(exc.headers.items()), exc.read()

    def _rewrite_config(self, body: bytes) -> bytes:
        """A server-side `uri` override would send the client around the proxy."""
        try:
            config = json.loads(body)
        except ValueError:
            return body
        overrides = config.get("overrides") or {}
        if "uri" not in overrides:
            return body
        overrides["uri"] = self.endpoint
        return json.dumps(config).encode()

    def _handler(self):
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args) -> None:  # quiet: the probe prints what matters
                pass

            def _body(self) -> bytes | None:
                if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                    chunks = []
                    while True:
                        size = int(self.rfile.readline().split(b";")[0].strip(), 16)
                        if size == 0:
                            self.rfile.readline()
                            break
                        chunks.append(self.rfile.read(size))
                        self.rfile.readline()
                    return b"".join(chunks)
                length = int(self.headers.get("Content-Length") or 0)
                return self.rfile.read(length) if length else None

            def _serve(self) -> None:
                body = self._body()
                targets = commit_target(self.command, self.path, body or b"")
                for key in targets:
                    proxy._inject(key)
                headers = {
                    k: v
                    for k, v in self.headers.items()
                    if k.lower() not in _HOP_BY_HOP | {"host", "accept-encoding", "expect"}
                }
                status, out_headers, out = proxy._forward(self.command, self.path, headers, body)
                snapshot = None
                if self.command == "GET" and _TABLE_COMMIT.search(self.path.split("?")[0]):
                    with contextlib.suppress(ValueError, KeyError, TypeError):
                        snapshot = json.loads(out)["metadata"].get("current-snapshot-id")
                proxy.log.append((self.command, self.path.split("?")[0], status, snapshot))
                if self.command == "GET" and self.path.split("?")[0].endswith("/v1/config"):
                    out = proxy._rewrite_config(out)
                for key in targets:
                    first = " ".join(out[:300].decode("utf-8", "replace").split())
                    with proxy._lock:
                        proxy.commits.setdefault(key, []).append(
                            (status, "" if status < 300 else first)
                        )
                self.send_response(status)
                for k, v in out_headers:
                    if k.lower() not in _HOP_BY_HOP:
                        self.send_header(k, v)
                self.send_header("Content-Length", str(len(out)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(out)

            do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = _serve

        return Handler
