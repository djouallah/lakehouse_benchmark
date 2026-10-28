"""Run a notebook from this repo in Fabric and fail if any cell fails.

Same REST sequence as duckrun's fabric_remote (c:/git/duckrun/duckrun/fabric_remote.py): create a
throwaway notebook item from the ipynb, start a RunNotebook job, poll it, delete the item.

The ipynb must carry Fabric's pure-Python notebook metadata (kernel_info.name "jupyter",
microsoft.language_group "jupyter_python"); without it Fabric makes a PySpark notebook, where
`notebookutils.session.restartPython()` kills the job.

The workspace and lakehouse GUIDs go in as job parameters, matched to the notebook's cell tagged
`parameters`, so the notebook needs no default lakehouse.

A failed job's failureReason only says "statement execution failures", so the uploaded copy gets one
extra cell, right after the restartPython() cell, that writes any cell's traceback to OneLake Files.
The runner prints it. The notebook in the repo stays clean.

    python .github/scripts/fabric_notebook_run.py notebooks/onelake_duckdb_polars.ipynb
"""

from __future__ import annotations

import base64
import json
import os
import sys
import time
import uuid

import requests

from bench import auth
from bench.config import ONELAKE_DFS, Config

FABRIC_API = "https://api.fabric.microsoft.com/v1"
POLL_SECONDS = 10
TIMEOUT_SECONDS = 45 * 60


def _headers() -> dict:
    token = auth.credential().get_token("https://api.fabric.microsoft.com/.default").token
    return {"Authorization": f"Bearer {token}"}


def _part(path: str, content: dict) -> dict:
    payload = base64.b64encode(json.dumps(content).encode()).decode()
    return {"path": path, "payload": payload, "payloadType": "InlineBase64"}


def _wait_operation(location: str) -> None:
    while True:
        time.sleep(POLL_SECONDS)
        r = requests.get(location, headers=_headers(), timeout=60)
        r.raise_for_status()
        status = r.json().get("status")
        if status == "Succeeded":
            return
        if status in ("Failed", "Undetermined"):
            raise RuntimeError(f"notebook create failed: {r.text}")


ERROR_HOOK = """import traceback, IPython
def _bench_exc(shell, etype, evalue, tb, tb_offset=None):
    notebookutils.fs.put({path!r}, "".join(traceback.format_exception(etype, evalue, tb)), True)
    shell.showtraceback((etype, evalue, tb), tb_offset=tb_offset)
IPython.get_ipython().set_custom_exc((BaseException,), _bench_exc)
"""


def with_error_hook(notebook: dict, path: str) -> dict:
    """The notebook plus the error-hook cell, after the restart so the restart doesn't wipe it."""
    cells = notebook["cells"]
    after = next(
        (i + 1 for i, c in enumerate(cells) if "restartPython" in "".join(c["source"])), 0
    )
    hook = {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {"microsoft": {"language": "python", "language_group": "jupyter_python"}},
        "outputs": [],
        "source": ERROR_HOOK.format(path=path).splitlines(keepends=True),
    }
    return {**notebook, "cells": cells[:after] + [hook] + cells[after:]}


def read_error(url: str) -> str | None:
    headers = {"Authorization": f"Bearer {auth.onelake_token()}", "x-ms-version": "2023-11-03"}
    r = requests.get(url, headers=headers, timeout=60)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    requests.delete(url, headers=headers, timeout=60)
    return r.text


def create(ws: str, name: str, notebook: dict) -> str:
    platform = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Notebook", "displayName": name},
        "config": {"version": "2.0", "logicalId": str(uuid.uuid4())},
    }
    body = {
        "displayName": name,
        "definition": {
            "format": "ipynb",
            "parts": [_part("notebook-content.ipynb", notebook), _part(".platform", platform)],
        },
    }
    r = requests.post(f"{FABRIC_API}/workspaces/{ws}/notebooks", headers=_headers(), json=body, timeout=60)
    if r.status_code in (200, 201):
        return r.json()["id"]
    if r.status_code != 202:
        raise RuntimeError(f"create notebook: HTTP {r.status_code} {r.text}")
    location = r.headers["Location"]
    _wait_operation(location)
    r = requests.get(f"{location}/result", headers=_headers(), timeout=60)
    r.raise_for_status()
    return r.json()["id"]


def run(ws: str, item: str, parameters: dict) -> dict:
    body = {
        "executionData": {
            "parameters": {k: {"value": v, "type": "string"} for k, v in parameters.items()}
        }
    }
    r = requests.post(
        f"{FABRIC_API}/workspaces/{ws}/items/{item}/jobs/instances?jobType=RunNotebook",
        headers=_headers(),
        json=body,
        timeout=60,
    )
    if r.status_code not in (200, 201, 202):
        raise RuntimeError(f"start job: HTTP {r.status_code} {r.text}")
    instance = r.headers["Location"]
    deadline, last = time.time() + TIMEOUT_SECONDS, None
    while time.time() < deadline:
        time.sleep(POLL_SECONDS)
        r = requests.get(instance, headers=_headers(), timeout=60)
        r.raise_for_status()
        job = r.json()
        if job.get("status") != last:
            last = job.get("status")
            print(f"job {job.get('id')}: {last}", flush=True)
        if last in ("Completed", "Failed", "Cancelled", "Deduped"):
            return job
    raise RuntimeError(f"job still {last} after {TIMEOUT_SECONDS}s")


def main(path: str) -> int:
    cfg = Config.from_env()
    with open(path, encoding="utf-8") as f:
        notebook = json.load(f)
    name = f"bench_{os.path.splitext(os.path.basename(path))[0]}_{os.environ.get('GITHUB_RUN_ID', 'local')}"

    error_path = f"Files/_bench_notebook/{name}.txt"
    abfss = f"abfss://{cfg.workspace_id}@{ONELAKE_DFS}/{cfg.lakehouse_id}/{error_path}"
    https = f"https://{ONELAKE_DFS}/{cfg.workspace_id}/{cfg.lakehouse_id}/{error_path}"

    item = create(cfg.workspace_id, name, with_error_hook(notebook, abfss))
    print(f"created notebook {name} ({item})", flush=True)
    try:
        job = run(
            cfg.workspace_id,
            item,
            {"workspace_id": cfg.workspace_id, "lakehouse_id": cfg.lakehouse_id},
        )
    finally:
        requests.delete(f"{FABRIC_API}/workspaces/{cfg.workspace_id}/items/{item}", headers=_headers(), timeout=60)

    if job["status"] != "Completed":
        print(json.dumps(job.get("failureReason") or job, indent=2))
        print(read_error(https) or "no traceback written: the job failed before or in the restart cell")
        return 1
    print("notebook ran clean")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
