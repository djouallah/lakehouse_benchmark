"""Run a notebook from this repo in Fabric and fail if any cell fails.

Same REST sequence as duckrun's fabric_remote (c:/git/duckrun/duckrun/fabric_remote.py): create a
throwaway notebook item from the ipynb, start a RunNotebook job, poll it, delete the item.

The ipynb must carry Fabric's pure-Python notebook metadata (kernel_info.name "jupyter",
microsoft.language_group "jupyter_python"); without it Fabric makes a PySpark notebook, where
`notebookutils.session.restartPython()` kills the job.

The workspace and lakehouse GUIDs go in as job parameters, matched to the notebook's cell tagged
`parameters`, so the notebook needs no default lakehouse.

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
from bench.config import Config

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

    item = create(cfg.workspace_id, name, notebook)
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
        return 1
    print("notebook ran clean")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
