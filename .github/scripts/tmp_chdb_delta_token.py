"""Throwaway: which OneLake credential, if any, lets chDB's Azure engines in.

Run 44866693699 got 403 on every DeltaLakeAzure statement with the lakehouse-directory user
delegation SAS. This asks each piece apart: a plain azureBlobStorage read of a file we upload,
then a DeltaLakeAzure CREATE + INSERT + SELECT, with each SAS scope OneLake will sign, over the
blob and the dfs endpoint. Prints one line per attempt; nothing is published.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import tempfile
import uuid

from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DirectorySasPermissions,
    FileSystemSasPermissions,
    generate_directory_sas,
    generate_file_system_sas,
)
from chdb import session

from bench import auth, scrub
from bench.config import ONELAKE_BLOB, ONELAKE_DFS, Config

cfg = Config.from_env()
run = uuid.uuid4().hex[:8]
folder = f"{cfg.lakehouse_id}/Files/_tmp_chdb_token_{run}"
service = DataLakeServiceClient(f"https://{ONELAKE_DFS}", credential=auth.credential())
fs = service.get_file_system_client(cfg.workspace_id)
fs.get_file_client(f"{folder}/x.csv").upload_data(b"a,b\n1,2\n3,4\n", overwrite=True)

start = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=5)
expiry = start + dt.timedelta(hours=1)
key = service.get_user_delegation_key(start, expiry)
perms = dict(read=True, list=True, create=True, write=True, delete=True)
sas = {}
for label, make in {
    "lakehouse directory": lambda: generate_directory_sas(
        "onelake",
        cfg.workspace_id,
        cfg.lakehouse_id,
        key,
        permission=DirectorySasPermissions(**perms),
        expiry=expiry,
        start=start,
    ),
    "Files folder": lambda: generate_directory_sas(
        "onelake",
        cfg.workspace_id,
        folder,
        key,
        permission=DirectorySasPermissions(**perms),
        expiry=expiry,
        start=start,
    ),
    "workspace (file system)": lambda: generate_file_system_sas(
        "onelake",
        cfg.workspace_id,
        key,
        permission=FileSystemSasPermissions(**perms, add=True),
        expiry=expiry,
        start=start,
    ),
}.items():
    try:
        token = make()
        scrub.register(token)
        sas[label] = token
        fields = {
            k: v for k, v in (p.split("=", 1) for p in token.split("&")) if k in ("sr", "sp", "sdd")
        }
        print(f"SAS {label}: signed {fields}")
    except Exception as exc:  # noqa: BLE001
        print(f"SAS {label}: OneLake refused to sign: {scrub.scrub_exc(exc, 200)}")

db = session.Session(tempfile.mkdtemp(prefix="chdb_tok_"))
for s in ("SET allow_delta_lake_writes = 1", "SET allow_delta_lake_create_table = 1"):
    with contextlib.suppress(Exception):
        db.query(s)


def ask(what: str, sql: str) -> None:
    try:
        out = str(db.query(sql, "CSV")).strip().replace("\n", " | ")
        print(f"  OK   {what}: {out[:200]}")
    except Exception as exc:  # noqa: BLE001
        with contextlib.suppress(Exception):
            db.query("SELECT 1")
        print(f"  FAIL {what}: {scrub.scrub_exc(exc, 300)}")


for label, token in sas.items():
    for host in (ONELAKE_BLOB, ONELAKE_DFS):
        conn = f"BlobEndpoint=https://{host};SharedAccessSignature={token}"
        scrub.register(conn)
        print(f"\n== {label} SAS on {host}")
        ask(
            "azureBlobStorage read",
            f"SELECT * FROM azureBlobStorage('{conn}', '{cfg.workspace_id}', '{folder}/x.csv', 'CSVWithNames')",
        )
        t = f"t_{run}_{abs(hash((label, host))) % 10000}"
        blob = f"{folder}/{t}/"
        ask(
            "DeltaLakeAzure CREATE",
            f"CREATE TABLE {t} (id Nullable(Int64)) ENGINE = DeltaLakeAzure('{conn}', '{cfg.workspace_id}', '{blob}')",
        )
        ask("DeltaLakeAzure INSERT", f"INSERT INTO {t} VALUES (1), (2)")
        ask("DeltaLakeAzure SELECT", f"SELECT count() FROM {t}")

with contextlib.suppress(Exception):
    fs.get_directory_client(folder).delete_directory()
print("\ncleaned up")
