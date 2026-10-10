# Running it yourself

Written for whoever replicates the benchmark, human or agent: every step is a setting or a
`gh` command, and every failure has a known signature.

You need a Microsoft Fabric workspace you own and a GitHub account. The end state is a public fork
that generates TPC-H and TPC-DS data into your lakehouse, lands the ETL CSVs there, and benchmarks
each engine against it from GitHub-hosted runners, with no secret that can leak.

## 1. Fork it, public, with Actions on

- **Public is required.** 4 vCPU / 16 GB runners are for public repos only. A private fork
  gets 2 vCPU / 7 GB, and SF=10 is OOM-killed.
- **Actions are off on a new fork.** Enable them in the fork's Actions tab.
- **The fork inherits the owner's `results/` and `docs/`.** Charts average each engine's last 3
  complete runs (`RECENT_RUNS` in [`bench/charts.py`](bench/charts.py)), so your own numbers only
  show once 3 of your runs replace the owner's.

## 2. Azure and Fabric setup

1. **Entra app.** Entra ID → App registrations → New. No client secret, no API permissions.
   Record the client ID and tenant ID.
2. **Federated credential.** App → Certificates & secrets → Federated credentials → Add. Scenario
   *GitHub Actions deploying Azure resources*, entity *Branch*, branch `main`. The subject is
   `repo:<you>/<fork-name>:ref:refs/heads/main`, and the audience stays at the default
   `api://AzureADTokenExchange`. Only runs dispatched from `main` can authenticate.
3. **Tenant settings** (needs a Fabric admin):
   - *Service principals can use Fabric APIs*: on, scoped to a group that contains the app.
   - *Users can access data stored in OneLake with apps external to Fabric*: on.
   - *Use short-lived user-delegated SAS tokens*: on. This is the default.
4. **Workspace setting.** *Authenticate with OneLake user-delegated SAS tokens*: on. It is **off by
   default**. The Gluten engine reads through Velox, which cannot use an Entra token, so it mints a
   1-hour user-delegation SAS at runtime (`onelake_sas()` in
   [`pyspark_gluten_iceberg.py`](bench/tpch/engines/pyspark_gluten_iceberg.py)). Nothing is stored.
5. **Workspace role: Contributor.** Viewer is not enough, because runs create namespaces, tables
   and snapshots:
   - `CH{sf:04d}` and `DS{sf:04d}` for TPC-H and TPC-DS
   - `Files/csv` plus `T10`/`T100`/`T1000` for the ETL
   - `_bench_probe` for the auth smoke test
   - `candidate` for the candidate engine
6. **Lakehouse with schemas.** Create it with *Lakehouse schemas* enabled. The option cannot be
   turned on later. The CH and T names match the Fabric notebooks this was ported from, so a
   notebook can share the data.

## 3. Secrets

`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `FABRIC_WORKSPACE_ID` and `FABRIC_LAKEHOUSE_ID` (both GUIDs).
There are no variables. OneLake refuses account keys, and any token minted at runtime is scrubbed
from logs. `publish` also refuses to commit anything JWT-shaped (`leak_check` in
[`bench/report.py`](bench/report.py)).

## 4. Run order

Each step fails cheaply and points at one of the settings above. All workflows run on
`ubuntu-latest`. The four benchmarks share the concurrency group `onelake`, so a second dispatch
queues and never runs alongside the first. `publish` defaults to **true** and commits results and
charts to `main`, so pass `-f publish=false` for trial runs.

```bash
gh workflow run smoke.yml -f catalog=false                     # dialect only: SF=1 local parquet, no Azure
gh workflow run auth_smoke.yml                                 # credential chain; probe 5 SKIPs on an empty lakehouse
gh workflow run bench.yml -f sf=1 -f engines=duckdb_iceberg -f publish=false   # creates CH0001
gh workflow run auth_smoke.yml                                 # again: probe 5 now reads real parquet
gh workflow run smoke.yml                                      # catalog phase needs CH0001
gh workflow run bench.yml -f sf=1 -f publish=false             # all six engines
gh workflow run bench.yml -f sf=10                             # TPC-H headline; 30/60/100 feed the totals chart
gh workflow run smoke.yml -f suite=tpcds -f catalog=false
gh workflow run tpcds.yml -f sf=1 -f publish=false             # creates DS0001
gh workflow run smoke.yml -f suite=tpcds
gh workflow run tpcds.yml -f sf=10                             # default sf is 60, the TPC-DS headline
gh workflow run etl.yml -f files=10 -f publish=false           # lands Files/csv (idempotent)
gh workflow run etl.yml -f files=1000                          # ETL headline
gh workflow run candidate_engine.yml -f candidate=starrocks    # needs CH0010 and Files/csv
```

| Workflow | Inputs (default) |
|---|---|
| `bench.yml` (tpch bench) | `sf` 1/10/30/60/100 (10), `engines` (6), `publish` (true), `override`: a wheel installed over each engine, for unreleased builds |
| `tpcds.yml` | `sf` (60), `engines` (duckdb, pyspark, pyspark_gluten), `regenerate` (false), `publish` |
| `etl.yml` | `files` 10/100/1000 (100), `engines` (7, includes daft), `publish` |
| `smoke.yml` | `suite` tpch/tpcds, `engines`, `catalog` (true), `queries` (e.g. `3,72`) |
| `candidate_engine.yml` | `candidate`, `image`, `sf` (10) |
| `capability.yml` (iceberg support) | `clients` polars/duckdb/sail/chdb/duckdb_isolation (all), `keep` (false) |

`prepare` generates a namespace once and later runs reuse it. `ci.yml` (ruff + pytest) and the
smoke test's `sql` phase run on every push. A `schedule:` block is left commented out at the top
of `bench.yml`.

## Cost

OneLake storage, reads and Fabric CU are billed to **your** workspace, not to GitHub. The data sizes
are TPC-H SF=100 ≈ 27 GiB, TPC-DS SF=60 ≈ 17.6 GiB, and ETL 1000 files ≈ 52 GB of CSV. Every
engine reads the whole working set on every run. Start at SF=1.

## Failure signatures

- **`AzureStorageFileSystem could not open file` from DuckDB, after `ATTACH` succeeded.** The azure
  extension is on its default transport, which fails the OneLake TLS handshake on Linux (on
  Windows it is the reverse). `config.azure_transport()` picks `curl` off Windows, and the workflows
  set `AZURE_TRANSPORT_OPTION_TYPE`. A bad credential says `Unauthorized` instead.
- **401/403 that looks like a role problem.** A tenant setting is missing. Probes 2 and 3 tell them apart.
- **pyiceberg dies inside `add_files` after the upload.** `adlfs` is missing: use
  `pyiceberg[pyarrow,adlfs]`. It imports lazily, so the install and table creation both succeed first.
- **`namespace CH0001 is not in the catalog` from smoke's catalog phase.** The lakehouse is fresh.
  Run `bench.yml -f sf=1` first.
- **Gluten 401s about 55 minutes into a run.** The SAS expired. The engine restarts its JVM when
  less than 15 minutes remain (`TOKEN_MIN_LIFETIME_SECONDS`). If it 401s from the first query,
  the workspace SAS setting (step 2.4) is off.
- **`NotAuthorizedException` from the Spark catalog mid-run.** The catalog cache expired and the
  fixed bearer was presented again. `CATALOG_CACHE_SECONDS` must outlive the job.

The auth smoke probes, in order:

1. Mint a storage token.
2. Fabric API: list lakehouses (service-principal setting, workspace role).
3. Iceberg REST: list namespaces (external-apps setting).
4. Create `_bench_probe.probe_<run>` and resolve its FileIO (Contributor, adlfs).
5. DuckDB reads one real parquet over `abfss://` with each transport.

Probes 4 and 5 use different HTTP stacks on purpose: 4 passed while DuckDB could not read a byte.

Credential vending is **off** on purpose (`ACCESS_DELEGATION_MODE 'none'` plus a secret). It costs
~7 s per table inside the first query that touches it, and the other engines pay nothing comparable.

## Smoke test

`smoke.yml` runs these jobs in order:

1. `plan`: generates SF=1 once and caches it.
2. `sql`: each engine runs the whole suite against local parquet, with no Azure.
3. `compare`: checks that every engine returns the same row count per query.
4. `catalog`: dispatch only. It runs the engine's real `setup()` against OneLake and two queries.

`compare` is the only correctness check in the repo. It caught chDB returning 41 rows for TPC-H Q13
where the others returned 42, because `join_use_nulls=0` fills an unmatched outer-join cell with
`0` rather than `NULL`.

Locally, with no credentials (DuckDB is a CLI on PATH, not a wheel: `bench/duckdb_cli.py`):

```bash
pip install -r requirements/smoke.txt -r requirements/duckdb_iceberg.txt
PYTHONPATH=. python .github/scripts/smoke_sql.py duckdb_iceberg   # BENCH_SUITE=tpcds for TPC-DS
```

## Adding an engine

Gate it first with `candidate_engine.yml`, which checks the three criteria in the README. Then
wire it in. Engine lists are cross-checked by `tests/test_config.py`.

- `requirements/<engine>.txt`, plus `etl_<engine>.txt` if it joins the ETL.
- A class with `name`, `version`, `setup`, `execute` and `close` (optionally `refresh`), per
  [`bench/tpch/engines/base.py`](bench/tpch/engines/base.py). It lives in `bench/tpch/engines/`,
  and also in `bench/etl/engines/` for the ETL.
- `ENGINES` in `bench/{tpch,tpcds,etl}/config.py`.
- `IDENT_STYLE` in `bench/tpch/queries.py`.
- `LIGHT`/`DARK`/`LABEL` in `bench/charts.py`.
- `ADAPTERS` in `.github/scripts/smoke_sql.py`.
- The engine lists and display names in the workflows.

Set its catalog cache from `CATALOG_CACHE_SECONDS` rather than a literal. Then run `smoke.yml`
before `bench.yml`.

## Engines tried and left out

| Engine | Why | Revisit when |
|---|---|---|
| Daft | Runs 16/22 TPC-H queries. It stops at decimal precision 38, and `l_extendedprice * (1 - l_discount)` needs more. It rejects backticks (hence `IDENT_STYLE = "quoted"`). It is in the ETL but not in `bench.yml`. | [Eventual-Inc/Daft#7532](https://github.com/Eventual-Inc/Daft/issues/7532) |
| DataFusion Comet | Its native Iceberg scan declines `abfss://` (`icebergReadableSchemes` in `CometScanRule.scala`), so OneLake scans run on the JVM anyway. It would also bypass `WorkloadIdentityTokenProvider`. | `abfss` appears in that set |

TPC-DS runs only three engines. The reason each of the others was dropped is in the comment
above `ENGINES` in [`bench/tpcds/config.py`](bench/tpcds/config.py). What broke each engine, at
what scale, and how it was fixed, is in [LEARNING.md](LEARNING.md), with one section per engine.

Back to the [README](README.md). What the runs taught is in [LEARNING.md](LEARNING.md).
