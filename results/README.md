One JSON file per benchmark run lands here. Four jobs write them:

- `tpcds/`: `.github/scripts/publish.py`. TPC-H's `*.json`, retired 2026-11-07, are in git history.
- `etl/`: `.github/scripts/etl_publish.py`.
- `capability/`: the publish job of `.github/workflows/capability.yml`.
- `workload/`: `.github/scripts/workload_publish.py`.

A run file is never edited after it is written. That is why matrix jobs and overlapping workflow
runs cannot produce a merge conflict.

The one exception is `capability/`. It keeps only each client's latest reading, so every run of
`capability.yml` overwrites its client's file.
