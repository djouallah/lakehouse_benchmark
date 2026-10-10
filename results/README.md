One JSON file per benchmark run lands here. Three jobs write them:

- `*.json` (TPC-H) and `tpcds/` (TPC-DS): `.github/scripts/publish.py`.
- `etl/`: `.github/scripts/etl_publish.py`.
- `capability/`: the publish job of `.github/workflows/capability.yml`.

A run file is never edited after it is written. That is why matrix jobs and overlapping workflow
runs cannot produce a merge conflict.

The one exception is `capability/`. It keeps only each client's latest reading, so every run of
`capability.yml` overwrites its client's file.
