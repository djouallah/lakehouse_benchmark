"""The "My workload" test: the fabric-dbt-examples dbt project, run by bench on Iceberg.

The dbt project itself is at the repo root, `workload/` (see workload/SOURCE). This package is
the Python around it: the config (`config.py`) and the landing (`landing.py`). The entry points
are .github/scripts/workload_*.py, and the workflow is .github/workflows/workload.yml (every 2
hours); the Real workload tab reads docs/data/workload.json.
"""
