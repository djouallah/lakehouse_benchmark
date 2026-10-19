"""The "My workload" test: the fabric-dbt-examples dbt project, run by bench on Iceberg.

The dbt project itself is at the repo root, `workload/` (see workload/SOURCE). This package is
the Python around it: the config (`config.py`), the landing (`landing.py`) and the parity
grade (`parity.py`). The entry points are .github/scripts/workload_*.py, and the workflow is
.github/workflows/workload.yml. NOT ON THE DASHBOARD YET: docs/index.html reads nothing here.
"""
