"""Drop every table the workload legs wrote, so they all start from empty. ONE-OFF, on request.

`python workload_reset.py` -- the `reset` job of workload.yml, only when dispatched with
reset=true, never on the schedule: after a reset every leg starts from empty over the same
landing. It is not a
full refresh and nothing passes --full-refresh: the runs after it are ordinary incremental runs,
merges included. The landing zone (Files/dbt) is untouched.
"""

from __future__ import annotations

from bench import auth, scrub
from bench.workload.config import ENGINES, WorkloadConfig

LAYERS = ("landing", "mart")

if __name__ == "__main__":
    cfg = WorkloadConfig.from_env()
    catalog = auth.catalog(cfg)
    existing = {ns[0] for ns in catalog.list_namespaces()}
    for engine in ENGINES:
        for layer in LAYERS:
            namespace = f"dbt_{engine}_{layer}"
            if namespace not in existing:
                continue
            for identifier in catalog.list_tables(namespace):
                name = ".".join(identifier)
                try:
                    catalog.purge_table(identifier)
                except Exception as exc:  # noqa: BLE001 - purge needs more than drop
                    scrub.safe_print(f"  purge refused ({scrub.scrub_exc(exc, 160)}); dropping")
                    catalog.drop_table(identifier)
                scrub.safe_print(f"  dropped {name}")
