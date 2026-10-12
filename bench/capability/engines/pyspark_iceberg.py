"""Apache Spark for the Iceberg support probes: the TPC-H session, plus what the probes need.

The same jars, storage login and catalog bearer as bench.tpch.engines.pyspark_iceberg, so the
column shows the Spark this benchmark runs. On top of it, through `_extra_config`:

- `rest-catalog-purge`, new in Iceberg 1.12 (apache/iceberg#15614, fixes #14743; in the Spark 4.1
  runtime by #17186). Without it Spark's DROP TABLE ... PURGE deletes the files itself and sends a
  plain drop, purgeRequested=false. With it, Spark leaves the files to the catalog and sends
  purgeRequested=true. The REST spec's way, so it is on.
- No table cache. pyiceberg changes the probe tables between Spark's statements (the stats files,
  spec evolution, the races); a cached table would hand Spark the metadata it read before.
- `race`, a second catalog on the race proxy (bench/capability/race.py).
- `vended`, a third catalog that asks for vended credentials and reads storage through Iceberg's
  ADLSFileIO, so the only storage credential it has is the one the catalog vends.
"""

from __future__ import annotations

from bench.tpch.engines.pyspark_iceberg import PACKAGES, PysparkIceberg, _catalog_conf

AZURE_BUNDLE = "org.apache.iceberg:iceberg-azure-bundle:1.12.0"


class PysparkCapability(PysparkIceberg):
    name = "pyspark_iceberg"

    def __init__(self, cfg):
        super().__init__(cfg)
        self._race_endpoint: str | None = None

    def setup(self, race_endpoint: str | None = None) -> None:
        self._race_endpoint = race_endpoint
        super().setup()
        self._spark.conf.set("spark.sql.session.timeZone", "UTC")

    def _extra_config(self) -> dict[str, str]:
        conf = {"spark.jars.packages": f"{PACKAGES},{AZURE_BUNDLE}"}
        catalogs = ["onelake", "vended"]
        vended = _catalog_conf("vended", self.cfg.warehouse)
        vended["spark.sql.catalog.vended.io-impl"] = "org.apache.iceberg.azure.adlsv2.ADLSFileIO"
        vended["spark.sql.catalog.vended.header.X-Iceberg-Access-Delegation"] = (
            "vended-credentials"
        )
        conf.update(vended)
        if self._race_endpoint:
            race = _catalog_conf("race", self.cfg.warehouse)
            race["spark.sql.catalog.race.uri"] = self._race_endpoint
            conf.update(race)
            catalogs.append("race")
        for name in catalogs:
            conf[f"spark.sql.catalog.{name}.cache-enabled"] = "false"
            conf[f"spark.sql.catalog.{name}.rest-catalog-purge"] = "true"
        return conf
