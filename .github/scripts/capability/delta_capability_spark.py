"""What Apache Spark can do writing Delta tables into OneLake, in SQL.

Spark is Delta's home engine: delta-spark is the reference implementation of the protocol. The
session is bench.capability.engines.pyspark_delta's (Spark 4.2 + delta-spark 4.4.1, storage on
hadoop-azure's workload-identity provider). The statements are delta_probe.DeltaProbe's, which
is Spark SQL: Sail is asked the same text.
"""

from __future__ import annotations

from delta_probe import DeltaProbe, run

from bench.capability.engines.pyspark_delta import PysparkDelta


class SparkDelta(DeltaProbe):
    CLIENT, TITLE, PREFIX = "spark", "Spark", "sp"

    def start(self) -> str:
        self.engine = PysparkDelta(self.cfg)
        self.engine.setup()
        self.spark = self.engine.session
        return self.engine.version

    def execute(self, statement: str) -> list[tuple]:
        return [tuple(r) for r in self.spark.sql(statement).collect()]

    def close(self) -> None:
        if getattr(self, "engine", None) is not None:
            self.engine.close()


if __name__ == "__main__":
    run(SparkDelta)
