"""The TPC-H query benchmark: 22 statements, one cold pass, against Iceberg tables in OneLake.

The port of the TPC-H notebook. `bench/` holds what both benchmarks share -- credentials,
OneLake access, the results format, the palette, the base Config -- and this package holds what
is TPC-H's: the data generator, the queries, the engines and their charts. bench/etl is its
sibling, the CSV-to-Iceberg load benchmark, which reuses three of these engines' setup.
"""
