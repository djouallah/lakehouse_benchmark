"""The TPC-DS query benchmark: 99 statements, one cold pass, on the engines that finish.

The second query suite. It owns only what differs from TPC-H -- its config (24 tables, its
namespace, sql/tpcds.sql) and its generator (tpcgen-cli parquet, registered with pyiceberg
`add_files`). The runner, the query loader, the engines and the charts are bench/tpch's,
which read the suite off the config they are handed.
"""
