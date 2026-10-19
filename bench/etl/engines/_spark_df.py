"""The CSV read and transform in the Spark DataFrame API, shared by LakeSail and Spark.

The notebook's `sail_clean_csv` up to (not including) the write. Sail speaks the Spark API over
Spark Connect and Spark speaks it natively, so one function serves both; the two engines differ
only in how they start a session and how they create the table.

THE PAD COLUMNS are the notebook's trick for a ragged file. A DISPATCHLOAD row has 53 fields but
other record types in the same file have up to ~130, and in PERMISSIVE mode a row with MORE
tokens than the schema is malformed and comes back as all nulls -- which would drop it BEFORE the
`I='D' AND UNIT='DUNIT'` filter could keep or discard it on its merits. Declaring `COL_54..COL_130`
as extra string columns gives every token a home; they are dropped right after the filter.
"""

from __future__ import annotations

from bench.etl.schema import COLUMNS, TIMESTAMP_FORMAT_SPARK

PAD_COLS = [f"COL_{i}" for i in range(54, 131)]
BASE_SCHEMA = ",".join(f"{column} STRING" for column in COLUMNS)
USER_SCHEMA = BASE_SCHEMA + "," + ",".join(f"{column} STRING" for column in PAD_COLS)
KEEP_AS_IS = {"SETTLEMENTDATE", "DUID", "UNIT", "filename"}


def transform(session, uris: list[str], filename: bool = False):
    """The notebook's DataFrame pipeline over `uris`, ready to write. Lazy until the write runs.

    `filename` adds the source path as `filename`, from `input_file_name()` taken right at the scan.
    """
    from pyspark.sql import functions as f

    df = (
        session.read.format("csv")
        .option("header", "false")
        # Sail's option; Spark ignores an option it does not know, and its PERMISSIVE default
        # pads short rows with nulls on its own.
        .option("allowTruncatedRows", "true")
        .schema(USER_SCHEMA)
        .load(uris)
    )
    if filename:
        df = df.withColumn("filename", f.input_file_name())
    df = (
        df.filter((f.col("I") == "D") & (f.col("UNIT") == "DUNIT") & (f.col("VERSION") == "3"))
        .drop("XX", "I", *PAD_COLS)
        .withColumn(
            "SETTLEMENTDATE", f.to_timestamp(f.col("SETTLEMENTDATE"), TIMESTAMP_FORMAT_SPARK)
        )
    )
    df = df.withColumns({c: f.col(c).cast("double") for c in df.columns if c not in KEEP_AS_IS})
    return df.withColumn("DATE", f.to_date(f.col("SETTLEMENTDATE"))).withColumn(
        "YEAR", f.year(f.col("SETTLEMENTDATE"))
    )
