# Iceberg support: what each engine can do against the OneLake Iceberg REST catalog

Spark is the reference: it uses the Iceberg Java library, as Trino does under its own SQL. Polars, DuckDB, Sail, chDB, Databend and Daft have their own Iceberg code, no JVM.

`yes` works · `no` refused · `no-op` accepted but not applied · `na` the engine has no such operation · `—` not probed · `?` the probe could not ask

| Operation | Spark | Trino | DuckDB | chDB | Sail | Databend | Polars | Daft |
|---|---|---|---|---|---|---|---|---|
| Version | 4.1.3 + iceberg Apache Iceberg 1.12.0 (commit 212aa894e919ff9fdf89160da4aadcbe6858ac01) | 484 (trinodb/trino:484) | v2.0.0-alpha48730 | 4.4.0 | 0.7.2 | 8.0.90-v1.2.952-nightly-8b1dc20(rust-1.100.0-nightly-2026-10-17T09:12:10.008161229Z) (ghcr.io/djouallah/databend:main-8b1dc20714) | 2.0.0 | 0.7.26 |
| Language | Java | Java | C++ | C++ | Rust | Rust | Rust | Rust |
| Iceberg implementation | the Iceberg Java library, the reference | the Iceberg Java library, Trino's own SQL | own | ClickHouse's own | own, on DataFusion | iceberg-rust (its fork), Databend's own SQL | own parquet reader and writer, pyiceberg for the catalog and commit | own parquet reader and writer, pyiceberg for the catalog and commit |
| CREATE TABLE | yes | yes | yes | yes | yes | yes | yes | yes |
| CREATE TABLE AS SELECT | no ¹ | yes | yes | yes | yes | no ² | yes | yes |
| CREATE TABLE IF NOT EXISTS, existing table | yes | yes | yes | yes | yes | yes | na | na |
| INSERT / append | yes | yes | yes | yes | yes | yes | yes | no-op ³ |
| INSERT ... SELECT | yes | yes | yes | yes | yes | yes | na | na |
| DELETE | yes | yes | yes | yes | yes | no ⁴ | na | na |
| UPDATE | yes | yes | yes | yes | yes | no ⁵ | na | na |
| MERGE with two actions (UPDATE + INSERT) | yes | yes | yes | no ⁶ | yes | no ⁷ | na | na |
| INSERT OVERWRITE, whole table | yes | no ⁸ | no ⁹ | no ¹⁰ | yes | no-op ¹¹ | yes | — |
| TRUNCATE | yes | yes | no ¹² | no ¹³ | no ¹⁴ | no-op ¹⁵ | na | na |
| Partitioned table | yes | yes | yes | yes | yes | no ¹⁶ | yes | no-op ¹⁷ |
| Partition transform: bucket | yes | yes | yes | yes | yes | no ¹⁸ | no ¹⁹ | yes |
| Partition transform: truncate | yes | yes | yes | yes | yes | no ²⁰ | yes | yes |
| Partition transforms: year / month / day / hour | yes | yes | yes | yes | yes | no ²¹ | yes | yes |
| Types: decimal, date, timestamp, timestamptz, uuid, binary | no ²² | yes | yes | yes | no ²³ | no ²⁴ | yes | no-op ²⁵ |
| Nested types: struct, list, map | yes | yes | yes | yes | yes | no ²⁶ | yes | no-op ²⁷ |
| Add column | yes | yes | yes | yes | no ²⁸ | no-op ²⁹ | yes | na |
| Drop column | yes | yes | yes | yes | no ³⁰ | no-op ³¹ | na | na |
| Rename column | yes | yes | yes | yes | no ³² | no ³³ | na | na |
| Type promotion (int → long) | yes | yes | yes | yes | no ³⁴ | no ³⁵ | yes | na |
| Partition evolution | yes | yes | yes | no ³⁶ | no ³⁷ | no ³⁸ | na | na |
| Write after partition evolution | yes | yes | yes | yes | yes | no ³⁹ | yes | ? ⁴⁰ |
| Set table property | yes | yes | yes | no ⁴¹ | no ⁴² | no ⁴³ | na | na |
| Sort order evolution | yes | yes | yes | no ⁴⁴ | no ⁴⁵ | no ⁴⁶ | na | na |
| Time travel | yes | yes | yes | yes | yes | no-op ⁴⁷ | yes | — |
| Metadata tables | yes | yes | yes | yes | no ⁴⁸ | yes | na | na |
| File pruning from min/max in the metadata | yes | yes | yes | yes | yes | yes | yes | ? ⁴⁹ |
| MAX from the metadata, no data file read | yes | no ⁵⁰ | no ⁵¹ | no ⁵² | yes | no ⁵³ | no ⁵⁴ | ? ⁵⁵ |
| Compaction | yes | yes | yes | no ⁵⁶ | no ⁵⁷ | no ⁵⁸ | na | na |
| Rewrite manifests | yes | yes | no ⁵⁹ | no ⁶⁰ | no ⁶¹ | no ⁶² | na | na |
| Create branch | yes | no ⁶³ | no ⁶⁴ | no ⁶⁵ | no ⁶⁶ | no ⁶⁷ | na | na |
| Create tag | yes | no ⁶⁸ | no ⁶⁹ | no ⁷⁰ | no ⁷¹ | no ⁷² | na | na |
| Drop table with purge | yes | yes | yes | yes | yes | yes | na | na |
| Create / drop namespace | yes | yes | yes | no ⁷³ | yes | yes | na | na |
| CREATE SCHEMA IF NOT EXISTS, existing schema | yes | yes | yes | no ⁷⁴ | yes | yes | na | na |
| Concurrent append: both kept | yes | yes | yes | yes | yes | yes | yes | — |
| Concurrent writer: DELETE loses nothing | yes | yes | yes | yes | yes | no ⁷⁵ | na | na |
| Concurrent writer: UPDATE loses nothing | yes | yes | yes | yes | yes | no ⁷⁶ | na | na |

## Notes

1. Spark, CREATE TABLE AS SELECT: the OneLake catalog does not support staged create, which Spark's CTAS uses.
2. Databend, CREATE TABLE AS SELECT: CREATE TABLE ... AS SELECT: `OperationalError: (1105, 'TokioError. Code: 1068, Text = Cannot join handle from context\'s runtime, cause: task 648 panicked with message "internal error: table_id_seq must have been set. CTAS(replace) of table".')`
3. Daft, INSERT / append: write_iceberg(mode='append'): `write_iceberg(mode='append'), a append snapshot returned success; expected [(1, 10), (2, 20), (3, 30)], pyiceberg reads pyiceberg could not read it: ValueError: No registered filesystem for scheme: az, Daft reads [(1, 10), (2, 20), (3, 30)]`
4. Databend, DELETE: DELETE FROM: `OperationalError: (1105, 'Unimplemented. Code: 1002, Text = table db_39032752740_1_delete, engine type iceberg, does not support DELETE.')`
5. Databend, UPDATE: UPDATE: `OperationalError: (1105, 'Unimplemented. Code: 1002, Text = table db_39032752740_1_update, engine type iceberg, does not support UPDATE.')`
6. chDB, MERGE with two actions (UPDATE + INSERT): MERGE INTO: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 12 (onelake): onelake.`_bench_capability.ch_39179243597_1_merge` t USING (SELECT * FROM values('id Int64, v Int64', (1, 777), (9, 90))) s ON t.id = s.id WHEN MATCHED THEN UPD... Expected end of query. (SYNTAX_ERROR)`
7. Databend, MERGE with two actions (UPDATE + INSERT): MERGE, two actions: MATCHED UPDATE + NOT MATCHED INSERT: `OperationalError: (1105, 'Unimplemented. Code: 1002, Text = Table db_39032752740_1_merge does not support MERGE.')`
8. Trino, INSERT OVERWRITE, whole table: CREATE OR REPLACE TABLE ... AS (whole-table overwrite): `io.trino.spi.TrinoException: Failed to commit the transaction during create table: Commit failed: One or more requirements failed. The client may retry. <- org.apache.iceberg.exceptions.CommitFailedException: Commit failed: One or more requirements failed. The client may retry.`
9. DuckDB, INSERT OVERWRITE, whole table: INSERT OVERWRITE, whole table: `Parser Error: syntax error at or near "OVERWRITE" LINE 1: INSERT OVERWRITE onelake."_bench_capability"."dk_38991566476_1_ove... ^^^^^^^^^`
10. chDB, INSERT OVERWRITE, whole table: INSERT OVERWRITE TABLE: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 18 (TABLE): TABLE onelake.`_bench_capability.ch_39179243597_1_overwrite` SELECT * FROM values('id Int64, v Int64', (7, 70), (8, 80)). Expected one of: token, Comma, FROM, PREWHERE, WHERE, GROUP BY, WITH, HAVING, WINDOW, QUALIFY, O…`
11. Databend, INSERT OVERWRITE, whole table: INSERT OVERWRITE (whole-table overwrite): `INSERT OVERWRITE returned success; expected [(7, 70), (8, 80)], pyiceberg reads [(1, 10), (2, 20), (3, 30), (7, 70), (8, 80)], Databend reads [(1, 10), (2, 20), (3, 30), (7, 70), (8, 80)]`
12. DuckDB, TRUNCATE: Runs, but not as a metadata operation. The data files stay, and a delete file is written for them: slower than dropping the files, and every reader then has to apply the deletes.
13. chDB, TRUNCATE: TRUNCATE TABLE: `ChdbError: Code: 48. DB::Exception: Truncate is not supported for data lake engine. (NOT_IMPLEMENTED)`
14. Sail, TRUNCATE: TRUNCATE TABLE: `IllegalArgumentException: invalid argument: found TRUNCATE at 0:8 expected something else, ';', statement, or end of input`
15. Databend, TRUNCATE: TRUNCATE TABLE: `TRUNCATE returned success; expected [], pyiceberg reads [(1, 10), (2, 20), (3, 30)], Databend reads [(1, 10), (2, 20), (3, 30)]`
16. Databend, Partitioned table: PARTITION BY (p), then INSERT: `OperationalError: (1105, "Internal. Code: 1001, Text = Failed to commit transaction: DataInvalid => Found duplicate 'field.id' 0. Field ids must be unique.\n.")`
17. Daft, Partitioned table: write an identity-partitioned table: `identity(id), 2 partitions returned success; expected [(1, 10), (2, 20)], pyiceberg reads pyiceberg could not read it: ValueError: No registered filesystem for scheme: az, Daft reads [(1, 10), (2, 20)]`
18. Databend, Partition transform: bucket: PARTITION BY (bucket(4, id)), then INSERT: `works: none; refused: bucket: OperationalError: (1105, 'BadArguments. Code: 1006, Text = Iceberg PARTITION BY only supports column identifiers, got `bucket(4, id)`.')`
19. Polars, Partition transform: bucket: write a table partitioned by bucket(4, id): `works: none; refused: bucket: NotImplementedError: NotImplementedError: sink to Iceberg table with 'bucket[4]' partition transform`
20. Databend, Partition transform: truncate: PARTITION BY (truncate(2, x)), then INSERT: `works: none; refused: truncate: OperationalError: (1105, 'BadArguments. Code: 1006, Text = Iceberg PARTITION BY only supports column identifiers, got `truncate(2, x)`.')`
21. Databend, Partition transforms: year / month / day / hour: PARTITION BY year / month / day / hour, then INSERT: `works: none; refused: year: OperationalError: (1105, 'BadArguments. Code: 1006, Text = Iceberg PARTITION BY only supports column identifiers, got `year(x)`.'); month: OperationalError: (1105, 'BadArguments. Code: 1006, Text = Iceberg PARTITION BY only supports column identifiers, got `month(x)`.');…`
22. Spark, Types: decimal, date, timestamp, timestamptz, uuid, binary: types: decimal, date, timestamp, timestamptz, uuid, binary: `works: ['decimal', 'date', 'timestamp', 'timestamptz', 'binary']; refused: uuid: ParseException: [UNSUPPORTED_DATATYPE] Unsupported data type "UUID". SQLSTATE: 0A000 == SQL (line 1, position 85) == ...3754_1_type_uuid` (id BIGINT, x UUID) USING iceberg ^^^^`
23. Sail, Types: decimal, date, timestamp, timestamptz, uuid, binary: types: decimal, date, timestamp, timestamptz, uuid, binary: `works: ['decimal', 'date', 'timestamp', 'timestamptz', 'binary']; refused: uuid: IllegalArgumentException: invalid argument: found UUID at 84:88 expected data type`
24. Databend, Types: decimal, date, timestamp, timestamptz, uuid, binary: types: decimal, date, timestamp, timestamptz, uuid, binary: `works: ['date', 'timestamp', 'binary']; refused: decimal: OperationalError: (1105, 'Internal. Code: 1001, Text = Iceberg create table _bench_capability.db_39032752740_1_type_decimal failed: DataInvalid => Unsupported Arrow data type: Decimal64(10, 2)\n.'); timestamptz: OperationalError: (1105, "Bad…`
25. Daft, Types: decimal, date, timestamp, timestamptz, uuid, binary: types: decimal, date, timestamp, timestamptz, uuid, binary: `works: none; not applied: decimal: written; pyiceberg cannot read it: ValueError: No registered filesystem for scheme: az; date: written; pyiceberg cannot read it: ValueError: No registered filesystem for scheme: az; timestamp: written; pyiceberg cannot read it: ValueError: No registered filesystem…`
26. Databend, Nested types: struct, list, map: nested types: struct, list, map: `works: none; refused: struct: OperationalError: (1105, 'Internal. Code: 1001, Text = Iceberg create table _bench_capability.db_39032752740_1_type_struct failed: DataInvalid => Field id not found in metadata\n.'); list: OperationalError: (1105, 'Internal. Code: 1001, Text = Iceberg create table _ben…`
27. Daft, Nested types: struct, list, map: nested types: struct, list, map: `works: none; not applied: struct: written; pyiceberg cannot read it: ValueError: No registered filesystem for scheme: az; list: written; pyiceberg cannot read it: ValueError: No registered filesystem for scheme: az; map: written; pyiceberg cannot read it: ValueError: No registered filesystem for sc…`
28. Sail, Add column: ALTER TABLE ADD COLUMN: `UnsupportedOperationException: unsupported ALTER TABLE operation`
29. Databend, Add column: ALTER TABLE ADD COLUMN: `returned success and the schema is ['id', 'v']`
30. Sail, Drop column: ALTER TABLE DROP COLUMN: `UnsupportedOperationException: unsupported ALTER TABLE operation`
31. Databend, Drop column: ALTER TABLE DROP COLUMN: `returned success and the schema is still ['id', 'v']`
32. Sail, Rename column: ALTER TABLE RENAME COLUMN: `UnsupportedOperationException: unsupported ALTER TABLE operation`
33. Databend, Rename column: ALTER TABLE RENAME COLUMN: `OperationalError: (1105, "TableEngineNotSupported. Code: 1302, Text = _bench_capability.db_39032752740_1_renamecol engine is iceberg that doesn't support rename column name.")`
34. Sail, Type promotion (int → long): ALTER COLUMN c TYPE BIGINT (int -> long): `AnalysisException: external error: This feature is not implemented: ALTER TABLE is not yet supported for catalog-managed Iceberg tables: onelake._bench_capability.sl_38253752194_1_promote`
35. Databend, Type promotion (int → long): ALTER TABLE MODIFY COLUMN c BIGINT (int -> long): `OperationalError: (1105, 'Internal. Code: 1001, Text = [FUSE-TABLE] Expected FUSE engine table, but got iceberg.')`
36. chDB, Partition evolution: ALTER TABLE ADD PARTITION FIELD (partition evolution): `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 70 (PARTITION): PARTITION FIELD bucket(4, id). Expected one of: COLUMN, INDEX, STATISTICS, PROJECTION, CONSTRAINT, end of query. (SYNTAX_ERROR)`
37. Sail, Partition evolution: ALTER TABLE ADD PARTITION FIELD (partition evolution): `IllegalArgumentException: invalid argument: found FIELD at 81:86 expected '('`
38. Databend, Partition evolution: ALTER TABLE PARTITION BY (partition evolution): `OperationalError: (1105, 'UnsupportedEngineParams. Code: 2703, Text = ALTER TABLE PARTITION BY is not supported for engine ICEBERG.')`
39. Databend, Write after partition evolution: INSERT, pyiceberg adds bucket(4, id), INSERT: `OperationalError: (1105, 'UnwindError. Code: 1104, Text = internal error: entered unreachable code.')`
40. Daft, Write after partition evolution: write before and after partition evolution: `ValueError: No registered filesystem for scheme: az`
41. chDB, Set table property: ALTER TABLE SET TBLPROPERTIES: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 64 (SET): SET TBLPROPERTIES ('probed-at' = '39179243597_1'). Expected one of: ON, a list of ALTER commands, ALTER command, ADD COLUMN, RENAME COLUMN, MATERIALIZE COLUMN, DROP PARTITION, DROP PART, FORGET PARTITION, DROP DETACHED P…`
42. Sail, Set table property: ALTER TABLE SET TBLPROPERTIES: `AnalysisException: external error: This feature is not implemented: ALTER TABLE is not yet supported for catalog-managed Iceberg tables: onelake._bench_capability.sl_38253752194_1_props`
43. Databend, Set table property: ALTER TABLE SET OPTIONS: `OperationalError: (1105, 'TableOptionInvalid. Code: 1301, Text = table option probed_at is invalid for alter table statement.')`
44. chDB, Sort order evolution: ALTER TABLE MODIFY ORDER BY (sort order evolution): `ChdbError: Code: 48. DB::Exception: Alter of type 'MODIFY_ORDER_BY' is not supported by Iceberg storage. (NOT_IMPLEMENTED)`
45. Sail, Sort order evolution: ALTER TABLE ... WRITE ORDERED BY (sort order): `IllegalArgumentException: invalid argument: found WRITE at 66:71 expected '.', 'RENAME', 'PARTITION', 'ADD', 'DROP', 'ALTER', 'CHANGE', 'REPLACE', 'SET', 'UNSET', or 'RECOVER'`
46. Databend, Sort order evolution: ALTER TABLE CLUSTER BY (sort order): `OperationalError: (1105, 'Internal. Code: 1001, Text = [FUSE-TABLE] Expected FUSE engine table, but got iceberg.')`
47. Databend, Time travel: time travel, AT (SNAPSHOT => ...): `AT (SNAPSHOT => the seed's) reads 4 rows, not 3`
48. Sail, Metadata tables: metadata tables (t.snapshots): `IllegalArgumentException: invalid argument: table reference: [Identifier("onelake"), Identifier("_bench_capability"), Identifier("sl_38253752194_1_inspect"), Identifier("snapshots")]`
49. Daft, File pruning from min/max in the metadata: where(id == 22) with the other data files deleted (file pruning): `ValueError: No registered filesystem for scheme: az`
50. Trino, MAX from the metadata, no data file read: max(id) with every data file deleted (min/max from metadata): `SELECT max(id) fails with 4 of 4 data files deleted, so it opens them: io.trino.spi.TrinoException: Error opening Iceberg split abfss://1c52481c-0523-4a5a-bbde-fdc932bd77c2@onelake.dfs.fabric.microsoft.com/ac303243-4441-4885-9e7d-f4f5e7af194c/Tables/_bench_capability/tr_38909350278_1_max/data/00000…`
51. DuckDB, MAX from the metadata, no data file read: max(id) with every data file deleted (min/max from metadata): `SELECT max(id) fails with 4 of 4 data files deleted, so it opens them: IO Error: AzureBlobStorageFileSystem Read to 'abfss://1c52481c-0523-4a5a-bbde-fdc932bd77c2@onelake.dfs.fabric.microsoft.com/ac303243-4441-4885-9e7d-f4f5e7af194c/Tables/_bench_capability/dk_38891149513_1_max/data/00000-0-8ad340ad…`
52. chDB, MAX from the metadata, no data file read: max(id) with every data file deleted (min/max from metadata): `SELECT max(id) fails with 4 of 4 data files deleted, so it opens them: ChdbError: Code: 1001. DB::Exception: Azure::Storage::StorageException: 404 The specified blob does not exist. Request ID: 5d42841c-e01e-001f-4d5e-5f6564000000. (STD_EXCEPTION)`
53. Databend, MAX from the metadata, no data file read: max(id) with every data file deleted (min/max from metadata): `SELECT max(id) fails with 4 of 4 data files deleted, so it opens them: OperationalError: (1105, 'StorageNotFound. Code: 3001, Text = NotFound (permanent) at read, context: { uri: https://onelake.dfs.fabric.microsoft.com/1c52481c-0523-4a5a-bbde-fdc932bd77c2/ac303243-4441-4885-9e7d-f4f5e7af194c/Table…`
54. Polars, MAX from the metadata, no data file read: id.max() with every data file deleted (min/max from metadata): `select(id.max()) fails with 4 of 4 data files deleted, so it opens them: FileNotFoundError: object-store error: Object at location ac303243-4441-4885-9e7d-f4f5e7af194c/Tables/_bench_capability/pl_38022167812_1_max/data/00000-0-ad655b41-7953-4d9e-9182-3e0137e09cd3.parquet not found: Error performing…`
55. Daft, MAX from the metadata, no data file read: id.max() with every data file deleted (min/max from metadata): `ValueError: No registered filesystem for scheme: az`
56. chDB, Compaction: OPTIMIZE TABLE (compaction): `ChdbError: Code: 48. DB::Exception: OPTIMIZE is not supported for catalog-backed Iceberg tables in this build. (NOT_IMPLEMENTED)`
57. Sail, Compaction: CALL system.rewrite_data_files (compaction): `IllegalArgumentException: invalid argument: found CALL at 0:4 expected something else, ';', statement, or end of input`
58. Databend, Compaction: OPTIMIZE TABLE COMPACT (compaction): `OperationalError: (1105, "Unimplemented. Code: 1002, Text = The 'compact_blocks' operation is not supported for the table 'db_39032752740_1_compact'. Table engine: 'iceberg'..")`
59. DuckDB, Rewrite manifests: iceberg_rewrite_manifests: `Catalog Error: Table Function with name iceberg_rewrite_manifests does not exist! Did you mean "iceberg_rewrite_data_files"?`
60. chDB, Rewrite manifests: rewrite manifests (no statement): `ClickHouse has no statement to rewrite an Iceberg table's manifests`
61. Sail, Rewrite manifests: CALL system.rewrite_manifests: `IllegalArgumentException: invalid argument: found CALL at 0:4 expected something else, ';', statement, or end of input`
62. Databend, Rewrite manifests: OPTIMIZE TABLE COMPACT SEGMENT (rewrite manifests): `OperationalError: (1105, "Unimplemented. Code: 1002, Text = The operation 'compact_segments' is not supported for the table 'db_39032752740_1_manifests', which is using the 'iceberg' engine..")`
63. Trino, Create branch: CREATE BRANCH ... IN TABLE: `io.trino.spi.TrinoException: This connector does not support creating branches`
64. DuckDB, Create branch: ALTER TABLE ... CREATE BRANCH: `Parser Error: syntax error at or near "CREATE" LINE 1: ... TABLE onelake."_bench_capability"."dk_38891149513_1_branch" CREATE BRANCH probe_branch ^^^^^^`
65. chDB, Create branch: ALTER TABLE ... CREATE BRANCH: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 65 (CREATE): CREATE BRANCH probe_branch. Expected one of: ON, a list of ALTER commands, ALTER command, ADD COLUMN, RENAME COLUMN, MATERIALIZE COLUMN, DROP PARTITION, DROP PART, FORGET PARTITION, DROP DETACHED PARTITION, DROP DETAC…`
66. Sail, Create branch: ALTER TABLE ... CREATE BRANCH: `IllegalArgumentException: invalid argument: found CREATE at 66:72 expected '.', 'RENAME', 'PARTITION', 'ADD', 'DROP', 'ALTER', 'CHANGE', 'REPLACE', 'SET', 'UNSET', or 'RECOVER'`
67. Databend, Create branch: ALTER TABLE CREATE BRANCH: `OperationalError: (1105, 'Unimplemented. Code: 1002, Text = Legacy experimental table refs were removed: ALTER TABLE ... CREATE/DROP BRANCH. The remaining syntax is reserved for a future redesign and is intentionally rejected during binding..')`
68. Trino, Create tag: CREATE TAG ... IN TABLE: `io.trino.sql.parser.ParsingException: line 1:8: mismatched input 'TAG'. Expecting: 'BRANCH', 'CATALOG', 'FUNCTION', 'MATERIALIZED', 'OR', 'ROLE', 'SCHEMA', 'TABLE', 'VIEW' <- org.antlr.v4.runtime.NoViableAltException: None`
69. DuckDB, Create tag: ALTER TABLE ... CREATE TAG: `Parser Error: syntax error at or near "CREATE" LINE 1: ... TABLE onelake."_bench_capability"."dk_38891149513_1_tag" CREATE TAG probe_tag ^^^^^^`
70. chDB, Create tag: ALTER TABLE ... CREATE TAG: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 62 (CREATE): CREATE TAG probe_tag. Expected one of: ON, a list of ALTER commands, ALTER command, ADD COLUMN, RENAME COLUMN, MATERIALIZE COLUMN, DROP PARTITION, DROP PART, FORGET PARTITION, DROP DETACHED PARTITION, DROP DETACHED PA…`
71. Sail, Create tag: ALTER TABLE ... CREATE TAG: `IllegalArgumentException: invalid argument: found CREATE at 63:69 expected '.', 'RENAME', 'PARTITION', 'ADD', 'DROP', 'ALTER', 'CHANGE', 'REPLACE', 'SET', 'UNSET', or 'RECOVER'`
72. Databend, Create tag: ALTER TABLE CREATE TAG: `OperationalError: (1105, 'Unimplemented. Code: 1002, Text = Table ref is an experimental feature, `set enable_experimental_table_ref=1` to use this feature.')`
73. chDB, Create / drop namespace: CREATE TABLE in a new namespace (no namespace statement): `created through CREATE TABLE; ClickHouse has no statement to drop one`
74. chDB, CREATE SCHEMA IF NOT EXISTS, existing schema: namespace IF NOT EXISTS (no namespace statement): `ClickHouse has no statement for a catalog namespace`
75. Databend, Concurrent writer: DELETE loses nothing: DELETE id 1; B appends: `error: final [(1, 10), (2, 20), (3, 30)]; commits [none]; raised Refused: Refused: OperationalError: (1105, 'Unimplemented. Code: 1002, Text = table db_39032752740_1_race_delete, engine type iceberg, does not support DELETE.')`
76. Databend, Concurrent writer: UPDATE loses nothing: UPDATE v = v + 1 on id 2; B appends: `error: final [(1, 10), (2, 20), (3, 30)]; commits [none]; raised Refused: Refused: OperationalError: (1105, 'Unimplemented. Code: 1002, Text = table db_39032752740_1_race_update, engine type iceberg, does not support UPDATE.')`

## DuckDB: isolation levels and transactions

DuckDB is the only one with transactions. Writer B (pyiceberg) commits between DuckDB's
read and DuckDB's commit; the race is injected at the REST commit through a local proxy
(`bench/capability/race.py`), so it is deterministic. Every DuckDB connection runs
`SET iceberg_use_metadata_log = false`
([duckdb-iceberg#1475](https://github.com/duckdb/duckdb-iceberg/issues/1475)).

`refused` DuckDB's commit fails and B's change stands · `retried` both changes kept ·
`lost` B's change is gone · `broken` the race could not be run

| DuckDB writes, B commits in between | serializable | snapshot | no retries |
|---|---|---|---|
| INSERT, B appends | retried | retried | refused |
| DELETE a row, B appends | refused | retried | refused |
| UPDATE another row, B appends | refused | refused | refused |
| MERGE on another row, B appends | refused | refused | refused |
| DELETE a row, B deletes another row | refused | refused | refused |

Columns are table properties: `serializable` nothing set; `snapshot` `write.delete.isolation-level = snapshot`, `write.update.isolation-level = snapshot`, `write.merge.isolation-level = snapshot`; `no retries` `commit.retry.num-retries = 0`.

| Transaction (`BEGIN ... COMMIT`), B commits in the middle | Outcome | What came back |
|---|---|---|
| BEGIN; read; B appends; read again | repeatable | `reads 3, B appends, reads 3; COMMIT ok` |
| B commits after BEGIN, before the first read | at first read | `BEGIN, B appends, the first read sees 4 rows; B appends again, the next read sees 4` |
| INSERT + DELETE, then ROLLBACK | nothing sent | `INSERT + DELETE, ROLLBACK; commits [none]; [(1, 10), (2, 20), (3, 30)]` |
| a failing statement inside the transaction | all rolled back | `INSERT ok, then INSERT failed (RuntimeError: RuntimeError: Conversion Error: Could not convert string 'not a number' to …); COMMIT ok; [(1, 10), (2, 20), (3, 30)]` |
| own uncommitted rows, inside and from another connection | yes | `uncommitted INSERT: this transaction reads 4, another connection 3; COMMIT ok; catalog 4 rows` |
| two DuckDB transactions update the same row | second refused | `both read v = 10, +1 and +100; first COMMIT ok; second COMMIT RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.…` |

| Statements in one `BEGIN ... COMMIT`, no concurrent writer | Outcome | What came back |
|---|---|---|
| DROP TABLE, CREATE TABLE the same name with a new column, INSERT | refused | `CREATE refused: RuntimeError: RuntimeError: Not implemented Error: Cannot create table deleted within a transaction: onelake._bench_capability.iso_dk_38892463084_1_cb_drop_create; t (('id', 'v'), [(1…` |
| CREATE OR REPLACE TABLE t AS SELECT ... FROM t | refused | `CREATE refused: RuntimeError: RuntimeError: Not implemented Error: CREATE OR REPLACE not supported in DuckDB-Iceberg. Please use separate Drop and Create Statements; t (('id', 'v'), [(1, 10), (2, 20)…` |
| CREATE TABLE, INSERT | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]); n (('id', 'v'), [(1, 1)]); sent ['POST tables 200', 'POST n 200']; n's commit updates add-snapshot, set-snapshot-ref` |
| CREATE TABLE AS SELECT from the seeded table | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]); n (('id', 'v'), [(1, 20), (2, 40), (3, 60)]); sent ['POST tables 200', 'POST n 200']; n's commit updates add-snapshot, set-snapshot-ref` |
| INSERT, INSERT | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]); n None; sent ['POST t 200']; t's commit updates add-snapshot, add-snapshot, set-snapshot-ref` |
| INSERT, DELETE | works | `t (('id', 'v'), [(2, 20), (3, 30), (4, 40)]); n None; sent ['POST t 200']; t's commit updates add-snapshot, add-snapshot, set-snapshot-ref` |
| MERGE, WHEN MATCHED UPDATE + WHEN NOT MATCHED INSERT | works | `t (('id', 'v'), [(1, 777), (2, 20), (3, 30), (9, 90)]); n None; sent ['POST t 200']; t's commit updates add-snapshot, set-snapshot-ref` |
| ADD COLUMN, INSERT a row that fills it | works | `t (('id', 'v', 'w'), [(1, 10, None), (2, 20, None), (3, 30, None), (4, 40, 400)]); n None; sent ['POST t 200']; t's commit updates add-schema, set-current-schema, add-snapshot, set-snapshot-ref` |
| ADD COLUMN, UPDATE it | works | `t (('id', 'v', 'w'), [(1, 10, 100), (2, 20, 200), (3, 30, 300)]); n None; sent ['POST t 200']; t's commit updates add-schema, set-current-schema, add-snapshot, set-snapshot-ref` |
| RENAME COLUMN, INSERT | works | `t (('id', 'v2'), [(1, 10), (2, 20), (3, 30), (4, 40)]); n None; sent ['POST t 200']; t's commit updates add-schema, set-current-schema, add-snapshot, set-snapshot-ref` |
| DROP COLUMN, INSERT | works | `t (('id',), [(1,), (2,), (3,), (4,)]); n None; sent ['POST t 200']; t's commit updates add-schema, set-current-schema, add-snapshot, set-snapshot-ref` |
| SET PARTITIONED BY (bucket(4, id)), INSERT | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30), (4, 40)]); n None; sent ['POST t 200']; t's commit updates add-spec, set-default-spec, add-snapshot, set-snapshot-ref; spec ['bucket[4]']` |
| TRUNCATE, INSERT, ROLLBACK | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]); n None; sent []` |
| DROP TABLE, ROLLBACK | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]); n None; sent []` |
| DROP TABLE, CREATE TABLE the same name, ROLLBACK | refused | `CREATE refused: RuntimeError: RuntimeError: Not implemented Error: Cannot create table deleted within a transaction: onelake._bench_capability.iso_dk_38892463084_1_cb_drop_create_rollback; t (('id', …` |

## Where these readings come from

The OneLake Iceberg REST catalog, read by CI (`.github/workflows/capability.yml`), which writes this file. Every cell is a reading taken by sending the request, not a property of the product: re-run rather than trust it.

- spark: 4.1.3 + iceberg Apache Iceberg 1.12.0 (commit 212aa894e919ff9fdf89160da4aadcbe6858ac01), [run 38991566476](https://github.com/djouallah/lakehouse_benchmark/actions/runs/38991566476), 2026-10-17
- trino: 484 (trinodb/trino:484), [run 38991566476](https://github.com/djouallah/lakehouse_benchmark/actions/runs/38991566476), 2026-10-17
- duckdb: v2.0.0-alpha48730, [run 38991566476](https://github.com/djouallah/lakehouse_benchmark/actions/runs/38991566476), 2026-10-17
- chdb: 4.4.0, [run 39179243597](https://github.com/djouallah/lakehouse_benchmark/actions/runs/39179243597), 2026-10-19
- sail: 0.7.2, [run 38991566476](https://github.com/djouallah/lakehouse_benchmark/actions/runs/38991566476), 2026-10-17
- databend: 8.0.90-v1.2.952-nightly-8b1dc20(rust-1.100.0-nightly-2026-10-17T09:12:10.008161229Z) (ghcr.io/djouallah/databend:main-8b1dc20714), [run 39032752740](https://github.com/djouallah/lakehouse_benchmark/actions/runs/39032752740), 2026-10-17
- polars: 2.0.0, [run 38991566476](https://github.com/djouallah/lakehouse_benchmark/actions/runs/38991566476), 2026-10-17
- daft: 0.7.26, [run 39230608955](https://github.com/djouallah/lakehouse_benchmark/actions/runs/39230608955), 2026-10-19
- duckdb_isolation: v2.0.0-alpha48520, [run 38892463084](https://github.com/djouallah/lakehouse_benchmark/actions/runs/38892463084), 2026-10-16
