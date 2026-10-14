# Iceberg support: what each engine can do against the OneLake Iceberg REST catalog

Spark is the reference: it uses the Iceberg Java library. Polars, DuckDB, Sail and chDB have their own Iceberg code, no JVM.

`yes` works · `no` refused · `no-op` accepted but not applied · `na` the engine has no such operation · `—` not probed · `?` the probe could not ask

| Operation | Spark | DuckDB | Sail | chDB | Polars |
|---|---|---|---|---|---|
| Version | 4.1.3 + iceberg Apache Iceberg 1.12.0 (commit 212aa894e919ff9fdf89160da4aadcbe6858ac01) | v2.0.0-alpha46057 | 0.7.2 | 4.4.0 | 2.0.0 |
| Language | Java | C++ | Rust | C++ | Rust |
| Iceberg implementation | the Iceberg Java library, the reference | own | own, on DataFusion | ClickHouse's own | own parquet reader and writer, pyiceberg for the catalog and commit |
| CREATE TABLE | yes | yes | yes | no ¹ | — |
| CREATE TABLE AS SELECT | no ² | yes | yes | no ³ | — |
| CREATE TABLE IF NOT EXISTS, existing table | yes | yes | yes | yes | na |
| INSERT / append | yes | yes | yes | yes | yes |
| INSERT ... SELECT | yes | yes | yes | yes | na |
| DELETE | yes | yes | yes | yes | na |
| UPDATE | yes | yes | yes | yes | na |
| MERGE with one action | yes | yes | yes | no ⁴ | na |
| MERGE with two actions (UPDATE + INSERT) | yes | no ⁵ | yes | no ⁶ | na |
| INSERT OVERWRITE, whole table | yes | no ⁷ | yes | no ⁸ | no ⁹ |
| TRUNCATE | yes | no ¹⁰ | no ¹¹ | no ¹² | na |
| Partitioned table | yes | yes | yes | yes | yes |
| Partition transform: bucket | yes | yes | yes | yes | no ¹³ |
| Partition transform: truncate | yes | yes | yes | yes | yes |
| Partition transforms: year / month / day / hour | yes | yes | yes | yes | yes |
| Types: decimal, date, timestamp, timestamptz, uuid, binary | no ¹⁴ | yes | no ¹⁵ | yes | yes |
| Nested types: struct, list, map | yes | yes | yes | yes | yes |
| Add column | yes | yes | no ¹⁶ | yes | yes |
| Drop column | yes | yes | no ¹⁷ | yes | na |
| Rename column | yes | yes | no ¹⁸ | yes | na |
| Type promotion (int → long) | yes | yes | no ¹⁹ | yes | yes |
| Partition evolution | yes | yes | no ²⁰ | no ²¹ | na |
| Write after partition evolution | yes | yes | yes | yes | yes |
| Set table property | yes | yes | no ²² | no ²³ | na |
| Sort order evolution | yes | yes | no ²⁴ | no ²⁵ | na |
| Time travel | yes | yes | yes | yes | yes |
| Metadata tables | yes | yes | no ²⁶ | yes | na |
| File pruning from min/max in the metadata | yes | yes | yes | yes | yes |
| MAX from the metadata, no data file read | yes | no ²⁷ | yes | no ²⁸ | no ²⁹ |
| Compaction | yes | yes | no ³⁰ | no-op ³¹ | na |
| Rewrite manifests | yes | no ³² | no ³³ | no ³⁴ | na |
| Create branch | yes | no ³⁵ | no ³⁶ | no ³⁷ | na |
| Create tag | yes | no ³⁸ | no ³⁹ | no ⁴⁰ | na |
| Drop table with purge | yes | yes | yes | no ⁴¹ | na |
| Create / drop namespace | yes | yes | yes | no ⁴² | na |
| CREATE SCHEMA IF NOT EXISTS, existing schema | yes | yes | yes | no ⁴³ | na |
| Concurrent append: both kept | yes | yes | yes | yes | yes |
| Concurrent writer: DELETE loses nothing | yes | yes | yes | yes | na |
| Concurrent writer: UPDATE loses nothing | yes | yes | yes | yes | na |

## Notes

1. chDB, CREATE TABLE: CREATE TABLE: `ChdbError: Code: 79. DB::Exception: MergeTree storages require data path. (INCORRECT_FILE_NAME)`
2. Spark, CREATE TABLE AS SELECT: the OneLake catalog does not support staged create, which Spark's CTAS uses.
3. chDB, CREATE TABLE AS SELECT: CREATE TABLE ... AS SELECT: `ChdbError: Code: 79. DB::Exception: MergeTree storages require data path. (INCORRECT_FILE_NAME)`
4. chDB, MERGE with one action: MERGE INTO: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 12 (onelake): onelake.`_bench_capability.ch_38254816796_1_merge` t USING (SELECT * FROM values('id Int64, v Int64', (1, 777), (9, 90))) s ON t.id = s.id WHEN MATCHED THEN UPD... Expected end of query. (SYNTAX_ERROR)`
5. DuckDB, MERGE with two actions (UPDATE + INSERT): MERGE, two actions: MATCHED UPDATE + NOT MATCHED INSERT: `TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'https://onelake.table.fabric.microsoft.com/iceberg/v1' returned a non-200 status code (BadRequest_400). message: Malformed request type: BadRequestException reason: Bad Request`
6. chDB, MERGE with two actions (UPDATE + INSERT): MERGE INTO: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 12 (onelake): onelake.`_bench_capability.ch_38254816796_1_merge` t USING (SELECT * FROM values('id Int64, v Int64', (1, 777), (9, 90))) s ON t.id = s.id WHEN MATCHED THEN UPD... Expected end of query. (SYNTAX_ERROR)`
7. DuckDB, INSERT OVERWRITE, whole table: INSERT OVERWRITE, whole table: `Parser Error: syntax error at or near "OVERWRITE" LINE 1: INSERT OVERWRITE onelake."_bench_capability"."dk_38506561909_1_ove... ^^^^^^^^^`
8. chDB, INSERT OVERWRITE, whole table: INSERT OVERWRITE TABLE: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 18 (TABLE): TABLE onelake.`_bench_capability.ch_38506561909_1_overwrite` SELECT * FROM values('id Int64, v Int64', (7, 70), (8, 80)). Expected one of: token, Comma, FROM, PREWHERE, WHERE, GROUP BY, WITH, HAVING, WINDOW, QUALIFY, O…`
9. Polars, INSERT OVERWRITE, whole table: sink_iceberg(mode='overwrite'): `BadRequestError: BadRequestError: BadRequestException: Malformed request`
10. DuckDB, TRUNCATE: Runs, but not as a metadata operation. The data files stay, and a delete file is written for them: slower than dropping the files, and every reader then has to apply the deletes.
11. Sail, TRUNCATE: TRUNCATE TABLE: `IllegalArgumentException: invalid argument: found TRUNCATE at 0:8 expected something else, ';', statement, or end of input`
12. chDB, TRUNCATE: TRUNCATE TABLE: `ChdbError: Code: 48. DB::Exception: Truncate is not supported for data lake engine. (NOT_IMPLEMENTED)`
13. Polars, Partition transform: bucket: write a table partitioned by bucket(4, id): `works: none; refused: bucket: NotImplementedError: NotImplementedError: sink to Iceberg table with 'bucket[4]' partition transform`
14. Spark, Types: decimal, date, timestamp, timestamptz, uuid, binary: types: decimal, date, timestamp, timestamptz, uuid, binary: `works: ['decimal', 'date', 'timestamp', 'timestamptz', 'binary']; refused: uuid: ParseException: [UNSUPPORTED_DATATYPE] Unsupported data type "UUID". SQLSTATE: 0A000 == SQL (line 1, position 85) == ...3754_1_type_uuid` (id BIGINT, x UUID) USING iceberg ^^^^`
15. Sail, Types: decimal, date, timestamp, timestamptz, uuid, binary: types: decimal, date, timestamp, timestamptz, uuid, binary: `works: ['decimal', 'date', 'timestamp', 'timestamptz', 'binary']; refused: uuid: IllegalArgumentException: invalid argument: found UUID at 84:88 expected data type`
16. Sail, Add column: ALTER TABLE ADD COLUMN: `UnsupportedOperationException: unsupported ALTER TABLE operation`
17. Sail, Drop column: ALTER TABLE DROP COLUMN: `UnsupportedOperationException: unsupported ALTER TABLE operation`
18. Sail, Rename column: ALTER TABLE RENAME COLUMN: `UnsupportedOperationException: unsupported ALTER TABLE operation`
19. Sail, Type promotion (int → long): ALTER COLUMN c TYPE BIGINT (int -> long): `AnalysisException: external error: This feature is not implemented: ALTER TABLE is not yet supported for catalog-managed Iceberg tables: onelake._bench_capability.sl_38253752194_1_promote`
20. Sail, Partition evolution: ALTER TABLE ADD PARTITION FIELD (partition evolution): `IllegalArgumentException: invalid argument: found FIELD at 81:86 expected '('`
21. chDB, Partition evolution: ALTER TABLE ADD PARTITION FIELD (partition evolution): `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 70 (PARTITION): PARTITION FIELD bucket(4, id). Expected one of: COLUMN, INDEX, STATISTICS, PROJECTION, CONSTRAINT, end of query. (SYNTAX_ERROR)`
22. Sail, Set table property: ALTER TABLE SET TBLPROPERTIES: `AnalysisException: external error: This feature is not implemented: ALTER TABLE is not yet supported for catalog-managed Iceberg tables: onelake._bench_capability.sl_38253752194_1_props`
23. chDB, Set table property: ALTER TABLE SET TBLPROPERTIES: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 64 (SET): SET TBLPROPERTIES ('probed-at' = '38254816796_1'). Expected one of: ON, a list of ALTER commands, ALTER command, ADD COLUMN, RENAME COLUMN, MATERIALIZE COLUMN, DROP PARTITION, DROP PART, FORGET PARTITION, DROP DETACHED P…`
24. Sail, Sort order evolution: ALTER TABLE ... WRITE ORDERED BY (sort order): `IllegalArgumentException: invalid argument: found WRITE at 66:71 expected '.', 'RENAME', 'PARTITION', 'ADD', 'DROP', 'ALTER', 'CHANGE', 'REPLACE', 'SET', 'UNSET', or 'RECOVER'`
25. chDB, Sort order evolution: ALTER TABLE MODIFY ORDER BY (sort order evolution): `ChdbError: Code: 48. DB::Exception: Alter of type 'MODIFY_ORDER_BY' is not supported by Iceberg storage. (NOT_IMPLEMENTED)`
26. Sail, Metadata tables: metadata tables (t.snapshots): `IllegalArgumentException: invalid argument: table reference: [Identifier("onelake"), Identifier("_bench_capability"), Identifier("sl_38253752194_1_inspect"), Identifier("snapshots")]`
27. DuckDB, MAX from the metadata, no data file read: max(id) with every data file deleted (min/max from metadata): `SELECT max(id) fails with 4 of 4 data files deleted, so it opens them: IO Error: AzureBlobStorageFileSystem Read to 'abfss://1c52481c-0523-4a5a-bbde-fdc932bd77c2@onelake.dfs.fabric.microsoft.com/ac303243-4441-4885-9e7d-f4f5e7af194c/Tables/_bench_capability/dk_38499696276_1_max/data/00000-0-ad51d51e…`
28. chDB, MAX from the metadata, no data file read: max(id) with every data file deleted (min/max from metadata): `SELECT max(id) fails with 4 of 4 data files deleted, so it opens them: ChdbError: Code: 1001. DB::Exception: Azure::Storage::StorageException: 404 The specified blob does not exist. Request ID: a9be9bfa-501e-00be-744f-5aa356000000. (STD_EXCEPTION)`
29. Polars, MAX from the metadata, no data file read: id.max() with every data file deleted (min/max from metadata): `select(id.max()) fails with 4 of 4 data files deleted, so it opens them: FileNotFoundError: object-store error: Object at location ac303243-4441-4885-9e7d-f4f5e7af194c/Tables/_bench_capability/pl_38022167812_1_max/data/00000-0-ad655b41-7953-4d9e-9182-3e0137e09cd3.parquet not found: Error performing…`
30. Sail, Compaction: CALL system.rewrite_data_files (compaction): `IllegalArgumentException: invalid argument: found CALL at 0:4 expected something else, ';', statement, or end of input`
31. chDB, Compaction: OPTIMIZE TABLE (compaction): `returned success and data files went 2 -> 2`
32. DuckDB, Rewrite manifests: iceberg_rewrite_manifests: `Catalog Error: Table Function with name iceberg_rewrite_manifests does not exist! Did you mean "iceberg_rewrite_data_files"?`
33. Sail, Rewrite manifests: CALL system.rewrite_manifests: `IllegalArgumentException: invalid argument: found CALL at 0:4 expected something else, ';', statement, or end of input`
34. chDB, Rewrite manifests: rewrite manifests (no statement): `ClickHouse has no statement to rewrite an Iceberg table's manifests`
35. DuckDB, Create branch: ALTER TABLE ... CREATE BRANCH: `Parser Error: syntax error at or near "CREATE" LINE 1: ... TABLE onelake."_bench_capability"."dk_38499696276_1_branch" CREATE BRANCH probe_branch ^^^^^^`
36. Sail, Create branch: ALTER TABLE ... CREATE BRANCH: `IllegalArgumentException: invalid argument: found CREATE at 66:72 expected '.', 'RENAME', 'PARTITION', 'ADD', 'DROP', 'ALTER', 'CHANGE', 'REPLACE', 'SET', 'UNSET', or 'RECOVER'`
37. chDB, Create branch: ALTER TABLE ... CREATE BRANCH: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 65 (CREATE): CREATE BRANCH probe_branch. Expected one of: ON, a list of ALTER commands, ALTER command, ADD COLUMN, RENAME COLUMN, MATERIALIZE COLUMN, DROP PARTITION, DROP PART, FORGET PARTITION, DROP DETACHED PARTITION, DROP DETAC…`
38. DuckDB, Create tag: ALTER TABLE ... CREATE TAG: `Parser Error: syntax error at or near "CREATE" LINE 1: ... TABLE onelake."_bench_capability"."dk_38499696276_1_tag" CREATE TAG probe_tag ^^^^^^`
39. Sail, Create tag: ALTER TABLE ... CREATE TAG: `IllegalArgumentException: invalid argument: found CREATE at 63:69 expected '.', 'RENAME', 'PARTITION', 'ADD', 'DROP', 'ALTER', 'CHANGE', 'REPLACE', 'SET', 'UNSET', or 'RECOVER'`
40. chDB, Create tag: ALTER TABLE ... CREATE TAG: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 62 (CREATE): CREATE TAG probe_tag. Expected one of: ON, a list of ALTER commands, ALTER command, ADD COLUMN, RENAME COLUMN, MATERIALIZE COLUMN, DROP PARTITION, DROP PART, FORGET PARTITION, DROP DETACHED PARTITION, DROP DETACHED PA…`
41. chDB, Drop table with purge: DROP TABLE: `ChdbError: Code: 736. DB::Exception: Failed to drop table DB::HTTPException: Received error from remote server https://onelake.table.fabric.microsoft.com/iceberg/v1/namespaces/_bench_capability/tables/ch_38254816796_1_drop?purgeRequested=False. HTTP status code: 405 'Method Not Allowed', body lengt…`
42. chDB, Create / drop namespace: CREATE TABLE in a new namespace (no namespace statement): `ChdbError: Code: 79. DB::Exception: MergeTree storages require data path. (INCORRECT_FILE_NAME)`
43. chDB, CREATE SCHEMA IF NOT EXISTS, existing schema: namespace IF NOT EXISTS (no namespace statement): `ClickHouse has no statement for a catalog namespace`

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
| DROP TABLE, CREATE TABLE the same name with a new column, INSERT | refused | `CREATE refused: RuntimeError: RuntimeError: Not implemented Error: Cannot create table deleted within a transaction: onelake._bench_capability.iso_dk_38042313677_1_cb_drop_create; t (('id', 'v'), [(1…` |
| CREATE OR REPLACE TABLE t AS SELECT ... FROM t | refused | `CREATE refused: RuntimeError: RuntimeError: Not implemented Error: CREATE OR REPLACE not supported in DuckDB-Iceberg. Please use separate Drop and Create Statements; t (('id', 'v'), [(1, 10), (2, 20)…` |
| CREATE TABLE, INSERT | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]); n (('id', 'v'), [(1, 1)]); sent ['POST tables 200', 'POST n 200']` |
| CREATE TABLE AS SELECT from the seeded table | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]); n (('id', 'v'), [(1, 20), (2, 40), (3, 60)]); sent ['POST tables 200', 'POST n 200']` |
| ADD COLUMN, INSERT a row that fills it | works | `t (('id', 'v', 'w'), [(1, 10, None), (2, 20, None), (3, 30, None), (4, 40, 400)]); n None; sent ['POST t 200']` |
| ADD COLUMN, UPDATE it | works | `t (('id', 'v', 'w'), [(1, 10, 100), (2, 20, 200), (3, 30, 300)]); n None; sent ['POST t 200']` |
| RENAME COLUMN, INSERT | works | `t (('id', 'v2'), [(1, 10), (2, 20), (3, 30), (4, 40)]); n None; sent ['POST t 200']` |
| DROP COLUMN, INSERT | works | `t (('id',), [(1,), (2,), (3,), (4,)]); n None; sent ['POST t 200']` |
| SET PARTITIONED BY (bucket(4, id)), INSERT | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30), (4, 40)]); n None; sent ['POST t 200']; spec ['bucket[4]']` |
| TRUNCATE, INSERT, ROLLBACK | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]); n None; sent []` |
| DROP TABLE, ROLLBACK | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]); n None; sent []` |
| DROP TABLE, CREATE TABLE the same name, ROLLBACK | refused | `CREATE refused: RuntimeError: RuntimeError: Not implemented Error: Cannot create table deleted within a transaction: onelake._bench_capability.iso_dk_38042313677_1_cb_drop_create_rollback; t (('id', …` |

## Where these readings come from

The OneLake Iceberg REST catalog, read by CI (`.github/workflows/capability.yml`), which writes this file. Every cell is a reading taken by sending the request, not a property of the product: re-run rather than trust it.

- spark: 4.1.3 + iceberg Apache Iceberg 1.12.0 (commit 212aa894e919ff9fdf89160da4aadcbe6858ac01), [run 38506561909](https://github.com/djouallah/lakehouse_benchmark/actions/runs/38506561909), 2026-10-14
- duckdb: v2.0.0-alpha46057, [run 38506561909](https://github.com/djouallah/lakehouse_benchmark/actions/runs/38506561909), 2026-10-14
- sail: 0.7.2, [run 38506561909](https://github.com/djouallah/lakehouse_benchmark/actions/runs/38506561909), 2026-10-14
- chdb: 4.4.0, [run 38506561909](https://github.com/djouallah/lakehouse_benchmark/actions/runs/38506561909), 2026-10-14
- polars: 2.0.0, [run 38506561909](https://github.com/djouallah/lakehouse_benchmark/actions/runs/38506561909), 2026-10-14
- duckdb_isolation: v2.0.0-alpha46057, [run 38042313677](https://github.com/djouallah/lakehouse_benchmark/actions/runs/38042313677), 2026-10-10
