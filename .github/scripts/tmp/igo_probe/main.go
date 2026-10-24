// TEMPORARY: can apache/iceberg-go main do the workload's table maintenance on OneLake?
//
// 1. On a scratch table (tmp_igo_probe.t, 20 one-file appends): compact, expire, remove orphans,
//    re-reading the table after each step to confirm the commit landed and the rows survived.
// 2. On real workload tables, read-only: the compaction plan and an orphan dry run.
//
// Env: ONELAKE_TOKEN (storage scope, for the catalog and the blobs), WAREHOUSE (<ws>/<lh>).
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/apache/iceberg-go/table/maintenance"
)

const (
	uri      = "https://onelake.table.fabric.microsoft.com/iceberg"
	targetMB = 128
	scratch  = "tmp_igo_probe"
)

var realTables = []string{
	"dbt_duckdb_landing.fct_price",
	"dbt_duckdb_landing.fct_scada",
	"dbt_polars_landing.fct_scada",
	"dbt_trino_mart.fct_summary",
}

func must(step string, err error) {
	if err != nil {
		fmt.Printf("FAIL %s: %v\n", step, err)
		os.Exit(1)
	}
}

type state struct {
	snapshots, files int
	rows             int64
	op               string
}

func measure(ctx context.Context, tbl *table.Table) state {
	s := state{snapshots: len(tbl.Metadata().Snapshots())}
	if cur := tbl.CurrentSnapshot(); cur != nil && cur.Summary != nil {
		s.op = string(cur.Summary.Operation)
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	must("plan files", err)
	s.files = len(tasks)
	for _, t := range tasks {
		s.rows += t.File.Count()
	}
	return s
}

func (s state) String() string {
	return fmt.Sprintf("%d snapshots (current: %s), %d data files, %d rows", s.snapshots, s.op, s.files, s.rows)
}

func compactCfg() compaction.Config {
	cfg := compaction.DefaultConfig()
	cfg.TargetFileSizeBytes = targetMB << 20
	cfg.MinFileSizeBytes = cfg.TargetFileSizeBytes * 3 / 4
	cfg.MaxFileSizeBytes = cfg.TargetFileSizeBytes * 9 / 5
	return cfg
}

func compact(ctx context.Context, tbl *table.Table) (*table.RewriteResult, error) {
	plan, err := compaction.Analyze(ctx, tbl, compactCfg())
	if err != nil {
		return nil, fmt.Errorf("analyze: %w", err)
	}
	if len(plan.Groups) == 0 {
		return &table.RewriteResult{}, nil
	}
	groups := make([]table.CompactionTaskGroup, len(plan.Groups))
	for i, g := range plan.Groups {
		groups[i] = table.CompactionTaskGroup{PartitionKey: g.PartitionKey, Tasks: g.Tasks, TotalSizeBytes: g.TotalSizeBytes}
	}
	tx := tbl.NewTransaction()
	res, err := tx.RewriteDataFiles(ctx, groups, table.RewriteDataFilesOptions{})
	if err != nil {
		return res, fmt.Errorf("rewrite: %w", err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		return res, fmt.Errorf("commit: %w", err)
	}
	return res, nil
}

func batch(sc *arrow.Schema, start int64, n int) arrow.Table {
	mem := memory.DefaultAllocator
	ids, vs := array.NewInt64Builder(mem), array.NewStringBuilder(mem)
	for i := int64(0); i < int64(n); i++ {
		ids.Append(start + i)
		vs.Append(fmt.Sprintf("row-%d", start+i))
	}
	rec := array.NewRecordBatch(sc, []arrow.Array{ids.NewArray(), vs.NewArray()}, int64(n))
	return array.NewTableFromRecords(sc, []arrow.RecordBatch{rec})
}

func scratchProbe(ctx context.Context, cat *rest.Catalog) {
	fmt.Println("===== scratch table: compact, expire, remove orphans")
	ns := catalog.ToIdentifier(scratch)
	ident := catalog.ToIdentifier(scratch, "t")
	if ok, err := cat.CheckNamespaceExists(ctx, ns); err == nil && !ok {
		must("create namespace", cat.CreateNamespace(ctx, ns, nil))
	}
	if err := cat.DropTable(ctx, ident); err != nil && !errors.Is(err, catalog.ErrNoSuchTable) {
		fmt.Printf("  drop old scratch table: %v\n", err)
	}
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "v", Type: iceberg.PrimitiveTypes.String})
	tbl, err := cat.CreateTable(ctx, ident, schema)
	must("create table", err)
	fmt.Printf("  created at %s\n", tbl.Location())

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	for i := 0; i < 20; i++ {
		tx := tbl.NewTransaction()
		must("append", tx.AppendTable(ctx, batch(sc, int64(i*1000), 1000), 1000, nil))
		tbl, err = tx.Commit(ctx)
		must("append commit", err)
	}
	tbl, err = cat.LoadTable(ctx, ident)
	must("reload", err)
	fmt.Printf("  after 20 appends:  %s\n", measure(ctx, tbl))

	res, err := compact(ctx, tbl)
	must("compact", err)
	fmt.Printf("  compact returned:  %d files -> %d\n", res.RemovedDataFiles, res.AddedDataFiles)
	tbl, err = cat.LoadTable(ctx, ident)
	must("reload", err)
	fmt.Printf("  re-read:           %s\n", measure(ctx, tbl))

	tx := tbl.NewTransaction()
	must("expire", tx.ExpireSnapshots(table.WithOlderThan(0), table.WithRetainLast(1)))
	_, err = tx.Commit(ctx)
	must("expire commit", err)
	tbl, err = cat.LoadTable(ctx, ident)
	must("reload", err)
	fmt.Printf("  after expire:      %s\n", measure(ctx, tbl))

	time.Sleep(5 * time.Second)
	plan, err := maintenance.PlanOrphanFiles(ctx, tbl, maintenance.WithCleanupFilesOlderThan(time.Second))
	if err != nil {
		fmt.Printf("  orphan plan FAILED: %v\n", err)
	} else {
		fmt.Printf("  orphan plan:       %d files, %d bytes\n", len(plan.Files()), plan.TotalSizeBytes())
		out, err := maintenance.ExecuteOrphanCleanup(ctx, tbl, plan)
		if err != nil {
			fmt.Printf("  orphan delete FAILED: %v\n", err)
		} else {
			fmt.Printf("  orphans deleted:   %d\n", len(out.DeletedFiles))
		}
	}
	tbl, err = cat.LoadTable(ctx, ident)
	must("reload", err)
	arr, err := tbl.Scan().ToArrowTable(ctx)
	must("read back", err)
	fmt.Printf("  final read-back:   %s; full scan read %d rows\n", measure(ctx, tbl), arr.NumRows())
}

func realProbe(ctx context.Context, cat *rest.Catalog) {
	fmt.Println("\n===== workload tables, read-only: compaction plan and orphan dry run (older than 72h)")
	for _, name := range realTables {
		tbl, err := cat.LoadTable(ctx, catalog.ToIdentifier(name))
		if err != nil {
			fmt.Printf("%s: load FAILED: %v\n", name, err)
			continue
		}
		fmt.Printf("%s: %s\n", name, measure(ctx, tbl))
		plan, err := compaction.Analyze(ctx, tbl, compactCfg())
		if err != nil {
			fmt.Printf("  compaction plan FAILED: %v\n", err)
		} else {
			n := 0
			for _, g := range plan.Groups {
				n += len(g.Tasks)
			}
			fmt.Printf("  compaction plan: %d groups, %d of %d files to rewrite\n", len(plan.Groups), n, plan.TotalInputFiles)
		}
		t0 := time.Now()
		orph, err := maintenance.PlanOrphanFiles(ctx, tbl, maintenance.WithCleanupFilesOlderThan(72*time.Hour))
		if err != nil {
			fmt.Printf("  orphan dry run FAILED: %v\n", err)
		} else {
			fmt.Printf("  orphan dry run: %d files, %d bytes (%.0fs)\n", len(orph.Files()), orph.TotalSizeBytes(), time.Since(t0).Seconds())
			for i, f := range orph.Files() {
				if i == 3 {
					break
				}
				fmt.Printf("    e.g. %s\n", f)
			}
		}
	}
}

func main() {
	ctx := context.Background()
	tok := os.Getenv("ONELAKE_TOKEN")
	cat, err := rest.NewCatalog(ctx, "onelake", uri,
		rest.WithOAuthToken(tok),
		rest.WithWarehouseLocation(os.Getenv("WAREHOUSE")),
		rest.WithAdditionalProps(iceberg.Properties{
			"adls.token":    tok,
			"adls.endpoint": "blob.fabric.microsoft.com",
		}))
	must("connect", err)
	scratchProbe(ctx, cat)
	realProbe(ctx, cat)
}
