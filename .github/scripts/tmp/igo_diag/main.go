// TEMPORARY diagnostic for the iceberg-go maintenance job, never commits anything:
//  1. dbt_polars_landing.fct_scada killed the runner 75 s into its compaction. Rewrite its first
//     groups (files written, NOT committed: they become orphans) with the default pipeline, then
//     with bounded memory, logging the process and machine memory every 2 s.
//  2. Gluten's tables fail with "cannot promote timestamp to timestamptz": print the table's schema
//     and the Parquet schema of one of its data files.
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	_ "github.com/apache/iceberg-go/io/gocloud/azure"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
)

func must(step string, err error) {
	if err != nil {
		fmt.Printf("FAIL %s: %v\n", step, err)
		os.Exit(1)
	}
}

func memAvailableMB() int64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return -1
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		var kb int64
		if n, _ := fmt.Sscanf(s.Text(), "MemAvailable: %d kB", &kb); n == 1 {
			return kb / 1024
		}
	}
	return -1
}

var peakSys uint64

func logMem(label string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if m.Sys > peakSys {
		peakSys = m.Sys
	}
	fmt.Printf("    mem %-28s heap-inuse %5d MB  go-sys %5d MB  machine-available %5d MB\n",
		label, m.HeapInuse>>20, m.Sys>>20, memAvailableMB())
}

func watch(ctx context.Context, label string) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			logMem(label)
			if a := memAvailableMB(); a >= 0 && a < 1500 {
				fmt.Println("    machine below 1.5 GB available: stopping before the runner dies")
				os.Exit(3)
			}
		}
	}
}

func rewriteGroups(ctx context.Context, tbl *table.Table, label string, n int, opts ...table.CompactionGroupOption) {
	cfg := compaction.DefaultConfig()
	cfg.TargetFileSizeBytes = 128 << 20
	cfg.MinFileSizeBytes = cfg.TargetFileSizeBytes * 3 / 4
	cfg.MaxFileSizeBytes = cfg.TargetFileSizeBytes * 9 / 5
	plan, err := compaction.Analyze(ctx, tbl, cfg)
	must("analyze", err)
	fmt.Printf("  %s: %d groups planned\n", label, len(plan.Groups))
	peakSys = 0
	wctx, stop := context.WithCancel(ctx)
	go watch(wctx, label)
	for i, g := range plan.Groups {
		if i == n {
			break
		}
		var rows int64
		for _, t := range g.Tasks {
			rows += t.File.Count()
		}
		t0 := time.Now()
		res, err := table.ExecuteCompactionGroup(ctx, tbl,
			table.CompactionTaskGroup{PartitionKey: g.PartitionKey, Tasks: g.Tasks, TotalSizeBytes: g.TotalSizeBytes}, opts...)
		must("group", err)
		fmt.Printf("  group %d: %d files, %d rows, %d MB -> %d files in %.0fs\n",
			i, len(g.Tasks), rows, g.TotalSizeBytes>>20, len(res.NewDataFiles), time.Since(t0).Seconds())
		logMem(fmt.Sprintf("%s after group %d", label, i))
		runtime.GC()
	}
	stop()
	fmt.Printf("  %s: peak go-sys %d MB\n", label, peakSys>>20)
}

func parquetSchema(ctx context.Context, tbl *table.Table) {
	fmt.Println("  table schema:", tbl.Schema())
	tasks, err := tbl.Scan().PlanFiles(ctx)
	must("plan", err)
	fs, err := tbl.FS(ctx)
	must("fs", err)
	path := tasks[0].File.FilePath()
	fmt.Println("  data file:", path[strings.LastIndex(path, "/")+1:])
	f, err := fs.Open(path)
	must("open", err)
	defer f.Close()
	rdr, err := file.NewParquetReader(f)
	must("parquet", err)
	fmt.Println("  created by:", rdr.MetaData().GetCreatedBy())
	schema.PrintSchema(rdr.MetaData().Schema.Root(), os.Stdout, 2)
}

func main() {
	ctx := context.Background()
	tok := os.Getenv("ONELAKE_TOKEN")
	cat, err := rest.NewCatalog(ctx, "onelake", "https://onelake.table.fabric.microsoft.com/iceberg",
		rest.WithOAuthToken(tok),
		rest.WithWarehouseLocation(os.Getenv("WAREHOUSE")),
		rest.WithAdditionalProps(iceberg.Properties{"adls.token": tok, "adls.endpoint": "blob.fabric.microsoft.com"}))
	must("connect", err)
	fmt.Printf("CPUs %d, machine available %d MB\n", runtime.NumCPU(), memAvailableMB())

	_ = parquetSchema // answered: Velox writes isAdjustedToUTC=false into a timestamptz column

	fmt.Println("\n===== dbt_polars_landing.fct_scada: memory per compaction group (no commit)")
	tbl, err := cat.LoadTable(ctx, catalog.ToIdentifier("dbt_polars_landing.fct_scada"))
	must("load", err)
	fmt.Println("  table properties:", tbl.Properties())
	rewriteGroups(ctx, tbl, "bounded", 2,
		table.WithCompactionTargetFileSize(128<<20),
		table.WithCompactionScanConcurrency(2),
		table.WithCompactionArrowBatchSize(65536),
		table.WithCompactionRecordBatchBufferSize(4))
	rewriteGroups(ctx, tbl, "default", 2, table.WithCompactionTargetFileSize(128<<20))
}
