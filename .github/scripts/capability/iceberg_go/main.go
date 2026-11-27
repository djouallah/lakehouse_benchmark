// One operation of apache/iceberg-go against the OneLake Iceberg REST catalog: the iceberg-go
// column of the Iceberg support tab. catalog_capability_iceberg_go.py runs it once per probe.
//
// The request is one JSON object on stdin, {"op", "ns", "table", ...op arguments}; the answer is
// one JSON line on stdout, {"ok": true, "result": ...} or {"ok": false, "error": "<iceberg-go's
// own words>"}. It sets nothing up and checks nothing: the Python side creates and seeds the
// tables with pyiceberg and reads the effect back through pyiceberg, as for every other column.
// Each op is the iceberg-go call that does what the SQL engines' statement does; branches and
// tags are a set-snapshot-ref commit, the way iceberg-go's own CLI (cmd/iceberg) makes them.
//
// The catalog: ICEBERG_URI (the race proxy in the concurrency rows), ICEBERG_WAREHOUSE, and
// ICEBERG_TOKEN, the bearer bench.auth minted, which also signs the blobs (adls.token).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	_ "github.com/apache/iceberg-go/io/gocloud/azure"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/apache/iceberg-go/table/inspect"
	"github.com/apache/iceberg-go/table/maintenance"
)

const blobEndpoint = "blob.fabric.microsoft.com"

type request struct {
	Op    string `json:"op"`
	NS    string `json:"ns"`
	Table string `json:"table"`
	// create: the schema and the partition spec, as Iceberg JSON (pyiceberg's model_dump_json).
	Schema json.RawMessage `json:"schema,omitempty"`
	Spec   json.RawMessage `json:"spec,omitempty"`
	// append / overwrite: the rows, an Arrow IPC file written by the Python side.
	IPC string `json:"ipc,omitempty"`
	// delete: "id = <n>", or "all".
	Filter string `json:"filter,omitempty"`
	// scan: the columns, an optional id to match, an optional snapshot.
	Columns  []string `json:"columns,omitempty"`
	EqualsID *int64   `json:"equals_id,omitempty"`
	Snapshot *int64   `json:"snapshot,omitempty"`
	// add_files: the Parquet file; set_property: the value; branch / tag: the ref name.
	Path  string `json:"path,omitempty"`
	Value string `json:"value,omitempty"`
	Name  string `json:"name,omitempty"`
}

func version() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, d := range info.Deps {
			if d.Path == "github.com/apache/iceberg-go" {
				return d.Version
			}
		}
	}
	return ""
}

func connect(ctx context.Context) (*rest.Catalog, error) {
	token := os.Getenv("ICEBERG_TOKEN")
	return rest.NewCatalog(ctx, "onelake", os.Getenv("ICEBERG_URI"),
		rest.WithOAuthToken(token),
		rest.WithWarehouseLocation(os.Getenv("ICEBERG_WAREHOUSE")),
		rest.WithAdditionalProps(iceberg.Properties{"adls.token": token, "adls.endpoint": blobEndpoint}))
}

func readIPC(path string) (arrow.Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rdr, err := ipc.NewFileReader(f, ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		return nil, err
	}
	defer rdr.Close()
	var recs []arrow.RecordBatch
	for i := 0; i < rdr.NumRecords(); i++ {
		rec, err := rdr.RecordBatchAt(i)
		if err != nil {
			return nil, err
		}
		rec.Retain()
		recs = append(recs, rec)
	}
	return array.NewTableFromRecords(rdr.Schema(), recs), nil
}

// The rows of a scan as [[v, ...], ...], int64 columns only (every probe table reads id / v).
func scanRows(ctx context.Context, scan *table.Scan) ([][]any, error) {
	tbl, err := scan.ToArrowTable(ctx)
	if err != nil {
		return nil, err
	}
	defer tbl.Release()
	out := [][]any{}
	rdr := array.NewTableReader(tbl, 1024)
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.RecordBatch()
		for r := 0; r < int(rec.NumRows()); r++ {
			row := make([]any, rec.NumCols())
			for c := range row {
				col := rec.Column(c)
				if col.IsNull(r) {
					continue
				}
				if ints, ok := col.(*array.Int64); ok {
					row[c] = ints.Value(r)
				} else {
					row[c] = col.ValueStr(r)
				}
			}
			out = append(out, row)
		}
	}
	return out, rdr.Err()
}

func commit(ctx context.Context, tx *table.Transaction) error {
	if _, err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func setRef(ctx context.Context, cat *rest.Catalog, tbl *table.Table, name string, kind table.RefType) error {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return errors.New("the table has no current snapshot")
	}
	update := table.NewSetSnapshotRefUpdate(name, snap.SnapshotID, kind, 0, 0, 0)
	reqs := []table.Requirement{table.AssertTableUUID(tbl.Metadata().TableUUID()), table.AssertRefSnapshotID(name, nil)}
	_, _, err := cat.CommitTable(ctx, tbl.Identifier(), reqs, []table.Update{update})
	return err
}

func run(ctx context.Context, req request) (any, error) {
	if req.Op == "version" {
		return version(), nil
	}
	cat, err := connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("catalog: %w", err)
	}
	ident := catalog.ToIdentifier(req.NS, req.Table)
	switch req.Op {
	case "session":
		ns, err := cat.ListNamespaces(ctx, nil)
		return fmt.Sprintf("iceberg-go %s; the REST catalog lists %d namespace(s)", version(), len(ns)), err
	case "namespace_create":
		return nil, cat.CreateNamespace(ctx, catalog.ToIdentifier(req.NS), nil)
	case "namespace_drop":
		return nil, cat.DropNamespace(ctx, catalog.ToIdentifier(req.NS))
	case "create":
		schema := new(iceberg.Schema)
		if err := json.Unmarshal(req.Schema, schema); err != nil {
			return nil, fmt.Errorf("schema: %w", err)
		}
		var opts []catalog.CreateTableOpt
		if len(req.Spec) > 0 {
			spec := new(iceberg.PartitionSpec)
			if err := json.Unmarshal(req.Spec, spec); err != nil {
				return nil, fmt.Errorf("spec: %w", err)
			}
			opts = append(opts, catalog.WithPartitionSpec(spec))
		}
		_, err := cat.CreateTable(ctx, ident, schema, opts...)
		return nil, err
	case "purge":
		return nil, cat.PurgeTable(ctx, ident)
	}

	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, fmt.Errorf("load: %w", err)
	}
	tx := tbl.NewTransaction()
	switch req.Op {
	case "append", "overwrite":
		rows, err := readIPC(req.IPC)
		if err != nil {
			return nil, fmt.Errorf("rows: %w", err)
		}
		defer rows.Release()
		if req.Op == "append" {
			_, err = tbl.AppendTable(ctx, rows, 1024, nil)
		} else {
			_, err = tbl.OverwriteTable(ctx, rows, 1024, nil)
		}
		return nil, err
	case "delete":
		var filter iceberg.BooleanExpression = iceberg.AlwaysTrue{}
		if req.Filter != "all" {
			var id int64
			if _, err := fmt.Sscanf(req.Filter, "id = %d", &id); err != nil {
				return nil, fmt.Errorf("filter %q: %w", req.Filter, err)
			}
			filter = iceberg.EqualTo(iceberg.Reference("id"), id)
		}
		_, err := tbl.Delete(ctx, filter, nil)
		return nil, err
	case "scan":
		var opts []table.ScanOption
		if len(req.Columns) > 0 {
			opts = append(opts, table.WithSelectedFields(req.Columns...))
		}
		if req.EqualsID != nil {
			opts = append(opts, table.WithRowFilter(iceberg.EqualTo(iceberg.Reference("id"), *req.EqualsID)))
		}
		if req.Snapshot != nil {
			opts = append(opts, table.WithSnapshotID(*req.Snapshot))
		}
		return scanRows(ctx, tbl.Scan(opts...))
	case "add_column":
		err = tx.UpdateSchema(true, false).AddColumn([]string{"w"}, iceberg.PrimitiveTypes.Int64, "", false, nil).Commit()
	case "drop_column":
		err = tx.UpdateSchema(true, false).DeleteColumn([]string{"v"}).Commit()
	case "rename_column":
		err = tx.UpdateSchema(true, false).RenameColumn([]string{"v"}, "v2").Commit()
	case "type_promotion":
		err = tx.UpdateSchema(true, false).UpdateColumn([]string{"c"},
			table.ColumnUpdate{FieldType: iceberg.Optional[iceberg.Type]{Valid: true, Val: iceberg.PrimitiveTypes.Int64}}).Commit()
	case "partition_evolution":
		err = tx.UpdateSpec(true).AddField("id", iceberg.BucketTransform{NumBuckets: 4}, "id_bucket").Commit()
	case "sort_order":
		err = tx.NewReplaceSortOrder(true).Asc("id", table.NullsFirst).Commit()
	case "set_property":
		err = tx.SetProperties(iceberg.Properties{"probed-at": req.Value})
	case "snapshots":
		rdr, err := inspect.Snapshots(ctx, tbl)
		if err != nil {
			return nil, err
		}
		defer rdr.Release()
		n := 0
		for rdr.Next() {
			n += int(rdr.RecordBatch().NumRows())
		}
		return n, rdr.Err()
	case "branch":
		return nil, setRef(ctx, cat, tbl, req.Name, table.BranchRef)
	case "tag":
		return nil, setRef(ctx, cat, tbl, req.Name, table.TagRef)
	case "compaction":
		cfg := compaction.DefaultConfig()
		cfg.MinInputFiles = 2
		plan, err := compaction.Analyze(ctx, tbl, cfg)
		if err != nil {
			return nil, fmt.Errorf("plan: %w", err)
		}
		if len(plan.Groups) == 0 {
			return "the plan has no group to rewrite", nil
		}
		groups := make([]table.CompactionTaskGroup, len(plan.Groups))
		for i, g := range plan.Groups {
			groups[i] = table.CompactionTaskGroup{PartitionKey: g.PartitionKey, Tasks: g.Tasks, TotalSizeBytes: g.TotalSizeBytes}
		}
		if _, err := tx.RewriteDataFiles(ctx, groups, table.RewriteDataFilesOptions{}); err != nil {
			return nil, err
		}
	case "rewrite_manifests":
		if _, err := tx.RewriteManifests(ctx); err != nil {
			return nil, err
		}
	case "add_files":
		err = tx.AddDataFilesByPath(ctx, []string{req.Path}, nil, false)
	case "expire_snapshots":
		err = tx.ExpireSnapshots(table.WithOlderThan(0), table.WithRetainLast(1))
	case "remove_orphan_files":
		res, err := maintenance.DeleteOrphanFiles(ctx, tbl, maintenance.WithCleanupFilesOlderThan(0))
		if err != nil {
			return nil, err
		}
		return fmt.Sprintf("%d orphan file(s) found, %d deleted", len(res.OrphanFiles), len(res.DeletedFiles)), nil
	default:
		return nil, fmt.Errorf("unknown op %q", req.Op)
	}
	if err != nil {
		return nil, err
	}
	return nil, commit(ctx, tx)
}

func main() {
	out := json.NewEncoder(os.Stdout)
	body, err := io.ReadAll(os.Stdin)
	var req request
	if err == nil {
		err = json.Unmarshal(body, &req)
	}
	if err != nil {
		_ = out.Encode(map[string]any{"ok": false, "error": "request: " + err.Error()})
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	result, err := run(ctx, req)
	if err != nil {
		_ = out.Encode(map[string]any{"ok": false, "error": strings.Join(strings.Fields(err.Error()), " ")})
		return
	}
	_ = out.Encode(map[string]any{"ok": true, "result": result})
}
