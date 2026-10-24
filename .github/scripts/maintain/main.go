// Table maintenance for the workload's tables, on apache/iceberg-go: `maintain <record.json>`.
//
// Every 2 hours each workload leg merges into its own dbt_<engine>_{landing,mart} tables, and
// nothing on that write path folds the small files back together or drops a snapshot. This is
// the daily job that does, one table at a time (table_maintenance.yml, in the `workload` queue so
// it never overlaps a dbt run -- a rewrite commits optimistically, and overlapping a merge would
// fail one side):
//
//  1. compact: bin-pack the data files to TARGET_MB, a group of at least MIN_INPUT_FILES files;
//  2. expire: snapshots older than EXPIRE_OLDER_THAN_DAYS (the current one always kept), and the
//     files only they referenced are deleted with them;
//  3. orphans: files no snapshot or metadata references, older than ORPHAN_OLDER_THAN_DAYS.
//
// One library for all three, independent of every engine on the page. It replaced DuckDB's
// iceberg_rewrite_data_files plus a pyiceberg fork: the DuckDB rewrite returned its counts and no
// replace snapshot ever reached the catalog.
//
// The record (per table: before/after counts and each step's result or its exact error) is
// rewritten after every table, so a job killed by its timeout still leaves what it did.
// workload_maintain.py publish turns it into the step summary and the Real workload tab's data.
// Never fails on a table; exits 1 only when every attempted table errored.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	_ "github.com/apache/iceberg-go/io/gocloud/azure"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/apache/iceberg-go/table/maintenance"
)

const (
	catalogURI   = "https://onelake.table.fabric.microsoft.com/iceberg"
	blobEndpoint = "blob.fabric.microsoft.com"
	storageScope = "https://storage.azure.com/.default"
	oidcAudience = "api://AzureADTokenExchange"
	targetMB     = 128
	// The catalog and the blobs hold the bearer as a string: reconnect when less than this remains.
	refreshBefore = 10 * time.Minute
)

// dbt_<engine>_<layer>; dbt's own scratch relations (Spark's <model>__stage_<id>, Trino's
// <model>__dbt_tmp_<id>) are left alone.
var (
	namespaceRe = regexp.MustCompile(`^dbt_([a-z0-9]+)_(landing|mart)$`)
	scratch     = []string{"__stage", "__dbt_tmp"}
)

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil {
		return v
	}
	return def
}

var (
	minInputFiles = envInt("COMPACT_MIN_INPUT_FILES", 5)
	// Stop starting new tables past this much wall clock, so the job reports what it did instead
	// of being killed mid-rewrite. Whatever is skipped is tomorrow's: compaction is incremental.
	budgetMinutes = envFloat("COMPACT_BUDGET_MINUTES", 70)
	expireDays    = envFloat("EXPIRE_OLDER_THAN_DAYS", 1)
	// A file younger than this may belong to a commit in flight, so it is never a candidate.
	orphanDays = envFloat("ORPHAN_OLDER_THAN_DAYS", 3)
)

func days(d float64) time.Duration { return time.Duration(d * 24 * float64(time.Hour)) }

// --- record --------------------------------------------------------------------------------------

type tableState struct {
	Snapshots   int    `json:"snapshots"`
	DataFiles   int    `json:"data_files"`
	DeleteFiles int    `json:"delete_files"`
	Bytes       *int64 `json:"bytes"`
	Rows        *int64 `json:"rows"`
}

type compactResult struct {
	Rewritten      int   `json:"rewritten"`
	Added          int   `json:"added"`
	RewrittenBytes int64 `json:"rewritten_bytes"`
}

type orphanResult struct {
	Deleted int   `json:"deleted"`
	Failed  int   `json:"failed"`
	Bytes   int64 `json:"bytes"`
}

type entry struct {
	Engine  string            `json:"engine"`
	Table   string            `json:"table"`
	Skipped string            `json:"skipped,omitempty"`
	Before  *tableState       `json:"before,omitempty"`
	Compact *compactResult    `json:"compact,omitempty"`
	Expired *int              `json:"expired,omitempty"`
	Orphans *orphanResult     `json:"orphans,omitempty"`
	After   *tableState       `json:"after,omitempty"`
	Error   map[string]string `json:"error,omitempty"`
	Seconds float64           `json:"seconds,omitempty"`
}

type record struct {
	RunID    string            `json:"run_id"`
	RunURL   string            `json:"run_url"`
	Started  string            `json:"started"`
	Finished string            `json:"finished"`
	Settings map[string]any    `json:"settings"`
	Versions map[string]string `json:"versions"`
	Tables   []*entry          `json:"tables"`
}

func (r *record) save(path string) {
	b, _ := json.MarshalIndent(r, "", "  ")
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		fmt.Printf("::warning::could not write %s: %v\n", path, err)
	}
}

func oneline(err error) string {
	s := strings.Join(strings.Fields(err.Error()), " ")
	if len(s) > 600 {
		s = s[:600] + "..."
	}
	return s
}

func icebergGoVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, d := range info.Deps {
			if d.Path == "github.com/apache/iceberg-go" {
				return d.Version
			}
		}
	}
	return ""
}

// --- credentials and catalog ---------------------------------------------------------------------

// In Actions, a client assertion minted fresh from the GitHub OIDC endpoint on every token
// (bench/auth.py's reason: `az login --federated-token` cannot refresh past its first token);
// on a laptop, `az login`.
func credential() (azcore.TokenCredential, error) {
	reqURL := os.Getenv("ACTIONS_ID_TOKEN_REQUEST_URL")
	if reqURL == "" {
		return azidentity.NewAzureCLICredential(nil)
	}
	assertion := func(ctx context.Context) (string, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL+"&audience="+oidcAudience, nil)
		if err != nil {
			return "", err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN"))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		var body struct{ Value string }
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return "", err
		}
		return body.Value, nil
	}
	return azidentity.NewClientAssertionCredential(
		os.Getenv("AZURE_TENANT_ID"), os.Getenv("AZURE_CLIENT_ID"), assertion, nil)
}

type conn struct {
	cred    azcore.TokenCredential
	cat     *rest.Catalog
	expires time.Time
}

// fresh returns the catalog, reconnected on a new bearer when the current one runs low. The same
// bearer serves the catalog and, as adls.token, the table's blobs (OneLake's catalog vends none).
func (c *conn) fresh(ctx context.Context) (*rest.Catalog, error) {
	if c.cat != nil && time.Until(c.expires) > refreshBefore {
		return c.cat, nil
	}
	tok, err := c.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{storageScope}})
	if err != nil {
		return nil, fmt.Errorf("token: %w", err)
	}
	warehouse := os.Getenv("FABRIC_WORKSPACE_ID") + "/" + os.Getenv("FABRIC_LAKEHOUSE_ID")
	cat, err := rest.NewCatalog(ctx, "onelake", catalogURI,
		rest.WithOAuthToken(tok.Token),
		rest.WithWarehouseLocation(warehouse),
		rest.WithAdditionalProps(iceberg.Properties{"adls.token": tok.Token, "adls.endpoint": blobEndpoint}))
	if err != nil {
		return nil, fmt.Errorf("catalog: %w", err)
	}
	c.cat, c.expires = cat, tok.ExpiresOn
	return cat, nil
}

// --- measures and steps --------------------------------------------------------------------------

// What the current snapshot holds, from its manifests (not the snapshot summary: a writer may
// not keep totals there).
func state(ctx context.Context, tbl *table.Table) (*tableState, error) {
	s := &tableState{Snapshots: len(tbl.Metadata().Snapshots())}
	var bytes, rows int64
	s.Bytes, s.Rows = &bytes, &rows
	if tbl.CurrentSnapshot() == nil {
		return s, nil
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return nil, err
	}
	deletes := map[string]struct{}{}
	for _, t := range tasks {
		s.DataFiles++
		bytes += t.File.FileSizeBytes()
		rows += t.File.Count()
		for _, group := range [][]iceberg.DataFile{t.DeleteFiles, t.EqualityDeleteFiles, t.DeletionVectorFiles} {
			for _, d := range group {
				deletes[d.FilePath()] = struct{}{}
			}
		}
	}
	s.DeleteFiles = len(deletes)
	return s, nil
}

func compact(ctx context.Context, tbl *table.Table) (*compactResult, error) {
	cfg := compaction.DefaultConfig()
	cfg.TargetFileSizeBytes = targetMB << 20
	cfg.MinFileSizeBytes = cfg.TargetFileSizeBytes * 3 / 4
	cfg.MaxFileSizeBytes = cfg.TargetFileSizeBytes * 9 / 5
	cfg.MinInputFiles = uint(minInputFiles)
	plan, err := compaction.Analyze(ctx, tbl, cfg)
	if err != nil {
		return nil, fmt.Errorf("plan: %w", err)
	}
	if len(plan.Groups) == 0 {
		return &compactResult{}, nil
	}
	groups := make([]table.CompactionTaskGroup, len(plan.Groups))
	for i, g := range plan.Groups {
		groups[i] = table.CompactionTaskGroup{PartitionKey: g.PartitionKey, Tasks: g.Tasks, TotalSizeBytes: g.TotalSizeBytes}
	}
	// The default read pipeline (GOMAXPROCS workers, each holding a whole task, 64 buffered
	// batches) took all 16 GB of the runner in seconds on dbt_polars_landing.fct_scada, and the
	// runner died. Bounded, a 128 MiB group peaks at about 3.7 GB (tmp_igo_diag, 2026-10-24).
	tx := tbl.NewTransaction()
	res, err := tx.RewriteDataFiles(ctx, groups, table.RewriteDataFilesOptions{GroupOptions: []table.CompactionGroupOption{
		table.WithCompactionTargetFileSize(targetMB << 20),
		table.WithCompactionScanConcurrency(2),
		table.WithCompactionArrowBatchSize(65536),
		table.WithCompactionRecordBatchBufferSize(4),
	}})
	if err != nil {
		return nil, err
	}
	if _, err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return &compactResult{Rewritten: res.RemovedDataFiles, Added: res.AddedDataFiles, RewrittenBytes: res.BytesBefore}, nil
}

// Snapshots expired, counted by a re-read: a commit that returns is not proof it landed.
func expire(ctx context.Context, cat *rest.Catalog, ident table.Identifier) (int, error) {
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return 0, err
	}
	cutoff := time.Now().Add(-days(expireDays)).UnixMilli()
	victims := 0
	for _, s := range tbl.Metadata().Snapshots() {
		if s.TimestampMs < cutoff {
			victims++
		}
	}
	if victims == 0 {
		return 0, nil
	}
	before := len(tbl.Metadata().Snapshots())
	tx := tbl.NewTransaction()
	if err := tx.ExpireSnapshots(table.WithOlderThan(days(expireDays)), table.WithRetainLast(1)); err != nil {
		return 0, err
	}
	if _, err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	tbl, err = cat.LoadTable(ctx, ident)
	if err != nil {
		return 0, err
	}
	return before - len(tbl.Metadata().Snapshots()), nil
}

func removeOrphans(ctx context.Context, tbl *table.Table) (*orphanResult, error) {
	res, err := maintenance.DeleteOrphanFiles(ctx, tbl, maintenance.WithCleanupFilesOlderThan(days(orphanDays)))
	out := &orphanResult{Deleted: len(res.DeletedFiles), Failed: len(res.OrphanFiles) - len(res.DeletedFiles), Bytes: res.TotalSizeBytes}
	if out.Failed < 0 {
		out.Failed = 0
	}
	return out, err
}

// --- run -----------------------------------------------------------------------------------------

type target struct{ engine, name string }

func tables(ctx context.Context, cat *rest.Catalog) ([]target, error) {
	namespaces, err := cat.ListNamespaces(ctx, nil)
	if err != nil {
		return nil, err
	}
	var out []target
	for _, ns := range namespaces {
		m := namespaceRe.FindStringSubmatch(strings.Join(ns, "."))
		if m == nil {
			continue
		}
		var names []string
		for ident, err := range cat.ListTables(ctx, ns) {
			if err != nil {
				return nil, err
			}
			name := ident[len(ident)-1]
			if !isScratch(name) {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		for _, n := range names {
			out = append(out, target{m[1], m[0] + "." + n})
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].engine < out[j].engine })
	return out, nil
}

func isScratch(name string) bool {
	for _, s := range scratch {
		if strings.Contains(name, s) {
			return true
		}
	}
	return false
}

func run(path string) int {
	ctx := context.Background()
	cred, err := credential()
	if err != nil {
		fmt.Println("credential:", oneline(err))
		return 1
	}
	c := &conn{cred: cred}
	cat, err := c.fresh(ctx)
	if err != nil {
		fmt.Println(oneline(err))
		return 1
	}
	rec := &record{
		RunID:   envOr("GITHUB_RUN_ID", "local"),
		RunURL:  os.Getenv("BENCH_RUN_URL"),
		Started: time.Now().UTC().Format("2006-01-02T15:04:05Z"),
		Settings: map[string]any{
			"target_mb": targetMB, "min_input_files": minInputFiles,
			"expire_days": expireDays, "orphan_days": orphanDays,
		},
		Versions: map[string]string{"iceberg-go": icebergGoVersion()},
		Tables:   []*entry{},
	}
	todo, err := tables(ctx, cat)
	if err != nil {
		fmt.Println("list tables:", oneline(err))
		return 1
	}
	fmt.Printf("%d table(s): compact to %d MiB (min %d input files), expire snapshots older than %gd, "+
		"orphan files older than %gd; budget %g min; iceberg-go %s\n",
		len(todo), targetMB, minInputFiles, expireDays, orphanDays, budgetMinutes, rec.Versions["iceberg-go"])

	started := time.Now()
	for i, t := range todo {
		e := &entry{Engine: t.engine, Table: t.name}
		rec.Tables = append(rec.Tables, e)
		if time.Since(started).Minutes() >= budgetMinutes {
			e.Skipped = fmt.Sprintf("not attempted (%g min budget spent)", budgetMinutes)
			continue
		}
		fmt.Printf("[%d/%d] %s ...\n", i+1, len(todo), t.name)
		t0 := time.Now()
		maintainTable(ctx, c, e)
		e.Seconds = float64(time.Since(t0).Milliseconds()) / 1000
		rec.save(path)
	}
	rec.Finished = time.Now().UTC().Format("2006-01-02T15:04:05Z")
	rec.save(path)

	attempted, failed := 0, 0
	for _, e := range rec.Tables {
		if e.Skipped == "" {
			attempted++
			if e.Error != nil {
				failed++
			}
		}
	}
	if attempted > 0 && failed == attempted {
		fmt.Println("::error::every table errored")
		return 1
	}
	return 0
}

func maintainTable(ctx context.Context, c *conn, e *entry) {
	fail := func(step string, err error) {
		if e.Error == nil {
			e.Error = map[string]string{}
		}
		e.Error[step] = oneline(err)
		fmt.Printf("  %s: %s\n", step, e.Error[step])
	}
	cat, err := c.fresh(ctx)
	if err != nil {
		fail("load", err)
		return
	}
	ident := catalog.ToIdentifier(e.Table)
	tbl, err := cat.LoadTable(ctx, ident)
	if err == nil {
		e.Before, err = state(ctx, tbl)
	}
	if err != nil {
		fail("load", err)
		return
	}

	if e.Compact, err = compact(ctx, tbl); err != nil {
		fail("compact", err)
	} else {
		fmt.Printf("  compact: %d -> %d files\n", e.Compact.Rewritten, e.Compact.Added)
	}

	if n, err := expire(ctx, cat, ident); err != nil {
		fail("expired", err)
	} else {
		e.Expired = &n
		fmt.Printf("  expired: %d snapshots\n", n)
	}

	if tbl, err = cat.LoadTable(ctx, ident); err == nil {
		e.Orphans, err = removeOrphans(ctx, tbl)
	}
	if err != nil {
		fail("orphans", err)
	}
	if e.Orphans != nil {
		fmt.Printf("  orphans: %d deleted, %d failed, %d bytes\n", e.Orphans.Deleted, e.Orphans.Failed, e.Orphans.Bytes)
	}

	if tbl, err = cat.LoadTable(ctx, ident); err == nil {
		e.After, err = state(ctx, tbl)
	}
	if err != nil {
		fail("after", err)
	} else {
		fmt.Printf("  now: %d data files, %d snapshots\n", e.After.DataFiles, e.After.Snapshots)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: maintain <record.json>")
		os.Exit(2)
	}
	os.Exit(run(os.Args[1]))
}
