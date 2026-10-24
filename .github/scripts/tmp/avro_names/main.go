// TEMPORARY, read-only: the Avro record names each engine writes in its manifest list (the
// `partitions` element) and manifest files, read from the files' own header schema.
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	_ "github.com/apache/iceberg-go/io/gocloud/azure"
)

// The schema JSON from an Avro object container file's header.
func header(r io.Reader) (string, error) {
	br := bufio.NewReader(r)
	magic := make([]byte, 4)
	if _, err := io.ReadFull(br, magic); err != nil || string(magic) != "Obj\x01" {
		return "", fmt.Errorf("not an avro file")
	}
	str := func() (string, error) {
		n, err := binary.ReadVarint(br)
		if err != nil {
			return "", err
		}
		b := make([]byte, n)
		_, err = io.ReadFull(br, b)
		return string(b), err
	}
	for {
		count, err := binary.ReadVarint(br)
		if err != nil {
			return "", err
		}
		if count == 0 {
			return "", fmt.Errorf("no avro.schema")
		}
		if count < 0 {
			count = -count
			if _, err := binary.ReadVarint(br); err != nil {
				return "", err
			}
		}
		for i := int64(0); i < count; i++ {
			k, err := str()
			if err != nil {
				return "", err
			}
			v, err := str()
			if err != nil {
				return "", err
			}
			if k == "avro.schema" {
				return v, nil
			}
		}
	}
}

// Every record name in the schema, with the field path it sits at.
func records(node any, path string, out *[]string) {
	switch n := node.(type) {
	case map[string]any:
		if n["type"] == "record" {
			*out = append(*out, fmt.Sprintf("%s=%v", path, n["name"]))
		}
		if fs, ok := n["fields"].([]any); ok {
			for _, f := range fs {
				fm := f.(map[string]any)
				records(fm["type"], path+"."+fmt.Sprint(fm["name"]), out)
			}
		}
		for _, k := range []string{"items", "values"} {
			if v, ok := n[k]; ok {
				records(v, path+"[]", out)
			}
		}
		if t, ok := n["type"].(map[string]any); ok {
			records(t, path, out)
		}
		if t, ok := n["type"].([]any); ok {
			records(t, path, out)
		}
	case []any:
		for _, u := range n {
			records(u, path, out)
		}
	}
}

func main() {
	ctx := context.Background()
	tok := os.Getenv("ONELAKE_TOKEN")
	cat, err := rest.NewCatalog(ctx, "onelake", "https://onelake.table.fabric.microsoft.com/iceberg",
		rest.WithOAuthToken(tok), rest.WithWarehouseLocation(os.Getenv("WAREHOUSE")),
		rest.WithAdditionalProps(iceberg.Properties{"adls.token": tok, "adls.endpoint": "blob.fabric.microsoft.com"}))
	if err != nil {
		panic(err)
	}
	for _, name := range []string{"dbt_duckdb_mart.fct_summary", "dbt_polars_mart.fct_summary", "dbt_trino_mart.fct_summary", "dbt_gluten_mart.fct_summary", "dbt_sail_mart.fct_summary"} {
		tbl, err := cat.LoadTable(ctx, catalog.ToIdentifier(name))
		if err != nil {
			fmt.Printf("%s: load: %v\n", name, err)
			continue
		}
		snap := tbl.CurrentSnapshot()
		summary := ""
		if snap.Summary != nil {
			summary = snap.Summary.Properties["engine-name"] + " " + snap.Summary.Properties["engine-version"] + " " + snap.Summary.Properties["iceberg-version"]
		}
		fmt.Printf("\n%s (format v%d; writer: %s)\n", name, tbl.Metadata().Version(), strings.TrimSpace(summary))
		fs, _ := tbl.FS(ctx)
		f, err := fs.Open(snap.ManifestList)
		if err != nil {
			fmt.Println("  open:", err)
			continue
		}
		s, err := header(f)
		f.Close()
		if err != nil {
			fmt.Println("  header:", err)
			continue
		}
		var node any
		_ = json.Unmarshal([]byte(s), &node)
		var out []string
		records(node, "manifest_list", &out)
		fmt.Println("  manifest list records:", strings.Join(out, ", "))
	}
}
