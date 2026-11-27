module github.com/djouallah/lakehouse_benchmark/capability_iceberg_go

go 1.26.3

// The same iceberg-go as table maintenance (.github/scripts/maintain/go.mod): the column shows
// what the library the workload's maintenance runs on can do. capability.yml runs `go mod tidy`.
require (
	github.com/apache/arrow-go/v18 v18.9.0
	github.com/apache/iceberg-go v0.8.0-rc0
)
