module github.com/djouallah/lakehouse_benchmark/maintain

go 1.26.3

// apache/iceberg-go main: the Azure blob listing the orphan pass needs (#2187, #2218) is after
// the v0.7.0 release. Back to a release tag once one carries it.
require github.com/apache/iceberg-go v0.7.1-0.20261023230220-b7792e9e8799
