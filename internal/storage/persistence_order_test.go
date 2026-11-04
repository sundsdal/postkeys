package storage

import (
	"slices"
	"testing"
)

// The persistence conversion takes ACCESS EXCLUSIVE locks in dataTables order.
// Writers lock kv_meta first and then the data tables in this order, so the
// conversion must use the same order or it can deadlock with live traffic.
func TestDataTablesLockOrderMatchesWritePath(t *testing.T) {
	want := []string{"kv_meta", "kv_strings", "kv_hashes", "kv_lists", "kv_sets", "kv_zsets", "kv_hyperloglog"}
	if !slices.Equal(dataTables, want) {
		t.Fatalf("dataTables = %v, want %v (kv_meta first, matching deleteKeyFromAllTables)", dataTables, want)
	}
}
