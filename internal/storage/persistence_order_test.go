package storage

import (
	"slices"
	"testing"
)

// Lock order is no longer needed for deadlock safety (convertPersistence uses
// LOCK ... NOWAIT), but dataTables keeps the write-path order for consistency.
func TestDataTablesOrder(t *testing.T) {
	want := []string{"kv_meta", "kv_strings", "kv_hashes", "kv_lists", "kv_sets", "kv_zsets", "kv_hyperloglog"}
	if !slices.Equal(dataTables, want) {
		t.Fatalf("dataTables = %v, want %v", dataTables, want)
	}
}
