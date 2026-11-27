package config

import (
	"testing"
	"time"
)

func TestLoadDurabilityDefaults(t *testing.T) {
	t.Setenv("PG_SYNCHRONOUS_COMMIT", "")
	t.Setenv("PG_UNLOGGED_TABLES", "")
	c := Load()
	if c.PGSynchronousCommit != "" || c.PGUnloggedTables {
		t.Fatalf("expected defaults off, got %q %v", c.PGSynchronousCommit, c.PGUnloggedTables)
	}
}

func TestLoadDurabilityEnv(t *testing.T) {
	t.Setenv("PG_SYNCHRONOUS_COMMIT", "off")
	t.Setenv("PG_UNLOGGED_TABLES", "true")
	c := Load()
	if c.PGSynchronousCommit != "off" || !c.PGUnloggedTables {
		t.Fatalf("got %q %v", c.PGSynchronousCommit, c.PGUnloggedTables)
	}
}

func TestLoadCleanupInterval(t *testing.T) {
	t.Setenv("CLEANUP_INTERVAL", "")
	if got := Load().CleanupInterval; got != time.Second {
		t.Fatalf("default CleanupInterval = %v, want 1s", got)
	}
	t.Setenv("CLEANUP_INTERVAL", "30s")
	if got := Load().CleanupInterval; got != 30*time.Second {
		t.Fatalf("CleanupInterval = %v, want 30s", got)
	}
}
