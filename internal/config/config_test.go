package config

import "testing"

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
