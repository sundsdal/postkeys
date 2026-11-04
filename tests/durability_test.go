//go:build postgres
// +build postgres

package integration

import (
	"context"
	"net"
	"testing"

	"github.com/mnorrsken/postkeys/internal/handler"
	"github.com/mnorrsken/postkeys/internal/server"
	"github.com/mnorrsken/postkeys/internal/storage"
	"github.com/redis/go-redis/v9"
)

var durabilityTables = []string{
	"kv_strings", "kv_hashes", "kv_lists", "kv_sets", "kv_zsets", "kv_meta", "kv_hyperloglog",
}

func durabilityCfg(syncCommit string, unlogged bool) storage.Config {
	return storage.Config{
		Host:              getEnvOrDefault("PG_HOST", "localhost"),
		Port:              5789,
		User:              getEnvOrDefault("PG_USER", "postgres"),
		Password:          getEnvOrDefault("PG_PASSWORD", "testingpassword"),
		Database:          getEnvOrDefault("PG_DATABASE", "postgres"),
		SSLMode:           getEnvOrDefault("PG_SSLMODE", "disable"),
		SynchronousCommit: syncCommit,
		UnloggedTables:    unlogged,
	}
}

func TestSynchronousCommitSession(t *testing.T) {
	ctx := context.Background()
	st, err := storage.New(ctx, durabilityCfg("off", false))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var v string
	if err := st.Pool().QueryRow(ctx, "SHOW synchronous_commit").Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != "off" {
		t.Fatalf("synchronous_commit = %q, want off", v)
	}
}

func TestSynchronousCommitInvalid(t *testing.T) {
	st, err := storage.New(context.Background(), durabilityCfg("sometimes", false))
	if err == nil {
		st.Close()
		t.Fatal("expected error for invalid synchronous_commit")
	}
}

func persistence(t *testing.T, st *storage.Store) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, tbl := range durabilityTables {
		var p string
		if err := st.Pool().QueryRow(context.Background(),
			"SELECT relpersistence::text FROM pg_class WHERE oid = to_regclass($1)", tbl).Scan(&p); err != nil {
			t.Fatalf("%s: %v", tbl, err)
		}
		out[tbl] = p
	}
	return out
}

func expectPersistence(t *testing.T, cfg storage.Config, want string) *storage.Store {
	t.Helper()
	st, err := storage.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for tbl, p := range persistence(t, st) {
		if p != want {
			t.Errorf("%s relpersistence = %q, want %q", tbl, p, want)
		}
	}
	return st
}

func TestUnloggedTablesConvergence(t *testing.T) {
	// Always leave the DB logged for other tests.
	defer func() {
		if st, err := storage.New(context.Background(), durabilityCfg("", false)); err == nil {
			st.Close()
		}
	}()

	expectPersistence(t, durabilityCfg("", true), "u").Close()
	expectPersistence(t, durabilityCfg("", false), "p").Close()
	expectPersistence(t, durabilityCfg("", true), "u").Close()
	expectPersistence(t, durabilityCfg("", false), "p").Close()
}

func TestUnloggedTablesConcurrentStart(t *testing.T) {
	defer func() {
		if st, err := storage.New(context.Background(), durabilityCfg("", false)); err == nil {
			st.Close()
		}
	}()
	expectPersistence(t, durabilityCfg("", true), "u").Close()

	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() {
			st, err := storage.New(context.Background(), durabilityCfg("", false))
			if err == nil {
				st.Close()
			}
			errs <- err
		}()
	}
	for i := 0; i < 4; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent start: %v", err)
		}
	}
	expectPersistence(t, durabilityCfg("", false), "p").Close()
}

func TestSetGetWithDurabilityToggles(t *testing.T) {
	defer func() {
		if st, err := storage.New(context.Background(), durabilityCfg("", false)); err == nil {
			st.Close()
		}
	}()
	ctx := context.Background()
	st, err := storage.New(ctx, durabilityCfg("off", true))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(listener.Addr().String(), handler.New(st, ""))
	go func() { _ = srv.ServeWithListener(listener) }()
	defer srv.Stop()

	client := redis.NewClient(&redis.Options{Addr: listener.Addr().String()})
	defer client.Close()
	if err := client.Set(ctx, "dur:k", "v", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got, err := client.Get(ctx, "dur:k").Result(); err != nil || got != "v" {
		t.Fatalf("get = %q, %v", got, err)
	}
	client.Del(ctx, "dur:k")
}
