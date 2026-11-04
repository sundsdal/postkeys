//go:build postgres
// +build postgres

package integration

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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

// conversionSetup leaves the DB UNLOGGED (so a LOGGED start needs conversion)
// and restores LOGGED tables when the test ends.
func conversionSetup(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		if st, err := storage.New(context.Background(), durabilityCfg("", false)); err == nil {
			st.Close()
		}
	})
	expectPersistence(t, durabilityCfg("", true), "u").Close()
}

func durabilityConn(t *testing.T) *pgx.Conn {
	t.Helper()
	c := durabilityCfg("", false)
	dsn := fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=%s sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.Database, c.SSLMode)
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

type newResult struct {
	st  *storage.Store
	err error
}

func startNew(cfg storage.Config) <-chan newResult {
	ch := make(chan newResult, 1)
	go func() {
		st, err := storage.New(context.Background(), cfg)
		ch <- newResult{st, err}
	}()
	return ch
}

func TestPersistenceConversionRetriesWhileTableBusy(t *testing.T) {
	conversionSetup(t)
	ctx := context.Background()
	holder := durabilityConn(t)
	tx, err := holder.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "LOCK TABLE kv_strings IN ACCESS SHARE MODE"); err != nil {
		t.Fatal(err)
	}

	res := startNew(durabilityCfg("", false))
	select {
	case r := <-res:
		if r.st != nil {
			r.st.Close()
		}
		t.Fatalf("store finished while table was locked (err=%v)", r.err)
	case <-time.After(1500 * time.Millisecond):
	}

	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-res:
		if r.err != nil {
			t.Fatalf("store failed after lock release: %v", r.err)
		}
		defer r.st.Close()
		for tbl, p := range persistence(t, r.st) {
			if p != "p" {
				t.Errorf("%s relpersistence = %q, want p", tbl, p)
			}
		}
	case <-time.After(15 * time.Second):
		t.Fatal("store did not come up after lock release")
	}
}

// A live command reads kv_strings and then writes kv_meta. If the converter
// held ACCESS EXCLUSIVE on kv_meta while waiting for kv_strings, this would
// deadlock. With LOCK NOWAIT the converter holds nothing while it waits.
func TestPersistenceConversionNoDeadlockWithWriter(t *testing.T) {
	conversionSetup(t)
	ctx := context.Background()
	writer := durabilityConn(t)
	tx, err := writer.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var n int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM kv_strings").Scan(&n); err != nil {
		t.Fatal(err)
	}

	res := startNew(durabilityCfg("", false))
	// Give the converter time to attempt (and fail) its lock; it must be
	// retrying, not finished.
	select {
	case r := <-res:
		if r.st != nil {
			r.st.Close()
		}
		t.Fatalf("store finished while writer held kv_strings (err=%v)", r.err)
	case <-time.After(800 * time.Millisecond):
	}

	if _, err := tx.Exec(ctx,
		"INSERT INTO kv_meta (key, key_type) VALUES ('dl:k', 'string') ON CONFLICT (key) DO NOTHING"); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
			t.Fatalf("writer hit deadlock_detected: %v", err)
		}
		t.Fatalf("writer insert: %v", err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM kv_meta WHERE key = 'dl:k'"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("writer commit: %v", err)
	}

	select {
	case r := <-res:
		if r.err != nil {
			var pgErr *pgconn.PgError
			if errors.As(r.err, &pgErr) && pgErr.Code == "40P01" {
				t.Fatalf("conversion hit deadlock_detected: %v", r.err)
			}
			t.Fatalf("conversion failed: %v", r.err)
		}
		defer r.st.Close()
		for tbl, p := range persistence(t, r.st) {
			if p != "p" {
				t.Errorf("%s relpersistence = %q, want p", tbl, p)
			}
		}
	case <-time.After(15 * time.Second):
		t.Fatal("conversion did not finish after writer committed")
	}
}

func TestPersistenceConversionBudgetExhausted(t *testing.T) {
	conversionSetup(t)
	ctx := context.Background()
	holder := durabilityConn(t)
	tx, err := holder.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "LOCK TABLE kv_hashes IN ACCESS SHARE MODE"); err != nil {
		t.Fatal(err)
	}

	cfg := durabilityCfg("", false)
	cfg.PersistenceLockBudget = 500 * time.Millisecond
	st, err := storage.New(ctx, cfg)
	if err == nil {
		st.Close()
		t.Fatal("expected busy error")
	}
	if !strings.Contains(err.Error(), "stayed busy") {
		t.Fatalf("unexpected error: %v", err)
	}
	// Tables must be untouched by the failed attempt.
	var p string
	if err := holder.QueryRow(ctx, "SELECT relpersistence::text FROM pg_class WHERE oid = to_regclass('kv_meta')").Scan(&p); err != nil {
		t.Fatal(err)
	}
	if p != "u" {
		t.Errorf("kv_meta relpersistence = %q, want u (no partial conversion)", p)
	}
}
