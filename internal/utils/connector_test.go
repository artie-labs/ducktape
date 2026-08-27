package utils

import (
	"database/sql"
	"os"
	"strings"
	"sync"
	"testing"
)

func TestMain(m *testing.M) {
	// Disable caching by default for tests to maintain isolation
	SetCacheEnabled(false)
	os.Exit(m.Run())
}

func TestParseDSN(t *testing.T) {
	t.Run("no query params", func(t *testing.T) {
		target, result := parseDSN("/path/to/db")
		if target != "" {
			t.Errorf("target = %q, want %q", target, "")
		}
		if len(result) != 0 {
			t.Errorf("expected empty, got %v", result)
		}
	})

	t.Run("single param", func(t *testing.T) {
		target, result := parseDSN("/path/to/db?motherduck_token=abc123")
		if target != "/path/to/db" {
			t.Errorf("target = %q, want %q", target, "/path/to/db")
		}
		if got := result.Get("motherduck_token"); got != "abc123" {
			t.Errorf("motherduck_token = %q, want %q", got, "abc123")
		}
	})

	t.Run("multiple params", func(t *testing.T) {
		target, result := parseDSN(":memory:?motherduck_token=abc123&threads=4")
		if target != ":memory:" {
			t.Errorf("target = %q, want %q", target, ":memory:")
		}
		if got := result.Get("motherduck_token"); got != "abc123" {
			t.Errorf("motherduck_token = %q, want %q", got, "abc123")
		}
		if got := result.Get("threads"); got != "4" {
			t.Errorf("threads = %q, want %q", got, "4")
		}
	})

	t.Run("empty query string", func(t *testing.T) {
		target, result := parseDSN("/path/to/db?")
		if target != "/path/to/db" {
			t.Errorf("target = %q, want %q", target, "/path/to/db")
		}
		if len(result) != 0 {
			t.Errorf("expected empty, got %v", result)
		}
	})
}

func TestEscapeSQLString(t *testing.T) {
	t.Run("no quotes", func(t *testing.T) {
		if got := escapeSQLString("no quotes"); got != "no quotes" {
			t.Errorf("got %q, want %q", got, "no quotes")
		}
	})

	t.Run("single quote", func(t *testing.T) {
		if got := escapeSQLString("it's"); got != "it''s" {
			t.Errorf("got %q, want %q", got, "it''s")
		}
	})

	t.Run("already escaped quotes", func(t *testing.T) {
		if got := escapeSQLString("a''b"); got != "a''''b" {
			t.Errorf("got %q, want %q", got, "a''''b")
		}
	})

	t.Run("only a quote", func(t *testing.T) {
		if got := escapeSQLString("'"); got != "''" {
			t.Errorf("got %q, want %q", got, "''")
		}
	})

	t.Run("empty string", func(t *testing.T) {
		if got := escapeSQLString(""); got != "" {
			t.Errorf("got %q, want %q", got, "")
		}
	})
}

func TestNewConnector_NoToken(t *testing.T) {
	connector, err := NewConnector(t.Context(), "")
	if err != nil {
		t.Fatalf("NewConnector(%q) returned error: %v", "", err)
	}
	defer connector.Close()

	if connector.GetDB() == nil {
		t.Fatal("expected non-nil *sql.DB")
	}
}

func TestNewConnector_TokenEscaping(t *testing.T) {
	connector, err := NewConnector(t.Context(), ":memory:?motherduck_token=abc'def")
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "unterminated") || strings.Contains(errMsg, "syntax error") {
			t.Errorf("error message %q suggests SQL injection", errMsg)
		}
	} else {
		connector.Close()
	}
}

func TestConnectorCaching(t *testing.T) {
	ctx := t.Context()
	dsn := "test_caching.db"
	t.Cleanup(func() {
		SetCacheEnabled(false)
		ClearCache()
		os.Remove(dsn)
	})

	SetCacheEnabled(true)
	ClearCache()

	// 1. First call should create a new connection and cache it
	conn1, err := NewConnector(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to create conn1: %v", err)
	}
	defer conn1.Close()

	if !conn1.isCached {
		t.Error("expected conn1 to be marked as cached")
	}

	// 2. Second call should return the cached connection
	conn2, err := NewConnector(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to create conn2: %v", err)
	}
	defer conn2.Close()

	if !conn2.isCached {
		t.Error("expected conn2 to be marked as cached")
	}

	if conn1.GetDB() != conn2.GetDB() {
		t.Error("expected conn1 and conn2 to share the same *sql.DB instance")
	}

	// 3. ClearCache should empty the cache and close the DB
	if err := ClearCache(); err != nil {
		t.Fatalf("failed to clear cache: %v", err)
	}

	// 4. Third call after clearing cache should create a new DB instance
	conn3, err := NewConnector(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to create conn3: %v", err)
	}
	defer conn3.Close()

	if conn3.GetDB() == conn1.GetDB() {
		t.Error("expected conn3 to have a different *sql.DB instance after cache clear")
	}
}

func TestConnectorConcurrency(t *testing.T) {
	ctx := t.Context()
	dsn := "test_concurrency.db"
	t.Cleanup(func() {
		SetCacheEnabled(false)
		ClearCache()
		os.Remove(dsn)
	})

	SetCacheEnabled(true)
	ClearCache()

	const numGoroutines = 10
	conns := make([]*Connector, numGoroutines)
	errs := make([]error, numGoroutines)

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			conns[index], errs[index] = NewConnector(ctx, dsn)
		}(i)
	}
	wg.Wait()

	// Verify all succeeded and returned the exact same DB instance
	var firstDB *sql.DB
	for i := 0; i < numGoroutines; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d failed to get connector: %v", i, errs[i])
		}
		if conns[i] == nil {
			t.Fatalf("goroutine %d returned nil connector", i)
		}
		defer conns[i].Close()

		if firstDB == nil {
			firstDB = conns[i].GetDB()
		} else if conns[i].GetDB() != firstDB {
			t.Errorf("goroutine %d returned a different DB instance", i)
		}
	}
}

func BenchmarkConnectorWithAndWithoutCache(b *testing.B) {
	ctx := b.Context()
	dsn := "benchmark_connector.db"
	b.Cleanup(func() {
		SetCacheEnabled(false)
		ClearCache()
		os.Remove(dsn)
	})

	b.Run("Without Cache", func(b *testing.B) {
		SetCacheEnabled(false)
		ClearCache()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conn, err := NewConnector(ctx, dsn)
			if err != nil {
				b.Fatalf("failed to open: %v", err)
			}
			conn.Close()
		}
	})

	b.Run("With Cache", func(b *testing.B) {
		SetCacheEnabled(true)
		ClearCache()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conn, err := NewConnector(ctx, dsn)
			if err != nil {
				b.Fatalf("failed to open: %v", err)
			}
			conn.Close()
		}
	})
}

func TestConnectorMaxOpenConns(t *testing.T) {
	ctx := t.Context()
	dsn := "test_max_open_conns.db"
	t.Cleanup(func() {
		SetCacheEnabled(false)
		ClearCache()
		os.Remove(dsn)
	})

	SetCacheEnabled(true)
	ClearCache()

	conn, err := NewConnector(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to create connector: %v", err)
	}
	defer conn.Close()

	stats := conn.GetDB().Stats()
	if stats.MaxOpenConnections != 1 {
		t.Errorf("expected MaxOpenConnections to be 1, got %d", stats.MaxOpenConnections)
	}
}
