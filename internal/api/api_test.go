package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	_ "github.com/duckdb/duckdb-go/v2"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func TestQueryExecuteIntegration(t *testing.T) {
	ctx := context.Background()

	t.Run("create insert and query", func(t *testing.T) {
		dsn := "test_integration.db"
		t.Cleanup(func() { os.Remove(dsn) })

		// Create
		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_integration (id INTEGER, name VARCHAR, score DOUBLE)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Insert
		_, err = Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: "INSERT INTO test_integration VALUES (?, ?, ?)", Args: []any{1, "test", 95.5}},
			},
		})
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		// Query
		result, err := Query(ctx, dsn, ducktape.QueryRequest{
			Query: "SELECT * FROM test_integration WHERE id = ?",
			Args:  []any{1},
		})
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if len(result) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result))
		}

		if result[0]["name"] != "test" {
			t.Errorf("expected name='test', got %v", result[0]["name"])
		}
	})
}

func TestContextCancellation(t *testing.T) {
	t.Run("Execute with cancelled context", func(t *testing.T) {
		dsn := "test_context_cancel_exec.db"
		t.Cleanup(func() { os.Remove(dsn) })

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		// Create a table first
		_, err := Execute(context.Background(), dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: "CREATE TABLE test_cancel (id INTEGER)"},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// This may or may not fail depending on timing, but should not panic
		_, _ = Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: "INSERT INTO test_cancel VALUES (1)"},
			},
		})
	})

	t.Run("Query with cancelled context", func(t *testing.T) {
		dsn := "test_context_cancel_query.db"
		t.Cleanup(func() { os.Remove(dsn) })

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		// This may or may not fail depending on timing, but should not panic
		_, _ = Query(ctx, dsn, ducktape.QueryRequest{
			Query: "SELECT 1",
		})
	})
}

func TestBasicAuth(t *testing.T) {
	// Set environment variables for Basic Auth
	t.Setenv("DUCKTAPE_USERNAME", "admin")
	t.Setenv("DUCKTAPE_PASSWORD", "secret")

	mux := http.NewServeMux()
	RegisterApiRoutes(mux)

	// Create a test server with h2c support
	h2cHandler := h2c.NewHandler(mux, &http2.Server{})
	server := httptest.NewServer(h2cHandler)
	defer server.Close()

	client := ducktape.NewClient(server.URL)

	// 1. Request without Basic Auth should fail
	ctx := context.Background()
	err := client.Ping(ctx, ":memory:")
	if err == nil {
		t.Fatal("expected unauthorized error, got nil")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Fatalf("expected 401 error, got: %v", err)
	}

	// 2. Request with incorrect Basic Auth should fail
	client.SetBasicAuth("admin", "wrong_password")
	err = client.Ping(ctx, ":memory:")
	if err == nil {
		t.Fatal("expected unauthorized error with wrong password, got nil")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Fatalf("expected 401 error with wrong password, got: %v", err)
	}

	// 3. Request with correct Basic Auth should succeed
	client.SetBasicAuth("admin", "secret")
	err = client.Ping(ctx, ":memory:")
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
}
