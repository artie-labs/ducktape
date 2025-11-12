package api

import (
	"context"
	"os"
	"testing"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	_ "github.com/duckdb/duckdb-go/v2"
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
