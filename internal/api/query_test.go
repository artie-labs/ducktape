package api

import (
	"context"
	"os"
	"testing"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	_ "github.com/duckdb/duckdb-go/v2"
)

func TestQuery(t *testing.T) {
	ctx := context.Background()
	dsn := "test_query.db"
	t.Cleanup(func() { os.Remove(dsn) })

	// Setup: Create a table with test data
	_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
		Query: `CREATE TABLE test_query (
			id INTEGER,
			name VARCHAR,
			age INTEGER,
			active BOOLEAN,
			created_at TIMESTAMP
		)`,
	})
	if err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}
	defer Execute(ctx, dsn, ducktape.ExecuteRequest{Query: "DROP TABLE test_query"})

	_, err = Execute(ctx, dsn, ducktape.ExecuteRequest{
		Query: `INSERT INTO test_query VALUES
			(1, 'Alice', 30, true, '2024-01-15 10:00:00'),
			(2, 'Bob', 25, false, '2024-02-20 14:30:00'),
			(3, 'Charlie', 35, true, '2024-03-10 09:15:00')`,
	})
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	t.Run("select all rows", func(t *testing.T) {
		result, err := Query(ctx, dsn, ducktape.QueryRequest{
			Query: "SELECT * FROM test_query ORDER BY id",
		})
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if len(result) != 3 {
			t.Errorf("expected 3 rows, got %d", len(result))
		}

		if result[0]["name"] != "Alice" {
			t.Errorf("expected first row name=Alice, got %v", result[0]["name"])
		}

		if result[1]["id"] != int32(2) {
			t.Errorf("expected second row id=2, got %v", result[1]["id"])
		}
	})

	t.Run("select with WHERE clause", func(t *testing.T) {
		result, err := Query(ctx, dsn, ducktape.QueryRequest{
			Query: "SELECT * FROM test_query WHERE active = true",
		})
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if len(result) != 2 {
			t.Errorf("expected 2 rows, got %d", len(result))
		}

		for _, row := range result {
			if row["active"] != true {
				t.Errorf("expected active=true, got %v", row["active"])
			}
		}
	})

	t.Run("select with parameterized query", func(t *testing.T) {
		result, err := Query(ctx, dsn, ducktape.QueryRequest{
			Query: "SELECT * FROM test_query WHERE id = ?",
			Args:  []any{2},
		})
		if err != nil {
			t.Fatalf("failed to query with params: %v", err)
		}

		if len(result) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result))
		}

		if result[0]["name"] != "Bob" {
			t.Errorf("expected name=Bob, got %v", result[0]["name"])
		}
	})

	t.Run("select specific columns", func(t *testing.T) {
		result, err := Query(ctx, dsn, ducktape.QueryRequest{
			Query: "SELECT id, name FROM test_query ORDER BY id",
		})
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if len(result) != 3 {
			t.Errorf("expected 3 rows, got %d", len(result))
		}

		if len(result[0]) != 2 {
			t.Errorf("expected 2 columns, got %d", len(result[0]))
		}

		if _, exists := result[0]["id"]; !exists {
			t.Error("expected 'id' column to exist")
		}

		if _, exists := result[0]["name"]; !exists {
			t.Error("expected 'name' column to exist")
		}

		if _, exists := result[0]["age"]; exists {
			t.Error("expected 'age' column to not exist")
		}
	})

	t.Run("aggregate query", func(t *testing.T) {
		result, err := Query(ctx, dsn, ducktape.QueryRequest{
			Query: "SELECT COUNT(*) as count, AVG(age) as avg_age FROM test_query",
		})
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if len(result) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result))
		}

		count, ok := result[0]["count"].(int64)
		if !ok {
			t.Errorf("expected count to be int64, got %T", result[0]["count"])
		}

		if count != 3 {
			t.Errorf("expected count=3, got %v", count)
		}
	})

	t.Run("empty result set", func(t *testing.T) {
		result, err := Query(ctx, dsn, ducktape.QueryRequest{
			Query: "SELECT * FROM test_query WHERE id = 999",
		})
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if len(result) != 0 {
			t.Errorf("expected 0 rows, got %d", len(result))
		}
	})

	t.Run("query with ORDER BY", func(t *testing.T) {
		result, err := Query(ctx, dsn, ducktape.QueryRequest{
			Query: "SELECT name FROM test_query ORDER BY age DESC",
		})
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if len(result) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(result))
		}

		if result[0]["name"] != "Charlie" {
			t.Errorf("expected first name=Charlie (age 35), got %v", result[0]["name"])
		}

		if result[2]["name"] != "Bob" {
			t.Errorf("expected last name=Bob (age 25), got %v", result[2]["name"])
		}
	})

	t.Run("query with LIMIT", func(t *testing.T) {
		result, err := Query(ctx, dsn, ducktape.QueryRequest{
			Query: "SELECT * FROM test_query LIMIT 2",
		})
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if len(result) != 2 {
			t.Errorf("expected 2 rows, got %d", len(result))
		}
	})

	t.Run("invalid SQL", func(t *testing.T) {
		_, err := Query(ctx, "", ducktape.QueryRequest{
			Query: "INVALID SQL QUERY",
		})
		if err == nil {
			t.Error("expected error for invalid SQL, got none")
		}
	})

	t.Run("query non-existent table", func(t *testing.T) {
		_, err := Query(ctx, "", ducktape.QueryRequest{
			Query: "SELECT * FROM non_existent_table",
		})
		if err == nil {
			t.Error("expected error for non-existent table, got none")
		}
	})

	t.Run("query with NULL values", func(t *testing.T) {
		// Create temp table with NULL values
		nullDsn := "test_query_nulls.db"
		t.Cleanup(func() { os.Remove(nullDsn) })
		defer Execute(ctx, nullDsn, ducktape.ExecuteRequest{Query: "DROP TABLE IF EXISTS test_query_nulls"})

		_, err := Execute(ctx, nullDsn, ducktape.ExecuteRequest{
			Query: `CREATE TABLE test_query_nulls (id INTEGER, value VARCHAR)`,
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		_, err = Execute(ctx, nullDsn, ducktape.ExecuteRequest{
			Query: `INSERT INTO test_query_nulls VALUES (1, NULL), (2, 'test')`,
		})
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		result, err := Query(ctx, nullDsn, ducktape.QueryRequest{
			Query: "SELECT * FROM test_query_nulls ORDER BY id",
		})
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if result[0]["value"] != nil {
			t.Errorf("expected NULL value, got %v", result[0]["value"])
		}

		if result[1]["value"] != "test" {
			t.Errorf("expected value='test', got %v", result[1]["value"])
		}
	})

	t.Run("invalid DSN", func(t *testing.T) {
		_, err := Query(ctx, "invalid://dsn", ducktape.QueryRequest{
			Query: "SELECT 1",
		})
		if err == nil {
			t.Error("expected error for invalid DSN, got none")
		}
	})
}
