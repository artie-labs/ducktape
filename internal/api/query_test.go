package api

import (
	"context"
	"os"
	"testing"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	"github.com/artie-labs/ducktape/internal/utils"
	_ "github.com/duckdb/duckdb-go/v2"
)

func TestQuery(t *testing.T) {
	ctx := context.Background()
	dsn := "test_query.db"
	t.Cleanup(func() { os.Remove(dsn) })

	// Setup: Create a table with test data
	_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
		Statements: []ducktape.ExecuteStatement{
			{Query: `CREATE TABLE test_query (
			id INTEGER,
			name VARCHAR,
			age INTEGER,
			active BOOLEAN,
			created_at TIMESTAMP
		)`},
		},
	})
	if err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	_, err = Execute(ctx, dsn, ducktape.ExecuteRequest{
		Statements: []ducktape.ExecuteStatement{
			{Query: `INSERT INTO test_query VALUES
			(1, 'Alice', 30, true, '2024-01-15 10:00:00'),
			(2, 'Bob', 25, false, '2024-02-20 14:30:00'),
			(3, 'Charlie', 35, true, '2024-03-10 09:15:00')`},
		},
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

		obj0 := result[0].(*utils.OrderedMap)
		name0, _ := obj0.Get("name")
		if name0 != "Alice" {
			t.Errorf("expected first row name=Alice, got %v", name0)
		}

		obj1 := result[1].(*utils.OrderedMap)
		id1, _ := obj1.Get("id")
		if id1 != int32(2) {
			t.Errorf("expected second row id=2, got %v", id1)
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
			obj := row.(*utils.OrderedMap)
			active, _ := obj.Get("active")
			if active != true {
				t.Errorf("expected active=true, got %v", active)
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

		obj := result[0].(*utils.OrderedMap)
		name, _ := obj.Get("name")
		if name != "Bob" {
			t.Errorf("expected name=Bob, got %v", name)
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

		obj := result[0].(*utils.OrderedMap)
		if _, exists := obj.Get("id"); !exists {
			t.Error("expected 'id' column to exist")
		}

		if _, exists := obj.Get("name"); !exists {
			t.Error("expected 'name' column to exist")
		}

		if _, exists := obj.Get("age"); exists {
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

		obj := result[0].(*utils.OrderedMap)
		countVal, _ := obj.Get("count")
		count, ok := countVal.(int64)
		if !ok {
			t.Errorf("expected count to be int64, got %T", countVal)
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

		obj0 := result[0].(*utils.OrderedMap)
		name0, _ := obj0.Get("name")
		if name0 != "Charlie" {
			t.Errorf("expected first name=Charlie (age 35), got %v", name0)
		}

		obj2 := result[2].(*utils.OrderedMap)
		name2, _ := obj2.Get("name")
		if name2 != "Bob" {
			t.Errorf("expected last name=Bob (age 25), got %v", name2)
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

		_, err := Execute(ctx, nullDsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_query_nulls (id INTEGER, value VARCHAR)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		_, err = Execute(ctx, nullDsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `INSERT INTO test_query_nulls VALUES (1, NULL), (2, 'test')`},
			},
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

		obj0 := result[0].(*utils.OrderedMap)
		value0, _ := obj0.Get("value")
		if value0 != nil {
			t.Errorf("expected NULL value, got %v", value0)
		}

		obj1 := result[1].(*utils.OrderedMap)
		value1, _ := obj1.Get("value")
		if value1 != "test" {
			t.Errorf("expected value='test', got %v", value1)
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
