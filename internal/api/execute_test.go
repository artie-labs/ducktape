package api

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	_ "github.com/duckdb/duckdb-go/v2"
)

func TestExecute(t *testing.T) {
	ctx := context.Background()

	t.Run("create table", func(t *testing.T) {
		dsn := "test_execute_create.db"
		t.Cleanup(func() { os.Remove(dsn) })

		result, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_execute_create (id INTEGER, name VARCHAR)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to execute CREATE TABLE: %v", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			t.Fatalf("failed to get rows affected: %v", err)
		}

		if rowsAffected != 0 {
			t.Errorf("expected 0 rows affected for CREATE TABLE, got %d", rowsAffected)
		}
	})

	t.Run("insert data", func(t *testing.T) {
		dsn := "test_execute_insert.db"
		t.Cleanup(func() { os.Remove(dsn) })

		// Create table
		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_execute_insert (id INTEGER, name VARCHAR, age INTEGER)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Insert single row
		result, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: "INSERT INTO test_execute_insert VALUES (?, ?, ?)", Args: []any{1, "Alice", 30}},
			},
		})
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			t.Fatalf("failed to get rows affected: %v", err)
		}

		if rowsAffected != 1 {
			t.Errorf("expected 1 row affected, got %d", rowsAffected)
		}
	})

	t.Run("insert multiple rows", func(t *testing.T) {
		dsn := "test_execute_multi.db"
		t.Cleanup(func() { os.Remove(dsn) })

		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_execute_multi (id INTEGER, value VARCHAR)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		result, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `INSERT INTO test_execute_multi VALUES (1, 'a'), (2, 'b'), (3, 'c')`},
			},
		})
		if err != nil {
			t.Fatalf("failed to insert multiple rows: %v", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			t.Fatalf("failed to get rows affected: %v", err)
		}

		if rowsAffected != 3 {
			t.Errorf("expected 3 rows affected, got %d", rowsAffected)
		}
	})

	t.Run("update data", func(t *testing.T) {
		dsn := "test_execute_update.db"
		t.Cleanup(func() { os.Remove(dsn) })

		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_execute_update (id INTEGER, status VARCHAR)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Insert data
		_, err = Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `INSERT INTO test_execute_update VALUES (1, 'pending'), (2, 'pending'), (3, 'done')`},
			},
		})
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		// Update rows
		result, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: "UPDATE test_execute_update SET status = ? WHERE status = ?", Args: []any{"completed", "pending"}},
			},
		})
		if err != nil {
			t.Fatalf("failed to update: %v", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			t.Fatalf("failed to get rows affected: %v", err)
		}

		if rowsAffected != 2 {
			t.Errorf("expected 2 rows affected, got %d", rowsAffected)
		}
	})

	t.Run("delete data", func(t *testing.T) {
		dsn := "test_execute_delete.db"
		t.Cleanup(func() { os.Remove(dsn) })

		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_execute_delete (id INTEGER, active BOOLEAN)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Insert data
		_, err = Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `INSERT INTO test_execute_delete VALUES (1, true), (2, false), (3, false)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		// Delete rows
		result, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: "DELETE FROM test_execute_delete WHERE active = ?", Args: []any{false}},
			},
		})
		if err != nil {
			t.Fatalf("failed to delete: %v", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			t.Fatalf("failed to get rows affected: %v", err)
		}

		if rowsAffected != 2 {
			t.Errorf("expected 2 rows affected, got %d", rowsAffected)
		}
	})

	t.Run("invalid SQL", func(t *testing.T) {
		_, err := Execute(ctx, "", ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: "INVALID SQL STATEMENT"},
			},
		})
		if err == nil {
			t.Error("expected error for invalid SQL, got none")
		}
		if !strings.Contains(err.Error(), "failed to execute") {
			t.Errorf("expected error to mention execution failure, got: %v", err)
		}
	})

	t.Run("parameterized query with args", func(t *testing.T) {
		dsn := "test_execute_params.db"
		t.Cleanup(func() { os.Remove(dsn) })

		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_execute_params (id INTEGER, name VARCHAR, created_at TIMESTAMP)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		now := time.Now()
		result, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: "INSERT INTO test_execute_params VALUES (?, ?, ?)", Args: []any{1, "test", now}},
			},
		})
		if err != nil {
			t.Fatalf("failed to insert with params: %v", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			t.Fatalf("failed to get rows affected: %v", err)
		}

		if rowsAffected != 1 {
			t.Errorf("expected 1 row affected, got %d", rowsAffected)
		}
	})

	t.Run("invalid DSN", func(t *testing.T) {
		_, err := Execute(ctx, "invalid://dsn", ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: "SELECT 1"},
			},
		})
		if err == nil {
			t.Error("expected error for invalid DSN, got none")
		}
	})

	t.Run("multiple statements in single transaction", func(t *testing.T) {
		dsn := "test_execute_multi_statements.db"
		t.Cleanup(func() { os.Remove(dsn) })

		// Execute multiple statements in a single transaction
		result, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_multi_stmt (id INTEGER, status VARCHAR, count INTEGER)`},
				{Query: `INSERT INTO test_multi_stmt VALUES (1, 'pending', 10), (2, 'pending', 20), (3, 'done', 30)`},
				{Query: `UPDATE test_multi_stmt SET status = ? WHERE status = ?`, Args: []any{"completed", "pending"}},
				{Query: `DELETE FROM test_multi_stmt WHERE id = ?`, Args: []any{3}},
			},
		})
		if err != nil {
			t.Fatalf("failed to execute multiple statements: %v", err)
		}

		// Total rows affected should be:
		// CREATE: 0, INSERT: 3, UPDATE: 2, DELETE: 1 = 6 total
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			t.Fatalf("failed to get rows affected: %v", err)
		}

		if rowsAffected != 6 {
			t.Errorf("expected 6 total rows affected (0+3+2+1), got %d", rowsAffected)
		}

		// Verify the final state of the table
		rows, err := Query(ctx, dsn, ducktape.QueryRequest{
			Query: "SELECT * FROM test_multi_stmt ORDER BY id",
		})
		if err != nil {
			t.Fatalf("failed to query after multi-statement execution: %v", err)
		}

		// Should have 2 rows remaining (id 1 and 2, both with status 'completed')
		if len(rows) != 2 {
			t.Errorf("expected 2 rows remaining, got %d", len(rows))
		}

		if rows[0]["status"] != "completed" {
			t.Errorf("expected first row status='completed', got %v", rows[0]["status"])
		}

		if rows[1]["status"] != "completed" {
			t.Errorf("expected second row status='completed', got %v", rows[1]["status"])
		}
	})

	t.Run("multiple statements rollback on error", func(t *testing.T) {
		dsn := "test_execute_rollback.db"
		t.Cleanup(func() { os.Remove(dsn) })

		// First, create a table successfully
		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_rollback (id INTEGER, value VARCHAR)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Try to execute multiple statements where the last one fails
		// This should rollback the entire transaction
		_, err = Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `INSERT INTO test_rollback VALUES (1, 'first')`},
				{Query: `INSERT INTO test_rollback VALUES (2, 'second')`},
				{Query: `INVALID SQL THAT WILL FAIL`}, // This should cause rollback
			},
		})
		if err == nil {
			t.Fatal("expected error for invalid SQL, got none")
		}

		// Verify the table is empty (transaction was rolled back)
		rows, err := Query(ctx, dsn, ducktape.QueryRequest{
			Query: "SELECT * FROM test_rollback",
		})
		if err != nil {
			t.Fatalf("failed to query after rollback: %v", err)
		}

		if len(rows) != 0 {
			t.Errorf("expected 0 rows (transaction rolled back), got %d", len(rows))
		}
	})
}
