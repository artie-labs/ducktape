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
		defer Execute(ctx, dsn, ducktape.ExecuteRequest{Query: "DROP TABLE IF EXISTS test_execute_create"})

		result, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Query: `CREATE TABLE test_execute_create (id INTEGER, name VARCHAR)`,
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
		defer Execute(ctx, dsn, ducktape.ExecuteRequest{Query: "DROP TABLE IF EXISTS test_execute_insert"})

		// Create table
		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Query: `CREATE TABLE test_execute_insert (id INTEGER, name VARCHAR, age INTEGER)`,
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Insert single row
		result, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Query: "INSERT INTO test_execute_insert VALUES (?, ?, ?)",
			Args:  []any{1, "Alice", 30},
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
		defer Execute(ctx, dsn, ducktape.ExecuteRequest{Query: "DROP TABLE IF EXISTS test_execute_multi"})

		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Query: `CREATE TABLE test_execute_multi (id INTEGER, value VARCHAR)`,
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		result, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Query: `INSERT INTO test_execute_multi VALUES (1, 'a'), (2, 'b'), (3, 'c')`,
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
		defer Execute(ctx, dsn, ducktape.ExecuteRequest{Query: "DROP TABLE IF EXISTS test_execute_update"})

		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Query: `CREATE TABLE test_execute_update (id INTEGER, status VARCHAR)`,
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Insert data
		_, err = Execute(ctx, dsn, ducktape.ExecuteRequest{
			Query: `INSERT INTO test_execute_update VALUES (1, 'pending'), (2, 'pending'), (3, 'done')`,
		})
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		// Update rows
		result, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Query: "UPDATE test_execute_update SET status = ? WHERE status = ?",
			Args:  []any{"completed", "pending"},
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
		defer Execute(ctx, dsn, ducktape.ExecuteRequest{Query: "DROP TABLE IF EXISTS test_execute_delete"})

		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Query: `CREATE TABLE test_execute_delete (id INTEGER, active BOOLEAN)`,
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Insert data
		_, err = Execute(ctx, dsn, ducktape.ExecuteRequest{
			Query: `INSERT INTO test_execute_delete VALUES (1, true), (2, false), (3, false)`,
		})
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		// Delete rows
		result, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Query: "DELETE FROM test_execute_delete WHERE active = ?",
			Args:  []any{false},
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
			Query: "INVALID SQL STATEMENT",
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
		defer Execute(ctx, dsn, ducktape.ExecuteRequest{Query: "DROP TABLE IF EXISTS test_execute_params"})

		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Query: `CREATE TABLE test_execute_params (id INTEGER, name VARCHAR, created_at TIMESTAMP)`,
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		now := time.Now()
		result, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Query: "INSERT INTO test_execute_params VALUES (?, ?, ?)",
			Args:  []any{1, "test", now},
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
			Query: "SELECT 1",
		})
		if err == nil {
			t.Error("expected error for invalid DSN, got none")
		}
	})
}
