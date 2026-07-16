package api

import (
	"context"
	stdjson "encoding/json"
	"fmt"
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

	// Helper functions for Marshal and Unmarshal
	marshalExec := func(r ducktape.ExecuteRequest) ([]byte, error) {
		return stdjson.Marshal(r)
	}
	unmarshalExec := func(r []byte) (*ducktape.ExecuteResponse, error) {
		var resp ducktape.ExecuteResponse
		if err := stdjson.Unmarshal(r, &resp); err != nil {
			return nil, err
		}
		return &resp, nil
	}
	marshalQuery := func(r ducktape.QueryRequest) ([]byte, error) {
		return stdjson.Marshal(r)
	}
	unmarshalQuery := func(r []byte) (*ducktape.QueryResponse, error) {
		var resp ducktape.QueryResponse
		if err := stdjson.Unmarshal(r, &resp); err != nil {
			return nil, err
		}
		return &resp, nil
	}

	// 4. POST route (Execute) without Basic Auth should fail
	unauthenticatedClient := ducktape.NewClient(server.URL)
	_, err = unauthenticatedClient.Execute(ctx, ducktape.ExecuteRequest{
		Statements: []ducktape.ExecuteStatement{
			{Query: "SELECT 1"},
		},
	}, ":memory:", marshalExec, unmarshalExec)
	if err == nil {
		t.Fatal("expected unauthorized error for Execute, got nil")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Fatalf("expected 401 error for Execute, got: %v", err)
	}

	// 5. POST route (Query) without Basic Auth should fail
	_, err = unauthenticatedClient.Query(ctx, ducktape.QueryRequest{
		Query: "SELECT 1",
	}, ":memory:", marshalQuery, unmarshalQuery)
	if err == nil {
		t.Fatal("expected unauthorized error for Query, got nil")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Fatalf("expected 401 error for Query, got: %v", err)
	}

	// 6. POST routes with correct Basic Auth should succeed
	client.SetBasicAuth("admin", "secret")
	execResp, err := client.Execute(ctx, ducktape.ExecuteRequest{
		Statements: []ducktape.ExecuteStatement{
			{Query: "CREATE TABLE test_auth (id INTEGER)"},
		},
	}, ":memory:", marshalExec, unmarshalExec)
	if err != nil {
		t.Fatalf("expected Execute to succeed, got error: %v", err)
	}
	if execResp.Error != nil {
		t.Fatalf("expected Execute response with no error, got: %s", *execResp.Error)
	}

	queryResp, err := client.Query(ctx, ducktape.QueryRequest{
		Query: "SELECT 1 as val",
	}, ":memory:", marshalQuery, unmarshalQuery)
	if err != nil {
		t.Fatalf("expected Query to succeed, got error: %v", err)
	}
	if queryResp.Error != nil {
		t.Fatalf("expected Query response with no error, got: %s", *queryResp.Error)
	}
	if len(queryResp.Rows) != 1 {
		t.Fatalf("unexpected query result length: %d", len(queryResp.Rows))
	}
	val := queryResp.Rows[0]["val"]
	if fmt.Sprintf("%v", val) != "1" {
		t.Fatalf("unexpected query result value: %v", val)
	}
}
