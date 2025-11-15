package api

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	_ "github.com/duckdb/duckdb-go/v2"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func TestAppend(t *testing.T) {
	ctx := context.Background()

	t.Run("append basic data", func(t *testing.T) {
		dsn := "test_append_basic.db"
		t.Cleanup(func() { os.Remove(dsn) })

		// Create table
		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_append_basic (id INTEGER, name VARCHAR, age INTEGER)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Prepare NDJSON data
		ndjson := `{"rv":[1,"Alice",30]}
{"rv":[2,"Bob",25]}
{"rv":[3,"Charlie",35]}`

		reader := strings.NewReader(ndjson)
		rowsAppended, bytesRead, err := Append(ctx, dsn, "test_append_basic", "main", "test_append_basic", reader)
		if err != nil {
			t.Fatalf("failed to append: %v", err)
		}

		if rowsAppended != 3 {
			t.Errorf("expected 3 rows appended, got %d", rowsAppended)
		}

		if bytesRead == 0 {
			t.Error("expected bytesRead > 0")
		}

		// Verify data was inserted
		result, err := Query(ctx, dsn, ducktape.QueryRequest{
			Query: "SELECT * FROM test_append_basic ORDER BY id",
		})
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if len(result) != 3 {
			t.Errorf("expected 3 rows in table, got %d", len(result))
		}

		if result[0]["name"] != "Alice" {
			t.Errorf("expected first row name=Alice, got %v", result[0]["name"])
		}
	})

	t.Run("append with empty lines", func(t *testing.T) {
		dsn := "test_append_empty_lines.db"
		t.Cleanup(func() { os.Remove(dsn) })

		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_append_empty_lines (id INTEGER, value VARCHAR)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		ndjson := `{"rv":[1,"a"]}

{"rv":[2,"b"]}


{"rv":[3,"c"]}`

		reader := strings.NewReader(ndjson)
		rowsAppended, _, err := Append(ctx, dsn, "test_append_empty_lines", "main", "test_append_empty_lines", reader)
		if err != nil {
			t.Fatalf("failed to append: %v", err)
		}

		if rowsAppended != 3 {
			t.Errorf("expected 3 rows appended (empty lines should be skipped), got %d", rowsAppended)
		}
	})

	t.Run("append with temporal types", func(t *testing.T) {
		dsn := "test_append_temporal.db"
		t.Cleanup(func() { os.Remove(dsn) })

		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_append_temporal (
				id INTEGER,
				event_date DATE,
				event_timestamp TIMESTAMP,
				event_time TIME
			)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		ndjson := `{"rv":[1,"2024-03-15","2024-03-15T14:30:00","14:30:00"]}`

		reader := strings.NewReader(ndjson)
		rowsAppended, _, err := Append(ctx, dsn, "test_append_temporal", "main", "test_append_temporal", reader)
		if err != nil {
			t.Fatalf("failed to append temporal data: %v", err)
		}

		if rowsAppended != 1 {
			t.Errorf("expected 1 row appended, got %d", rowsAppended)
		}

		// Verify data
		result, err := Query(ctx, dsn, ducktape.QueryRequest{
			Query: "SELECT * FROM test_append_temporal",
		})
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if len(result) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result))
		}

		if result[0]["id"] != int32(1) {
			t.Errorf("expected id=1, got %v", result[0]["id"])
		}
	})

	t.Run("append with NULL values", func(t *testing.T) {
		dsn := "test_append_nulls.db"
		t.Cleanup(func() { os.Remove(dsn) })

		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_append_nulls (id INTEGER, value VARCHAR, count INTEGER)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		ndjson := `{"rv":[1,null,10]}
{"rv":[2,"test",null]}`

		reader := strings.NewReader(ndjson)
		rowsAppended, _, err := Append(ctx, dsn, "test_append_nulls", "main", "test_append_nulls", reader)
		if err != nil {
			t.Fatalf("failed to append: %v", err)
		}

		if rowsAppended != 2 {
			t.Errorf("expected 2 rows appended, got %d", rowsAppended)
		}

		result, err := Query(ctx, dsn, ducktape.QueryRequest{
			Query: "SELECT * FROM test_append_nulls ORDER BY id",
		})
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if result[0]["value"] != nil {
			t.Errorf("expected NULL value, got %v", result[0]["value"])
		}

		if result[1]["count"] != nil {
			t.Errorf("expected NULL count, got %v", result[1]["count"])
		}
	})

	t.Run("append with boolean values", func(t *testing.T) {
		dsn := "test_append_boolean.db"
		t.Cleanup(func() { os.Remove(dsn) })

		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_append_boolean (id INTEGER, active BOOLEAN, verified BOOLEAN)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		ndjson := `{"rv":[1,true,false]}
{"rv":[2,false,true]}`

		reader := strings.NewReader(ndjson)
		rowsAppended, _, err := Append(ctx, dsn, "test_append_boolean", "main", "test_append_boolean", reader)
		if err != nil {
			t.Fatalf("failed to append: %v", err)
		}

		if rowsAppended != 2 {
			t.Errorf("expected 2 rows appended, got %d", rowsAppended)
		}

		result, err := Query(ctx, dsn, ducktape.QueryRequest{
			Query: "SELECT * FROM test_append_boolean ORDER BY id",
		})
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if result[0]["active"] != true {
			t.Errorf("expected active=true, got %v", result[0]["active"])
		}

		if result[1]["verified"] != true {
			t.Errorf("expected verified=true, got %v", result[1]["verified"])
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		dsn := "test_append_invalid.db"
		t.Cleanup(func() { os.Remove(dsn) })

		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_append_invalid (id INTEGER, name VARCHAR)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		ndjson := `{invalid json}`

		reader := strings.NewReader(ndjson)
		_, _, err = Append(ctx, dsn, "test_append_invalid", "main", "test_append_invalid", reader)
		if err == nil {
			t.Error("expected error for invalid JSON, got none")
		}
		if !strings.Contains(err.Error(), "unmarshal") {
			t.Errorf("expected unmarshal error, got: %v", err)
		}
	})

	t.Run("non-existent table", func(t *testing.T) {
		ndjson := `{"rv":[1,"test"]}`

		reader := strings.NewReader(ndjson)
		_, _, err := Append(ctx, "", "memory", "main", "non_existent_table", reader)
		if err == nil {
			t.Error("expected error for non-existent table, got none")
		}
	})

	t.Run("column count mismatch", func(t *testing.T) {
		dsn := "test_append_mismatch.db"
		t.Cleanup(func() { os.Remove(dsn) })

		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_append_mismatch (id INTEGER, name VARCHAR)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Provide 3 values for a 2-column table
		ndjson := `{"rv":[1,"test","extra"]}`

		reader := strings.NewReader(ndjson)
		_, _, err = Append(ctx, dsn, "test_append_mismatch", "main", "test_append_mismatch", reader)
		if err == nil {
			t.Error("expected error for column count mismatch, got none")
		}
		if !strings.Contains(err.Error(), "exceeds number of columns") {
			t.Errorf("expected column count error, got: %v", err)
		}
	})

	t.Run("empty input", func(t *testing.T) {
		dsn := "test_append_empty.db"
		t.Cleanup(func() { os.Remove(dsn) })

		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_append_empty (id INTEGER, name VARCHAR)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		reader := strings.NewReader("")
		rowsAppended, _, err := Append(ctx, dsn, "test_append_empty", "main", "test_append_empty", reader)
		if err != nil {
			t.Fatalf("failed to append empty data: %v", err)
		}

		if rowsAppended != 0 {
			t.Errorf("expected 0 rows appended, got %d", rowsAppended)
		}
	})

	t.Run("large batch", func(t *testing.T) {
		dsn := "test_append_large.db"
		t.Cleanup(func() { os.Remove(dsn) })

		_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
			Statements: []ducktape.ExecuteStatement{
				{Query: `CREATE TABLE test_append_large (id INTEGER, value DOUBLE)`},
			},
		})
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Generate 1000 rows
		var buf bytes.Buffer
		for i := 0; i < 1000; i++ {
			buf.WriteString(fmt.Sprintf(`{"rv":[%d,%d.5]}`, i, i*100))
			buf.WriteString("\n")
		}

		reader := bytes.NewReader(buf.Bytes())
		rowsAppended, bytesRead, err := Append(ctx, dsn, "test_append_large", "main", "test_append_large", reader)
		if err != nil {
			t.Fatalf("failed to append large batch: %v", err)
		}

		if rowsAppended != 1000 {
			t.Errorf("expected 1000 rows appended, got %d", rowsAppended)
		}

		if bytesRead == 0 {
			t.Error("expected bytesRead > 0")
		}

		// Verify count
		result, err := Query(ctx, dsn, ducktape.QueryRequest{
			Query: "SELECT COUNT(*) as count FROM test_append_large",
		})
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		count, ok := result[0]["count"].(int64)
		if !ok {
			t.Fatalf("expected count to be int64, got %T", result[0]["count"])
		}

		if count != 1000 {
			t.Errorf("expected 1000 rows in table, got %d", count)
		}
	})
}

func BenchmarkAppend(b *testing.B) {
	ctx := b.Context()
	dsn := "benchmark_append.db"
	databaseName := "benchmark_append"
	schemaName := "b"
	tableName := "benchmark_append"
	fullyQualifiedTableName := fmt.Sprintf("%s.%s.%s", databaseName, schemaName, tableName)
	b.Cleanup(func() { os.Remove(dsn) })

	_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
		Statements: []ducktape.ExecuteStatement{
			{Query: fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s; CREATE TABLE %s (id BIGINT, name VARCHAR);", schemaName, fullyQualifiedTableName)},
		},
	})
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	mux := http.NewServeMux()

	RegisterApiRoutes(mux)
	RegisterHealthCheckRoutes(mux)

	h2cHandler := h2c.NewHandler(mux, &http2.Server{})

	go func() {
		http.ListenAndServe("0.0.0.0:58080", h2cHandler)
	}()

	client := ducktape.NewClient("http://localhost:58080")

	var rowIndex uint64

	streamIterator := func(yield func(ducktape.RowMessageResult) bool) {
		for b.Loop() {
			var rowValues []any
			rowValues = append(rowValues, rowIndex)
			rowValues = append(rowValues, "3457236795623875623478562348756234876587236587234")

			if !yield(ducktape.RowMessageResult{Row: ducktape.RowMessage{Values: rowValues}}) {
				return
			}
			rowIndex++
		}
	}

	client.Append(ctx, dsn, databaseName, schemaName, tableName, streamIterator, func(r ducktape.RowMessage) ([]byte, error) {
		return json.Marshal(r)
	}, func(r []byte) (*ducktape.AppendResponse, error) {
		var resp ducktape.AppendResponse
		if err := json.Unmarshal(r, &resp); err != nil {
			return nil, err
		}
		return &resp, nil
	})

}

// BenchmarkAppend_1KB_Local tests small row throughput to local database
// Simulates: transaction logs, event streams, small CDC records
func BenchmarkAppend_1KB_Local(b *testing.B) {
	ctx := b.Context()
	dsn := "benchmark_1kb_local.db"
	databaseName := "benchmark_1kb_local"
	schemaName := "main"
	tableName := "rows_1kb"
	fullyQualifiedTableName := fmt.Sprintf("%s.%s.%s", databaseName, schemaName, tableName)

	b.Cleanup(func() { os.Remove(dsn) })

	// Schema mimics typical production: id, timestamps, metadata, small payload
	_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
		Statements: []ducktape.ExecuteStatement{
			{Query: fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;
				CREATE TABLE %s (
					id BIGINT,
					created_at TIMESTAMP,
					user_id BIGINT,
					event_type VARCHAR,
					metadata VARCHAR,
					payload VARCHAR
				);`, schemaName, fullyQualifiedTableName)},
		},
	})
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	mux := http.NewServeMux()
	RegisterApiRoutes(mux)
	RegisterHealthCheckRoutes(mux)
	h2cHandler := h2c.NewHandler(mux, &http2.Server{})

	go func() {
		http.ListenAndServe("0.0.0.0:58081", h2cHandler)
	}()

	client := ducktape.NewClient("http://localhost:58081")

	// Generate ~1KB per row (adjust string lengths to hit target)
	// 1KB = 1024 bytes, account for overhead, aim for ~900 bytes in payload
	payload := strings.Repeat("x", 900)

	var rowIndex uint64

	streamIterator := func(yield func(ducktape.RowMessageResult) bool) {
		for b.Loop() {
			rowValues := []any{
				rowIndex,
				"2024-11-15T10:30:00Z",
				rowIndex % 1000,
				"user_action",
				`{"source":"web","version":"1.0"}`,
				payload,
			}

			if !yield(ducktape.RowMessageResult{Row: ducktape.RowMessage{Values: rowValues}}) {
				return
			}
			rowIndex++
		}
	}

	client.Append(ctx, dsn, databaseName, schemaName, tableName, streamIterator,
		func(r ducktape.RowMessage) ([]byte, error) {
			return json.Marshal(r)
		},
		func(r []byte) (*ducktape.AppendResponse, error) {
			var resp ducktape.AppendResponse
			if err := json.Unmarshal(r, &resp); err != nil {
				return nil, err
			}
			return &resp, nil
		})
}

// BenchmarkAppend_64KB_Local tests large row throughput to local database
// Simulates: document storage, large JSON payloads, analytics data with wide schemas
func BenchmarkAppend_64KB_Local(b *testing.B) {
	ctx := b.Context()
	dsn := "benchmark_64kb_local.db"
	databaseName := "benchmark_64kb_local"
	schemaName := "main"
	tableName := "rows_64kb"
	fullyQualifiedTableName := fmt.Sprintf("%s.%s.%s", databaseName, schemaName, tableName)

	b.Cleanup(func() { os.Remove(dsn) })

	_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
		Statements: []ducktape.ExecuteStatement{
			{Query: fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;
				CREATE TABLE %s (
					id BIGINT,
					created_at TIMESTAMP,
					document_id VARCHAR,
					content TEXT,
					metadata JSON
				);`, schemaName, fullyQualifiedTableName)},
		},
	})
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	mux := http.NewServeMux()
	RegisterApiRoutes(mux)
	RegisterHealthCheckRoutes(mux)
	h2cHandler := h2c.NewHandler(mux, &http2.Server{})

	go func() {
		http.ListenAndServe("0.0.0.0:58082", h2cHandler)
	}()

	client := ducktape.NewClient("http://localhost:58082")

	// Generate ~64KB per row (65,536 bytes)
	// Account for JSON overhead, aim for ~63KB in content
	largePayload := strings.Repeat("Lorem ipsum dolor sit amet, consectetur adipiscing elit. ", 1100) // ~63KB

	var rowIndex uint64

	streamIterator := func(yield func(ducktape.RowMessageResult) bool) {
		for b.Loop() {
			rowValues := []any{
				rowIndex,
				"2024-11-15T10:30:00Z",
				fmt.Sprintf("doc-%d", rowIndex),
				largePayload,
				`{"tags":["important","archived"],"version":2,"author":"system"}`,
			}

			if !yield(ducktape.RowMessageResult{Row: ducktape.RowMessage{Values: rowValues}}) {
				return
			}
			rowIndex++
		}
	}

	client.Append(ctx, dsn, databaseName, schemaName, tableName, streamIterator,
		func(r ducktape.RowMessage) ([]byte, error) {
			return json.Marshal(r)
		},
		func(r []byte) (*ducktape.AppendResponse, error) {
			var resp ducktape.AppendResponse
			if err := json.Unmarshal(r, &resp); err != nil {
				return nil, err
			}
			return &resp, nil
		})
}
