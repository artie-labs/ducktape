package utils

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestRowsToObjects(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("multiple rows with different data types", func(t *testing.T) {
		_, err := db.Exec(`
			CREATE TABLE test_rows (
				id INTEGER,
				name VARCHAR,
				age INTEGER,
				active BOOLEAN,
				created_at TIMESTAMP
			)
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}
		defer db.Exec("DROP TABLE test_rows")

		_, err = db.Exec(`
			INSERT INTO test_rows VALUES
				(1, 'Alice', 30, true, '2024-01-15 10:30:00'),
				(2, 'Bob', 25, false, '2024-02-20 14:45:00'),
				(3, 'Charlie', 35, true, '2024-03-10 09:15:00')
		`)
		if err != nil {
			t.Fatalf("failed to insert data: %v", err)
		}

		rows, err := db.Query("SELECT * FROM test_rows ORDER BY id")
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		objects, err := RowsToObjects(rows)
		if err != nil {
			t.Fatalf("RowsToObjects failed: %v", err)
		}

		if len(objects) != 3 {
			t.Errorf("expected 3 objects, got %d", len(objects))
		}

		if objects[0]["id"] != int32(1) {
			t.Errorf("expected id=1, got %v", objects[0]["id"])
		}
		if objects[0]["name"] != "Alice" {
			t.Errorf("expected name=Alice, got %v", objects[0]["name"])
		}
		if objects[0]["active"] != true {
			t.Errorf("expected active=true, got %v", objects[0]["active"])
		}

		if objects[1]["name"] != "Bob" {
			t.Errorf("expected name=Bob, got %v", objects[1]["name"])
		}
		if objects[1]["active"] != false {
			t.Errorf("expected active=false, got %v", objects[1]["active"])
		}
	})

	t.Run("empty result set", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE test_empty (id INTEGER, name VARCHAR)`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}
		defer db.Exec("DROP TABLE test_empty")

		rows, err := db.Query("SELECT * FROM test_empty")
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		objects, err := RowsToObjects(rows)
		if err != nil {
			t.Fatalf("RowsToObjects failed: %v", err)
		}

		if len(objects) != 0 {
			t.Errorf("expected 0 objects, got %d", len(objects))
		}
	})

	t.Run("null values", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE test_nulls (id INTEGER, name VARCHAR, age INTEGER)`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}
		defer db.Exec("DROP TABLE test_nulls")

		_, err = db.Exec(`INSERT INTO test_nulls VALUES (1, NULL, NULL)`)
		if err != nil {
			t.Fatalf("failed to insert data: %v", err)
		}

		rows, err := db.Query("SELECT * FROM test_nulls")
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		objects, err := RowsToObjects(rows)
		if err != nil {
			t.Fatalf("RowsToObjects failed: %v", err)
		}

		if len(objects) != 1 {
			t.Errorf("expected 1 object, got %d", len(objects))
		}

		if objects[0]["name"] != nil {
			t.Errorf("expected name=nil, got %v", objects[0]["name"])
		}
		if objects[0]["age"] != nil {
			t.Errorf("expected age=nil, got %v", objects[0]["age"])
		}
	})

	t.Run("single column", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE test_single (value INTEGER)`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}
		defer db.Exec("DROP TABLE test_single")

		_, err = db.Exec(`INSERT INTO test_single VALUES (42), (100), (256)`)
		if err != nil {
			t.Fatalf("failed to insert data: %v", err)
		}

		rows, err := db.Query("SELECT * FROM test_single")
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		objects, err := RowsToObjects(rows)
		if err != nil {
			t.Fatalf("RowsToObjects failed: %v", err)
		}

		if len(objects) != 3 {
			t.Errorf("expected 3 objects, got %d", len(objects))
		}

		if objects[0]["value"] != int32(42) {
			t.Errorf("expected value=42, got %v", objects[0]["value"])
		}
	})
}

func TestGetColumnMetadata(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	t.Run("table with multiple columns", func(t *testing.T) {
		_, err := db.Exec(`
			CREATE TABLE test_metadata (
				id BIGINT,
				name VARCHAR,
				email VARCHAR,
				age INTEGER,
				active BOOLEAN,
				created_at TIMESTAMP,
				birth_date DATE,
				login_time TIME
			)
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}
		defer db.Exec("DROP TABLE test_metadata")

		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}
		defer conn.Close()

		// Get column metadata (DuckDB uses 'memory' as the default database)
		columns, err := GetColumnMetadata(ctx, conn, "memory", "main", "test_metadata")
		if err != nil {
			t.Fatalf("GetColumnMetadata failed: %v", err)
		}

		if len(columns) != 8 {
			t.Errorf("expected 8 columns, got %d", len(columns))
		}

		expectedColumns := map[string]string{
			"id":         "BIGINT",
			"name":       "VARCHAR",
			"email":      "VARCHAR",
			"age":        "INTEGER",
			"active":     "BOOLEAN",
			"created_at": "TIMESTAMP",
			"birth_date": "DATE",
			"login_time": "TIME",
		}

		for _, col := range columns {
			expectedType, exists := expectedColumns[col.Name]
			if !exists {
				t.Errorf("unexpected column: %s", col.Name)
				continue
			}
			if col.Type != expectedType {
				t.Errorf("column %s: expected type %s, got %s", col.Name, expectedType, col.Type)
			}
		}

		// Verify order is maintained (should match creation order)
		expectedOrder := []string{"id", "name", "email", "age", "active", "created_at", "birth_date", "login_time"}
		for i, expectedName := range expectedOrder {
			if i >= len(columns) {
				break
			}
			if columns[i].Name != expectedName {
				t.Errorf("position %d: expected %s, got %s", i, expectedName, columns[i].Name)
			}
		}
	})

	t.Run("empty table", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE test_empty_meta (id INTEGER)`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}
		defer db.Exec("DROP TABLE test_empty_meta")

		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}
		defer conn.Close()

		columns, err := GetColumnMetadata(ctx, conn, "memory", "main", "test_empty_meta")
		if err != nil {
			t.Fatalf("GetColumnMetadata failed: %v", err)
		}

		if len(columns) != 1 {
			t.Errorf("expected 1 column, got %d", len(columns))
		}
	})

	t.Run("nonexistent table", func(t *testing.T) {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}
		defer conn.Close()

		columns, err := GetColumnMetadata(ctx, conn, "memory", "main", "nonexistent_table")
		if err != nil {
			t.Fatalf("GetColumnMetadata failed: %v", err)
		}

		if len(columns) != 0 {
			t.Errorf("expected 0 columns for non-existent table, got %d", len(columns))
		}
	})
}

func TestConvertValue(t *testing.T) {
	t.Run("DATE type conversions", func(t *testing.T) {
		metadata := ColumnMetadata{Name: "test_date", Type: "DATE"}

		t.Run("RFC3339 format", func(t *testing.T) {
			result, err := ConvertValue("2024-01-15T10:30:00Z", metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result == nil {
				t.Error("expected non-nil result for valid input")
			}
		})

		t.Run("RFC3339Nano format", func(t *testing.T) {
			result, err := ConvertValue("2024-01-15T10:30:00.123456789Z", metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result == nil {
				t.Error("expected non-nil result for valid input")
			}
		})

		t.Run("ISO 8601 without timezone", func(t *testing.T) {
			result, err := ConvertValue("2024-01-15T10:30:00", metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result == nil {
				t.Error("expected non-nil result for valid input")
			}
		})

		t.Run("date only", func(t *testing.T) {
			result, err := ConvertValue("2024-01-15", metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result == nil {
				t.Error("expected non-nil result for valid input")
			}
		})

		t.Run("invalid format", func(t *testing.T) {
			_, err := ConvertValue("not-a-date", metadata)
			if err == nil {
				t.Error("expected error but got none")
			}
		})

		t.Run("nil value", func(t *testing.T) {
			result, err := ConvertValue(nil, metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result != nil {
				t.Errorf("expected nil result for nil input, got %v", result)
			}
		})
	})

	t.Run("TIMESTAMP type conversions", func(t *testing.T) {
		metadata := ColumnMetadata{Name: "test_timestamp", Type: "TIMESTAMP"}

		t.Run("RFC3339 format", func(t *testing.T) {
			result, err := ConvertValue("2024-01-15T10:30:00Z", metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result == nil {
				t.Error("expected non-nil result for valid input")
			}
		})

		t.Run("RFC3339Nano format", func(t *testing.T) {
			result, err := ConvertValue("2024-01-15T10:30:00.123456789Z", metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result == nil {
				t.Error("expected non-nil result for valid input")
			}
		})

		t.Run("standard timestamp format", func(t *testing.T) {
			result, err := ConvertValue("2024-01-15 10:30:00", metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result == nil {
				t.Error("expected non-nil result for valid input")
			}
		})

		t.Run("ISO 8601 format", func(t *testing.T) {
			result, err := ConvertValue("2024-01-15T10:30:00", metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result == nil {
				t.Error("expected non-nil result for valid input")
			}
		})

		t.Run("invalid format", func(t *testing.T) {
			_, err := ConvertValue("invalid-timestamp", metadata)
			if err == nil {
				t.Error("expected error but got none")
			}
		})

		t.Run("nil value", func(t *testing.T) {
			result, err := ConvertValue(nil, metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result != nil {
				t.Errorf("expected nil result for nil input, got %v", result)
			}
		})
	})

	t.Run("TIMESTAMP WITH TIME ZONE type conversions", func(t *testing.T) {
		metadata := ColumnMetadata{Name: "test_tstz", Type: "TIMESTAMP WITH TIME ZONE"}

		value := "2024-01-15T10:30:00Z"
		result, err := ConvertValue(value, metadata)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if _, ok := result.(time.Time); !ok {
			t.Errorf("expected time.Time, got %T", result)
		}
	})

	t.Run("TIME type conversions", func(t *testing.T) {
		metadata := ColumnMetadata{Name: "test_time", Type: "TIME"}

		t.Run("RFC3339 format", func(t *testing.T) {
			result, err := ConvertValue("2024-01-15T10:30:00Z", metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result == nil {
				t.Error("expected non-nil result for valid input")
			}
		})

		t.Run("time with nanoseconds", func(t *testing.T) {
			result, err := ConvertValue("10:30:00.123456789", metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result == nil {
				t.Error("expected non-nil result for valid input")
			}
		})

		t.Run("time only", func(t *testing.T) {
			result, err := ConvertValue("10:30:00", metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result == nil {
				t.Error("expected non-nil result for valid input")
			}
		})

		t.Run("invalid format", func(t *testing.T) {
			_, err := ConvertValue("invalid-time", metadata)
			if err == nil {
				t.Error("expected error but got none")
			}
		})

		t.Run("nil value", func(t *testing.T) {
			result, err := ConvertValue(nil, metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result != nil {
				t.Errorf("expected nil result for nil input, got %v", result)
			}
		})
	})

	t.Run("pass-through types", func(t *testing.T) {
		t.Run("BIGINT", func(t *testing.T) {
			metadata := ColumnMetadata{Name: "test_col", Type: "BIGINT"}
			value := int64(12345)
			result, err := ConvertValue(value, metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result != value {
				t.Errorf("expected %v, got %v", value, result)
			}
		})

		t.Run("INTEGER", func(t *testing.T) {
			metadata := ColumnMetadata{Name: "test_col", Type: "INTEGER"}
			value := int32(42)
			result, err := ConvertValue(value, metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result != value {
				t.Errorf("expected %v, got %v", value, result)
			}
		})

		t.Run("BOOLEAN", func(t *testing.T) {
			metadata := ColumnMetadata{Name: "test_col", Type: "BOOLEAN"}
			value := true
			result, err := ConvertValue(value, metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result != value {
				t.Errorf("expected %v, got %v", value, result)
			}
		})

		t.Run("VARCHAR", func(t *testing.T) {
			metadata := ColumnMetadata{Name: "test_col", Type: "VARCHAR"}
			value := "hello world"
			result, err := ConvertValue(value, metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result != value {
				t.Errorf("expected %v, got %v", value, result)
			}
		})

		t.Run("DOUBLE", func(t *testing.T) {
			metadata := ColumnMetadata{Name: "test_col", Type: "DOUBLE"}
			value := 3.14159
			result, err := ConvertValue(value, metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result != value {
				t.Errorf("expected %v, got %v", value, result)
			}
		})

		t.Run("REAL", func(t *testing.T) {
			metadata := ColumnMetadata{Name: "test_col", Type: "REAL"}
			value := float32(2.718)
			result, err := ConvertValue(value, metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result != value {
				t.Errorf("expected %v, got %v", value, result)
			}
		})

		t.Run("BLOB", func(t *testing.T) {
			metadata := ColumnMetadata{Name: "test_col", Type: "BLOB"}
			value := []byte{0x01, 0x02, 0x03}
			result, err := ConvertValue(value, metadata)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			resultBytes, ok := result.([]byte)
			if !ok {
				t.Errorf("expected []byte, got %T", result)
			} else {
				if len(resultBytes) != len(value) {
					t.Errorf("expected length %d, got %d", len(value), len(resultBytes))
				} else {
					for i := range value {
						if resultBytes[i] != value[i] {
							t.Errorf("byte at index %d: expected %v, got %v", i, value[i], resultBytes[i])
						}
					}
				}
			}
		})
	})

	t.Run("nil values for all types", func(t *testing.T) {
		t.Run("DATE", func(t *testing.T) {
			metadata := ColumnMetadata{Name: "test_col", Type: "DATE"}
			result, err := ConvertValue(nil, metadata)
			if err != nil {
				t.Errorf("unexpected error for nil value: %v", err)
			}
			if result != nil {
				t.Errorf("expected nil result, got %v", result)
			}
		})

		t.Run("TIMESTAMP", func(t *testing.T) {
			metadata := ColumnMetadata{Name: "test_col", Type: "TIMESTAMP"}
			result, err := ConvertValue(nil, metadata)
			if err != nil {
				t.Errorf("unexpected error for nil value: %v", err)
			}
			if result != nil {
				t.Errorf("expected nil result, got %v", result)
			}
		})

		t.Run("TIMESTAMP WITH TIME ZONE", func(t *testing.T) {
			metadata := ColumnMetadata{Name: "test_col", Type: "TIMESTAMP WITH TIME ZONE"}
			result, err := ConvertValue(nil, metadata)
			if err != nil {
				t.Errorf("unexpected error for nil value: %v", err)
			}
			if result != nil {
				t.Errorf("expected nil result, got %v", result)
			}
		})

		t.Run("TIME", func(t *testing.T) {
			metadata := ColumnMetadata{Name: "test_col", Type: "TIME"}
			result, err := ConvertValue(nil, metadata)
			if err != nil {
				t.Errorf("unexpected error for nil value: %v", err)
			}
			if result != nil {
				t.Errorf("expected nil result, got %v", result)
			}
		})

		t.Run("BIGINT", func(t *testing.T) {
			metadata := ColumnMetadata{Name: "test_col", Type: "BIGINT"}
			result, err := ConvertValue(nil, metadata)
			if err != nil {
				t.Errorf("unexpected error for nil value: %v", err)
			}
			if result != nil {
				t.Errorf("expected nil result, got %v", result)
			}
		})

		t.Run("VARCHAR", func(t *testing.T) {
			metadata := ColumnMetadata{Name: "test_col", Type: "VARCHAR"}
			result, err := ConvertValue(nil, metadata)
			if err != nil {
				t.Errorf("unexpected error for nil value: %v", err)
			}
			if result != nil {
				t.Errorf("expected nil result, got %v", result)
			}
		})

		t.Run("BOOLEAN", func(t *testing.T) {
			metadata := ColumnMetadata{Name: "test_col", Type: "BOOLEAN"}
			result, err := ConvertValue(nil, metadata)
			if err != nil {
				t.Errorf("unexpected error for nil value: %v", err)
			}
			if result != nil {
				t.Errorf("expected nil result, got %v", result)
			}
		})
	})

	t.Run("non-string values for temporal types", func(t *testing.T) {
		// When a non-string value is passed to a temporal type, it should be returned as-is
		metadata := ColumnMetadata{Name: "test_date", Type: "DATE"}

		// Test with an integer
		result, err := ConvertValue(12345, metadata)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result != 12345 {
			t.Errorf("expected 12345, got %v", result)
		}

		// Test with a time.Time value (already correct type)
		now := time.Now()
		result, err = ConvertValue(now, metadata)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result != now {
			t.Errorf("expected %v, got %v", now, result)
		}
	})

	t.Run("edge cases and error messages", func(t *testing.T) {
		t.Run("invalid date with column info in error", func(t *testing.T) {
			metadata := ColumnMetadata{Name: "birth_date", Type: "DATE"}
			_, err := ConvertValue("not-a-date", metadata)
			if err == nil {
				t.Error("expected error but got none")
			} else if !strings.Contains(err.Error(), "birth_date") {
				t.Errorf("error message should contain %q, got: %v", "birth_date", err)
			}
		})

		t.Run("invalid timestamp with column info in error", func(t *testing.T) {
			metadata := ColumnMetadata{Name: "created_at", Type: "TIMESTAMP"}
			_, err := ConvertValue("bad-timestamp", metadata)
			if err == nil {
				t.Error("expected error but got none")
			} else if !strings.Contains(err.Error(), "created_at") {
				t.Errorf("error message should contain %q, got: %v", "created_at", err)
			}
		})

		t.Run("invalid time with column info in error", func(t *testing.T) {
			metadata := ColumnMetadata{Name: "login_time", Type: "TIME"}
			_, err := ConvertValue("bad-time", metadata)
			if err == nil {
				t.Error("expected error but got none")
			} else if !strings.Contains(err.Error(), "login_time") {
				t.Errorf("error message should contain %q, got: %v", "login_time", err)
			}
		})
	})
}

func TestConvertValueRoundTrip(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("temporal types round trip", func(t *testing.T) {
		_, err := db.Exec(`
			CREATE TABLE test_temporal (
				id INTEGER,
				event_date DATE,
				event_timestamp TIMESTAMP,
				event_time TIME
			)
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}
		defer db.Exec("DROP TABLE test_temporal")

		dateStr := "2024-03-15"
		timestampStr := "2024-03-15T14:30:00"
		timeStr := "14:30:00"

		dateMetadata := ColumnMetadata{Name: "event_date", Type: "DATE"}
		timestampMetadata := ColumnMetadata{Name: "event_timestamp", Type: "TIMESTAMP"}
		timeMetadata := ColumnMetadata{Name: "event_time", Type: "TIME"}

		dateVal, err := ConvertValue(dateStr, dateMetadata)
		if err != nil {
			t.Fatalf("failed to convert date: %v", err)
		}

		timestampVal, err := ConvertValue(timestampStr, timestampMetadata)
		if err != nil {
			t.Fatalf("failed to convert timestamp: %v", err)
		}

		timeVal, err := ConvertValue(timeStr, timeMetadata)
		if err != nil {
			t.Fatalf("failed to convert time: %v", err)
		}

		_, err = db.Exec(
			"INSERT INTO test_temporal (id, event_date, event_timestamp, event_time) VALUES (?, ?, ?, ?)",
			1, dateVal, timestampVal, timeVal,
		)
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		rows, err := db.Query("SELECT event_date, event_timestamp, event_time FROM test_temporal WHERE id = 1")
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		objects, err := RowsToObjects(rows)
		if err != nil {
			t.Fatalf("RowsToObjects failed: %v", err)
		}

		if len(objects) != 1 {
			t.Fatalf("expected 1 row, got %d", len(objects))
		}

		if objects[0]["event_date"] == nil {
			t.Error("event_date should not be nil")
		}

		if objects[0]["event_timestamp"] == nil {
			t.Error("event_timestamp should not be nil")
		}

		if objects[0]["event_time"] == nil {
			t.Error("event_time should not be nil")
		}
	})
}

func BenchmarkRowsToObjects(b *testing.B) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		b.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE bench_rows (
			id INTEGER,
			name VARCHAR,
			value DOUBLE,
			active BOOLEAN
		)
	`)
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}
	defer db.Exec("DROP TABLE bench_rows")

	for i := 0; i < 1000; i++ {
		_, err = db.Exec("INSERT INTO bench_rows VALUES (?, ?, ?, ?)", i, "name"+string(rune(i)), float64(i)*1.5, i%2 == 0)
		if err != nil {
			b.Fatalf("failed to insert: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT * FROM bench_rows")
		if err != nil {
			b.Fatalf("failed to query: %v", err)
		}

		_, err = RowsToObjects(rows)
		if err != nil {
			b.Fatalf("RowsToObjects failed: %v", err)
		}
	}
}

func BenchmarkConvertValue(b *testing.B) {
	tests := []struct {
		name     string
		metadata ColumnMetadata
		value    any
	}{
		{
			name:     "DATE",
			metadata: ColumnMetadata{Name: "test_date", Type: "DATE"},
			value:    "2024-03-15",
		},
		{
			name:     "TIMESTAMP",
			metadata: ColumnMetadata{Name: "test_timestamp", Type: "TIMESTAMP"},
			value:    "2024-03-15T14:30:00",
		},
		{
			name:     "TIME",
			metadata: ColumnMetadata{Name: "test_time", Type: "TIME"},
			value:    "14:30:00",
		},
		{
			name:     "INTEGER",
			metadata: ColumnMetadata{Name: "test_int", Type: "INTEGER"},
			value:    int32(42),
		},
		{
			name:     "VARCHAR",
			metadata: ColumnMetadata{Name: "test_varchar", Type: "VARCHAR"},
			value:    "hello world",
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := ConvertValue(tt.value, tt.metadata)
				if err != nil {
					b.Fatalf("ConvertValue failed: %v", err)
				}
			}
		})
	}
}

func BenchmarkGetColumnMetadata(b *testing.B) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		b.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE bench_metadata (
			id BIGINT,
			name VARCHAR,
			email VARCHAR,
			age INTEGER,
			active BOOLEAN,
			created_at TIMESTAMP,
			birth_date DATE,
			login_time TIME
		)
	`)
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}
	defer db.Exec("DROP TABLE bench_metadata")

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		b.Fatalf("failed to get connection: %v", err)
	}
	defer conn.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := GetColumnMetadata(ctx, conn, "memory", "main", "bench_metadata")
		if err != nil {
			b.Fatalf("GetColumnMetadata failed: %v", err)
		}
	}
}
