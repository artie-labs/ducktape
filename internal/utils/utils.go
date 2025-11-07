package utils

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"
)

func RowsToObjects(rows *sql.Rows) ([]map[string]any, error) {
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var objects []map[string]any
	for rows.Next() {
		row := make([]any, len(columns))
		rowPointers := make([]any, len(columns))
		for i := range row {
			rowPointers[i] = &row[i]
		}

		if err = rows.Scan(rowPointers...); err != nil {
			return nil, err
		}

		object := make(map[string]any)
		for i, column := range columns {
			object[column] = row[i]
		}

		objects = append(objects, object)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate over rows: %w", err)
	}

	return objects, nil
}

type ColumnMetadata struct {
	Name string
	Type string
}

func GetColumnMetadata(ctx context.Context, conn *sql.Conn, database, schema, table string) ([]ColumnMetadata, error) {
	query := fmt.Sprintf(`
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_catalog = '%s' AND table_schema = '%s' AND table_name = '%s'
		ORDER BY ordinal_position`, database, schema, table)

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query column metadata: %w", err)
	}
	defer rows.Close()

	var columns []ColumnMetadata
	for rows.Next() {
		var columnName, dataType string
		if err := rows.Scan(&columnName, &dataType); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		columns = append(columns, ColumnMetadata{Name: columnName, Type: dataType})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over column metadata: %w", err)
	}

	return columns, nil
}

// ConvertValue converts a value from JSON (typically string or number) to the appropriate Go type
// based on the DuckDB column type
func ConvertValue(value any, columnMetadata ColumnMetadata) (driver.Value, error) {
	if value == nil {
		return nil, nil
	}

	switch columnMetadata.Type {
	case "DATE":
		// Handle date strings (may include timestamp portion)
		if s, ok := value.(string); ok {
			// Try multiple date/timestamp formats
			formats := []string{
				time.RFC3339,          // 2006-01-02T15:04:05Z07:00
				time.RFC3339Nano,      // 2006-01-02T15:04:05.999999999Z07:00
				"2006-01-02T15:04:05", // ISO 8601 without timezone
				"2006-01-02",          // Just date
			}
			for _, format := range formats {
				if t, err := time.Parse(format, s); err == nil {
					return t, nil
				}
			}
			return nil, fmt.Errorf("failed to parse date %q for column %q (expected type %s)", s, columnMetadata.Name, columnMetadata.Type)
		}
		return value, nil

	case "TIMESTAMP", "TIMESTAMP WITH TIME ZONE":
		// Handle timestamp strings
		if s, ok := value.(string); ok {
			// Try multiple timestamp formats
			formats := []string{
				time.RFC3339,
				time.RFC3339Nano,
				"2006-01-02 15:04:05",
				"2006-01-02T15:04:05",
			}
			for _, format := range formats {
				if t, err := time.Parse(format, s); err == nil {
					return t, nil
				}
			}
			return nil, fmt.Errorf("failed to parse timestamp %q for column %q (expected type %s)", s, columnMetadata.Name, columnMetadata.Type)
		}
		return value, nil

	case "TIME":
		// Handle time strings (may include full timestamp with date)
		if s, ok := value.(string); ok {
			// Try multiple time/timestamp formats
			formats := []string{
				time.RFC3339,          // 2006-01-02T15:04:05Z07:00
				time.RFC3339Nano,      // 2006-01-02T15:04:05.999999999Z07:00
				"2006-01-02T15:04:05", // ISO 8601 without timezone
				"15:04:05.999999999",  // Time with nanoseconds
				"15:04:05",            // Just time
			}
			for _, format := range formats {
				if t, err := time.Parse(format, s); err == nil {
					return t, nil
				}
			}
			return nil, fmt.Errorf("failed to parse time %q for column %q (expected type %s)", s, columnMetadata.Name, columnMetadata.Type)
		}
		return value, nil

	default:
		// For all other types (BIGINT, BOOLEAN, VARCHAR, etc.), pass through as-is
		// The driver will handle basic conversions
		return value, nil
	}
}
