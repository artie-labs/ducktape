package utils

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
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

	metadataType := strings.ToUpper(strings.TrimSpace(columnMetadata.Type))

	switch metadataType {
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
			return nil, fmt.Errorf("failed to parse date %q for column %q (expected type %s)", s, columnMetadata.Name, metadataType)
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
			return nil, fmt.Errorf("failed to parse timestamp %q for column %q (expected type %s)", s, columnMetadata.Name, metadataType)
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
			return nil, fmt.Errorf("failed to parse time %q for column %q (expected type %s)", s, columnMetadata.Name, metadataType)
		}
		return value, nil
	default:
		// Handle parameterized types like DECIMAL(15,2), NUMERIC(10,0)
		if strings.HasPrefix(metadataType, "DECIMAL") || strings.HasPrefix(metadataType, "NUMERIC") {
			if s, ok := value.(string); ok {
				width, scale, err := parseDecimalType(metadataType)
				if err != nil {
					return nil, fmt.Errorf("failed to parse decimal type %q: %w", metadataType, err)
				}
				unscaled, err := parseDecimalString(s, scale)
				if err != nil {
					return nil, fmt.Errorf("failed to parse decimal value %q for column %q: %w", s, columnMetadata.Name, err)
				}
				return duckdb.Decimal{Width: width, Scale: scale, Value: unscaled}, nil
			}
			return value, nil
		}
		// For all other types (BIGINT, BOOLEAN, VARCHAR, etc.), pass through as-is
		// The driver will handle basic conversions
		return value, nil
	}
}

func parseDecimalType(typ string) (width uint8, scale uint8, err error) {
	start := strings.IndexByte(typ, '(')
	end := strings.IndexByte(typ, ')')
	if start == -1 || end == -1 {
		return 0, 0, fmt.Errorf("missing (width,scale) in type %q", typ)
	}
	parts := strings.SplitN(typ[start+1:end], ",", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("expected (width,scale) in type %q", typ)
	}
	w, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 8)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid width in type %q: %w", typ, err)
	}
	s, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 8)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid scale in type %q: %w", typ, err)
	}
	return uint8(w), uint8(s), nil
}

// parseDecimalString converts a decimal string like "123.45" into the unscaled
// *big.Int representation for the given scale (e.g. scale=2 → 12345).
func parseDecimalString(s string, scale uint8) (*big.Int, error) {
	parts := strings.SplitN(strings.TrimSpace(s), ".", 2)
	intPart := parts[0]
	var fracPart string
	if len(parts) == 2 {
		fracPart = parts[1]
	}

	if len(fracPart) < int(scale) {
		fracPart += strings.Repeat("0", int(scale)-len(fracPart))
	} else if len(fracPart) > int(scale) {
		for _, c := range fracPart[scale:] {
			if c != '0' {
				return nil, fmt.Errorf("value %q has %d fractional digits, exceeding scale %d", s, len(fracPart), scale)
			}
		}
		fracPart = fracPart[:scale]
	}

	unscaled := intPart + fracPart
	result, ok := new(big.Int).SetString(unscaled, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse %q as integer", unscaled)
	}
	return result, nil
}
