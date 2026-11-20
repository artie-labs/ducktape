package api

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"database/sql/driver"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	"github.com/artie-labs/ducktape/internal/utils"
	"github.com/duckdb/duckdb-go/v2"
)

var (
	// flushInterval is the maximum number of rows the Appender buffers before flushing.
	// Tunable via DUCKTAPE_FLUSH_ROWS.
	flushInterval = envIntDefault("DUCKTAPE_FLUSH_ROWS", 100_000)

	// flushBytesLimit is the maximum payload size the Appender buffers before flushing.
	// Tunable via DUCKTAPE_FLUSH_BYTES. Larger values amortise round-trip cost to remote
	// destinations (e.g. MotherDuck) at the price of more in-process memory per stream.
	flushBytesLimit = envIntDefault("DUCKTAPE_FLUSH_BYTES", 32*1024*1024)

	// maxScannerBuffer caps the size of a single NDJSON line read by the bufio.Scanner.
	// Tunable via DUCKTAPE_SCANNER_BUFFER for tables with large text/blob columns.
	maxScannerBuffer = envIntDefault("DUCKTAPE_SCANNER_BUFFER", 4*1024*1024)
)

func envIntDefault(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		slog.Warn(fmt.Sprintf("Invalid value for environment variable %q: %q, using fallback %d", key, v, fallback))
		return fallback
	}
	if n <= 0 {
		slog.Warn(fmt.Sprintf("Non-positive value for environment variable %q: %q, using fallback %d", key, v, fallback))
		return fallback
	}
	return n
}

func handleAppend(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.ProtoMajor != 2 {
		err := fmt.Errorf("HTTP/2 is required, got %s", r.Proto)
		errMsg := err.Error()
		handleBadRequestJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return
	}

	dsn := r.Header.Get(ducktape.DuckDBConnectionStringHeader)
	if dsn == "" {
		err := fmt.Errorf("%q header is required", ducktape.DuckDBConnectionStringHeader)
		errMsg := err.Error()
		handleBadRequestJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return
	}

	database := r.Header.Get(ducktape.DuckDBDatabaseHeader)
	if database == "" {
		err := fmt.Errorf("%q header is required", ducktape.DuckDBDatabaseHeader)
		errMsg := err.Error()
		handleBadRequestJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return
	}

	schema := cmp.Or(r.Header.Get(ducktape.DuckDBSchemaHeader), "main")

	table := r.Header.Get(ducktape.DuckDBTableHeader)
	if table == "" {
		err := fmt.Errorf("%q header is required", ducktape.DuckDBTableHeader)
		errMsg := err.Error()
		handleBadRequestJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return
	}

	ctx := r.Context()

	rowsAppended, bytesRead, err := Append(ctx, dsn, database, schema, table, r.Body)
	if err != nil {
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return
	}

	// Return success response
	response := ducktape.AppendResponse{
		RowsAppended: rowsAppended,
	}
	body, err := json.Marshal(response)
	if err != nil {
		err := fmt.Errorf("failed to marshal response: %v", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(body)
	slog.Info(fmt.Sprintf("append complete for table %s.%s.%s", database, schema, table), slog.Int64("totalRowsAppended", rowsAppended), slog.Uint64("totalBytesRead", bytesRead), slog.Duration("elapsed", time.Since(start)))
}

func Append(ctx context.Context, dsn string, database string, schema string, table string, input io.Reader) (rowsAppended int64, bytesRead uint64, err error) {
	connector, err := utils.NewConnector(ctx, dsn)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to start a SQL client for append(%q): %w", "duckdb", err)
	}
	defer connector.Close()

	db := connector.GetDB()

	if err = db.Ping(); err != nil {
		return 0, 0, fmt.Errorf("failed to validate the DB connection for append(%q): %w", "duckdb", err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get a connection for append(%q): %w", "duckdb", err)
	}
	defer conn.Close()

	columnMetadata, err := utils.GetColumnMetadata(ctx, conn, database, schema, table)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get column metadata for append(%q): %w", "duckdb", err)
	}

	var appender *duckdb.Appender
	err = conn.Raw(func(driverConn any) error {
		var appErr error
		appender, appErr = duckdb.NewAppender(driverConn.(driver.Conn), database, schema, table)
		if appErr != nil {
			return appErr
		}
		return nil
	})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create an appender(%q): %w", "duckdb", err)
	}
	var appenderClosed bool
	defer func() {
		if !appenderClosed {
			appender.Close()
		}
	}()

	// Stream NDJSON from request body
	scanner := bufio.NewScanner(input)
	// Per-line scanner buffer; tunable via DUCKTAPE_SCANNER_BUFFER for unusually wide rows.
	// Initial capacity is capped at maxScannerBuffer so the two values stay consistent.
	buf := make([]byte, 0, min(64*1024, maxScannerBuffer))
	scanner.Buffer(buf, maxScannerBuffer)
	var bytesSinceFlush uint64

	var exactReader bytes.Reader
	exactDecoder := json.NewDecoder(&exactReader)
	exactDecoder.UseNumber()
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue // Skip empty lines
		}

		lineBytes := uint64(len(line))
		bytesRead += lineBytes
		bytesSinceFlush += lineBytes

		var rowMsg ducktape.RowMessage
		if appendMayNeedExactNumbers(line) {
			// Some backends accept invalid tokens with UseNumber. Validate the
			// complete line before decoding original integer tokens.
			if !stdjson.Valid(line) {
				return 0, 0, fmt.Errorf("failed to unmarshal row message for database %s, schema %s, table %s: invalid JSON", database, schema, table)
			}
			exactReader.Reset(line)
			if err := exactDecoder.Decode(&rowMsg); err != nil {
				return 0, 0, fmt.Errorf("failed to unmarshal row message for database %s, schema %s, table %s: %w", database, schema, table, err)
			}
			for i, value := range rowMsg.Values {
				value, err = normalizeAppendNumbers(value)
				if err != nil {
					return 0, 0, fmt.Errorf("failed to unmarshal row message for database %s, schema %s, table %s: failed to convert numeric value at index %d: %w", database, schema, table, i, err)
				}
				rowMsg.Values[i] = value
			}
		} else if err := json.Unmarshal(line, &rowMsg); err != nil {
			return 0, 0, fmt.Errorf("failed to unmarshal row message for database %s, schema %s, table %s: %w", database, schema, table, err)
		}

		values := make([]driver.Value, len(rowMsg.Values))
		for i, v := range rowMsg.Values {
			if i >= len(columnMetadata) {
				return 0, 0, fmt.Errorf("value index %d exceeds number of columns %d for database %s, schema %s, table %s", i, len(columnMetadata), database, schema, table)
			}
			convertedValue, err := utils.ConvertValue(v, columnMetadata[i])
			if err != nil {
				return 0, 0, fmt.Errorf("failed to convert value while appending for database %s, schema %s, table %s: %w", database, schema, table, err)
			}
			values[i] = convertedValue
		}

		if err := appender.AppendRow(values...); err != nil {
			return 0, 0, fmt.Errorf("failed to append row for database %s, schema %s, table %s: %w", database, schema, table, err)
		}

		rowsAppended++

		// Flush if we've reached row limit OR bytes limit
		if rowsAppended%int64(flushInterval) == 0 || bytesSinceFlush >= uint64(flushBytesLimit) {
			slog.Info(fmt.Sprintf("flushing appender for database %s, schema %s, table %s", database, schema, table), slog.Int64("rowsAppended", rowsAppended), slog.Uint64("bytesRead", bytesRead), slog.Uint64("bytesSinceFlush", bytesSinceFlush))
			if err := appender.Flush(); err != nil {
				return 0, 0, fmt.Errorf("failed to flush appender for database %s, schema %s, table %s: %w", database, schema, table, err)
			}
			bytesSinceFlush = 0 // Reset counter after flush
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		if errors.Is(err, bufio.ErrTooLong) {
			return 0, 0, fmt.Errorf(
				"failed to read request stream for database %s, schema %s, table %s: row %d exceeded the scanner buffer limit of %d bytes; set DUCKTAPE_SCANNER_BUFFER env var to a larger value",
				database, schema, table, rowsAppended+1, maxScannerBuffer,
			)
		}
		return 0, 0, fmt.Errorf("failed to read request stream for database %s, schema %s, table %s: %w", database, schema, table, err)
	}

	appenderClosed = true
	if err := appender.Close(); err != nil {
		return 0, 0, fmt.Errorf("failed to close appender for database %s, schema %s, table %s: %w", database, schema, table, err)
	}

	return rowsAppended, bytesRead, nil
}

// Begin append value helpers.

// appendMayNeedExactNumbers avoids exact-decode allocations for ordinary rows.
// Every integer outside float64's exact range has at least 16 decimal digits.
// Skip strings using byte search so large text payloads do not require a scalar
// visit per byte. Backslash parity distinguishes escaped quotes from string ends.
// This selects a decoder only; that decoder must still validate the whole line.
func appendMayNeedExactNumbers(line []byte) bool {
	digits := 0
	for i := 0; i < len(line); i++ {
		c := line[i]
		if c == '"' {
			digits = 0
			next := bytes.IndexByte(line[i+1:], '"')
			if next < 0 {
				// Unmarshal will reject an unterminated string.
				return false
			}
			end := i + 1 + next
			slashes := 0
			for j := end - 1; j > i && line[j] == '\\'; j-- {
				slashes++
			}
			i = end
			if slashes%2 != 0 {
				// Escape-dense text is cheaper to walk than to search per quote.
				for i++; i < len(line); i++ {
					if line[i] == '\\' {
						i++ // Skip the escaped byte, including an escaped backslash.
					} else if line[i] == '"' {
						break
					}
				}
			}
		} else if c >= '0' && c <= '9' {
			digits++
			if digits == 16 {
				return true
			}
		} else {
			digits = 0
		}
	}
	return false
}

// normalizeAppendNumbers keeps integer tokens exact while supplying concrete
// numeric types accepted by the appender, including inside lists and structs.
func normalizeAppendNumbers(value any) (any, error) {
	switch v := value.(type) {
	case stdjson.Number:
		// Preserve the sign of floating-point zero. Fractional/exponent forms and
		// integers outside 64-bit ranges retain the existing float64 conversion.
		if v.String() != "-0" && !strings.ContainsAny(v.String(), ".eE") {
			if n, err := v.Int64(); err == nil {
				return n, nil
			}
			if n, err := strconv.ParseUint(v.String(), 10, 64); err == nil {
				return n, nil
			}
		}
		return v.Float64()
	case []any:
		for i, child := range v {
			converted, err := normalizeAppendNumbers(child)
			if err != nil {
				return nil, err
			}
			v[i] = converted
		}
	case map[string]any:
		for key, child := range v {
			converted, err := normalizeAppendNumbers(child)
			if err != nil {
				return nil, err
			}
			v[key] = converted
		}
	}
	return value, nil
}

// End append value helpers.
