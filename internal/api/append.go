package api

import (
	"bufio"
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	"github.com/artie-labs/ducktape/internal/utils"
	"github.com/duckdb/duckdb-go/v2"
)

const (
	flushInterval   = 100_000
	flushBytesLimit = 3 * 1024 * 1024 // 3MB - flush before reaching DuckDB's 4MB limit
)

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
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to start a SQL client for append(%q): %w", "duckdb", err)
	}
	defer db.Close()

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
	defer appender.Close()

	// Stream NDJSON from request body
	scanner := bufio.NewScanner(input)
	var bytesSinceFlush uint64

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue // Skip empty lines
		}

		lineBytes := uint64(len(line))
		bytesRead += lineBytes
		bytesSinceFlush += lineBytes

		var rowMsg ducktape.RowMessage
		if err := json.Unmarshal(line, &rowMsg); err != nil {
			return 0, 0, fmt.Errorf("failed to unmarshal row message: %w", err)
		}

		values := make([]driver.Value, len(rowMsg.Values))
		for i, v := range rowMsg.Values {
			if i >= len(columnMetadata) {
				return 0, 0, fmt.Errorf("value index %d exceeds number of columns %d", i, len(columnMetadata))
			}
			convertedValue, err := utils.ConvertValue(v, columnMetadata[i])
			if err != nil {
				return 0, 0, fmt.Errorf("failed to convert value while appending: %w", err)
			}
			values[i] = convertedValue
		}

		if err := appender.AppendRow(values...); err != nil {
			return 0, 0, fmt.Errorf("failed to append row: %w", err)
		}

		rowsAppended++

		// Flush if we've reached row limit OR bytes limit
		if rowsAppended%flushInterval == 0 || bytesSinceFlush >= flushBytesLimit {
			slog.Info("flushing appender", slog.Int64("rowsAppended", rowsAppended), slog.Uint64("bytesRead", bytesRead), slog.Uint64("bytesSinceFlush", bytesSinceFlush))
			if err := appender.Flush(); err != nil {
				return 0, 0, fmt.Errorf("failed to flush appender: %w", err)
			}
			bytesSinceFlush = 0 // Reset counter after flush
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		return 0, 0, fmt.Errorf("failed to read request stream: %w", err)
	}

	if err := appender.Flush(); err != nil {
		return 0, 0, fmt.Errorf("failed to flush appender: %w", err)
	}

	return rowsAppended, bytesRead, nil
}
