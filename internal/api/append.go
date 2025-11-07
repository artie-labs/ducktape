package api

import (
	"bufio"
	"cmp"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	"github.com/duckdb/duckdb-go/v2"
)

const flushInterval = 100_000

func handleAppend(w http.ResponseWriter, r *http.Request) {
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

	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		err := fmt.Errorf("failed to start a SQL client for driver %q: %v", "duckdb", err)
		errMsg := err.Error()
		handleBadRequestJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		err := fmt.Errorf("failed to validate the DB connection for driver %q: %v", "duckdb", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		err := fmt.Errorf("failed to get a connection: %v", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return
	}
	defer conn.Close()

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
		err := fmt.Errorf("failed to create an appender: %v", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return
	}
	defer appender.Close()

	// Stream NDJSON from request body
	scanner := bufio.NewScanner(r.Body)
	var rowsAppended int64
	var bytesRead int64

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue // Skip empty lines
		}

		bytesRead += int64(len(line))

		var rowMsg ducktape.RowMesssage
		if err := json.Unmarshal(line, &rowMsg); err != nil {
			err := fmt.Errorf("failed to unmarshal row message: %v", err)
			errMsg := err.Error()
			handleBadRequestJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
			return
		}

		// Convert []any to []driver.Value
		values := make([]driver.Value, len(rowMsg.Values))
		for i, v := range rowMsg.Values {
			values[i] = v
		}

		if err := appender.AppendRow(values...); err != nil {
			err := fmt.Errorf("failed to append row: %v", err)
			errMsg := err.Error()
			handleInternalServerErrorJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
			return
		}

		rowsAppended++

		if rowsAppended%flushInterval == 0 {
			slog.Info("flushing appender", slog.Int64("rowsAppended", rowsAppended), slog.Int64("bytesRead", bytesRead))
			if err := appender.Flush(); err != nil {
				err := fmt.Errorf("failed to flush appender: %v", err)
				errMsg := err.Error()
				handleInternalServerErrorJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
				return
			}
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		err := fmt.Errorf("error reading request stream: %v", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return
	}

	if err := appender.Flush(); err != nil {
		err := fmt.Errorf("failed to flush appender: %v", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return
	}

	slog.Info("append complete", slog.Int64("totalRowsAppended", rowsAppended), slog.Int64("totalBytesRead", bytesRead))

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
}
