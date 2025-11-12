package api

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	_ "github.com/duckdb/duckdb-go/v2"
)

func handleExecute(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	dsn := r.Header.Get(ducktape.DuckDBConnectionStringHeader)
	if dsn == "" {
		err := fmt.Errorf("%q header is required", ducktape.DuckDBConnectionStringHeader)
		errMsg := err.Error()
		handleBadRequestJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
		return
	}

	request, err := getRequestBody[ducktape.ExecuteRequest](r)
	if err != nil {
		errMsg := err.Error()
		handleBadRequestJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
		return
	}
	ctx := r.Context()

	result, err := Execute(ctx, dsn, request)
	if err != nil {
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.ExecuteResponse{Error: &errMsg}, err)
		return
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		err := fmt.Errorf("failed to get the rows affected: %v", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.ExecuteResponse{Error: &errMsg}, err)
		return
	}

	response := ducktape.ExecuteResponse{
		RowsAffectedCount: rowsAffected,
	}
	body, err := json.Marshal(response)
	if err != nil {
		err := fmt.Errorf("failed to marshal the response: %v", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.ExecuteResponse{Error: &errMsg}, err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(body)
	slog.Debug("execution results", slog.Any("rows affected", rowsAffected), slog.Duration("elapsed", time.Since(start)))
}

func Execute(ctx context.Context, dsn string, request ducktape.ExecuteRequest) (sql.Result, error) {
	if len(request.Statements) == 0 {
		return nil, fmt.Errorf("at least one statement is required")
	}

	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to start a SQL client for execute(%q): %w", "duckdb", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to validate the DB connection for execute(%q): %w", "duckdb", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin a transaction for execute(%q): %w", "duckdb", err)
	}
	defer tx.Rollback()

	var totalRowsAffected int64

	for _, statement := range request.Statements {

		slog.Debug("executing duckdb query", slog.String("query", statement.Query), slog.Any("args", statement.Args))

		result, err := tx.ExecContext(ctx, statement.Query, statement.Args...)
		if err != nil {
			return nil, fmt.Errorf("failed to execute the query: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("failed to get the rows affected: %v", err)
		}
		totalRowsAffected += rowsAffected
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit the transaction: %w", err)
	}
	return ducktape.ExecuteResponse{RowsAffectedCount: totalRowsAffected}, nil
}
