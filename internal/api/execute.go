package api

import (
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	_ "github.com/duckdb/duckdb-go/v2"
)

func handleExecute(w http.ResponseWriter, r *http.Request) {
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

	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		err := fmt.Errorf("failed to start a SQL client for driver %q: %v", "duckdb", err)
		errMsg := err.Error()
		handleBadRequestJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
		return
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		err := fmt.Errorf("failed to validate the DB connection for driver %q: %v", "duckdb", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
		return
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		err := fmt.Errorf("failed to get a connection: %v", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
		return
	}
	defer conn.Close()

	slog.Debug("executing duckdb query", slog.String("query", request.Query), slog.Any("args", request.Args))

	result, err := conn.ExecContext(ctx, request.Query, request.Args...)
	if err != nil {
		err := fmt.Errorf("failed to execute the query: %v", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
		return
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		err := fmt.Errorf("failed to get the rows affected: %v", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
		return
	}

	slog.Debug("execution results", slog.Any("result", result))

	response := ducktape.ExecuteResponse{
		RowsAffectedCount: rowsAffected,
	}
	body, err := json.Marshal(response)
	if err != nil {
		err := fmt.Errorf("failed to marshal the response: %v", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}
