package api

import (
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	"github.com/artie-labs/ducktape/internal/utils"
	_ "github.com/duckdb/duckdb-go/v2"
)

func handleQuery(w http.ResponseWriter, r *http.Request) {
	dsn := r.Header.Get(ducktape.DuckDBConnectionStringHeader)
	if dsn == "" {
		err := fmt.Errorf("%q header is required", ducktape.DuckDBConnectionStringHeader)
		errMsg := err.Error()
		handleBadRequestJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
		return
	}

	request, err := getRequestBody[ducktape.QueryRequest](r)
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

	slog.Debug("querying duckdb", slog.String("query", request.Query), slog.Any("args", request.Args))

	rows, err := conn.QueryContext(ctx, request.Query, request.Args...)
	if err != nil {
		err := fmt.Errorf("failed to query the DB: %v", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
		return
	}
	defer rows.Close()

	objects, err := utils.RowsToObjects(rows)
	if err != nil {
		err := fmt.Errorf("failed to convert rows to objects: %v", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
		return
	}

	slog.Debug("query results", slog.Any("rows", objects))

	response := ducktape.QueryResponse{
		Rows: objects,
	}
	body, err := json.Marshal(response)
	if err != nil {
		err := fmt.Errorf("failed to marshal the response: %v", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}
