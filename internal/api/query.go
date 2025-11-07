package api

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	"github.com/artie-labs/ducktape/internal/utils"
	_ "github.com/duckdb/duckdb-go/v2"
)

func handleQuery(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
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

	objects, err := Query(ctx, dsn, request)
	if err != nil {
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
		return
	}

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
	slog.Debug("query results", slog.Any("rows", objects), slog.Duration("elapsed", time.Since(start)))
}

func Query(ctx context.Context, dsn string, request ducktape.QueryRequest) ([]map[string]any, error) {
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to start a SQL client for queries(%q): %w", "duckdb", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to validate the DB connection for queries(%q): %w", "duckdb", err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get a connection for queries(%q): %w", "duckdb", err)
	}
	defer conn.Close()

	slog.Debug("querying duckdb", slog.String("query", request.Query), slog.Any("args", request.Args))

	rows, err := conn.QueryContext(ctx, request.Query, request.Args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query the DB: %w", err)
	}
	defer rows.Close()

	objects, err := utils.RowsToObjects(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to convert rows to objects: %w", err)
	}
	return objects, nil
}
