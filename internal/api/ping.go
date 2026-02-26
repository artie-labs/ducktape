package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	"github.com/artie-labs/ducktape/internal/utils"
	_ "github.com/duckdb/duckdb-go/v2"
)

func handlePing(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	dsn := r.Header.Get(ducktape.DuckDBConnectionStringHeader)
	if dsn == "" {
		err := fmt.Errorf("%q header is required", ducktape.DuckDBConnectionStringHeader)
		errMsg := err.Error()
		handleBadRequestJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
		return
	}

	ctx := r.Context()

	connector, err := utils.NewConnector(ctx, dsn)
	if err != nil {
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
		return
	}
	defer connector.Close()

	db := connector.GetDB()

	if err = db.PingContext(ctx); err != nil {
		err := fmt.Errorf("failed to validate the DB connection for ping(%q): %w", "duckdb", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
		return
	}

	w.WriteHeader(http.StatusOK)

	slog.Debug("ping result", slog.Duration("elapsed", time.Since(start)))
}
