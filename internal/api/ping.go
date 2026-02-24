package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
)

func handlePing(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx := r.Context()
	db, err := DBFromContext(ctx)
	if err != nil {
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
		return
	}

	if err := db.PingContext(ctx); err != nil {
		err := fmt.Errorf("failed to validate the DB connection: %w", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
		return
	}

	w.WriteHeader(http.StatusOK)

	slog.Debug("ping result", slog.Duration("elapsed", time.Since(start)))
}
