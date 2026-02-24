package api

import (
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	jsoniter "github.com/json-iterator/go"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	_ "github.com/duckdb/duckdb-go/v2"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func RegisterHealthCheckRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
}

func RegisterApiRoutes(mux *http.ServeMux) {
	mux.HandleFunc(fmt.Sprintf("POST %s", ducktape.ExecuteRoute), withDB(handleExecute))
	mux.HandleFunc(fmt.Sprintf("POST %s", ducktape.QueryRoute), withDB(handleQuery))
	mux.HandleFunc(fmt.Sprintf("POST %s", ducktape.AppendRoute), withDB(handleAppend))
	mux.HandleFunc(fmt.Sprintf("GET %s", ducktape.PingRoute), withDB(handlePing))
}

func withDB(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		dsn := r.Header.Get(ducktape.DuckDBConnectionStringHeader)
		if dsn == "" {
			err := fmt.Errorf("%q header is required", ducktape.DuckDBConnectionStringHeader)
			errMsg := err.Error()
			handleBadRequestJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
			return
		}

		db, err := sql.Open("duckdb", dsn)
		if err != nil {
			errMsg := err.Error()
			handleInternalServerErrorJSON(w, ducktape.QueryResponse{Error: &errMsg}, err)
			return
		}
		defer db.Close()

		next(w, r.WithContext(WithDB(r.Context(), db)))
	}
}

func getRequestBody[T any](r *http.Request) (T, error) {
	var request T
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("failed to read the request body: %v", err)
	}
	if err := json.Unmarshal(body, &request); err != nil {
		var zero T
		return zero, fmt.Errorf("failed to unmarshal the request: %v", err)
	}
	return request, nil
}

func handleBadRequestJSON[T any](w http.ResponseWriter, response T, err error) {
	slog.Error("returning bad request", slog.Any("error", err))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)

	body, marshalErr := json.Marshal(response)
	if marshalErr != nil {
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(body)
}

func handleInternalServerErrorJSON[T any](w http.ResponseWriter, response T, err error) {
	slog.Error("returning internal server error", slog.Any("error", err))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)

	body, marshalErr := json.Marshal(response)
	if marshalErr != nil {
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(body)
}
