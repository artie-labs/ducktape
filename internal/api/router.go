package api

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"

	jsoniter "github.com/json-iterator/go"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func RegisterHealthCheckRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
}

func RegisterApiRoutes(mux *http.ServeMux) {
	mux.HandleFunc(fmt.Sprintf("POST %s", ducktape.ExecuteRoute), handleExecute)
	mux.HandleFunc(fmt.Sprintf("POST %s", ducktape.QueryRoute), handleQuery)

	// Wrap the append handler with h2c to support HTTP/2 cleartext
	h2cHandler := h2c.NewHandler(http.HandlerFunc(handleAppend), &http2.Server{})
	mux.Handle(fmt.Sprintf("POST %s", ducktape.AppendRoute), h2cHandler)
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
