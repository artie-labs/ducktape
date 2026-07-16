package api

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"

	jsoniter "github.com/json-iterator/go"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary
var draining atomic.Bool

func SetDraining(v bool) {
	draining.Store(v)
}

func RegisterHealthCheckRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if draining.Load() {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("READY"))
	})
}

func RegisterApiRoutes(mux *http.ServeMux) {
	username := os.Getenv("DUCKTAPE_USERNAME")
	password := os.Getenv("DUCKTAPE_PASSWORD")

	var wrap func(http.HandlerFunc) http.HandlerFunc
	if username != "" && password != "" {
		wrap = func(next http.HandlerFunc) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				u, p, ok := r.BasicAuth()
				if !ok || u != username || p != password {
					w.Header().Set("WWW-Authenticate", `Basic realm="restricted", charset="UTF-8"`)
					http.Error(w, "Unauthorized", http.StatusUnauthorized)
					return
				}
				next(w, r)
			}
		}
	} else {
		wrap = func(next http.HandlerFunc) http.HandlerFunc {
			return next
		}
	}

	mux.HandleFunc(fmt.Sprintf("POST %s", ducktape.ExecuteRoute), wrap(handleExecute))
	mux.HandleFunc(fmt.Sprintf("POST %s", ducktape.QueryRoute), wrap(handleQuery))
	mux.HandleFunc(fmt.Sprintf("POST %s", ducktape.AppendRoute), wrap(handleAppend))
	mux.HandleFunc(fmt.Sprintf("GET %s", ducktape.PingRoute), wrap(handlePing))
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
