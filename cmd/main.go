package main

import (
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/artie-labs/ducktape/internal/api"
	"github.com/artie-labs/ducktape/internal/logging"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func main() {
	var level slog.Level
	logLevelEnv := os.Getenv("DUCKTAPE_LOG")

	switch strings.ToLower(logLevelEnv) {
	case "debug", "d":
		level = slog.LevelDebug
	case "info", "i":
		level = slog.LevelInfo
	case "warn", "w", "warning":
		level = slog.LevelWarn
	case "error", "e":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	infoHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug, // Don't filter here, we'll filter in the custom handler
	})

	errorHandler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug, // Don't filter here, we'll filter in the custom handler
	})

	logger := slog.New(&logging.SplitHandler{
		Level:        level,
		InfoHandler:  infoHandler,
		ErrorHandler: errorHandler,
	})
	slog.SetDefault(logger)

	mux := http.NewServeMux()

	api.RegisterApiRoutes(mux)
	api.RegisterHealthCheckRoutes(mux)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Wrap the mux with h2c to support both HTTP/1.1 and HTTP/2
	h2cHandler := h2c.NewHandler(mux, &http2.Server{})

	log.Printf("Starting server on port %s\n", port)
	log.Fatal(http.ListenAndServe("0.0.0.0:"+port, h2cHandler))
}
