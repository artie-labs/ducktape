package main

import (
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/artie-labs/ducktape/internal/api"
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

	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)

	mux := http.NewServeMux()

	api.RegisterApiRoutes(mux)
	api.RegisterHealthCheckRoutes(mux)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting server on port %s\n", port)
	log.Fatal(http.ListenAndServe("0.0.0.0:"+port, mux))
}
