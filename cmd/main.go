package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	"github.com/artie-labs/ducktape/internal/api"
	"github.com/artie-labs/ducktape/internal/logging"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const (
	defaultDrainDelay = 10 * time.Second
	shutdownTimeout   = 5 * time.Minute
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

	username := os.Getenv("DUCKTAPE_USERNAME")
	password := os.Getenv("DUCKTAPE_PASSWORD")
	if username != "" && password != "" {
		slog.Info("Basic authentication is enabled.")
	} else if username != "" || password != "" {
		slog.Warn("Basic authentication is disabled because both DUCKTAPE_USERNAME and DUCKTAPE_PASSWORD must be non-empty.")
	}

	mux := http.NewServeMux()

	api.RegisterApiRoutes(mux)
	api.RegisterHealthCheckRoutes(mux)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Wrap the mux with h2c to support both HTTP/1.1 and HTTP/2
	h2cHandler := h2c.NewHandler(mux, &http2.Server{
		MaxReadFrameSize:             ducktape.RecommendedBufferSize,
		MaxUploadBufferPerConnection: ducktape.RecommendedBufferSize * 16, // 16 MB connection window
		MaxUploadBufferPerStream:     ducktape.RecommendedBufferSize * 4,  // 4 MB per stream
	})

	api.SetDraining(false)
	server := &http.Server{
		Addr:    "0.0.0.0:" + port,
		Handler: h2cHandler,
	}

	serverErrCh := make(chan error, 1)
	go func() {
		log.Printf("Starting server on port %s\n", port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
	}()

	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-serverErrCh:
		log.Fatal(err)
	case <-signalContext.Done():
	}

	drainDelay := 0 * time.Second
	if drainDelayEnv := os.Getenv("DUCKTAPE_DRAIN_DELAY"); drainDelayEnv != "" {
		parsedDrainDelay, err := time.ParseDuration(drainDelayEnv)
		if err != nil {
			log.Printf("Invalid DUCKTAPE_DRAIN_DELAY=%q, using %s", drainDelayEnv, defaultDrainDelay)
			drainDelay = defaultDrainDelay
		} else if parsedDrainDelay < 0 {
			log.Printf("Ignoring negative DUCKTAPE_DRAIN_DELAY=%q, using %s", drainDelayEnv, defaultDrainDelay)
			drainDelay = defaultDrainDelay
		} else {
			drainDelay = parsedDrainDelay
		}
	}

	api.SetDraining(true)
	if drainDelay > 0 {
		log.Printf("Received shutdown signal, entering drain period for %s", drainDelay)
		time.Sleep(drainDelay)
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownContext); err != nil {
		log.Printf("Graceful shutdown failed: %v", err)
		if closeErr := server.Close(); closeErr != nil {
			log.Printf("Forced close failed: %v", closeErr)
		}
	}
}
