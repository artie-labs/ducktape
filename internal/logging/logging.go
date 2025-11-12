package logging

import (
	"context"
	"log/slog"
)

// splitHandler routes logs to different handlers based on level
type SplitHandler struct {
	Level        slog.Level
	InfoHandler  slog.Handler
	ErrorHandler slog.Handler
}

func (h *SplitHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.Level
}

func (h *SplitHandler) Handle(ctx context.Context, r slog.Record) error {
	// Only process logs at or above the configured level
	if r.Level < h.Level {
		return nil
	}

	// Route ERROR and above to stderr, everything else to stdout
	if r.Level >= slog.LevelError {
		return h.ErrorHandler.Handle(ctx, r)
	}
	return h.InfoHandler.Handle(ctx, r)
}

func (h *SplitHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &SplitHandler{
		Level:        h.Level,
		InfoHandler:  h.InfoHandler.WithAttrs(attrs),
		ErrorHandler: h.ErrorHandler.WithAttrs(attrs),
	}
}

func (h *SplitHandler) WithGroup(name string) slog.Handler {
	return &SplitHandler{
		Level:        h.Level,
		InfoHandler:  h.InfoHandler.WithGroup(name),
		ErrorHandler: h.ErrorHandler.WithGroup(name),
	}
}
