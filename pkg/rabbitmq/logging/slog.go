package logging

import (
	"context"
	"log/slog"
)

// SlogLogger adapts a *slog.Logger to the Logger interface, so callers relying
// on the standard library need no third-party logging dependency.
type SlogLogger struct {
	logger *slog.Logger
}

// NewSlogLogger returns a Logger backed by the given *slog.Logger. If logger is
// nil, slog.Default() is used.
func NewSlogLogger(logger *slog.Logger) *SlogLogger {
	if logger == nil {
		logger = slog.Default()
	}
	return &SlogLogger{logger: logger}
}

func (l *SlogLogger) log(ctx context.Context, level slog.Level, msg string, fields []Field) {
	if !l.logger.Enabled(ctx, level) {
		return
	}
	attrs := make([]any, 0, len(fields))
	for _, f := range fields {
		attrs = append(attrs, slog.Any(f.Key, f.Value))
	}
	l.logger.Log(ctx, level, msg, attrs...)
}

// Debug implements Logger.
func (l *SlogLogger) Debug(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, slog.LevelDebug, msg, fields)
}

// Info implements Logger.
func (l *SlogLogger) Info(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, slog.LevelInfo, msg, fields)
}

// Warn implements Logger.
func (l *SlogLogger) Warn(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, slog.LevelWarn, msg, fields)
}

// Error implements Logger.
func (l *SlogLogger) Error(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, slog.LevelError, msg, fields)
}
