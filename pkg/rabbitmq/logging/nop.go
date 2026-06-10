package logging

import "context"

// NopLogger is a Logger that discards everything. It is the default used when
// no Logger is configured.
type NopLogger struct{}

// Debug implements Logger.
func (NopLogger) Debug(context.Context, string, ...Field) {}

// Info implements Logger.
func (NopLogger) Info(context.Context, string, ...Field) {}

// Warn implements Logger.
func (NopLogger) Warn(context.Context, string, ...Field) {}

// Error implements Logger.
func (NopLogger) Error(context.Context, string, ...Field) {}
