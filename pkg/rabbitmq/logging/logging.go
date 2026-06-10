// Package logging defines the structured logging interface used by the rabbitmq
// library and ships two ready-to-use adapters: NopLogger (the default) and
// SlogLogger (backed by the standard library's log/slog). Bridge any other
// logging stack by implementing Logger — see the project README for a zap
// example.
package logging

import (
	"context"
	"time"
)

// Logger is the structured logging interface used by the library. Every method
// receives the active context.Context so implementations can pull out
// correlation data (request IDs, trace/span IDs) when emitting a line.
type Logger interface {
	Debug(ctx context.Context, msg string, fields ...Field)
	Info(ctx context.Context, msg string, fields ...Field)
	Warn(ctx context.Context, msg string, fields ...Field)
	Error(ctx context.Context, msg string, fields ...Field)
}

// Field is a single structured key/value pair attached to a log line. Use the
// constructor helpers (String, Int, Err, ...) so adapters can rely on Value's
// dynamic type.
type Field struct {
	Key   string
	Value any
}

// String returns a string-valued Field.
func String(key, value string) Field { return Field{Key: key, Value: value} }

// Int returns an int-valued Field.
func Int(key string, value int) Field { return Field{Key: key, Value: value} }

// Int64 returns an int64-valued Field.
func Int64(key string, value int64) Field { return Field{Key: key, Value: value} }

// Bool returns a bool-valued Field.
func Bool(key string, value bool) Field { return Field{Key: key, Value: value} }

// Duration returns a time.Duration-valued Field.
func Duration(key string, value time.Duration) Field { return Field{Key: key, Value: value} }

// Any returns a Field holding an arbitrary value.
func Any(key string, value any) Field { return Field{Key: key, Value: value} }

// Err returns a Field carrying an error under the conventional "error" key.
func Err(err error) Field { return Field{Key: "error", Value: err} }
