package logging

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestFieldConstructors(t *testing.T) {
	cases := []struct {
		field Field
		key   string
		value any
	}{
		{String("k", "v"), "k", "v"},
		{Int("n", 5), "n", 5},
		{Int64("n64", 5), "n64", int64(5)},
		{Bool("b", true), "b", true},
		{Duration("d", time.Second), "d", time.Second},
		{Any("a", 1.5), "a", 1.5},
	}
	for _, c := range cases {
		if c.field.Key != c.key || c.field.Value != c.value {
			t.Errorf("field = %+v, want {%s %v}", c.field, c.key, c.value)
		}
	}

	errField := Err(context.Canceled)
	if errField.Key != "error" || errField.Value != context.Canceled {
		t.Errorf("Err() = %+v, want error field", errField)
	}
}

func TestNopLoggerDoesNotPanic(t *testing.T) {
	var l Logger = NopLogger{}
	ctx := context.Background()
	l.Debug(ctx, "d", String("a", "b"))
	l.Info(ctx, "i")
	l.Warn(ctx, "w")
	l.Error(ctx, "e", Err(context.Canceled))
}

func TestSlogLoggerEmitsFields(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	l := NewSlogLogger(slog.New(handler))

	l.Info(context.Background(), "publishing", String("exchange", "events"), Int("size", 42))

	out := buf.String()
	if !strings.Contains(out, "msg=publishing") {
		t.Errorf("output missing message: %q", out)
	}
	if !strings.Contains(out, "exchange=events") {
		t.Errorf("output missing string field: %q", out)
	}
	if !strings.Contains(out, "size=42") {
		t.Errorf("output missing int field: %q", out)
	}
}

func TestSlogLoggerWarnAndError(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	l := NewSlogLogger(slog.New(handler))

	l.Debug(context.Background(), "debug-msg")
	l.Warn(context.Background(), "warn-msg", String("k", "v"))
	l.Error(context.Background(), "error-msg", Err(context.Canceled))

	out := buf.String()
	for _, want := range []string{"debug-msg", "warn-msg", "error-msg"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q: %q", want, out)
		}
	}
}

func TestSlogLoggerRespectsLevel(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})
	l := NewSlogLogger(slog.New(handler))

	l.Debug(context.Background(), "debug-should-be-filtered")
	if buf.Len() != 0 {
		t.Errorf("debug line should be filtered at warn level, got: %q", buf.String())
	}
}

func TestSlogLoggerNilUsesDefault(t *testing.T) {
	if NewSlogLogger(nil) == nil {
		t.Error("NewSlogLogger(nil) should return a usable logger")
	}
}
