package rabbitmq

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsNonRetryable(t *testing.T) {
	base := errors.New("boom")

	if IsNonRetryable(base) {
		t.Error("plain error should be retryable")
	}
	if !IsNonRetryable(NewNonRetryableError(base)) {
		t.Error("wrapped error should be non-retryable")
	}

	// Wrapped further down the chain.
	chained := fmt.Errorf("context: %w", NewNonRetryableError(base))
	if !IsNonRetryable(chained) {
		t.Error("non-retryable error nested in a chain should be detected")
	}
}

func TestNonRetryableErrorUnwrap(t *testing.T) {
	base := errors.New("boom")
	wrapped := NewNonRetryableError(base)

	if wrapped.Error() != "boom" {
		t.Errorf("Error() = %q, want %q", wrapped.Error(), "boom")
	}
	if !errors.Is(wrapped, base) {
		t.Error("errors.Is should find the wrapped base error")
	}
}

func TestNonRetryableErrorNilSafe(t *testing.T) {
	var e *NonRetryableError
	if e.Unwrap() != nil {
		t.Error("nil NonRetryableError Unwrap should be nil")
	}
	if e.Error() != "non-retryable error" {
		t.Errorf("nil NonRetryableError Error = %q", e.Error())
	}
}
