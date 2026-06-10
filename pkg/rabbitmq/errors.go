package rabbitmq

import "errors"

// NonRetryableError wraps an error to signal that the message that produced it
// must not be retried. A Consumer routes such messages straight to the
// dead-letter queue instead of redelivering them.
type NonRetryableError struct {
	Err error
}

// Error implements the error interface.
func (e *NonRetryableError) Error() string {
	if e == nil || e.Err == nil {
		return "non-retryable error"
	}
	return e.Err.Error()
}

// Unwrap exposes the wrapped error for errors.Is / errors.As.
func (e *NonRetryableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// NewNonRetryableError wraps err so a Consumer treats it as non-retryable.
func NewNonRetryableError(err error) *NonRetryableError {
	return &NonRetryableError{Err: err}
}

// IsNonRetryable reports whether err, or any error in its chain, is a
// NonRetryableError.
func IsNonRetryable(err error) bool {
	var target *NonRetryableError
	return errors.As(err, &target)
}
