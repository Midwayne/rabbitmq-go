package otelrabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/propagation"
)

// headerCarrier adapts an amqp.Table to the OpenTelemetry TextMapCarrier
// interface so trace context can be injected into and extracted from message
// headers.
type headerCarrier amqp.Table

var _ propagation.TextMapCarrier = headerCarrier{}

// Get returns the string value stored under key, coercing common AMQP header
// value types to string.
func (c headerCarrier) Get(key string) string {
	if c == nil {
		return ""
	}

	value, ok := c[key]
	if !ok {
		return ""
	}

	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(typed)
	}
}

// Set stores value under key.
func (c headerCarrier) Set(key, value string) {
	if c == nil {
		return
	}
	c[key] = value
}

// Keys returns the header keys currently present in the carrier.
func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	return keys
}
