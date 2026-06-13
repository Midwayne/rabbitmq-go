package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Instrumentation hooks into publish and consume operations so observability
// backends (distributed tracing, metrics, context propagation) can be plugged
// in without the core library depending on them.
//
// The default is NopInstrumentation, which does nothing — so by default this
// library has no OpenTelemetry (or any other observability) dependency. A ready
// -made OpenTelemetry implementation that creates spans, records metrics, and
// propagates W3C trace context is available in the separate module
// github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq.
//
// Implementations must be safe for concurrent use.
type Instrumentation interface {
	// StartPublish is called at the start of a publish. Implementations may
	// mutate p.Headers (for example to inject trace context that is sent with
	// the message) and must return a context for the operation plus a callback
	// invoked, with the publish error or nil, when it completes.
	StartPublish(ctx context.Context, p *PublishContext) (context.Context, func(error))

	// StartConsume is called before a delivery is handled. Implementations may
	// read d.Headers (for example to extract trace context) and must return a
	// context — passed to the message handler — plus a callback invoked with the
	// outcome when handling completes.
	StartConsume(ctx context.Context, d *DeliveryContext) (context.Context, func(ConsumeResult))
}

// PublishContext describes a message being published. Headers is mutable so
// instrumentation can inject propagation data; whatever it contains when
// StartPublish returns is sent with the message.
type PublishContext struct {
	Exchange   string
	RoutingKey string
	BodySize   int
	Headers    amqp.Table
}

// DeliveryContext describes a delivery being handled. Headers carries the
// incoming message headers (read-only by convention).
type DeliveryContext struct {
	Queue      string
	RoutingKey string
	BodySize   int
	RetryCount int
	Headers    amqp.Table
}

// ConsumeResult reports the outcome of handling a delivery to the completion
// callback returned by StartConsume.
type ConsumeResult struct {
	// Err is the handler error, or nil on success.
	Err error
	// DeadLettered is true when the message was nacked without requeue once
	// retries were exhausted or the error was non-retryable.
	DeadLettered bool
	// NonRetryable is true when Err was (or wrapped) a NonRetryableError.
	NonRetryable bool
	// Acked is true when the original delivery was acknowledged.
	Acked bool
	// Nacked is true when the original delivery was negatively acknowledged.
	Nacked bool
	// Requeued is true when the original delivery was nacked for redelivery.
	Requeued bool
	// Retried is true when a retry copy was republished and confirmed.
	Retried bool
}

// NopInstrumentation is the default Instrumentation. It performs no tracing,
// metrics, or propagation.
type NopInstrumentation struct{}

// isNopInstrumentation reports whether instr is the no-op default, letting hot
// paths skip per-message instrumentation contexts and header tables entirely.
func isNopInstrumentation(instr Instrumentation) bool {
	switch instr.(type) {
	case NopInstrumentation, *NopInstrumentation:
		return true
	default:
		return false
	}
}

// StartPublish implements Instrumentation.
func (NopInstrumentation) StartPublish(
	ctx context.Context,
	_ *PublishContext,
) (context.Context, func(error)) {
	return ctx, func(error) {}
}

// StartConsume implements Instrumentation.
func (NopInstrumentation) StartConsume(
	ctx context.Context,
	_ *DeliveryContext,
) (context.Context, func(ConsumeResult)) {
	return ctx, func(ConsumeResult) {}
}
