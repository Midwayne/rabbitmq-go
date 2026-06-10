// Package rabbitmq is a small, production-grade wrapper around the RabbitMQ
// AMQP 0-9-1 client (github.com/rabbitmq/amqp091-go). It provides two
// independent, reusable building blocks:
//
//   - Publisher: a connection-managed publisher with an idle channel cache,
//     optional publisher confirms, advanced AMQP message properties, mandatory
//     publishing support, automatic reconnection with exponential backoff, and
//     generic instrumentation hooks.
//
//   - Consumer: a connection-managed consumer with QoS prefetch, a
//     dead-letter exchange/queue, bounded in-place retries (tracked via the
//     "x-retry-count" header), metadata-rich context-aware handlers, automatic
//     reconnection, and generic instrumentation hooks.
//
// Both are constructed from a single Config and are safe for concurrent use.
// Always Close them on shutdown.
//
// # Observability is optional
//
// By default this package has no observability dependency at all — it imports
// only the AMQP client. Tracing, metrics, and context propagation are plugged
// in through the Instrumentation interface (default: NopInstrumentation). A
// ready-made OpenTelemetry implementation lives in the separate module
// github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq, so the OpenTelemetry
// dependency is only pulled in if you actually use it.
//
// # Package layout
//
//   - pkg/rabbitmq (this package): the public Publisher and Consumer API.
//   - pkg/rabbitmq/logging: the pluggable Logger interface and the built-in
//     NopLogger and SlogLogger adapters.
//   - pkg/otelrabbitmq (separate module): the OpenTelemetry Instrumentation.
//   - internal/amqpx: low-level AMQP plumbing (not part of the public API).
//
// Logging defaults to logging.NopLogger; supply Config.Logger to enable it.
package rabbitmq
