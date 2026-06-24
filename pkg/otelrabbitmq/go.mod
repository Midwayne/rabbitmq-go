// otelrabbitmq is a separate module so the OpenTelemetry dependency is opt-in:
// the core github.com/Midwayne/rabbitmq-go library depends only on the AMQP
// client. Import this module only if you want OpenTelemetry tracing and metrics.
module github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq

go 1.26.0

toolchain go1.26.4

require (
	github.com/Midwayne/rabbitmq-go v0.1.2
	github.com/rabbitmq/amqp091-go v1.11.0
	go.opentelemetry.io/otel v1.44.0
	go.opentelemetry.io/otel/metric v1.44.0
	go.opentelemetry.io/otel/trace v1.44.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
)
