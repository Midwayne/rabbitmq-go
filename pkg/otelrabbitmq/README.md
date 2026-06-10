# otelrabbitmq

`otelrabbitmq` is the optional OpenTelemetry adapter for
`github.com/Midwayne/rabbitmq-go/pkg/rabbitmq`. It is a separate Go module so
the core RabbitMQ package has no OpenTelemetry dependency.

```sh
go get github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq
```

The package is side-effect-free. It does not configure exporters, set global
providers, or call `otel.SetTracerProvider`. Pass explicit providers when you
have them; otherwise OpenTelemetry globals are used only as defaults.

```go
instr := otelrabbitmq.New(
    otelrabbitmq.WithTracerProvider(tp),
    otelrabbitmq.WithMeterProvider(mp),
    otelrabbitmq.WithPropagator(propagation.TraceContext{}),
)
```

## Instrumentation

The adapter instruments:

- publish spans;
- consume spans;
- message-count metric (`<prefix>.messages.total`);
- handler-duration metric (`<prefix>.message.duration.seconds`);
- dead-letter metric (`<prefix>.deadletter.total`);
- publish errors;
- handler errors;
- retry-count attributes;
- ack / nack / requeue / retried outcomes (recorded as consume-span attributes
  and metric dimensions);
- dead-letter outcomes;
- message-size attributes on publish and consume spans;
- trace-context injection into AMQP headers;
- trace-context extraction from AMQP headers before invoking handlers.

There is no dedicated message-size metric; size is recorded only as a span
attribute. Span and metric names and attributes may evolve while RabbitMQ
semantic conventions continue to mature. This module is pre-v1.
