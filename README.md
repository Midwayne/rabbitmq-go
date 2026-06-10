# rabbitmq-go

[![CI](https://github.com/Midwayne/rabbitmq-go/actions/workflows/ci.yml/badge.svg)](https://github.com/Midwayne/rabbitmq-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/Midwayne/rabbitmq-go/pkg/rabbitmq.svg)](https://pkg.go.dev/github.com/Midwayne/rabbitmq-go/pkg/rabbitmq)
[![Latest Release](https://img.shields.io/github/v/release/Midwayne/rabbitmq-go)](https://github.com/Midwayne/rabbitmq-go/releases)
[![License](https://img.shields.io/github/license/Midwayne/rabbitmq-go)](LICENSE)

A small RabbitMQ AMQP 0-9-1 wrapper for Go. It packages connection management,
publisher confirms, channel reuse, consumer retries, dead-lettering, logging
and optional instrumentation into a reusable library.

This was built since I couldn't find a library that fit my needs and I had the same implmentation across multiple projects.

This project is pre-v1. APIs may change while the package hardens for broader
open-source use.

## Packages

```text
pkg/rabbitmq      # core library; no OpenTelemetry dependency
pkg/otelrabbitmq  # optional first-party OpenTelemetry adapter; separate module
integration       # broker-backed integration tests; separate module
```

Install the core library:

```sh
go get github.com/Midwayne/rabbitmq-go/pkg/rabbitmq
```

Install optional OpenTelemetry support:

```sh
go get github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq
```

All modules target Go 1.26+.

## Publishing

```go
pub, err := rabbitmq.NewPublisher(rabbitmq.Config{
    URL:      "amqp://guest:guest@localhost:5672/",
    Exchange: "events",
})
if err != nil {
    log.Fatal(err)
}
defer pub.Close()

err = pub.Publish(ctx, "events", "user.created", []byte(`{"id":1}`), true)
```

For headers, properties, mandatory routing, or non-default exchanges, use
`PublishMessage`:

```go
err = pub.PublishMessage(ctx, rabbitmq.PublishMessage{
    RoutingKey:     "user.created",
    Body:           body,
    Headers:        amqp.Table{"schema": "user.created.v1"},
    ContentType:    "application/json",
    CorrelationID:  correlationID,
    MessageID:      messageID,
    Mandatory:      true,
    WaitForConfirm: true,
})
```

A publisher confirm ack means RabbitMQ accepted the publish on the channel. It
does not prove the message routed to a queue. Use `Mandatory: true` and handle
`*rabbitmq.PublishReturnedError` when unroutable messages must be detected.

`Config.Exchange` is the exchange declared by `NewPublisher` unless
`SkipExchangeDeclare` is true. `PublishMessage.Exchange` defaults to
`Config.Exchange`; if callers publish to another exchange, they are responsible
for ensuring that exchange exists.

`ChannelPoolSize` is a max idle-channel cache size, not a concurrency cap.
`GetChannel` and `ReturnChannel` remain available for advanced low-level uses,
but callers must not use borrowed channels concurrently and must return each
borrowed channel exactly once.

## Consuming

```go
consumer, err := rabbitmq.NewConsumer(ctx, rabbitmq.Config{
    URL:           "amqp://guest:guest@localhost:5672/",
    Exchange:      "events",
    PrefetchCount: 10,
    MaxRetries:    3,
})
if err != nil {
    log.Fatal(err)
}
defer consumer.Close()

handler := func(ctx context.Context, msg rabbitmq.Message) error {
    var event UserCreated
    if err := json.Unmarshal(msg.Body, &event); err != nil {
        return rabbitmq.NewNonRetryableError(err)
    }
    return process(ctx, event, msg.CorrelationID)
}

go func() { _ = consumer.Consume("events-worker", "user.*", handler) }()
```

`Message` includes body, headers, content properties, routing key, exchange,
redelivery flag and retry count. Ack/nack methods are intentionally not exposed;
the library owns acknowledgement semantics.

Retry behavior is at-least-once. On handler error, the consumer republishes a
retry copy first and waits for publisher confirms before acking the original. If
retry republish fails, the original is nacked with requeue so it can be
redelivered. Retry copies preserve AMQP properties and the original delivery
routing key, which matters for topic bindings such as `user.*`.

`PrefetchCount` defaults to `0`, which means unlimited prefetch. Production
consumers should set it explicitly.

## Optional OpenTelemetry

The core package exposes a generic `rabbitmq.Instrumentation` interface and does
not import OpenTelemetry. The optional `pkg/otelrabbitmq` module implements that
interface:

```go
instr := otelrabbitmq.New(
    otelrabbitmq.WithTracerProvider(tp),
    otelrabbitmq.WithMeterProvider(mp),
    otelrabbitmq.WithPropagator(propagation.TraceContext{}),
)

cfg := rabbitmq.Config{Instrumentation: instr}
```

`otelrabbitmq` is side-effect-free. It does not initialize exporters, configure
global OpenTelemetry state, or call `otel.SetTracerProvider`. With no options it
uses OpenTelemetry globals only as defaults.

The adapter instruments:

- publish spans and publish errors;
- consume spans and handler duration;
- handler errors and dead-letter outcomes;
- retry count attributes;
- ack/nack/requeue/retried outcomes as consume-span attributes and metric
  dimensions;
- message size attributes on publish and consume spans (no dedicated size metric);
- trace-context injection into AMQP headers on publish;
- trace-context extraction from AMQP headers before invoking handlers.

RabbitMQ OpenTelemetry semantic conventions are still evolving and this package
is pre-v1, so span and metric names and attributes may change.

## Configuration

Only `URL` and `Exchange` are required. Important production options include:

| Field                                 | Notes                                                       |
| ------------------------------------- | ----------------------------------------------------------- |
| `ExchangeType`                        | `direct`, `topic`, `fanout`, or `headers`; default `direct` |
| `SkipExchangeDeclare`                 | use when exchanges are managed externally                   |
| `ChannelPoolSize`                     | max idle confirm-channel cache size                         |
| `MaxChannelAge`                       | total channel lifetime before replacement                   |
| `PublishConfirmTimeout`               | bound for waiting on broker confirms                        |
| `PrefetchCount`                       | default `0` means unlimited                                 |
| `MaxRetries`                          | in-place retry count before dead-lettering                  |
| `TLSClientConfig`                     | TLS, custom CAs, or mTLS                                    |
| `Heartbeat`                           | AMQP heartbeat interval                                     |
| `ClientProperties` / `ConnectionName` | connection metadata visible in RabbitMQ                     |
| `SASL`                                | custom authentication mechanisms                            |
| `DialTimeout` / `ConnectionTimeout`   | startup connection bounds                                   |

## Testing

This repository has nested Go modules. Prefer the Makefile or run commands per
module; plain `go test ./...` from one module does not cover the others.

```sh
make test
make test-race
make vet
make installability
make integration
make cover-all   # combined core coverage across unit + integration suites
```

Because the integration module exercises most of the publisher/consumer/pool
code through a real broker, `make cover-all` merges coverage across all suites
with `go tool covdata` for an accurate figure; per-module `go test -cover`
undercounts. Integration tests start RabbitMQ with testcontainers. Locally, Docker failures
skip the integration suite unless `RABBITMQ_TEST_URL` points at an existing
broker. In CI, Docker/testcontainers failures fail the job.

## Release Notes

Because `pkg/otelrabbitmq` is a nested module, release both modules with their
own tags:

```sh
git tag v0.1.0
git tag pkg/otelrabbitmq/v0.1.0
```

Before tagging, run `make check`, `make integration`, `make govulncheck` and
`make installability`, then update `CHANGELOG.md`. See `docs/RELEASING.md` for
the full checklist.

## License

MIT. See [LICENSE](LICENSE).
