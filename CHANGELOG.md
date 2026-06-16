# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project follows semantic versioning after v1. Before v1, APIs may change
when needed for correctness and maintainability.

## [Unreleased]

## [0.1.2] - 2026-06-17

### Added

- `Consumer.ConsumeConcurrent` for bounded concurrent message handling while
  preserving the same ack, retry and dead-letter semantics as `Consume`.
- `Consumer.ConsumeBodyConcurrent` body-only adapter for concurrent consumers.
  `workers <= 1` falls back to the sequential `ConsumeBody` path.
- Broker-backed integration coverage for concurrent body consumers, including
  parallel handler execution, effective prefetch raising, retry handling and
  dead-lettering on exhausted concurrent failures.

## [0.1.1] - 2026-06-13

### Added

- `Publisher.DeclareQueueTopology` declares a durable queue, its binding, and
  its dead-letter topology from the publisher side — argument-identical to
  what `Consumer` declares in `Consume` — so messages published before any
  consumer has started are buffered instead of dropped as unroutable.

## [0.1.0] - 2026-06-10

Initial release.

### Added

- **Core library (`pkg/rabbitmq`)**
  - `Publisher` with publisher confirms, an idle confirm-channel pool
    (`ChannelPoolSize`, `MaxChannelAge`), and `Publish`/`PublishMessage` for
    simple and fully-specified publishes (headers, content properties,
    correlation/message IDs, per-message exchange override).
  - Mandatory routing support: unroutable messages surface as
    `*PublishReturnedError` with the returned message attached.
  - `Consumer` with automatic connection recovery, configurable prefetch,
    at-least-once in-place retries up to `MaxRetries`, and dead-lettering of
    exhausted or non-retryable messages. Retry copies preserve AMQP properties
    and the original routing key.
  - `NewNonRetryableError` to send a message straight to the dead-letter queue,
    and a `Check` method for health probes.
  - `Message` delivery type exposing body, headers, content properties, routing
    key, exchange, redelivery flag and retry count; the library owns ack/nack
    semantics.
  - Exchange/queue topology declaration with `direct`, `topic`, `fanout` and
    `headers` exchange types, plus `SkipExchangeDeclare` for externally managed
    topologies.
  - Connection options for production use: TLS/mTLS (`TLSClientConfig`), custom
    `SASL` mechanisms, `Heartbeat`, `DialTimeout`/`ConnectionTimeout`,
    `PublishConfirmTimeout`, and connection metadata via `ConnectionName` and
    `ClientProperties`.
  - All tunables live on `Config` with sane zero-value defaults; credentials are
    masked in logs.
- **Logging (`pkg/rabbitmq/logging`)**
  - Pluggable `Logger` interface so no concrete logging framework enters the
    library, with `Nop` and `log/slog` adapters included.
- **Instrumentation**
  - Generic `rabbitmq.Instrumentation` hook interface in the core library (no
    OpenTelemetry dependency) with a `NopInstrumentation` default.
- **OpenTelemetry adapter (`pkg/otelrabbitmq`, separate module)**
  - Publish and consume spans, handler duration and error metrics, retry-count
    and message-size attributes, ack/nack/requeue/retried and dead-letter
    outcomes, and W3C trace-context propagation through AMQP headers.
  - Side-effect-free: never mutates global OpenTelemetry state; uses globals
    only as defaults when no providers are supplied.
- **Integration test module (`integration/`, separate module)**
  - Broker-backed end-to-end suite covering delivery, confirms, retries,
    dead-lettering, connection recovery and trace propagation, running against
    a throwaway RabbitMQ container via testcontainers with Toxiproxy-based
    fault-injection tests.
- **Project tooling**
  - Three-module layout (`go.work` workspace) keeping OpenTelemetry and test
    dependencies out of the core library's dependency graph; published module
    files are replace-free.
  - Makefile targets for build, unit/race tests, vet, golangci-lint v2,
    `govulncheck`, merged unit+integration coverage (`cover-all`) and an
    external-module installability check, wired into GitHub Actions CI.

[Unreleased]: https://github.com/Midwayne/rabbitmq-go/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/Midwayne/rabbitmq-go/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/Midwayne/rabbitmq-go/releases/tag/v0.1.1
[0.1.0]: https://github.com/Midwayne/rabbitmq-go/releases/tag/v0.1.0
