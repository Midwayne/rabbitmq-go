# Contributing

Thanks for your interest in improving `rabbitmq-go`! This guide covers how to
set up, test and submit changes.

## Prerequisites

- **Go 1.26+** for working across all modules.
- **Docker** — required only for the integration tests, which start a real
  RabbitMQ broker via [testcontainers](https://golang.testcontainers.org/).
- **[golangci-lint](https://golangci-lint.run/) v2** — the config is
  `.golangci.yml` (`version: "2"`).

## Repository layout

This repo contains **three Go modules**:

```
rabbitmq-go/
├── go.mod                # the core library module
├── go.work               # local multi-module workspace
├── pkg/rabbitmq/         # public Publisher and Consumer API
│   └── logging/          # pluggable Logger interface + Nop/Slog adapters
├── pkg/otelrabbitmq/
│   └── go.mod            # optional OpenTelemetry adapter module
├── internal/amqpx/       # private AMQP plumbing (compiler-enforced internal)
└── integration/
    └── go.mod            # separate module for end-to-end tests
```

The OpenTelemetry adapter and integration tests live in their own modules so
their dependencies never enter the core library's dependency graph. Published
module files do not contain local `replace` directives. Local development uses
`go.work` so edits across modules are picked up without publishing first.

## Common tasks

A `Makefile` wraps the everyday commands:

| Command               | What it does                                          |
| --------------------- | ----------------------------------------------------- |
| `make test`           | Unit tests for the library module (no broker needed)  |
| `make test-race`      | Unit tests with the race detector                     |
| `make cover`          | Per-module unit-test coverage                         |
| `make cover-all`      | Combined core coverage (unit + integration; Docker)   |
| `make build`          | `go build ./...`                                      |
| `make lint`           | `golangci-lint` over all modules                      |
| `make fmt`            | `gofmt -w .`                                          |
| `make fmt-check`      | fail if Go files are not gofmt-clean                  |
| `make tidy`           | `go mod tidy` for all modules                         |
| `make vet`            | `go vet ./...` for all modules                        |
| `make govulncheck`    | vulnerability scan for all modules                    |
| `make installability` | verifies core and otel modules from a temp module     |
| `make integration`    | End-to-end tests (starts RabbitMQ via testcontainers) |
| `make check`          | `fmt` + `build` + `test` + `lint` (run before a PR)   |

### Running the integration tests

```sh
make integration
```

This boots a throwaway RabbitMQ container (`rabbitmq:4-management-alpine`, which
ships the management plugin) plus a Toxiproxy container (used by the
fault-injection tests to cut/slow the network), runs the suite against them and
tears them down — Docker just needs to be running. The fault-injection tests
skip automatically if Toxiproxy or its shared network cannot start. To run against a broker you already have instead
of starting a container, set `RABBITMQ_TEST_URL`:

```sh
RABBITMQ_TEST_URL=amqp://guest:guest@localhost:5672/ make integration
```

The connection-recovery tests force the broker to drop a connection through the
HTTP management API. With the testcontainers broker this is automatic. Against an
external broker, also set `RABBITMQ_TEST_MGMT_URL` to its management base URL;
when it is unset those tests skip:

```sh
RABBITMQ_TEST_URL=amqp://guest:guest@localhost:5672/ \
RABBITMQ_TEST_MGMT_URL=http://localhost:15672 make integration
```

If Docker is unavailable and `RABBITMQ_TEST_URL` is unset, the integration tests
skip locally. In CI, Docker/testcontainers failures fail the job.

## Coding standards

- **Formatting**: code must be `gofmt`-clean (`make fmt`).
- **Linting**: `make lint` must pass with zero issues. The linters enforce, among
  others, `errcheck`, `gosec`, `govet`, complexity limits and magic-number
  checks. Keep line length ≤ 128.
- **Public API**: this is pre-v1, so correctness and long-term maintainability
  are more important than compatibility. Every exported symbol needs a doc
  comment. New tunables belong on `Config` with a sane zero-value default
  applied in `normalize`.
- **Internals**: low-level AMQP helpers go in `internal/amqpx`; they are not part
  of the public contract and may change freely.
- **Logging**: never import a concrete logging framework into the library. Log
  through the `logging.Logger` interface.
- **Errors**: wrap errors with context (`fmt.Errorf("...: %w", err)`); prefix
  sentinel/library errors with `rabbitmq:`.

## Tests

- Add **unit tests** next to the code for any broker-independent logic
  (config, masking, retry-header handling, error classification, logging).
- Add **integration tests** under `integration/` for behaviour that requires a
  broker (delivery, confirms, retries, dead-lettering, trace propagation). Use a
  fresh `newTopology(t, conn)` per test so names never collide on the shared
  broker and rely on the `waitFor`/`queueDepth` helpers rather than `time.Sleep`.

## Submitting changes

1. Branch off `main`.
2. Run `make check` (and `make integration` if your change touches broker
   behaviour).
3. Write a clear commit message following
   [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/): a
   summary line like `feat: add publisher confirm timeout` or
   `fix(consumer): requeue on retry republish failure`, then a body explaining
   the _why_. Common types: `feat`, `fix`, `docs`, `test`, `refactor`, `chore`,
   `ci`. Mark breaking changes with a `!` (e.g. `feat!:`) — allowed pre-v1, but
   call them out in `CHANGELOG.md`.
4. Sign off every commit ([Developer Certificate of Origin](https://developercertificate.org/)).
   `git commit -s` adds the required trailer automatically:

   ```
   Signed-off-by: Your Name <your.email@example.com>
   ```

   By signing off, you certify that you wrote the change or otherwise have the
   right to submit it under the project's license.
5. Open a pull request describing the change and how you tested it. CI must be
   green before merge.

## Releases

See [docs/RELEASING.md](docs/RELEASING.md). The nested `pkg/otelrabbitmq` module
requires subdirectory-prefixed tags such as `pkg/otelrabbitmq/v0.1.0` in
addition to root tags such as `v0.1.0`.

By contributing, you agree that your contributions are licensed under the
project's [MIT License](LICENSE).
