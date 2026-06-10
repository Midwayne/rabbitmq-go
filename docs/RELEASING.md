# Releasing

This project is pre-v1. API changes are allowed when they improve correctness,
reliability, or maintainability, but they must be documented in `CHANGELOG.md`.

## Checklist

1. Update `CHANGELOG.md` with the release date and notable changes.
2. Run `make set-version v=v0.1.0` with the version being released. The
   `VERSION` file is the single source of truth; the target propagates it to
   the nested modules' `go.mod` requirements and the `go.work` replace
   directives (the installability script reads `VERSION` directly).
3. Run `make fmt-check`.
4. Run `make test` and `make test-race`.
5. Run `make vet`, `make lint`, and `make govulncheck`.
6. Run `make integration` with Docker available.
7. Run `make installability`.
8. Commit the release prep changes.
9. Tag the root module: `git tag v0.1.0`.
10. Tag the nested OpenTelemetry module: `git tag pkg/otelrabbitmq/v0.1.0`.
11. Push both tags: `git push origin v0.1.0 pkg/otelrabbitmq/v0.1.0`.

## Local Development

The repository uses `go.work` for local multi-module development. Do not commit
local `replace` directives to `pkg/otelrabbitmq/go.mod`; nested module metadata
must remain publishable.

## Installability Check

`scripts/check-installability.sh` creates a temporary external module and checks
that both imports resolve:

```go
import (
    rabbitmq "github.com/Midwayne/rabbitmq-go/pkg/rabbitmq"
    "github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq"
)
```

Before tags exist, the script uses temporary local replacements inside the temp
module only. Published module files remain replace-free.
