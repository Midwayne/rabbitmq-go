# Importable modules with unit tests (no broker required).
UNIT_MODULES := . pkg/otelrabbitmq
# Every module in the repo (also linted/tidied).
ALL_MODULES := . pkg/otelrabbitmq integration

.PHONY: test test-race cover cover-all build lint fmt fmt-check tidy vet govulncheck installability integration integration-race check

# --- Unit tests / build (library + otelrabbitmq) ---

test:
	@for m in $(UNIT_MODULES); do echo "==> test $$m"; (cd $$m && go test ./...) || exit 1; done

test-race:
	@for m in $(UNIT_MODULES); do echo "==> test -race $$m"; (cd $$m && go test -race ./...) || exit 1; done

cover:
	@for m in $(UNIT_MODULES); do echo "==> cover $$m"; (cd $$m && go test -cover ./...) || exit 1; done

# Combined coverage of the core packages across all three modules (unit +
# broker-backed integration tests), merged with `go tool covdata`. Needs Docker.
cover-all:
	./scripts/coverage.sh

build:
	@for m in $(UNIT_MODULES); do echo "==> build $$m"; (cd $$m && go build ./...) || exit 1; done

# --- Tooling (all modules) ---

lint:
	@for m in $(ALL_MODULES); do echo "==> lint $$m"; (cd $$m && golangci-lint run ./...) || exit 1; done

fmt:
	gofmt -w .

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

tidy:
	@echo "==> tidy ."; go mod tidy
	@echo "==> tidy pkg/otelrabbitmq"; \
		cd pkg/otelrabbitmq && \
		trap 'go mod edit -dropreplace github.com/Midwayne/rabbitmq-go >/dev/null 2>&1 || true' EXIT INT TERM && \
		go mod edit -replace github.com/Midwayne/rabbitmq-go=../../ && \
		go mod tidy && \
		go mod edit -dropreplace github.com/Midwayne/rabbitmq-go
	@echo "==> tidy integration"; \
		cd integration && \
		trap 'go mod edit -dropreplace github.com/Midwayne/rabbitmq-go >/dev/null 2>&1 || true; go mod edit -dropreplace github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq >/dev/null 2>&1 || true' EXIT INT TERM && \
		go mod edit -replace github.com/Midwayne/rabbitmq-go=../ && \
		go mod edit -replace github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq=../pkg/otelrabbitmq && \
		go mod tidy && \
		go mod edit -dropreplace github.com/Midwayne/rabbitmq-go && \
		go mod edit -dropreplace github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq

vet:
	@for m in $(ALL_MODULES); do echo "==> vet $$m"; (cd $$m && go vet ./...) || exit 1; done

govulncheck:
	@for m in $(ALL_MODULES); do echo "==> govulncheck $$m"; (cd $$m && go run golang.org/x/vuln/cmd/govulncheck@latest ./...) || exit 1; done

installability:
	./scripts/check-installability.sh

# --- Integration module ---

# End-to-end tests. Spins up RabbitMQ via testcontainers, so Docker must be
# running. Set RABBITMQ_TEST_URL to run against an existing broker instead.
integration:
	cd integration && go test -count=1 ./...

integration-race:
	cd integration && go test -race -count=1 ./...

# What CI runs before broker-backed integration tests.
check: fmt-check build test vet lint installability
