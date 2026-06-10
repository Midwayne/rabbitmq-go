#!/usr/bin/env sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
version=$(cat "$repo_root/VERSION")
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT INT TERM

cd "$tmpdir"
go mod init example.com/rabbitmq-go-install-check
go mod edit -require github.com/Midwayne/rabbitmq-go@"$version"
go mod edit -require github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq@"$version"
go mod edit -replace github.com/Midwayne/rabbitmq-go="$repo_root"
go mod edit -replace github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq="$repo_root/pkg/otelrabbitmq"

cat > main.go <<'GO'
package main

import (
	rabbitmq "github.com/Midwayne/rabbitmq-go/pkg/rabbitmq"
	"github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq"
)

func main() {
	_ = rabbitmq.Config{Instrumentation: otelrabbitmq.New()}
}
GO

GOWORK=off go mod tidy
GOWORK=off go test ./...
