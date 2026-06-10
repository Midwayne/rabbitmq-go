#!/usr/bin/env sh
# The VERSION file is the single source of truth for the release version.
# Go requires concrete versions in go.mod require and go.work replace
# directives, so this script propagates VERSION to every file that must
# carry it: the nested modules' requirements on the root module, and the
# go.work replace directives that resolve those requirements locally.
#
# Usage:
#   scripts/set-version.sh v0.2.0   # update VERSION and propagate
#   scripts/set-version.sh          # re-propagate the current VERSION
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
old_version=$(cat "$repo_root/VERSION")
version="${1:-$old_version}"

case "$version" in
v[0-9]*.[0-9]*.[0-9]*) ;;
*)
	echo "usage: set-version.sh vMAJOR.MINOR.PATCH (got '$version')" >&2
	exit 1
	;;
esac

printf '%s\n' "$version" >"$repo_root/VERSION"
go mod edit -require="github.com/Midwayne/rabbitmq-go@$version" "$repo_root/pkg/otelrabbitmq/go.mod"
go mod edit -require="github.com/Midwayne/rabbitmq-go@$version" "$repo_root/integration/go.mod"
go mod edit -require="github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq@$version" "$repo_root/integration/go.mod"
go work edit \
	-dropreplace="github.com/Midwayne/rabbitmq-go@$old_version" \
	-dropreplace="github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq@$old_version" \
	-replace="github.com/Midwayne/rabbitmq-go@$version=." \
	-replace="github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq@$version=./pkg/otelrabbitmq" \
	"$repo_root/go.work"

echo "version set to $version in VERSION, pkg/otelrabbitmq/go.mod, integration/go.mod and go.work"
