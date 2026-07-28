.PHONY: test test-integration test-race test-integration-race test-ci bench bench-integration bench-all build fmt

CGO_ENABLED ?= 0
export CGO_ENABLED

test:
	go test ./...

test-integration:
	cd tests/postgres && go test -v ./...

# The race detector needs cgo, so these override the CGO_ENABLED=0 default above.
test-race:
	CGO_ENABLED=1 go test -race ./...

test-integration-race:
	cd tests/postgres && CGO_ENABLED=1 go test -race ./...

# Mirrors .github/workflows/pr-checks.yml. tests/postgres and pkg/store/postgres/pgtest are
# separate modules, so a root `go test ./...` alone leaves every Postgres test unrun.
test-ci: test-race test-integration-race
	cd pkg/store/postgres/pgtest && CGO_ENABLED=1 go test -race ./...

bench:
	go test -bench=. -benchmem -benchtime=1s ./pkg/monotonic

bench-integration:
	cd tests/postgres && go test -bench=. -benchmem -benchtime=1s ./...

bench-all: bench bench-integration

build:
	go build ./...

fmt:
	gofmt -w .
