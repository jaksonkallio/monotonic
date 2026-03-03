.PHONY: test test-integration bench bench-integration bench-all

test:
	go test ./...

test-integration:
	cd tests/postgres && go test -v ./...

bench:
	go test -bench=. -benchmem -benchtime=1s ./pkg/monotonic

bench-integration:
	cd tests/postgres && go test -bench=. -benchmem -benchtime=1s ./...

bench-all: bench bench-integration
