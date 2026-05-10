.PHONY: test test-integration bench bench-integration bench-all build

CGO_ENABLED ?= 0
export CGO_ENABLED

test:
	go test ./...

test-integration:
	cd tests/postgres && go test -v ./...

bench:
	go test -bench=. -benchmem -benchtime=1s ./pkg/monotonic

bench-integration:
	cd tests/postgres && go test -bench=. -benchmem -benchtime=1s ./...

bench-all: bench bench-integration

build:
	go build ./...

proto:
	protoc \
		--go_out=./pkg/mesh/protobuf \
		--go_opt=paths=source_relative \
		--go-grpc_out=./pkg/mesh/protobuf \
		--go-grpc_opt=paths=source_relative \
		-I ./pkg/mesh/protobuf \
		./pkg/mesh/protobuf/mesh.proto
