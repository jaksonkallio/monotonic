.PHONY: test test-integration

test:
	go test ./...

test-integration:
	cd tests/postgres && go test -v ./...
