.PHONY: build test lint ci

build:
	go build ./...

test:
	go test ./...

lint:
	go vet ./...

ci: lint test
