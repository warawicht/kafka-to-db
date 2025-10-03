BINARY ?= bin/worker

.PHONY: build run test lint

build:
	GO111MODULE=on go build -o $(BINARY) ./cmd/worker

run:
	GO111MODULE=on go run ./cmd/worker

test:
	GO111MODULE=on go test ./...

lint:
	golangci-lint run ./...
