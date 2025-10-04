BINARY ?= bin/worker

.PHONY: build build-generator run run-generator test lint

build:
	GO111MODULE=on go build -o $(BINARY) ./cmd/worker

build-generator:
	GO111MODULE=on go build -o bin/generator ./cmd/generator

run:
	GO111MODULE=on go run ./cmd/worker

run-generator:
	GO111MODULE=on go run ./cmd/generator

test:
	GO111MODULE=on go test ./...

lint:
	golangci-lint run ./...
