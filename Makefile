SHELL := /bin/bash

# Tool versions installed by `make devsetup`.
STATICCHECK_VERSION    ?= latest
GOLANGCI_LINT_VERSION  ?= latest

.PHONY: all test fasttest cover checkfmt checkvet lint staticcheck race \
	tidy devsetup check

all: check

# Install the developer tooling used by the lint targets.  Uses the modern
# `go install tool@version` mechanism (go get for tools was removed in Go 1.18).
devsetup:
	go install honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION)
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

test:
	go test ./...

fasttest:
	go test -short ./...

cover:
	go test -coverprofile=cover.out ./...
	go tool cover -func=cover.out

checkfmt:
	@fmt=$$(gofmt -l .); if [ -n "$$fmt" ]; then \
		echo "gofmt needs to be run on:"; echo "$$fmt"; exit 1; fi

checkvet:
	go vet ./...

staticcheck:
	staticcheck ./...

lint:
	golangci-lint run ./...

race:
	go test -race ./...

tidy:
	go mod tidy

# The aggregate check run in CI.
check: checkfmt checkvet test
