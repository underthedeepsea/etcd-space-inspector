VERSION := $(shell tr -d '\n' < VERSION)
GO := env GOCACHE=/tmp/etcd-analyzer-go-cache GOPATH=/tmp/etcd-analyzer-gopath go
LDFLAGS := -X etcd-analyzer/internal/version.Value=$(VERSION)

.PHONY: build test vet

build:
	mkdir -p bin
	$(GO) build -ldflags "$(LDFLAGS)" -o bin/etcd-analyzer ./cmd/etcd-analyzer

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...
