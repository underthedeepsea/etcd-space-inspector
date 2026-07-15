VERSION := $(shell tr -d '\n' < VERSION)
GO := env GOCACHE=/tmp/etcd-analyzer-go-cache GOPATH=/tmp/etcd-analyzer-gopath go
LDFLAGS := -X etcd-analyzer/internal/version.Value=$(VERSION)

.PHONY: build check test vet web typecheck

build: web
	mkdir -p bin
	$(GO) build -ldflags "$(LDFLAGS)" -o bin/etcd-analyzer ./cmd/etcd-analyzer

web:
	cd web && npm run build

typecheck:
	cd web && npm run typecheck

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

check: test vet typecheck web
