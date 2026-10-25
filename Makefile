VERSION ?= $(shell git describe --tags --always --dirty)
IMAGE   ?= ghcr.io/ipsupport-llc/plugpool

.PHONY: test lint build image proto
test:
	go vet ./...
	go test -race ./...
lint:
	golangci-lint run ./...
build:
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o bin/plugpool ./cmd/plugpool
image:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .
# Generated code is committed. Regenerated with protoc 3.21.12,
# protoc-gen-go v1.36.11, protoc-gen-go-grpc 1.6.2. The proto is upstream
# cluster-autoscaler-1.36.1 with only go_package changed.
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		internal/protos/externalgrpc.proto
