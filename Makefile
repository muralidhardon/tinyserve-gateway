GO ?= go
BIN := bin
STATICCHECK := honnef.co/go/tools/cmd/staticcheck@v0.8.2

.PHONY: all build proto test race lint image kind kind-observability kind-cilium kind-down run fake clean

all: build

proto:
	protoc -I api/proto \
		--go_out=gen --go_opt=paths=source_relative \
		--go-grpc_out=gen --go-grpc_opt=paths=source_relative \
		api/proto/inference/v1/inference.proto

build:
	$(GO) build -o $(BIN)/ ./cmd/...

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

# What CI checks, besides the tests.
lint:
	$(GO) vet ./...
	@test -z "$$(gofmt -l cmd internal)" || { gofmt -l cmd internal; exit 1; }
	$(GO) mod tidy -diff
	$(GO) run $(STATICCHECK) ./...
	helm lint deploy/helm/tinyserve-gateway

image:
	docker build -t tinyserve-gateway:dev .

# A local 3-node cluster with TinyServe behind the gateway (see deploy/kind/up.sh).
kind:
	deploy/kind/up.sh

# The same, plus Prometheus, Jaeger and Grafana, with every request traced.
kind-observability:
	OBSERVABILITY=1 deploy/kind/up.sh

# The same on Cilium, with network policy around both components and Hubble.
kind-cilium:
	CNI=cilium deploy/kind/up.sh

kind-down:
	kind delete cluster --name tinyserve

# Two fake replicas and a gateway in front of them; Ctrl-C stops all three.
fake: build
	@trap 'kill 0' INT TERM; \
	$(BIN)/fakebackend -listen :8101 & \
	$(BIN)/fakebackend -listen :8102 & \
	$(BIN)/gateway -listen :9000 -backends http://localhost:8101,http://localhost:8102 & \
	wait

clean:
	rm -rf $(BIN)
