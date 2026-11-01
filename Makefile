GO ?= go
BIN := bin

.PHONY: all build proto test race lint run fake clean

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

lint:
	$(GO) vet ./...

# Two fake replicas and a gateway in front of them; Ctrl-C stops all three.
fake: build
	@trap 'kill 0' INT TERM; \
	$(BIN)/fakebackend -listen :8101 & \
	$(BIN)/fakebackend -listen :8102 & \
	$(BIN)/gateway -listen :9000 -backends http://localhost:8101,http://localhost:8102 & \
	wait

clean:
	rm -rf $(BIN)
