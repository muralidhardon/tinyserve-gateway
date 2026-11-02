# One image with every command: the gateway, plus loadgen, tsctl and
# fakebackend for in-cluster load tests and demos.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
EXPOSE 9000 9090
ENTRYPOINT ["/usr/local/bin/gateway"]
