// Command gateway serves the Inference gRPC API in front of TinyServe replicas.
//
//	gateway -listen :9000 -backends http://localhost:8000,http://localhost:8001
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	pb "github.com/muralidhardon/tinyserve-gateway/gen/inference/v1"
	"github.com/muralidhardon/tinyserve-gateway/internal/backend"
	"github.com/muralidhardon/tinyserve-gateway/internal/router"
	"github.com/muralidhardon/tinyserve-gateway/internal/server"
)

func main() {
	listen := flag.String("listen", ":9000", "gRPC listen address")
	backends := flag.String("backends", "http://localhost:8000", "comma-separated TinyServe base URLs")
	maxAttempts := flag.Int("max-attempts", 0, "replicas one request may try before its first token (0 = all)")
	drainTimeout := flag.Duration("drain-timeout", 30*time.Second, "how long shutdown waits for open streams")
	verbose := flag.Bool("v", false, "debug logging")
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	httpClient := &http.Client{Transport: &http.Transport{
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
		DialContext:         (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
	}}
	var pool []*backend.Backend
	for _, u := range strings.Split(*backends, ",") {
		if u = strings.TrimSpace(u); u != "" {
			pool = append(pool, backend.New(u, httpClient))
		}
	}
	if len(pool) == 0 {
		log.Error("no backends given")
		os.Exit(2)
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterInferenceServer(grpcServer, server.New(server.Config{
		Backends: pool, Router: &router.RoundRobin{}, MaxAttempts: *maxAttempts, Logger: log,
	}))
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	reflection.Register(grpcServer)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		// Fail health checks first so load balancers stop sending new streams,
		// then let the open ones finish.
		log.Info("draining", "timeout", *drainTimeout)
		healthSrv.Shutdown()
		done := make(chan struct{})
		go func() { grpcServer.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(*drainTimeout):
			log.Warn("drain timeout, closing open streams")
			grpcServer.Stop()
		}
	}()

	names := make([]string, len(pool))
	for i, b := range pool {
		names[i] = b.Name
	}
	log.Info("gateway listening", "addr", lis.Addr().String(), "backends", names)
	if err := grpcServer.Serve(lis); err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
	log.Info("stopped")
}
