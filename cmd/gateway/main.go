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
	grpchealth "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	pb "github.com/muralidhardon/tinyserve-gateway/gen/inference/v1"
	"github.com/muralidhardon/tinyserve-gateway/internal/backend"
	"github.com/muralidhardon/tinyserve-gateway/internal/health"
	"github.com/muralidhardon/tinyserve-gateway/internal/router"
	"github.com/muralidhardon/tinyserve-gateway/internal/server"
)

func main() {
	listen := flag.String("listen", ":9000", "gRPC listen address")
	backends := flag.String("backends", "http://localhost:8000", "comma-separated TinyServe base URLs")
	routerName := flag.String("router", "least_loaded", "routing strategy: "+strings.Join(router.Names, ", "))
	scrapeInterval := flag.Duration("scrape-interval", 250*time.Millisecond, "how often to read each replica's /metrics")
	var prefix router.PrefixConfig
	flag.IntVar(&prefix.BlockBytes, "prefix-block-bytes", 64, "prefix_aware: bytes per prefix block")
	flag.Float64Var(&prefix.LoadFactor, "prefix-load-factor", 1.25, "prefix_aware: how far above an even share a replica may be loaded and still win on cache affinity")
	flag.IntVar(&prefix.Slack, "prefix-slack", 4, "prefix_aware: extra sequences above that share a replica may carry (-1 = none)")
	flag.IntVar(&prefix.IndexBlocks, "prefix-index-blocks", 1<<16, "prefix_aware: prefix blocks remembered")
	maxAttempts := flag.Int("max-attempts", 0, "replicas one request may try before its first token (0 = all)")
	var hc health.Config
	flag.DurationVar(&hc.Interval, "health-interval", time.Second, "active /health check interval (0 = off)")
	flag.DurationVar(&hc.Timeout, "health-timeout", time.Second, "per health check")
	flag.IntVar(&hc.UnhealthyThreshold, "unhealthy-threshold", 2, "consecutive failed health checks that eject a replica")
	flag.IntVar(&hc.ConsecutiveFailures, "outlier-failures", 5, "consecutive failed requests that eject a replica (0 = off)")
	flag.Float64Var(&hc.MaxEjectedFraction, "max-ejected", 0.5, "most of the pool that failed requests may eject")
	flag.DurationVar(&hc.BaseEjection, "ejection-base", 2*time.Second, "first ejection; doubles with each further one")
	flag.DurationVar(&hc.MaxEjection, "ejection-max", 60*time.Second, "longest ejection")
	firstTokenTimeout := flag.Duration("first-token-timeout", 5*time.Second, "abandon an attempt with no text after this and try another replica (0 = off)")
	retryRatio := flag.Float64("retry-budget", 0.1, "retries allowed as a fraction of requests (negative = unlimited)")
	retryMin := flag.Float64("retry-min-per-sec", 10, "retries per second allowed regardless of -retry-budget")
	maxInflight := flag.Int("max-inflight", 1024, "streams served at once before shedding, lowest priority first (0 = no limit)")
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
	rt, err := router.New(*routerName, pool, prefix)
	if err != nil {
		log.Error("router", "err", err)
		os.Exit(2)
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	hc.Logger = log
	monitor := health.New(hc, pool)
	var budget *server.RetryBudget
	if *retryRatio >= 0 {
		budget = server.NewRetryBudget(*retryRatio, *retryMin)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterInferenceServer(grpcServer, server.New(server.Config{
		Backends: pool, Router: rt, MaxAttempts: *maxAttempts,
		Health: monitor, RetryBudget: budget, FirstTokenTimeout: *firstTokenTimeout, MaxInflight: *maxInflight,
		Logger: log,
	}))
	healthSrv := grpchealth.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	reflection.Register(grpcServer)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go monitor.Run(ctx)
	go backend.ScrapeLoop(ctx, pool, *scrapeInterval, func(b *backend.Backend, err error) {
		log.Debug("scrape failed", "backend", b.Name, "err", err)
	})
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
	log.Info("gateway listening", "addr", lis.Addr().String(), "router", *routerName, "backends", names)
	if err := grpcServer.Serve(lis); err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
	log.Info("stopped")
}
