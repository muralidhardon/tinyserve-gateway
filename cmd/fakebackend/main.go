// Command fakebackend runs a stand-in TinyServe replica.
//
//	fakebackend -listen :8101 -token-delay 2ms -fail-rate 0.1
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/muralidhardon/tinyserve-gateway/internal/fakebackend"
)

func main() {
	listen := flag.String("listen", ":8101", "address to serve on")
	var cfg fakebackend.Config
	flag.DurationVar(&cfg.TokenDelay, "token-delay", time.Millisecond, "delay per generated token")
	flag.DurationVar(&cfg.PrefillDelay, "prefill-delay", 5*time.Millisecond, "delay before the first token")
	flag.IntVar(&cfg.TokensPerSSE, "tokens-per-event", 8, "tokens per SSE token event")
	flag.Float64Var(&cfg.FailRate, "fail-rate", 0, "probability of answering 500")
	flag.Float64Var(&cfg.OverloadRate, "overload-rate", 0, "probability of answering 503")
	flag.IntVar(&cfg.DropAfter, "drop-after", 0, "cut the connection after this many tokens (0 = never)")
	flag.Parse()

	slog.Info("fake TinyServe listening", "addr", *listen)
	if err := http.ListenAndServe(*listen, fakebackend.New(cfg).Handler()); err != nil {
		slog.Error("serve", "err", err)
		os.Exit(1)
	}
}
