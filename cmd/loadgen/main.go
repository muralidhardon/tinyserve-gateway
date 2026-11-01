// Command loadgen drives the gateway and reports client-side latency.
//
// Open loop (Poisson arrivals, the realistic case: load doesn't slow down
// because the server did):
//
//	loadgen -rate 200 -duration 20s -max-tokens 64
//
// Closed loop (fixed number of clients, each sending as soon as the last finished):
//
//	loadgen -concurrency 32 -requests 2000
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "github.com/muralidhardon/tinyserve-gateway/gen/inference/v1"
)

type result struct {
	Start     time.Duration `json:"start_ns"`
	TTFT      time.Duration `json:"ttft_ns"`
	Latency   time.Duration `json:"latency_ns"`
	Tokens    uint32        `json:"tokens"`
	Prompt    uint32        `json:"prompt_tokens"`
	Reused    uint32        `json:"prefix_tokens_reused"`
	Tenant    int           `json:"tenant"`
	Backend   string        `json:"backend,omitempty"`
	Attempts  uint32        `json:"attempts,omitempty"`
	ErrorCode string        `json:"error,omitempty"`
}

func main() {
	addr := flag.String("addr", "localhost:9000", "gateway address")
	rate := flag.Float64("rate", 0, "open loop: requests per second (Poisson arrivals)")
	duration := flag.Duration("duration", 10*time.Second, "open loop: how long to send")
	concurrency := flag.Int("concurrency", 16, "closed loop: concurrent clients")
	requests := flag.Int("requests", 1000, "closed loop: total requests")
	maxTokens := flag.Uint("max-tokens", 64, "tokens per request")
	workload := flag.String("workload", "unique",
		"unique | shared-prefix (one system prompt, varied questions) | tenants (one system prompt per tenant)")
	tenants := flag.Int("tenants", 24, "tenants workload: number of distinct system prompts")
	zipf := flag.Float64("zipf", 1.1, "tenants workload: Zipf exponent of tenant popularity (> 1; larger = more skewed)")
	seed := flag.Uint64("seed", 1, "workload random seed")
	timeout := flag.Duration("timeout", 30*time.Second, "per-request deadline")
	label := flag.String("label", "", "name for this run in the summary")
	out := flag.String("out", "", "write per-request results as JSON to this file")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()
	client := pb.NewInferenceClient(conn)

	prompt := prompts(*workload, *tenants, *zipf, *seed)
	var (
		mu      sync.Mutex
		results []result
		wg      sync.WaitGroup
		seq     int
		nextSeq = func() int { mu.Lock(); defer mu.Unlock(); seq++; return seq }
	)
	begin := time.Now()
	fire := func(i int) {
		text, tenant := prompt(i)
		r := one(client, text, uint32(*maxTokens), *timeout, begin)
		r.Tenant = tenant
		mu.Lock()
		results = append(results, r)
		mu.Unlock()
	}

	if *rate > 0 {
		deadline := begin.Add(*duration)
		next := begin
		for next.Before(deadline) {
			time.Sleep(time.Until(next))
			wg.Add(1)
			i := nextSeq()
			go func() { defer wg.Done(); fire(i) }()
			next = next.Add(time.Duration(rand.ExpFloat64() / *rate * float64(time.Second)))
		}
	} else {
		for c := 0; c < *concurrency; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := nextSeq()
					if i > *requests {
						return
					}
					fire(i)
				}
			}()
		}
	}
	wg.Wait()
	elapsed := time.Since(begin)

	summarize(*label, results, elapsed)
	if *out != "" {
		b, _ := json.MarshalIndent(results, "", " ")
		if err := os.WriteFile(*out, b, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func one(client pb.InferenceClient, prompt string, maxTokens uint32, timeout time.Duration, begin time.Time) result {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	r := result{Start: start.Sub(begin)}
	stream, err := client.Generate(ctx, &pb.GenerateRequest{Prompt: prompt, MaxTokens: maxTokens})
	for err == nil {
		var chunk *pb.GenerateChunk
		chunk, err = stream.Recv()
		if err != nil {
			break
		}
		if r.TTFT == 0 && chunk.Text != "" {
			r.TTFT = time.Since(start)
		}
		if u := chunk.Usage; u != nil {
			r.Tokens, r.Backend, r.Attempts = u.TokensGenerated, u.Backend, u.Attempts
			r.Prompt, r.Reused = u.PromptTokens, u.BackendPrefixTokensReused
		}
	}
	r.Latency = time.Since(start)
	if err != io.EOF {
		r.ErrorCode = status.Code(err).String()
	}
	return r
}

// Sentences tenants' system prompts are assembled from, in the model's domain.
var sentences = []string{
	"Alice was beginning to get very tired of sitting by her sister on the bank.",
	"Once or twice she had peeped into the book her sister was reading.",
	"It had no pictures or conversations in it, and what is the use of a book?",
	"So she was considering in her own mind, as well as she could, for the hot day made her feel sleepy.",
	"Suddenly a White Rabbit with pink eyes ran close by her.",
	"There was nothing so very remarkable in that, nor did Alice think it so very much out of the way.",
	"The Queen had only one way of settling all difficulties, great or small.",
	"The Hatter opened his eyes very wide on hearing this, but all he said was nothing.",
}

// prompts returns the i-th prompt of the workload and the tenant it belongs to.
func prompts(workload string, tenants int, zipf float64, seed uint64) func(int) (string, int) {
	switch workload {
	case "unique":
		return func(i int) (string, int) { return fmt.Sprintf("Request %d: Alice was", i), 0 }
	case "shared-prefix":
		system := "The Queen had only one way of settling all difficulties, great or small. "
		return func(i int) (string, int) { return fmt.Sprintf("%sQuestion %d: Alice was", system, i), 0 }
	case "tenants":
		// Each tenant has its own ~300-byte system prompt (about 80 tokens), distinct from its
		// first bytes, followed by a short per-request question.
		systems := make([]string, tenants)
		for t := range systems {
			var b strings.Builder
			fmt.Fprintf(&b, "Tenant %02d instructions: ", t)
			for k := 0; b.Len() < 300; k++ {
				b.WriteString(sentences[(t+k*3)%len(sentences)])
				b.WriteByte(' ')
			}
			systems[t] = b.String()
		}
		var mu sync.Mutex
		z := rand.NewZipf(rand.New(rand.NewPCG(seed, seed)), zipf, 1, uint64(tenants-1))
		return func(i int) (string, int) {
			mu.Lock()
			t := int(z.Uint64())
			mu.Unlock()
			return fmt.Sprintf("%sQuestion %d: Alice was", systems[t], i), t
		}
	}
	fmt.Fprintf(os.Stderr, "unknown workload %q\n", workload)
	os.Exit(2)
	return nil
}

func summarize(label string, rs []result, elapsed time.Duration) {
	var ttft, lat []time.Duration
	var tokens, promptTokens, reused uint64
	errs := map[string]int{}
	backends := map[string]int{}
	retried := 0
	for _, r := range rs {
		if r.ErrorCode != "" {
			errs[r.ErrorCode]++
			continue
		}
		ttft = append(ttft, r.TTFT)
		lat = append(lat, r.Latency)
		tokens += uint64(r.Tokens)
		promptTokens += uint64(r.Prompt)
		reused += uint64(r.Reused)
		backends[r.Backend]++
		if r.Attempts > 1 {
			retried++
		}
	}
	if label != "" {
		fmt.Printf("== %s\n", label)
	}
	ok := len(lat)
	fmt.Printf("requests %d  ok %d  errors %v  retried %d  elapsed %.1fs\n", len(rs), ok, errs, retried, elapsed.Seconds())
	fmt.Printf("throughput %.1f req/s  %.0f tokens/s\n", float64(ok)/elapsed.Seconds(), float64(tokens)/elapsed.Seconds())
	fmt.Printf("ttft     p50 %7.1fms  p95 %7.1fms  p99 %7.1fms\n", ms(pct(ttft, 50)), ms(pct(ttft, 95)), ms(pct(ttft, 99)))
	fmt.Printf("latency  p50 %7.1fms  p95 %7.1fms  p99 %7.1fms\n", ms(pct(lat, 50)), ms(pct(lat, 95)), ms(pct(lat, 99)))
	if promptTokens > 0 {
		fmt.Printf("prefix   %.1f%% of prompt tokens reused from the engines' prefix caches\n",
			100*float64(reused)/float64(promptTokens))
	}
	names := make([]string, 0, len(backends))
	for b := range backends {
		names = append(names, b)
	}
	sort.Strings(names)
	var parts []string
	for _, b := range names {
		parts = append(parts, fmt.Sprintf("%s %.0f%%", b, 100*float64(backends[b])/float64(ok)))
	}
	fmt.Printf("served   %s\n", strings.Join(parts, "  "))
}

func pct(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), ds...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(p/100*float64(len(sorted))+0.5) - 1
	return sorted[min(max(idx, 0), len(sorted)-1)]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
