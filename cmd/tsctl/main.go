// Command tsctl streams one completion from the gateway.
//
//	tsctl -addr localhost:9000 -max-tokens 80 "Alice was"
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/muralidhardon/tinyserve-gateway/gen/inference/v1"
)

func main() {
	addr := flag.String("addr", "localhost:9000", "gateway address")
	maxTokens := flag.Uint("max-tokens", 128, "tokens to generate")
	temperature := flag.Float64("temperature", -1, "sampling temperature (-1 = server default)")
	seed := flag.Int64("seed", -1, "sampling seed (-1 = random)")
	timeout := flag.Duration("timeout", 30*time.Second, "request deadline")
	flag.Parse()
	prompt := strings.Join(flag.Args(), " ")
	if prompt == "" {
		fmt.Fprintln(os.Stderr, "usage: tsctl [flags] PROMPT")
		os.Exit(2)
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fail(err)
	}
	defer conn.Close()

	req := &pb.GenerateRequest{Prompt: prompt, MaxTokens: uint32(*maxTokens)}
	if *temperature >= 0 {
		t := float32(*temperature)
		req.Temperature = &t
	}
	if *seed >= 0 {
		s := uint32(*seed)
		req.Seed = &s
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	ctx, cancel = context.WithTimeout(ctx, *timeout)
	defer cancel()

	stream, err := pb.NewInferenceClient(conn).Generate(ctx, req)
	if err != nil {
		fail(err)
	}
	fmt.Print(prompt)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Println()
			fail(err)
		}
		fmt.Print(chunk.Text)
		if u := chunk.Usage; u != nil {
			fmt.Printf("\n\n[%d tokens from %s, attempts=%d, gateway ttft=%.1fms, backend latency=%.1fms]\n",
				u.TokensGenerated, u.Backend, u.Attempts, u.GatewayTtftMs, u.BackendLatencyMs)
		}
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "tsctl:", err)
	os.Exit(1)
}
