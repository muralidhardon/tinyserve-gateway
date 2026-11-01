package server

import (
	"context"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/muralidhardon/tinyserve-gateway/gen/inference/v1"
	"github.com/muralidhardon/tinyserve-gateway/internal/backend"
	"github.com/muralidhardon/tinyserve-gateway/internal/fakebackend"
	"github.com/muralidhardon/tinyserve-gateway/internal/router"
)

type replica struct {
	fake *fakebackend.Server
	http *httptest.Server
}

func startReplicas(t *testing.T, cfgs ...fakebackend.Config) []replica {
	t.Helper()
	var rs []replica
	for _, cfg := range cfgs {
		f := fakebackend.New(cfg)
		h := httptest.NewServer(f.Handler())
		t.Cleanup(h.Close)
		rs = append(rs, replica{f, h})
	}
	return rs
}

// startGateway serves the gateway over an in-memory listener and returns a client.
func startGateway(t *testing.T, rs []replica) pb.InferenceClient {
	t.Helper()
	var pool []*backend.Backend
	for _, r := range rs {
		pool = append(pool, backend.New(r.http.URL, nil))
	}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterInferenceServer(srv, New(Config{Backends: pool, Router: &router.RoundRobin{}}))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewInferenceClient(conn)
}

// generate collects a whole stream: the concatenated text, the usage, and the error that ended it.
func generate(ctx context.Context, c pb.InferenceClient, req *pb.GenerateRequest) (string, *pb.Usage, error) {
	stream, err := c.Generate(ctx, req)
	if err != nil {
		return "", nil, err
	}
	var text strings.Builder
	var usage *pb.Usage
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return text.String(), usage, nil
		}
		if err != nil {
			return text.String(), usage, err
		}
		text.WriteString(chunk.Text)
		if chunk.Usage != nil {
			usage = chunk.Usage
		}
	}
}

func expected(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteString(fakebackend.Token(i))
	}
	return b.String()
}

func TestStreamsCompletion(t *testing.T) {
	rs := startReplicas(t, fakebackend.Config{})
	c := startGateway(t, rs)

	stream, err := c.Generate(context.Background(), &pb.GenerateRequest{Prompt: "Alice was", MaxTokens: 20})
	if err != nil {
		t.Fatal(err)
	}
	var chunks []*pb.GenerateChunk
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, chunk)
	}
	// 20 tokens at 8 per event: 3 text chunks, then usage
	if len(chunks) != 4 {
		t.Fatalf("got %d chunks, want 4", len(chunks))
	}
	var text string
	for _, ch := range chunks[:3] {
		text += ch.Text
	}
	if text != expected(20) {
		t.Errorf("text = %q, want %q", text, expected(20))
	}
	u := chunks[3].Usage
	if u == nil || chunks[3].Text != "" {
		t.Fatalf("last chunk = %v, want usage only", chunks[3])
	}
	if u.TokensGenerated != 20 || u.Attempts != 1 || u.Backend != rs[0].http.URL || u.GatewayTtftMs <= 0 {
		t.Errorf("usage = %v", u)
	}
}

func TestDefaultMaxTokens(t *testing.T) {
	c := startGateway(t, startReplicas(t, fakebackend.Config{}))
	_, u, err := generate(context.Background(), c, &pb.GenerateRequest{Prompt: "x"})
	if err != nil || u.TokensGenerated != defaultMaxTokens {
		t.Fatalf("usage = %v, err = %v", u, err)
	}
}

func TestRoundRobinSpreadsLoad(t *testing.T) {
	rs := startReplicas(t, fakebackend.Config{}, fakebackend.Config{}, fakebackend.Config{})
	c := startGateway(t, rs)
	for i := 0; i < 9; i++ {
		if _, _, err := generate(context.Background(), c, &pb.GenerateRequest{Prompt: "x", MaxTokens: 4}); err != nil {
			t.Fatal(err)
		}
	}
	for i, r := range rs {
		if got := r.fake.Requests(); got != 3 {
			t.Errorf("replica %d got %d requests, want 3", i, got)
		}
	}
}

func TestFailsOverBeforeFirstToken(t *testing.T) {
	for _, tc := range []struct {
		name string
		bad  fakebackend.Config
	}{
		{"500", fakebackend.Config{FailRate: 1}},
		{"503", fakebackend.Config{OverloadRate: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := startReplicas(t, tc.bad, fakebackend.Config{})
			c := startGateway(t, rs)
			for i := 0; i < 4; i++ {
				text, u, err := generate(context.Background(), c, &pb.GenerateRequest{Prompt: "x", MaxTokens: 10})
				if err != nil {
					t.Fatal(err)
				}
				if text != expected(10) || u.Backend != rs[1].http.URL {
					t.Errorf("served by %s, text %q", u.Backend, text)
				}
			}
		})
	}
}

func TestUnreachableReplicaIsSkipped(t *testing.T) {
	rs := startReplicas(t, fakebackend.Config{}, fakebackend.Config{})
	rs[0].http.Close()
	c := startGateway(t, rs)
	for i := 0; i < 4; i++ {
		if _, u, err := generate(context.Background(), c, &pb.GenerateRequest{Prompt: "x", MaxTokens: 4}); err != nil || u.Backend != rs[1].http.URL {
			t.Fatalf("usage = %v, err = %v", u, err)
		}
	}
}

func TestAllReplicasFailing(t *testing.T) {
	cases := []struct {
		cfg  fakebackend.Config
		code codes.Code
	}{
		{fakebackend.Config{OverloadRate: 1}, codes.ResourceExhausted},
		{fakebackend.Config{FailRate: 1}, codes.Unavailable},
	}
	for _, tc := range cases {
		rs := startReplicas(t, tc.cfg, tc.cfg)
		c := startGateway(t, rs)
		_, _, err := generate(context.Background(), c, &pb.GenerateRequest{Prompt: "x", MaxTokens: 4})
		if status.Code(err) != tc.code {
			t.Errorf("err = %v, want %s", err, tc.code)
		}
		if rs[0].fake.Requests() != 1 || rs[1].fake.Requests() != 1 {
			t.Errorf("each replica should be tried exactly once")
		}
	}
}

func TestMidStreamFailureIsNotRetried(t *testing.T) {
	rs := startReplicas(t, fakebackend.Config{DropAfter: 10}, fakebackend.Config{DropAfter: 10})
	c := startGateway(t, rs)
	text, _, err := generate(context.Background(), c, &pb.GenerateRequest{Prompt: "x", MaxTokens: 40})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("err = %v, want Unavailable", err)
	}
	if text != expected(8) {
		t.Errorf("client should keep the partial text it received, got %q", text)
	}
	if total := rs[0].fake.Requests() + rs[1].fake.Requests(); total != 1 {
		t.Errorf("mid-stream failure was retried: %d backend requests", total)
	}
}

func TestInvalidArgument(t *testing.T) {
	rs := startReplicas(t, fakebackend.Config{})
	c := startGateway(t, rs)
	for _, req := range []*pb.GenerateRequest{{Prompt: ""}, {Prompt: strings.Repeat("a", maxPromptBytes+1)}} {
		if _, _, err := generate(context.Background(), c, req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("err = %v, want InvalidArgument", err)
		}
	}
	if rs[0].fake.Requests() != 0 {
		t.Error("invalid requests reached the backend")
	}
}

func TestClientCancelReachesBackend(t *testing.T) {
	rs := startReplicas(t, fakebackend.Config{TokenDelay: 10 * time.Millisecond})
	c := startGateway(t, rs)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.Generate(ctx, &pb.GenerateRequest{Prompt: "x", MaxTokens: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	cancel()

	deadline := time.Now().Add(2 * time.Second)
	for rs[0].fake.Canceled() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("backend never saw the cancellation")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDeadlinePropagates(t *testing.T) {
	c := startGateway(t, startReplicas(t, fakebackend.Config{PrefillDelay: time.Second}))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := generate(ctx, c, &pb.GenerateRequest{Prompt: "x", MaxTokens: 4}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
}
