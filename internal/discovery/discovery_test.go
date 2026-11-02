package discovery

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	"github.com/muralidhardon/tinyserve-gateway/internal/pool"
)

func endpoint(ip string, ready *bool) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{Addresses: []string{ip}, Conditions: discoveryv1.EndpointConditions{Ready: ready}}
}

func slice(name, service string, port int32, eps ...discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "ns",
			Labels: map[string]string{discoveryv1.LabelServiceName: service},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: ptr.To("http"), Port: ptr.To(port)}},
		Endpoints:   eps,
	}
}

func TestURLs(t *testing.T) {
	s1 := slice("a", "tinyserve", 8000,
		endpoint("10.0.0.2", ptr.To(true)),
		endpoint("10.0.0.1", nil),           // unknown readiness counts as ready
		endpoint("10.0.0.3", ptr.To(false)), // terminating or not ready yet: drained
	)
	s2 := slice("b", "tinyserve", 8000, endpoint("10.0.0.2", ptr.To(true))) // duplicate across slices
	v6 := slice("c", "tinyserve", 8000, endpoint("fd00::7", ptr.To(true)))
	v6.AddressType = discoveryv1.AddressTypeIPv6
	other := slice("d", "tinyserve", 9000, endpoint("10.0.0.9", ptr.To(true)))
	other.Ports[0].Name = ptr.To("metrics")

	got := URLs([]*discoveryv1.EndpointSlice{s1, s2, v6, other}, "http")
	want := []string{"http://10.0.0.1:8000", "http://10.0.0.2:8000", "http://[fd00::7]:8000"}
	if !slices.Equal(got, want) {
		t.Fatalf("URLs = %v, want %v", got, want)
	}
	if got := URLs([]*discoveryv1.EndpointSlice{other}, ""); !slices.Equal(got, []string{"http://10.0.0.9:9000"}) {
		t.Fatalf("empty port name should take the first port, got %v", got)
	}
}

func TestStartFollowsEndpointSlices(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := fake.NewSimpleClientset(
		slice("ts-1", "tinyserve", 8000, endpoint("10.0.0.1", ptr.To(true)), endpoint("10.0.0.2", ptr.To(true))),
		slice("other-1", "something-else", 8000, endpoint("10.9.9.9", ptr.To(true))),
	)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	p := pool.New(nil, log)
	if err := Start(ctx, client, Config{Namespace: "ns", Service: "tinyserve", PortName: "http", Logger: log}, p); err != nil {
		t.Fatal(err)
	}
	names := func() []string {
		var out []string
		for _, b := range p.Backends() {
			out = append(out, b.Name)
		}
		return out
	}
	if got := names(); !slices.Equal(got, []string{"http://10.0.0.1:8000", "http://10.0.0.2:8000"}) {
		t.Fatalf("after Start: %v", got)
	}
	kept := p.Backends()[0]

	// A pod starts terminating and a new one becomes ready.
	sl := client.DiscoveryV1().EndpointSlices("ns")
	_, err := sl.Update(ctx, slice("ts-1", "tinyserve", 8000,
		endpoint("10.0.0.1", ptr.To(true)), endpoint("10.0.0.2", ptr.To(false)), endpoint("10.0.0.3", ptr.To(true))),
		metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		return slices.Equal(names(), []string{"http://10.0.0.1:8000", "http://10.0.0.3:8000"})
	})
	if p.Backends()[0] != kept {
		t.Fatal("the unchanged replica should keep its Backend")
	}

	if err := sl.Delete(ctx, "ts-1", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return len(p.Backends()) == 0 })
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
