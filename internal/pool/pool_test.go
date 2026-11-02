package pool

import (
	"io"
	"log/slog"
	"testing"

	"github.com/muralidhardon/tinyserve-gateway/internal/backend"
)

func names(bs []*backend.Backend) []string {
	var out []string
	for _, b := range bs {
		out = append(out, b.Name)
	}
	return out
}

func TestSetKeepsSurvivingBackends(t *testing.T) {
	p := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var notified [][]string
	p.Subscribe(func(bs []*backend.Backend) { notified = append(notified, names(bs)) })

	p.Set([]string{"http://b:8000", "http://a:8000/", " http://a:8000"})
	first := p.Backends()
	if got := names(first); len(got) != 2 || got[0] != "http://a:8000" || got[1] != "http://b:8000" {
		t.Fatalf("Backends = %v, want a and b, sorted and deduplicated", got)
	}
	first[0].SetScraped(3, 4)

	p.Set([]string{"http://a:8000", "http://c:8000"})
	got := p.Backends()
	if names(got)[0] != "http://a:8000" || got[0] != first[0] || got[0].Load() != 7 {
		t.Fatal("a's Backend (and its load) should survive the update")
	}
	if names(got)[1] != "http://c:8000" {
		t.Fatalf("Backends = %v, want a and c", names(got))
	}
	if len(first) != 2 || first[1].Name != "http://b:8000" {
		t.Fatal("an old snapshot must not change")
	}

	p.Set([]string{"http://c:8000", "http://a:8000"}) // same set: no notification
	if len(notified) != 3 {
		t.Fatalf("%d notifications (%v), want 3: initial, first Set, second Set", len(notified), notified)
	}
}
