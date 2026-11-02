// Package pool holds the gateway's current set of replicas. On Kubernetes the
// set changes as pods come and go (package discovery); locally it is a fixed
// list of URLs.
package pool

import (
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/muralidhardon/tinyserve-gateway/internal/backend"
)

// Source is anything that can list the current replicas.
type Source interface {
	Backends() []*backend.Backend
}

// Static is a fixed Source.
type Static []*backend.Backend

func (s Static) Backends() []*backend.Backend { return s }

// Pool is a changing set of replicas, safe for concurrent use. Readers get an
// immutable snapshot, so a request that is routing or streaming is unaffected
// by a concurrent change: a replica removed mid-stream finishes its stream,
// it just gets no new ones.
type Pool struct {
	client *http.Client
	log    *slog.Logger
	cur    atomic.Pointer[[]*backend.Backend]

	mu   sync.Mutex // serializes Set and Subscribe
	subs []func([]*backend.Backend)
}

// New returns an empty pool whose backends will use client.
func New(client *http.Client, log *slog.Logger) *Pool {
	if log == nil {
		log = slog.Default()
	}
	p := &Pool{client: client, log: log}
	p.cur.Store(&[]*backend.Backend{})
	return p
}

// Backends is the current snapshot, sorted by name. Callers must not modify it.
func (p *Pool) Backends() []*backend.Backend { return *p.cur.Load() }

// Set makes the pool exactly the replicas at urls. Replicas already in the
// pool keep their Backend, and with it their in-flight count, scraped load
// and health history.
func (p *Pool) Set(urls []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	old := p.Backends()
	byName := make(map[string]*backend.Backend, len(old))
	for _, b := range old {
		byName[b.Name] = b
	}
	var next []*backend.Backend
	seen := map[string]bool{}
	for _, u := range urls {
		name := strings.TrimRight(strings.TrimSpace(u), "/")
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		b := byName[name]
		if b == nil {
			b = backend.New(name, p.client)
			p.log.Info("replica added", "backend", name)
		}
		next = append(next, b)
	}
	for _, b := range old {
		if !seen[b.Name] {
			p.log.Info("replica removed", "backend", b.Name, "inflight", b.Inflight())
		}
	}
	slices.SortFunc(next, func(a, b *backend.Backend) int { return strings.Compare(a.Name, b.Name) })
	if slices.Equal(next, old) {
		return
	}
	p.cur.Store(&next)
	for _, f := range p.subs {
		f(next)
	}
}

// Subscribe calls f with the current replicas now and again after every change.
func (p *Pool) Subscribe(f func([]*backend.Backend)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subs = append(p.subs, f)
	f(p.Backends())
}
