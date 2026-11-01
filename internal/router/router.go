// Package router picks which replica serves a request.
package router

import (
	"sync/atomic"

	"github.com/muralidhardon/tinyserve-gateway/internal/backend"
)

// Router chooses one of the candidate replicas for a prompt. Candidates is
// never empty. Implementations must be safe for concurrent use.
type Router interface {
	Pick(prompt string, candidates []*backend.Backend) *backend.Backend
}

// RoundRobin cycles through the candidates in order.
type RoundRobin struct {
	next atomic.Uint64
}

func (r *RoundRobin) Pick(_ string, candidates []*backend.Backend) *backend.Backend {
	n := r.next.Add(1) - 1
	return candidates[n%uint64(len(candidates))]
}
