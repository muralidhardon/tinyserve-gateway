// Package router picks which replica serves a request.
package router

import (
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"slices"
	"sort"
	"strconv"
	"sync/atomic"

	"github.com/muralidhardon/tinyserve-gateway/internal/backend"
)

// Router chooses one of the candidate replicas for a prompt. Candidates is
// never empty. Implementations must be safe for concurrent use.
type Router interface {
	Pick(prompt string, candidates []*backend.Backend) *backend.Backend
}

// Decision says why a router picked the replica it did.
type Decision struct {
	// Reason is prefix_match, spill (the replicas holding the prefix were over
	// their load bound) or no_match for prefix_aware; other routers report
	// their own name.
	Reason string
	// MatchBlocks is how many prompt blocks the pick was predicted to have
	// cached, or -1 for routers that don't track prefixes.
	MatchBlocks int
}

// Explain picks like r.Pick and also says why.
func Explain(r Router, prompt string, candidates []*backend.Backend) (*backend.Backend, Decision) {
	if p, ok := r.(*PrefixAware); ok {
		return p.PickExplained(prompt, candidates)
	}
	name := "unknown"
	switch r.(type) {
	case *RoundRobin:
		name = "round_robin"
	case Random:
		name = "random"
	case LeastLoaded:
		name = "least_loaded"
	case *ConsistentHash:
		name = "consistent_hash"
	}
	return r.Pick(prompt, candidates), Decision{Reason: name, MatchBlocks: -1}
}

// Reasons lists the Decision reasons the named strategy reports.
func Reasons(name string) []string {
	if name == "prefix_aware" {
		return []string{"prefix_match", "spill", "no_match"}
	}
	return []string{name}
}

// Names lists the strategies New accepts.
var Names = []string{"round_robin", "random", "least_loaded", "consistent_hash", "prefix_aware"}

// New builds the named strategy over the gateway's replicas. prefix only
// applies to prefix_aware.
func New(name string, backends []*backend.Backend, prefix PrefixConfig) (Router, error) {
	switch name {
	case "round_robin":
		return &RoundRobin{}, nil
	case "random":
		return Random{}, nil
	case "least_loaded":
		return LeastLoaded{}, nil
	case "consistent_hash":
		return NewConsistentHash(backends, DefaultHashKeyBytes), nil
	case "prefix_aware":
		return NewPrefixAware(prefix), nil
	}
	return nil, fmt.Errorf("unknown router %q (want one of %v)", name, Names)
}

// RoundRobin cycles through the candidates in order. It ignores load and
// overload signals, which makes it the baseline the other strategies are
// measured against.
type RoundRobin struct {
	next atomic.Uint64
}

func (r *RoundRobin) Pick(_ string, candidates []*backend.Backend) *backend.Backend {
	n := r.next.Add(1) - 1
	return candidates[n%uint64(len(candidates))]
}

// Random picks uniformly. Also a baseline.
type Random struct{}

func (Random) Pick(_ string, candidates []*backend.Backend) *backend.Backend {
	return candidates[rand.IntN(len(candidates))]
}

// LeastLoaded is power-of-two-choices: sample two replicas, take the one with
// the lower Load. Comparing two random replicas instead of scanning for the
// global minimum avoids every gateway herding onto the same replica between
// scrapes, and still gives exponentially better balance than random.
// Avoided replicas (recent 503 or connection failure) are skipped unless all are.
type LeastLoaded struct{}

func (LeastLoaded) Pick(_ string, candidates []*backend.Backend) *backend.Backend {
	candidates = notAvoided(candidates)
	if len(candidates) == 1 {
		return candidates[0]
	}
	i := rand.IntN(len(candidates))
	j := rand.IntN(len(candidates) - 1)
	if j >= i {
		j++
	}
	a, b := candidates[i], candidates[j]
	if b.Load() < a.Load() {
		return b
	}
	return a
}

// DefaultHashKeyBytes is how much of the prompt ConsistentHash keys on, so
// that prompts sharing a system prompt or opening context land together.
const DefaultHashKeyBytes = 64

const vnodesPerBackend = 128

// ConsistentHash maps the start of the prompt onto a hash ring of replicas.
// Prompts that begin the same way go to the same replica, and adding or
// removing a replica moves only about 1/N of the keys. When the owning
// replica is excluded or avoided, the next one clockwise takes the request.
type ConsistentHash struct {
	keyBytes int
	ring     atomic.Pointer[[]vnode] // sorted by hash
}

type vnode struct {
	hash    uint64
	backend *backend.Backend
}

func NewConsistentHash(backends []*backend.Backend, keyBytes int) *ConsistentHash {
	c := &ConsistentHash{keyBytes: keyBytes}
	c.Update(backends)
	return c
}

// Update rebuilds the ring for a new set of replicas. A replica's virtual
// nodes depend only on its name, so keys move only to or from the replicas
// that were added or removed.
func (c *ConsistentHash) Update(backends []*backend.Backend) {
	ring := make([]vnode, 0, len(backends)*vnodesPerBackend)
	for _, b := range backends {
		for i := 0; i < vnodesPerBackend; i++ {
			ring = append(ring, vnode{hash64(b.Name + "#" + strconv.Itoa(i)), b})
		}
	}
	sort.Slice(ring, func(i, j int) bool { return ring[i].hash < ring[j].hash })
	c.ring.Store(&ring)
}

func (c *ConsistentHash) Pick(prompt string, candidates []*backend.Backend) *backend.Backend {
	key := prompt
	if len(key) > c.keyBytes {
		key = key[:c.keyBytes]
	}
	h := hash64(key)
	ring := *c.ring.Load()
	start := sort.Search(len(ring), func(i int) bool { return ring[i].hash >= h })

	var fallback *backend.Backend
	for k := 0; k < len(ring); k++ {
		b := ring[(start+k)%len(ring)].backend
		if !slices.Contains(candidates, b) {
			continue
		}
		if !b.Avoided() {
			return b
		}
		if fallback == nil {
			fallback = b
		}
	}
	if fallback != nil {
		return fallback
	}
	return candidates[0] // added to the pool and not yet on the ring
}

func hash64(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	// Finish with splitmix64 so near-identical keys ("name#1", "name#2")
	// spread evenly around the ring.
	return mix64(h.Sum64())
}

func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func notAvoided(candidates []*backend.Backend) []*backend.Backend {
	ok := slices.DeleteFunc(slices.Clone(candidates), (*backend.Backend).Avoided)
	if len(ok) == 0 {
		return candidates
	}
	return ok
}
