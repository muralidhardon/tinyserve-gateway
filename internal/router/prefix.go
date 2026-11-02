package router

import (
	"container/list"
	"math"
	"sync"

	"github.com/muralidhardon/tinyserve-gateway/internal/backend"
)

// PrefixConfig tunes PrefixAware. Zero fields take the defaults.
type PrefixConfig struct {
	// BlockBytes is the granularity of prefix matching. TinyServe caches K/V
	// rows in 16-token blocks, about 50-70 bytes of English with its
	// tokenizer, so a one-block match here usually means at least one cached
	// block there. Default 64.
	BlockBytes int
	// LoadFactor and Slack bound how loaded a replica may be and still be
	// chosen for its cached prefix: ceil(LoadFactor * (total+1) / n) + Slack
	// sequences. The first term is consistent hashing with bounded loads
	// (Mirrokni et al. 2018). Slack is there because continuous batching
	// makes a few extra sequences nearly free (they share forward passes up
	// to TinyServe's max batch of 16) while a cache miss costs a full prefill,
	// so at low load the bare bound would give up affinity for nothing.
	// Defaults 1.25 and 4.
	LoadFactor float64
	Slack      int
	// IndexBlocks caps how many prefix blocks the index remembers (LRU).
	// Default 1<<16.
	IndexBlocks int
	// ReplicasPerBlock caps how many replicas one block remembers, most recent
	// first. Default 4.
	ReplicasPerBlock int
}

func (c PrefixConfig) withDefaults() PrefixConfig {
	if c.BlockBytes <= 0 {
		c.BlockBytes = 64
	}
	if c.LoadFactor <= 1 {
		c.LoadFactor = 1.25
	}
	if c.Slack < 0 {
		c.Slack = 0
	} else if c.Slack == 0 {
		c.Slack = 4
	}
	if c.IndexBlocks <= 0 {
		c.IndexBlocks = 1 << 16
	}
	if c.ReplicasPerBlock <= 0 {
		c.ReplicasPerBlock = 4
	}
	return c
}

// PrefixAware routes a prompt to the replica most likely to have its prefix in
// TinyServe's prefix cache, so the engine copies those K/V rows instead of
// prefilling them, unless that replica is carrying more than its bounded share
// of the load. Without a usable match it falls back to least-loaded.
//
// The gateway has no tokenizer, so it tracks prefixes as chained hashes of
// fixed-size byte blocks, recording which replicas it sent each prefix to. That
// is a prediction of the engines' caches, not a mirror of them: an engine may
// have evicted a block the index still lists. The index forgets old blocks by
// LRU, and the engines' own hit counters (prefix_tokens_reused) measure how
// good the prediction is.
type PrefixAware struct {
	cfg      PrefixConfig
	fallback LeastLoaded

	mu    sync.Mutex
	nodes map[uint64]*list.Element // of *prefixNode
	lru   *list.List               // front = most recently used
}

type prefixNode struct {
	key      uint64
	replicas []*backend.Backend // most recent first
}

func NewPrefixAware(cfg PrefixConfig) *PrefixAware {
	return &PrefixAware{cfg: cfg.withDefaults(), nodes: map[uint64]*list.Element{}, lru: list.New()}
}

// Match reports how many leading blocks of prompt the index has recorded for b.
func (p *PrefixAware) Match(prompt string, b *backend.Backend) int {
	keys := p.keys(prompt)
	p.mu.Lock()
	defer p.mu.Unlock()
	depth := 0
	for _, k := range keys {
		el, ok := p.nodes[k]
		if !ok || !containsBackend(el.Value.(*prefixNode).replicas, b) {
			break
		}
		depth++
	}
	return depth
}

func (p *PrefixAware) Pick(prompt string, candidates []*backend.Backend) *backend.Backend {
	b, _ := p.PickExplained(prompt, candidates)
	return b
}

// PickExplained is Pick, also reporting why that replica was chosen.
func (p *PrefixAware) PickExplained(prompt string, candidates []*backend.Backend) (*backend.Backend, Decision) {
	keys := p.keys(prompt)
	candidates = notAvoided(candidates)

	// Bounded load: a replica may take this request for its cache only if
	// that keeps it within its bounded share (see PrefixConfig).
	var total int64
	for _, b := range candidates {
		total += b.Load()
	}
	limit := int64(math.Ceil(p.cfg.LoadFactor*float64(total+1)/float64(len(candidates)))) + int64(p.cfg.Slack)

	p.mu.Lock()
	defer p.mu.Unlock()

	// Walk the prompt's blocks from the start, narrowing to the replicas that
	// still match. depth[b] is how far b matched; rank[b] is b's recency at
	// that depth (0 = the replica most recently sent this prefix).
	depth := make(map[*backend.Backend]int, len(candidates))
	rank := make(map[*backend.Backend]int, len(candidates))
	for i, k := range keys {
		el, ok := p.nodes[k]
		if !ok {
			break
		}
		matched := false
		for r, b := range el.Value.(*prefixNode).replicas {
			if depth[b] == i && containsBackend(candidates, b) {
				depth[b], rank[b] = i+1, r
				matched = true
			}
		}
		if !matched {
			break
		}
	}

	// Longest match wins; among equal matches, the most recent owner, so a
	// prefix stays put instead of hopping to whichever replica is momentarily
	// lighter and ending up cached (and evicting others) everywhere.
	var best *backend.Backend
	matched := false
	for _, b := range candidates {
		d := depth[b]
		if d == 0 {
			continue
		}
		matched = true
		if b.Load()+1 > limit {
			continue
		}
		if best == nil || d > depth[best] || (d == depth[best] && rank[b] < rank[best]) {
			best = b
		}
	}
	reason := "prefix_match"
	if best == nil {
		reason = "no_match"
		if matched {
			reason = "spill"
		}
		best = p.fallback.Pick(prompt, candidates)
	}
	p.record(keys, best)
	return best, Decision{Reason: reason, MatchBlocks: depth[best]}
}

// keys returns the chained hash of each whole block of the prompt: key i
// names the whole prefix through block i, not just block i's bytes.
func (p *PrefixAware) keys(prompt string) []uint64 {
	n := len(prompt) / p.cfg.BlockBytes
	keys := make([]uint64, n)
	h := uint64(0x9e3779b97f4a7c15)
	for i := 0; i < n; i++ {
		// FNV-1a over the block, seeded with the parent's key, then mixed
		x := h ^ 0xcbf29ce484222325
		for _, c := range []byte(prompt[i*p.cfg.BlockBytes : (i+1)*p.cfg.BlockBytes]) {
			x = (x ^ uint64(c)) * 0x100000001b3
		}
		h = mix64(x)
		keys[i] = h
	}
	return keys
}

// record notes that b is serving every block of the prompt. Called with mu held.
func (p *PrefixAware) record(keys []uint64, b *backend.Backend) {
	// Touch from the deepest block up, so a prefix's root is always more
	// recently used than its descendants and is evicted after them.
	for i := len(keys) - 1; i >= 0; i-- {
		k := keys[i]
		if el, ok := p.nodes[k]; ok {
			n := el.Value.(*prefixNode)
			n.replicas = moveToFront(n.replicas, b, p.cfg.ReplicasPerBlock)
			p.lru.MoveToFront(el)
			continue
		}
		p.nodes[k] = p.lru.PushFront(&prefixNode{key: k, replicas: []*backend.Backend{b}})
		for p.lru.Len() > p.cfg.IndexBlocks {
			old := p.lru.Back()
			p.lru.Remove(old)
			delete(p.nodes, old.Value.(*prefixNode).key)
		}
	}
}

// Blocks reports how many prefix blocks the index holds.
func (p *PrefixAware) Blocks() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lru.Len()
}

func moveToFront(rs []*backend.Backend, b *backend.Backend, limit int) []*backend.Backend {
	out := make([]*backend.Backend, 0, limit)
	out = append(out, b)
	for _, r := range rs {
		if r != b && len(out) < limit {
			out = append(out, r)
		}
	}
	return out
}

func containsBackend(bs []*backend.Backend, b *backend.Backend) bool {
	for _, x := range bs {
		if x == b {
			return true
		}
	}
	return false
}
