package router

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// 3 blocks of 64 bytes and change
var system = strings.Repeat("You are a helpful assistant who answers in the voice of a pirate. ", 3)

func TestPrefixAwareIsStickyOnSharedPrefix(t *testing.T) {
	bs := pool(4)
	p := NewPrefixAware(PrefixConfig{})
	first := p.Pick(system+"question 0", bs)
	for i := 1; i < 50; i++ {
		if got := p.Pick(fmt.Sprintf("%squestion %d", system, i), bs); got != first {
			t.Fatalf("request %d left the replica holding its prefix", i)
		}
	}
	if p.Match(system+"anything", first) != len(system)/64 {
		t.Errorf("match = %d blocks, want %d", p.Match(system+"anything", first), len(system)/64)
	}
}

func TestPrefixAwareSpreadsDistinctPrefixes(t *testing.T) {
	bs := pool(4)
	p := NewPrefixAware(PrefixConfig{})
	got := map[string]int{}
	for i := 0; i < 400; i++ {
		// distinct first block: no affinity, so least-loaded decides; with no
		// load every pick ties, and P2C spreads ties at random
		got[p.Pick(fmt.Sprintf("tenant %03d %s", i, system), bs).Name]++
	}
	for _, b := range bs {
		if got[b.Name] < 50 {
			t.Errorf("%s got %d of 400 distinct prefixes", b.Name, got[b.Name])
		}
	}
}

func TestPrefixAwarePrefersLongestMatch(t *testing.T) {
	bs := pool(3)
	p := NewPrefixAware(PrefixConfig{})
	short := system[:128] // 2 blocks
	p.record(p.keys(short), bs[0])
	p.record(p.keys(system), bs[1]) // 3 blocks, sharing the first 2
	if got := p.Pick(system+"more", bs); got != bs[1] {
		t.Errorf("picked %s, want the 3-block match", got.Name)
	}
	if got := p.Pick(short+"something else entirely, at least sixty-four bytes long......", bs); got != bs[1] && got != bs[0] {
		t.Errorf("picked %s, want one of the 2-block matches", got.Name)
	}
}

func TestPrefixAwareBoundedLoad(t *testing.T) {
	bs := pool(3)
	p := NewPrefixAware(PrefixConfig{LoadFactor: 1.25})
	owner := p.Pick(system+"q", bs)
	// The owner is far above an even share: affinity must give way.
	owner.SetScraped(0, 20)
	spill := p.Pick(system+"q2", bs)
	if spill == owner {
		t.Fatal("overloaded owner kept the request")
	}
	// The spill replica now holds the prefix too and wins it while the owner stays hot.
	for i := 0; i < 20; i++ {
		if got := p.Pick(fmt.Sprintf("%sq%d", system, i+3), bs); got != spill {
			t.Fatalf("request went to %s, want %s (the replica that now caches the prefix)", got.Name, spill.Name)
		}
	}
	// Both hold the prefix now; with both within bounds, the most recent owner
	// keeps it rather than the momentarily lighter one.
	owner.SetScraped(0, 0)
	spill.SetScraped(0, 2)
	if got := p.Pick(system+"later", bs); got != spill {
		t.Errorf("picked %s, want the most recent owner %s", got.Name, spill.Name)
	}
}

func TestPrefixAwareSlackKeepsAffinityAtLowLoad(t *testing.T) {
	bs := pool(3)
	p := NewPrefixAware(PrefixConfig{LoadFactor: 1.25, Slack: 4})
	owner := p.Pick(system+"q", bs)
	// 3 of the fleet's 3 sequences on the owner: the bare bound
	// ceil(1.25 * 4 / 3) = 2 would spill, the slack (2 + 4 = 6) keeps it
	owner.SetScraped(0, 3)
	if got := p.Pick(system+"q2", bs); got != owner {
		t.Errorf("affinity lost at low load")
	}

	bs2 := pool(3)
	strict := NewPrefixAware(PrefixConfig{LoadFactor: 1.25, Slack: -1})
	owner2 := strict.Pick(system+"q", bs2)
	owner2.SetScraped(0, 3)
	if got := strict.Pick(system+"q2", bs2); got == owner2 {
		t.Errorf("without slack the bound should have spilled")
	}
}

func TestPrefixAwareLoadWithinBoundKeepsAffinity(t *testing.T) {
	bs := pool(4)
	p := NewPrefixAware(PrefixConfig{LoadFactor: 1.25, Slack: -1})
	owner := p.Pick(system+"q", bs)
	for _, b := range bs {
		b.SetScraped(0, 4)
	}
	owner.SetScraped(0, 4) // mean 4, limit ceil(1.25 * 17 / 4) = 6: owner's 4+1 fits
	if got := p.Pick(system+"q2", bs); got != owner {
		t.Errorf("affinity lost at even load")
	}
}

func TestPrefixAwareSkipsAvoidedOwner(t *testing.T) {
	bs := pool(2)
	p := NewPrefixAware(PrefixConfig{})
	owner := p.Pick(system+"q", bs)
	owner.Avoid(time.Minute)
	if got := p.Pick(system+"q2", bs); got == owner {
		t.Fatal("routed to an avoided replica")
	}
}

func TestPrefixAwareExcludedOwner(t *testing.T) {
	// The server passes only replicas not yet tried for this request.
	bs := pool(3)
	p := NewPrefixAware(PrefixConfig{})
	owner := p.Pick(system+"q", bs)
	var candidates = bs[:0:0]
	for _, b := range bs {
		if b != owner {
			candidates = append(candidates, b)
		}
	}
	if got := p.Pick(system+"q2", candidates); got == owner {
		t.Fatal("picked a replica that was not a candidate")
	}
}

func TestPrefixAwareShortPromptFallsBack(t *testing.T) {
	bs := pool(2)
	p := NewPrefixAware(PrefixConfig{})
	if got := p.Pick("hi", bs); got == nil {
		t.Fatal("no pick")
	}
	if p.Blocks() != 0 {
		t.Errorf("a prompt shorter than one block was indexed")
	}
}

func TestPrefixIndexIsBounded(t *testing.T) {
	bs := pool(2)
	p := NewPrefixAware(PrefixConfig{IndexBlocks: 10})
	for i := 0; i < 100; i++ {
		p.Pick(fmt.Sprintf("tenant %03d %s", i, system), bs)
	}
	if p.Blocks() != 10 {
		t.Fatalf("index holds %d blocks, want 10", p.Blocks())
	}
	// LRU evicts descendants before roots: the most recent prompt's first block survives.
	last := fmt.Sprintf("tenant %03d %s", 99, system)
	if _, ok := p.nodes[p.keys(last)[0]]; !ok {
		t.Error("most recent prefix root was evicted")
	}
}

func TestPrefixAwareConcurrent(t *testing.T) {
	bs := pool(4)
	p := NewPrefixAware(PrefixConfig{IndexBlocks: 64})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				p.Pick(fmt.Sprintf("tenant %d %s %d", i%20, system, i), bs)
			}
		}()
	}
	wg.Wait()
}

func BenchmarkPrefixAwarePick(b *testing.B) {
	bs := pool(8)
	p := NewPrefixAware(PrefixConfig{})
	prompts := make([]string, 64)
	for i := range prompts {
		prompts[i] = fmt.Sprintf("tenant %02d %s", i, strings.Repeat(system, 3)) // ~600 bytes, 9 blocks
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Pick(prompts[i%len(prompts)], bs)
	}
}
