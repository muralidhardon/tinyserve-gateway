package router

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/muralidhardon/tinyserve-gateway/internal/backend"
)

func pool(n int) []*backend.Backend {
	bs := make([]*backend.Backend, n)
	for i := range bs {
		bs[i] = backend.New(fmt.Sprintf("http://replica-%d:8000", i), nil)
	}
	return bs
}

func counts(r Router, bs []*backend.Backend, n int, prompt func(int) string) map[*backend.Backend]int {
	got := map[*backend.Backend]int{}
	for i := 0; i < n; i++ {
		got[r.Pick(prompt(i), bs)]++
	}
	return got
}

func distinct(i int) string { return fmt.Sprintf("prompt %d", i) }

func TestNew(t *testing.T) {
	for _, name := range Names {
		if _, err := New(name, pool(2)); err != nil {
			t.Errorf("New(%q): %v", name, err)
		}
	}
	if _, err := New("nope", pool(2)); err == nil {
		t.Error("unknown name accepted")
	}
}

func TestRoundRobinCycles(t *testing.T) {
	bs := pool(3)
	r := &RoundRobin{}
	for i := 0; i < 9; i++ {
		if got := r.Pick("", bs); got != bs[i%3] {
			t.Fatalf("pick %d = %s", i, got.Name)
		}
	}
}

func TestLeastLoadedPrefersLighterReplica(t *testing.T) {
	bs := pool(2)
	bs[0].SetScraped(5, 16)
	bs[1].SetScraped(0, 3)
	for i := 0; i < 100; i++ {
		if got := (LeastLoaded{}).Pick("", bs); got != bs[1] {
			t.Fatalf("picked the loaded replica")
		}
	}
}

func TestLeastLoadedNeverPicksTheMostLoadedOfMany(t *testing.T) {
	// P2C can only pick the most loaded replica if both samples are it, which
	// distinct sampling rules out.
	bs := pool(5)
	for i, b := range bs {
		b.SetScraped(0, int64(i))
	}
	got := counts(LeastLoaded{}, bs, 2000, distinct)
	if got[bs[4]] != 0 {
		t.Errorf("most loaded replica picked %d times", got[bs[4]])
	}
	if got[bs[0]] <= got[bs[3]] {
		t.Errorf("lightest replica picked %d times, second heaviest %d", got[bs[0]], got[bs[3]])
	}
}

func TestLeastLoadedAvoidsOverloaded(t *testing.T) {
	bs := pool(3)
	bs[0].Avoid(time.Minute)
	bs[1].Avoid(time.Minute)
	bs[2].SetScraped(100, 100) // heavily loaded but not refusing work
	for i := 0; i < 50; i++ {
		if got := (LeastLoaded{}).Pick("", bs); got != bs[2] {
			t.Fatalf("picked an overloaded replica")
		}
	}
	bs[2].Avoid(time.Minute)
	if (LeastLoaded{}).Pick("", bs) == nil {
		t.Fatal("all overloaded should still pick one")
	}
}

func TestOverloadExpires(t *testing.T) {
	b := pool(1)[0]
	b.Avoid(20 * time.Millisecond)
	if !b.Avoided() {
		t.Fatal("not overloaded right after marking")
	}
	time.Sleep(30 * time.Millisecond)
	if b.Avoided() {
		t.Fatal("still overloaded after expiry")
	}
}

func TestConsistentHashIsStickyOnPromptPrefix(t *testing.T) {
	bs := pool(4)
	c := NewConsistentHash(bs, 16)
	system := "You are a pirate. " // 18 bytes: the key is entirely this prefix
	first := c.Pick(system+"hello", bs)
	for i := 0; i < 50; i++ {
		if got := c.Pick(fmt.Sprintf("%squestion %d", system, i), bs); got != first {
			t.Fatalf("shared prefix moved from %s to %s", first.Name, got.Name)
		}
	}
}

func TestConsistentHashSpreadsKeys(t *testing.T) {
	bs := pool(4)
	got := counts(NewConsistentHash(bs, 64), bs, 8000, distinct)
	for _, b := range bs {
		if share := float64(got[b]) / 8000; math.Abs(share-0.25) > 0.07 {
			t.Errorf("%s got %.1f%% of keys, want about 25%%", b.Name, share*100)
		}
	}
}

func TestConsistentHashMovesFewKeysWhenAReplicaGoes(t *testing.T) {
	bs := pool(5)
	c := NewConsistentHash(bs, 64)
	without := bs[:4] // replica-4 excluded, as when it is down
	moved, owned := 0, 0
	for i := 0; i < 5000; i++ {
		p := distinct(i)
		before := c.Pick(p, bs)
		after := c.Pick(p, without)
		if before == bs[4] {
			owned++
			continue
		}
		if before != after {
			moved++
		}
	}
	if moved != 0 {
		t.Errorf("%d keys not owned by the removed replica moved", moved)
	}
	if owned == 0 {
		t.Error("removed replica owned no keys")
	}
}

func TestConsistentHashSkipsOverloadedOwner(t *testing.T) {
	bs := pool(3)
	c := NewConsistentHash(bs, 64)
	owner := c.Pick("some prompt", bs)
	owner.Avoid(time.Minute)
	if got := c.Pick("some prompt", bs); got == owner {
		t.Fatal("still routed to the overloaded owner")
	}
}
