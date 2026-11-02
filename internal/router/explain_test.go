package router

import "testing"

func TestPrefixAwareExplainsDecisions(t *testing.T) {
	bs := pool(3)
	p := NewPrefixAware(PrefixConfig{})
	blocks := len(system) / 64

	owner, d := Explain(p, system+"q", bs)
	if d.Reason != "no_match" || d.MatchBlocks != 0 {
		t.Errorf("first request: %+v, want no_match with 0 blocks", d)
	}
	if b, d := Explain(p, system+"q2", bs); b != owner || d.Reason != "prefix_match" || d.MatchBlocks != blocks {
		t.Errorf("repeat prefix: %s %+v, want %s prefix_match with %d blocks", b.Name, d, owner.Name, blocks)
	}
	owner.SetScraped(0, 20)
	if b, d := Explain(p, system+"q3", bs); b == owner || d.Reason != "spill" || d.MatchBlocks != 0 {
		t.Errorf("hot owner: %s %+v, want another replica, spill with 0 blocks", b.Name, d)
	}
}

func TestExplainOtherRouters(t *testing.T) {
	bs := pool(2)
	for _, name := range []string{"round_robin", "random", "least_loaded", "consistent_hash"} {
		r, err := New(name, bs, PrefixConfig{})
		if err != nil {
			t.Fatal(err)
		}
		if b, d := Explain(r, "prompt", bs); b == nil || d.Reason != name || d.MatchBlocks != -1 {
			t.Errorf("%s: %v %+v, want reason %q with -1 blocks", name, b, d, name)
		}
	}
}
