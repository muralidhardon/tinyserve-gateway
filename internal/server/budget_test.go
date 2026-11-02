package server

import (
	"testing"
	"time"
)

func TestRetryBudget(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewRetryBudget(0.1, 0)
	b.now = func() time.Time { return now }

	for range 100 {
		b.Request()
	}
	for i := range 10 {
		if !b.TryRetry() {
			t.Fatalf("retry %d refused; 10%% of 100 requests is 10", i+1)
		}
	}
	if b.TryRetry() {
		t.Fatal("11th retry allowed")
	}

	// 10 s later the old requests and retries have aged out together.
	now = now.Add(budgetWindow * time.Second)
	if b.TryRetry() {
		t.Fatal("retry allowed with no recent requests")
	}
	for range 20 {
		b.Request()
	}
	allowed := 0
	for range 3 {
		if b.TryRetry() {
			allowed++
		}
	}
	if allowed != 2 {
		t.Fatalf("%d retries allowed for 20 fresh requests, want 2", allowed)
	}
}

func TestRetryBudgetFloor(t *testing.T) {
	b := NewRetryBudget(0.1, 1) // 1/s over a 10 s window: 10 retries with no traffic
	for i := range 10 {
		if !b.TryRetry() {
			t.Fatalf("retry %d refused under the per-second floor", i+1)
		}
	}
	if b.TryRetry() {
		t.Fatal("floor exceeded")
	}
}

func TestNilRetryBudgetAllowsAll(t *testing.T) {
	var b *RetryBudget
	b.Request()
	if !b.TryRetry() {
		t.Fatal("nil budget refused a retry")
	}
}
