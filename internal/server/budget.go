package server

import (
	"sync"
	"time"
)

// budgetWindow is how many seconds of history a RetryBudget counts.
const budgetWindow = 10

// RetryBudget caps retries at a fraction of recent requests, so that when the
// whole fleet is failing, retries can't multiply the load on it: without a
// budget, every request to a fleet of n failing replicas costs n attempts.
// Over the last 10 s, retries may be at most Ratio of requests plus
// MinPerSecond per second, the floor that lets low-traffic gateways retry at all.
// A nil *RetryBudget allows every retry.
type RetryBudget struct {
	ratio     float64
	minPerSec float64
	now       func() time.Time

	mu      sync.Mutex
	buckets [budgetWindow]bucket
}

type bucket struct {
	sec               int64
	requests, retries int
}

func NewRetryBudget(ratio, minPerSecond float64) *RetryBudget {
	return &RetryBudget{ratio: ratio, minPerSec: minPerSecond, now: time.Now}
}

// Request records one request's first attempt.
func (r *RetryBudget) Request() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.current().requests++
	r.mu.Unlock()
}

// TryRetry reports whether a retry fits in the budget, and spends it if so.
func (r *RetryBudget) TryRetry() bool {
	if r == nil {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	cur := r.current()
	var requests, retries int
	for _, b := range r.buckets {
		if b.sec > cur.sec-budgetWindow {
			requests += b.requests
			retries += b.retries
		}
	}
	if float64(retries+1) > r.ratio*float64(requests)+r.minPerSec*budgetWindow {
		return false
	}
	cur.retries++
	return true
}

// current returns this second's bucket, clearing it if it last held an older second.
func (r *RetryBudget) current() *bucket {
	sec := r.now().Unix()
	b := &r.buckets[sec%budgetWindow]
	if b.sec != sec {
		*b = bucket{sec: sec}
	}
	return b
}
