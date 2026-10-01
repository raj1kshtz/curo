// Package guard contains the failure boundary for Curo-owned request work.
package guard

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	failureThreshold = 3
	failureWindow    = time.Minute
)

// Outcome describes how a guarded operation finished.
type Outcome uint8

const (
	// Skipped means the guard was already disabled and did not run the operation.
	Skipped Outcome = iota

	// Completed means the operation returned without an internal failure.
	Completed

	// Failed means the operation returned an error or raised a recovered panic.
	Failed
)

// Guard contains failures from Curo-owned work and permanently disables itself
// after repeated failures. A Guard must not be copied after first use.
type Guard struct {
	failures  [failureThreshold]time.Time
	trippedAt time.Time
	now       func() time.Time

	mu           sync.Mutex
	failureCount int
	disabled     atomic.Bool
}

// New constructs an enabled Guard.
func New() *Guard {
	return newGuard(time.Now)
}

func newGuard(now func() time.Time) *Guard {
	if now == nil {
		now = time.Now
	}

	return &Guard{now: now}
}

// Disabled reports whether the guard has permanently selected pass-through.
//
// A nil Guard is disabled so an incomplete owner fails safe.
func (g *Guard) Disabled() bool {
	return g == nil || g.disabled.Load()
}

// Run executes operation inside a narrow recovery boundary.
//
// Run never holds the failure-window lock while operation executes. Returned
// errors and recovered panics both contribute to the same bounded failure
// signal. A disabled or nil Guard skips operation.
func (g *Guard) Run(operation func() error) (outcome Outcome) {
	if g == nil || g.disabled.Load() {
		return Skipped
	}

	outcome = Completed
	defer func() {
		if recovered := recover(); recovered != nil {
			g.recordFailure()
			outcome = Failed
		}
	}()

	if err := operation(); err != nil {
		g.recordFailure()
		return Failed
	}

	return outcome
}

func (g *Guard) recordFailure() {
	now := time.Now()
	if g.now != nil {
		now = g.now()
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.disabled.Load() {
		return
	}

	cutoff := now.Add(-failureWindow)
	retained := 0
	for index := 0; index < g.failureCount; index++ {
		if g.failures[index].Before(cutoff) {
			continue
		}

		g.failures[retained] = g.failures[index]
		retained++
	}

	for index := retained; index < g.failureCount; index++ {
		g.failures[index] = time.Time{}
	}

	g.failures[retained] = now
	g.failureCount = retained + 1

	if g.failureCount == failureThreshold {
		g.trippedAt = now
		g.disabled.Store(true)
	}
}
