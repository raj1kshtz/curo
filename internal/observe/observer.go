// Package observe contains Curo's bounded request observation state.
package observe

import (
	"context"
	"sync/atomic"
	"time"
)

// Request is the bounded request metadata accepted by Observer.
type Request struct {
	Context  context.Context
	Method   string
	Scheme   string
	Hostname string
	Port     string
}

// Result is a body-free transport result captured after an attempt.
type Result struct {
	Err         error
	StatusCode  int
	HasResponse bool
}

// Token carries request-local observation state between Begin and Finish.
type Token struct {
	target   *target
	context  context.Context
	started  time.Time
	overflow bool
}

// Stats is an aggregate snapshot of one Observer.
type Stats struct {
	ObservedRequests uint64
	TrackedTargets   uint64
	OverflowRequests uint64
}

// Observer owns a bounded target registry and fixed-size rolling evidence.
type Observer struct {
	now      func() time.Time
	origin   time.Time
	registry *registry

	observed atomic.Uint64
	overflow atomic.Uint64
}

// New constructs an Observer with bounded production defaults.
func New() *Observer {
	return newObserver(time.Now)
}

func newObserver(now func() time.Time) *Observer {
	if now == nil {
		now = time.Now
	}

	return &Observer{
		now:    now,
		origin: now(),
		registry: newRegistry(
			defaultTargetCapacity,
			defaultShardCount,
			defaultIdleTTL,
		),
	}
}

// Begin resolves bounded target state before a transport attempt.
func (observer *Observer) Begin(request Request) Token {
	if observer == nil {
		return Token{}
	}

	started := observer.now()
	tick := observer.tick(started)
	if request.Context == nil {
		request.Context = context.Background()
	}

	key, valid := normalizeTarget(request)
	if !valid {
		return Token{
			target:   observer.registry.overflow,
			context:  request.Context,
			started:  started,
			overflow: true,
		}
	}

	state, overflow := observer.registry.get(key, tick)
	return Token{
		target:   state,
		context:  request.Context,
		started:  started,
		overflow: overflow,
	}
}

// Finish records one completed initial transport attempt.
func (observer *Observer) Finish(token Token, result Result) {
	if observer == nil || token.target == nil {
		return
	}

	finished := observer.now()
	tick := observer.tick(finished)
	latency := finished.Sub(token.started)

	var contextErr error
	if token.context != nil {
		contextErr = token.context.Err()
	}

	value := classify(result, contextErr, latency)
	if !token.target.record(tick, value) {
		return
	}

	observer.observed.Add(1)
	if token.overflow {
		observer.overflow.Add(1)
	}
}

// Stats returns a concurrency-safe aggregate snapshot.
func (observer *Observer) Stats() Stats {
	if observer == nil {
		return Stats{}
	}

	overflow := observer.overflow.Load()
	observed := observer.observed.Load()

	return Stats{
		ObservedRequests: observed,
		TrackedTargets:   observer.registry.trackedTargets(),
		OverflowRequests: overflow,
	}
}

func (observer *Observer) tick(at time.Time) int64 {
	elapsed := at.Sub(observer.origin)
	if elapsed < 0 {
		return 0
	}

	return int64(elapsed)
}
