// Package observe contains Curo's bounded request observation state.
package observe

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/raj1kshtz/curo/internal/diagnose"
	"github.com/raj1kshtz/curo/internal/policy"
	"github.com/raj1kshtz/curo/internal/retry"
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

// Completion summarizes how Finish handled one initial attempt.
type Completion struct {
	// Latency is the observer-clock time between Begin and Finish, or zero
	// when the clock moved backwards.
	Latency time.Duration

	// Recorded reports whether the attempt entered rolling evidence.
	Recorded bool

	// DependencyFailure reports whether the attempt failed for a reason
	// attributed to the dependency rather than to the caller.
	DependencyFailure bool
}

// RetryReservation describes the outcome of ReserveRetry.
type RetryReservation uint8

const (
	// RetryIneligible means RetryPermitted reported false, so no budget was
	// consulted.
	RetryIneligible RetryReservation = iota

	// RetryReserved means the returned lease holds one token from both retry
	// budgets.
	RetryReserved

	// RetryBudgetExhausted means the target or instance retry budget was
	// empty, so nothing was taken.
	RetryBudgetExhausted
)

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
	changes  *journal

	observed    atomic.Uint64
	overflow    atomic.Uint64
	retryBudget retry.Budget
}

// New constructs an Observer with bounded production defaults.
func New() *Observer {
	return NewWithClock(time.Now)
}

// NewWithClock constructs an Observer that reads time from now. A nil clock
// uses time.Now. It exists so callers can drive deterministic tests.
func NewWithClock(now func() time.Time) *Observer {
	if now == nil {
		now = time.Now
	}

	changes := &journal{}
	return &Observer{
		now:    now,
		origin: now(),
		registry: newRegistry(
			defaultTargetCapacity,
			defaultShardCount,
			defaultIdleTTL,
			changes,
		),
		changes: changes,
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
//
// Each recorded attempt on a regular target funds that target's retry budget
// and the instance retry budget. Overflow attempts never fund retries.
func (observer *Observer) Finish(token Token, result Result) Completion {
	if observer == nil || token.target == nil {
		return Completion{}
	}

	finished := observer.now()
	tick := observer.tick(finished)
	latency := max(finished.Sub(token.started), 0)

	var contextErr error
	if token.context != nil {
		contextErr = token.context.Err()
	}

	value := classify(result, contextErr, latency)
	completion := Completion{
		Latency:           latency,
		DependencyFailure: value.signal == diagnosisDependencyFailure,
	}
	if !token.target.record(tick, value) {
		return completion
	}

	completion.Recorded = true
	observer.observed.Add(1)
	if token.overflow {
		observer.overflow.Add(1)
		return completion
	}

	retry.Deposit(&token.target.retryBudget, &observer.retryBudget)
	return completion
}

// ReserveRetry reserves one retry token from token's target budget and from
// the instance budget when RetryPermitted reports true. The returned lease
// holds the token until the caller commits or cancels it.
func (observer *Observer) ReserveRetry(token Token) (retry.Lease, RetryReservation) {
	if !observer.RetryPermitted(token) {
		return retry.Lease{}, RetryIneligible
	}

	lease, reserved := retry.Reserve(&token.target.retryBudget, &observer.retryBudget)
	if !reserved {
		return lease, RetryBudgetExhausted
	}

	return lease, RetryReserved
}

// RetryPermitted reports whether token's target is live and its published
// plan is an unexpired current-version plan containing the Retry candidate.
//
// Overflow tokens, retired targets, and expired or older plans are never
// permitted. RetryPermitted never evaluates a diagnosis.
func (observer *Observer) RetryPermitted(token Token) bool {
	if observer == nil || token.target == nil || token.overflow {
		return false
	}

	current, live := token.target.published()
	plan := current.plan
	return live &&
		plan.Version == policy.Version &&
		plan.Candidates&policy.CandidateRetry != 0 &&
		observer.tick(observer.now()) < plan.ExpiresAt
}

// Diagnosis returns the current bounded diagnosis for token's target.
//
// The zero value is returned for nil observers, empty tokens, and the
// non-actionable overflow aggregate.
func (observer *Observer) Diagnosis(token Token) diagnose.Result {
	if observer == nil || token.target == nil || token.overflow {
		return diagnose.Result{}
	}

	return token.target.diagnosisAt(observer.tick(observer.now()))
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
