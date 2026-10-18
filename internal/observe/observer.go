// Package observe contains Curo's bounded request observation state.
package observe

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/raj1kshtz/curo/internal/diagnose"
	"github.com/raj1kshtz/curo/internal/retry"
	"github.com/raj1kshtz/curo/internal/timeout"
)

// Request is the bounded request metadata accepted by Observer.
type Request struct {
	Context context.Context

	// Cancel is the request's deprecated Cancel channel, or nil. Closing it
	// cancels the request just like canceling Context.
	Cancel <-chan struct{}

	Method   string
	Scheme   string
	Hostname string
	Port     string
}

// Call is the bounded target metadata of an Engine call.
type Call struct {
	Context  context.Context
	Scheme   string
	Hostname string

	// Port is the target's port. Zero selects the default port of http and
	// https, and is not valid for any other scheme.
	Port   uint16
	Method MethodClass
}

// Outcome is how an Engine call's attempt ended, as the call classified it.
type Outcome uint8

const (
	// OutcomeUnclassified marks an HTTP result, which Finish classifies from
	// its error, status code, and response.
	OutcomeUnclassified Outcome = iota

	// OutcomeSuccess is an attempt that the dependency served.
	OutcomeSuccess

	// OutcomeClientFailure is an attempt that the dependency answered with a
	// failure caused by the request.
	OutcomeClientFailure

	// OutcomeRateLimited is an attempt that the dependency refused because of
	// a rate limit.
	OutcomeRateLimited

	// OutcomeDependencyFailure is an attempt that the dependency answered
	// with a failure of its own.
	OutcomeDependencyFailure

	// OutcomeTransportFailure is an attempt that got no answer from the
	// dependency.
	OutcomeTransportFailure
)

// Result is a body-free transport result captured after an attempt.
type Result struct {
	Err error

	// Timeout is the adaptive timeout that ended the attempt, or zero when
	// Curo did not end it. A non-zero Timeout overrides every other field.
	Timeout    time.Duration
	StatusCode int

	// Outcome classifies an Engine call's attempt. A non-zero Outcome
	// overrides StatusCode and HasResponse, though not a caller cancellation
	// that Err reports.
	Outcome     Outcome
	HasResponse bool
}

// Token carries request-local observation state between Begin and Finish.
type Token struct {
	target  *target
	context context.Context
	cancel  <-chan struct{}
	started time.Time

	// returned and returnedErr are when the attempt returned and the caller's
	// error at that moment, once Return stamped them.
	returned    time.Time
	returnedErr error
	probe       uint64
	stamped     bool
	overflow    bool
	admitted    bool
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

	// Trip reports that a closed dependency breaker admitted the attempt, the
	// attempt failed for a reason attributed to the dependency, the breaker is
	// still closed, and the target's current plan selects
	// CandidateBreakerOpen. The caller decides whether the request may still
	// act, and if so calls Trip.
	Trip bool
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

// New constructs an Observer with bounded production defaults. Regular
// targets of read requests select adaptive timeouts within bounds.
func New(bounds timeout.Bounds) *Observer {
	return NewWithClock(time.Now, bounds)
}

// NewWithClock constructs an Observer that reads time from now. A nil clock
// uses time.Now. It exists so callers can drive deterministic tests.
func NewWithClock(now func() time.Time, bounds timeout.Bounds) *Observer {
	if now == nil {
		now = time.Now
	}

	changes := &journal{}
	registry := newRegistry(
		defaultTargetCapacity,
		defaultShardCount,
		defaultIdleTTL,
		changes,
	)
	registry.timeouts = bounds

	return &Observer{
		now:      now,
		origin:   now(),
		registry: registry,
		changes:  changes,
	}
}

// Begin resolves bounded target state before a transport attempt.
func (observer *Observer) Begin(request Request) Token {
	if observer == nil {
		return Token{}
	}

	started := observer.now()
	if request.Context == nil {
		request.Context = context.Background()
	}

	key, valid := normalizeTarget(request)
	return observer.begin(started, request.Context, request.Cancel, key, valid)
}

// BeginCall resolves bounded target state before an Engine call's attempt.
func (observer *Observer) BeginCall(call Call) Token {
	if observer == nil {
		return Token{}
	}

	started := observer.now()
	if call.Context == nil {
		call.Context = context.Background()
	}

	key, valid := normalizeCall(call)
	return observer.begin(started, call.Context, nil, key, valid)
}

func (observer *Observer) begin(
	started time.Time,
	ctx context.Context,
	cancel <-chan struct{},
	key targetKey,
	valid bool,
) Token {
	tick := observer.tick(started)
	if !valid {
		return Token{
			target:   observer.registry.overflow,
			context:  ctx,
			cancel:   cancel,
			started:  started,
			overflow: true,
		}
	}

	state, overflow := observer.registry.get(key, tick)
	return Token{
		target:   state,
		context:  ctx,
		cancel:   cancel,
		started:  started,
		overflow: overflow,
	}
}

// Return records that token's attempt returned. Finish then measures the
// attempt's latency, and decides whether its caller owns a failure, as of this
// moment rather than its own, so that work done between the two, such as
// classifying an Engine call's error, counts for neither.
func (observer *Observer) Return(token Token) Token {
	if observer == nil || token.target == nil {
		return token
	}

	token.returned = observer.now()
	token.returnedErr = token.callerErr()
	token.stamped = true
	return token
}

// Discard ends token's attempt without recording it, for an attempt whose
// result Curo failed to record or that returned no result because it
// panicked. A breaker probe that Admit started settles as inconclusive, so the
// breaker reopens for the base cooldown instead of waiting for the probe's
// lease to expire and counting it as failed.
func (observer *Observer) Discard(token Token) {
	if observer == nil || token.target == nil || token.probe == 0 {
		return
	}

	token.target.discard(observer.tick(observer.now()), token.probe)
}

// Finish records one completed initial transport attempt.
//
// Each recorded attempt on a regular target funds that target's retry budget
// and the instance retry budget. Overflow attempts never fund retries. Finish
// also settles a breaker probe that Admit started.
//
// An attempt that an adaptive timeout ended is recorded as a latency sample
// at the timeout. It is a dependency failure only when the timeout reached
// the maximum of the target's bounds. Below that maximum it is only a latency
// sample, which does not count as evidence about the dependency's health.
func (observer *Observer) Finish(token Token, result Result) Completion {
	if observer == nil || token.target == nil {
		return Completion{}
	}

	finished, callerErr := token.returned, token.returnedErr
	if !token.stamped {
		finished, callerErr = observer.now(), token.callerErr()
	}
	tick := observer.tick(finished)
	latency := max(finished.Sub(token.started), 0)

	var value observation
	switch {
	case result.Timeout > 0:
		value = censored(result.Timeout, token.target.timeouts)
	case result.Outcome != OutcomeUnclassified:
		value = classifyOutcome(result, callerErr, latency)
	default:
		value = classify(result, callerErr, latency)
	}
	recorded, trip := token.target.complete(
		tick,
		value,
		token.probe,
		token.admitted,
	)
	completion := Completion{
		Latency:           latency,
		DependencyFailure: value.signal == diagnosisDependencyFailure,
		Trip:              trip,
	}
	if !recorded {
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

// RetryPermitted reports whether token may start a retry. The token must not
// be a breaker probe, its target must be live with a closed dependency
// breaker, and the target's published plan must be an unexpired
// current-version plan containing the Retry candidate.
//
// Overflow tokens, retired targets, and expired or older plans are never
// permitted. RetryPermitted never evaluates a diagnosis.
func (observer *Observer) RetryPermitted(token Token) bool {
	if observer == nil ||
		token.target == nil ||
		token.overflow ||
		token.probe != 0 ||
		token.target.engaged.Load() {
		return false
	}

	return token.target.retryPermitted(observer.tick(observer.now()))
}

// Timeout returns the adaptive timeout for token's request, or zero when the
// request has none.
//
// Only a request to a live regular target whose published plan is an
// unexpired current-version plan containing CandidateTimeout has a timeout,
// and a breaker probe never has one. When the published plan selected a
// timeout but expired, Timeout evaluates the target again, so a timeout does
// not lapse while its latency evidence is retained. The next relevant result
// still evaluates the target, as it would have without that evaluation.
func (observer *Observer) Timeout(token Token) time.Duration {
	if observer == nil ||
		token.target == nil ||
		token.overflow ||
		token.probe != 0 {
		return 0
	}

	published := token.target.timeout.Load()
	if published == nil {
		return 0
	}

	tick := observer.tick(token.started)
	if tick < published.expiresAt {
		return published.limit
	}

	return token.target.timeoutAt(tick)
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
