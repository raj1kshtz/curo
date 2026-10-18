package curo

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/raj1kshtz/curo/internal/breaker"
	"github.com/raj1kshtz/curo/internal/guard"
	"github.com/raj1kshtz/curo/internal/observe"
	"github.com/raj1kshtz/curo/internal/retry"
	"github.com/raj1kshtz/curo/internal/timeout"
)

// core is the state that each Transport and each Engine owns: the failure
// guard, the observer and the request stages that use it, the counters behind
// Stats, and the authority word.
type core struct {
	guard    *guard.Guard
	observer *observe.Observer
	stages   requestStages
	reports  func() observe.Report
	wait     func(context.Context, <-chan struct{}, time.Duration) bool
	after    timeout.AfterFunc

	retries   retryCounters
	breakers  breakerCounters
	timeouts  timeoutCounters
	authority atomic.Uint64
	closed    atomic.Bool
}

// The authority word holds the operating mode in its low 32 bits and a
// generation in its high 32 bits. Every mode change advances the generation,
// so a pending retry detects any change made after its request started, even
// one that later restored Enforce.
const (
	authorityGenerationShift = 32
	authorityModeMask        = 1<<authorityGenerationShift - 1
)

type requestStages interface {
	preflight(requestSnapshot, uint64) (requestState, error)
	postflight(requestState, attemptResult) (retryGrant, error)
	confirmRetry(requestState) bool
}

type requestState struct {
	requestContext context.Context
	cancel         <-chan struct{}
	observation    observe.Token
	authority      uint64

	// timeout is the adaptive timeout for the request's attempts, or zero.
	// Enforce applies it. Observe compares the initial attempt's latency
	// with it.
	timeout    time.Duration
	admission  breaker.Admission
	replaySafe bool
}

// requestSnapshot is the bounded metadata that preflight needs about an HTTP
// request or, when isCall is set, about an Engine call, whose target is call.
type requestSnapshot struct {
	requestContext context.Context
	cancel         <-chan struct{}
	method         string
	scheme         string
	hostname       string
	port           string
	call           observe.Call
	isCall         bool
	replaySafe     bool
}

type attemptResult struct {
	err    error
	header http.Header

	// classify is the Classify function of an Engine call, whose attempt
	// this is when call is set. The observer classifies an HTTP attempt from
	// err and the response instead.
	classify   func(error) Classification
	statusCode int

	// timeout is the adaptive timeout that ended the attempt, or zero.
	timeout     time.Duration
	call        bool
	hasResponse bool
}

// retryable reports whether the attempt failed in a way that a retry might
// fix. dependencyFailure is whether the observer attributed the failure to
// the dependency, and retryHint is whether an Engine call's classification
// marked the failure retryable.
func (result attemptResult) retryable(dependencyFailure, retryHint bool) bool {
	if result.call {
		return dependencyFailure && retryHint
	}

	return retry.Retryable(
		dependencyFailure,
		result.err,
		result.statusCode,
		result.header,
	)
}

// retryGrant holds the budget reservation for one retry attempt that has not
// started yet, the backoff before it, and the first attempt's latency.
type retryGrant struct {
	lease        retry.Lease
	delay        time.Duration
	firstAttempt time.Duration
}

type retryCounters struct {
	attempts      atomic.Uint64
	successes     atomic.Uint64
	budgetDenials atomic.Uint64
}

type breakerCounters struct {
	opens      atomic.Uint64
	probes     atomic.Uint64
	rejections atomic.Uint64
}

type timeoutCounters struct {
	expired atomic.Uint64
	shadow  atomic.Uint64
}

type adaptiveStages struct {
	observer   *observe.Observer
	retries    *retryCounters
	breakers   *breakerCounters
	timeouts   *timeoutCounters
	authorized func(uint64) bool
	jitter     func() time.Duration
}

func newAdaptiveStages(
	observer *observe.Observer,
	retries *retryCounters,
	breakers *breakerCounters,
	timeouts *timeoutCounters,
	authorized func(uint64) bool,
) adaptiveStages {
	return adaptiveStages{
		observer:   observer,
		retries:    retries,
		breakers:   breakers,
		timeouts:   timeouts,
		authorized: authorized,
		jitter:     retry.Jitter,
	}
}

// preflight resolves the request's target. In Enforce mode it also asks the
// target's dependency breaker to admit the request. A request that passes
// gets the target's adaptive timeout, unless the caller already canceled it
// or its deadline is no later than the timeout would be.
func (stages adaptiveStages) preflight(
	snapshot requestSnapshot,
	authority uint64,
) (requestState, error) {
	var token observe.Token
	if snapshot.isCall {
		token = stages.observer.BeginCall(snapshot.call)
	} else {
		token = stages.observer.Begin(observe.Request{
			Context:  snapshot.requestContext,
			Cancel:   snapshot.cancel,
			Method:   snapshot.method,
			Scheme:   snapshot.scheme,
			Hostname: snapshot.hostname,
			Port:     snapshot.port,
		})
	}
	admission := breaker.Pass
	if authorityMode(authority) == Enforce {
		token, admission = stages.observer.Admit(token)
	}

	state := requestState{
		requestContext: snapshot.requestContext,
		cancel:         snapshot.cancel,
		observation:    token,
		authority:      authority,
		admission:      admission,
		replaySafe:     snapshot.replaySafe,
	}
	if admission == breaker.Pass {
		state.timeout = timeoutWithin(
			snapshot.requestContext,
			snapshot.cancel,
			stages.observer.Timeout(token),
		)
	}

	return state, nil
}

// timeoutWithin returns limit when a request with ctx and cancel can still
// run for longer than limit, and zero otherwise.
func timeoutWithin(
	ctx context.Context,
	cancel <-chan struct{},
	limit time.Duration,
) time.Duration {
	if limit <= 0 ||
		retry.Canceled(cancel) ||
		!retry.DeadlineAllows(ctx, 0, limit) {
		return 0
	}

	return limit
}

// postflight records the initial attempt and settles a breaker probe. In
// Enforce mode, a dependency failure opens the target's dependency breaker
// when the target's plan selects BreakerOpen and the request's authority is
// still current. When every retry condition holds, its last step reserves
// retry budget for one retry. An attempt that the adaptive timeout ended is
// never retried.
func (stages adaptiveStages) postflight(
	state requestState,
	result attemptResult,
) (retryGrant, error) {
	observation := state.observation
	outcome, retryHint := observe.OutcomeUnclassified, false
	if result.call && result.timeout == 0 {
		// Classify runs after the attempt returned, so the observation ends
		// when it returned.
		observation = stages.observer.Return(observation)
		outcome, retryHint = classifyCall(result.classify, result.err)
	}
	completion := stages.observer.Finish(observation, observe.Result{
		Err:         result.err,
		Timeout:     result.timeout,
		StatusCode:  result.statusCode,
		Outcome:     outcome,
		HasResponse: result.hasResponse,
	})
	if authorityMode(state.authority) == Observe &&
		state.timeout > 0 &&
		completion.Recorded &&
		completion.Latency > state.timeout {
		stages.timeouts.shadow.Add(1)
	}
	if completion.Trip &&
		stages.authorized(state.authority) &&
		stages.observer.Trip(state.observation) {
		stages.breakers.opens.Add(1)
	}
	if authorityMode(state.authority) != Enforce ||
		!state.replaySafe ||
		!completion.Recorded ||
		result.timeout > 0 ||
		!result.retryable(completion.DependencyFailure, retryHint) {
		return retryGrant{}, nil
	}

	delay := stages.jitter()
	if retry.Canceled(state.cancel) ||
		!retry.DeadlineAllows(state.requestContext, delay, completion.Latency) {
		return retryGrant{}, nil
	}

	lease, reservation := stages.observer.ReserveRetry(state.observation)
	if reservation == observe.RetryBudgetExhausted {
		stages.retries.budgetDenials.Add(1)
	}

	return retryGrant{
		lease:        lease,
		delay:        delay,
		firstAttempt: completion.Latency,
	}, nil
}

// confirmRetry reports whether the target's published plan still permits the
// retry after the backoff.
func (stages adaptiveStages) confirmRetry(state requestState) bool {
	return stages.observer.RetryPermitted(state.observation)
}

// init prepares c, which nothing uses yet, with cfg.
func (c *core) init(cfg config) {
	observer := observe.New(cfg.timeouts)
	c.guard = guard.New(failureReporter(cfg.logger))
	c.observer = observer
	c.reports = observer.Report
	c.wait = retry.Wait
	c.after = timeout.SystemAfterFunc
	c.stages = newAdaptiveStages(
		observer,
		&c.retries,
		&c.breakers,
		&c.timeouts,
		c.authorized,
	)
	c.authority.Store(uint64(cfg.mode))
}

// authorized reports whether the authority a request started with is still
// current. Any mode change or Close after the request started revokes it.
// Self-disable revokes it through the guard, which skips later stages. Opening
// a dependency breaker, starting a retry, and ending an attempt with an
// adaptive timeout all take effect only after this check passes.
func (c *core) authorized(authority uint64) bool {
	return c.authority.Load() == authority && !c.closed.Load()
}

// setMode changes the operating mode for requests that start after the
// change, and advances the generation so that requests in progress lose
// their authority.
func (c *core) setMode(mode Mode) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if !mode.valid() {
		return invalidModeError(mode)
	}

	for {
		current := c.authority.Load()
		if authorityMode(current) == mode {
			return nil
		}

		generation := current>>authorityGenerationShift + 1
		next := generation<<authorityGenerationShift | uint64(mode)
		if c.authority.CompareAndSwap(current, next) {
			return nil
		}
	}
}

// discard settles the breaker probe of a request that recorded no result,
// because its postflight failed or was skipped or its attempt panicked, as
// inconclusive.
func (c *core) discard(state requestState) {
	_ = c.guard.Contain(guard.StagePostflight, func() error {
		c.observer.Discard(state.observation)
		return nil
	})
}

// commitRetry is the final check before a retry starts. Authority, the caller's
// cancellation and deadline, and the target's published Retry plan must all
// still hold after the backoff. ctx is the request's context. commitRetry also
// returns the request's adaptive timeout for the retry, or zero when the
// caller's deadline now comes first.
func (c *core) commitRetry(
	ctx context.Context,
	stages requestStages,
	state requestState,
	authority uint64,
	firstAttempt time.Duration,
) (bool, time.Duration) {
	var (
		permitted bool
		limit     time.Duration
	)
	_ = c.guard.Run(guard.StageRetry, func() error {
		permitted = c.authorized(authority) &&
			!retry.Canceled(state.cancel) &&
			retry.DeadlineAllows(ctx, 0, firstAttempt) &&
			stages.confirmRetry(state)
		if permitted {
			limit = timeoutWithin(ctx, state.cancel, state.timeout)
		}
		return nil
	})

	return permitted, limit
}

// startTimeout starts an adaptive timeout of limit for an attempt whose
// context derives from ctx, and whose deprecated Cancel channel, if any, is
// cancel. It returns nil when the timeout did not start.
func (c *core) startTimeout(
	ctx context.Context,
	cancel <-chan struct{},
	limit time.Duration,
	authority uint64,
) *timeout.Attempt {
	var attempt *timeout.Attempt
	if c.guard.Run(guard.StageTimeout, func() error {
		armed := timeout.New(ctx, cancel, limit)
		attempt = armed
		armed.Arm(c.after, func() {
			c.expire(armed, authority)
		})
		return nil
	}) == guard.Completed {
		return attempt
	}

	if attempt != nil {
		c.abandon(attempt)
	}
	return nil
}

// expire runs on the timer's goroutine when an attempt's adaptive timeout
// elapses. Like a retry that has not started, the timeout is withdrawn when
// the request's authority was revoked or Curo disabled itself.
func (c *core) expire(attempt *timeout.Attempt, authority uint64) {
	_ = c.guard.Run(guard.StageTimeout, func() error {
		if c.authorized(authority) {
			attempt.Expire()
		}
		return nil
	})
}

// returned settles a timed attempt whose call returned, and reports whether
// it returned before its adaptive timeout ended it.
func (c *core) returned(attempt *timeout.Attempt) bool {
	// Settle decides the attempt before it stops the timer, the only step
	// that can fail, so an attempt whose Settle failed still returned.
	returned := true
	_ = c.guard.Contain(guard.StageTimeout, func() error {
		returned = attempt.Settle()
		return nil
	})

	return returned
}

// abandon settles and releases a timed attempt that returned no result,
// because its call panicked or its timeout failed to start.
func (c *core) abandon(attempt *timeout.Attempt) {
	_ = c.guard.Contain(guard.StageTimeout, func() error {
		attempt.Abandon()
		return nil
	})
}

func authorityMode(authority uint64) Mode {
	return Mode(authority & authorityModeMask)
}
