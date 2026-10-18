package curo

import (
	"context"
	"time"

	"github.com/raj1kshtz/curo/internal/breaker"
	"github.com/raj1kshtz/curo/internal/guard"
	"github.com/raj1kshtz/curo/internal/observe"
	"github.com/raj1kshtz/curo/internal/timeout"
)

// Engine applies Curo to calls that do not go through net/http, such as gRPC
// calls, database queries, or the requests of a client library. Each call
// describes its target and passes the attempt to make. The Engine observes
// the attempt and, in Enforce mode, can fail the call fast while the target's
// dependency breaker is open, end a read-only attempt with an adaptive
// timeout, or retry a replay-safe call, as a Transport does for an HTTP
// request.
//
// An Engine has its own targets, evidence, and budgets, which no Transport or
// other Engine shares. It is safe for concurrent use, and must not be copied
// after first use.
//
// A nil or zero Engine runs every attempt directly, without Curo.
type Engine struct {
	core
}

// Call describes one call to a dependency.
//
// A Call whose target is invalid, such as the zero Call, counts toward the
// overflow aggregate, which Enforce never acts on.
type Call struct {
	// Classify classifies an error that an attempt returned. In Observe and
	// Enforce mode, Do calls it at most once for each attempt that returns a
	// non-nil error, after the attempt returns and before Do returns, but not
	// for an attempt that an adaptive timeout ended. It can run concurrently
	// with other calls, so it should be fast and free of side effects. A
	// panic in Classify is contained and counts as an internal failure, which
	// can disable the Engine, and Do then returns the attempt's error.
	//
	// A nil Classify classifies every error as an OutcomeTransportFailure that
	// is not retryable. Every such error then counts against the dependency,
	// even one that the application caused, so a call that can fail for
	// reasons of its own needs a Classify that tells them apart.
	Classify func(error) Classification

	// Scheme names the call's protocol, such as "grpc" or "postgres", and is
	// compared without regard to case. It must be a URI scheme of at most 32
	// bytes: a letter followed by letters, digits, "+", "-", or ".".
	Scheme string

	// Host is the DNS name or IP address of the dependency, without a port.
	Host string

	// Port is the dependency's port. It must not be zero, except that zero
	// selects the default port, 80 or 443, of http and https. A call without
	// a port counts toward the overflow aggregate.
	Port uint16

	// ReadOnly reports that the call changes nothing at the dependency, so an
	// adaptive timeout may end it. A call that is not read-only is never ended
	// early, because it might take effect anyway. Report shows read-only
	// calls with MethodRead and other calls with MethodWrite, as separate
	// targets.
	ReadOnly bool

	// ReplaySafe reports that the attempt may run a second time. Enforce
	// retries only a replay-safe call whose failure Classify marked
	// Retryable.
	ReplaySafe bool
}

// Classification is how an attempt that returned an error ended, as a Call's
// Classify reports it.
type Classification struct {
	// Outcome is how the attempt ended. OutcomeUnspecified, and any value
	// that is not an Outcome constant, counts as an OutcomeTransportFailure
	// that is not retryable.
	Outcome Outcome

	// Retryable reports that a retry might succeed where the attempt failed.
	// It applies only to OutcomeDependencyFailure and OutcomeTransportFailure.
	Retryable bool
}

// Outcome is how an attempt ended.
type Outcome uint8

const (
	// OutcomeUnspecified is the zero Outcome. An attempt classified with it
	// counts as an OutcomeTransportFailure that is not retryable.
	OutcomeUnspecified Outcome = iota

	// OutcomeSuccess means that the dependency served the call. Classify can
	// report it for an error that is a normal answer, such as a lookup that
	// found nothing.
	OutcomeSuccess

	// OutcomeClientFailure means that the dependency refused the call because
	// of the call itself, such as an invalid argument or a missing
	// permission. It shows that the dependency is serving calls.
	OutcomeClientFailure

	// OutcomeRateLimited means that the dependency refused the call because
	// of a rate limit or quota.
	OutcomeRateLimited

	// OutcomeDependencyFailure means that the dependency answered with a
	// failure of its own, such as an internal error.
	OutcomeDependencyFailure

	// OutcomeTransportFailure means that the call got no answer from the
	// dependency, such as when a connection failed or a deadline other than
	// the caller's expired.
	OutcomeTransportFailure
)

// String returns the outcome's name without its prefix, such as
// "DependencyFailure". A value that is not an Outcome constant returns
// "Outcome(N)", where N is its number.
func (outcome Outcome) String() string {
	switch outcome {
	case OutcomeUnspecified:
		return "Unspecified"
	case OutcomeSuccess:
		return "Success"
	case OutcomeClientFailure:
		return "ClientFailure"
	case OutcomeRateLimited:
		return "RateLimited"
	case OutcomeDependencyFailure:
		return "DependencyFailure"
	case OutcomeTransportFailure:
		return "TransportFailure"
	default:
		return unknownName("Outcome", uint64(outcome))
	}
}

// NewEngine constructs an Engine.
func NewEngine(options ...Option) (*Engine, error) {
	cfg, err := applyOptions(options)
	if err != nil {
		return nil, err
	}

	engine := &Engine{}
	engine.init(cfg)

	return engine, nil
}

// Do runs attempt, the work of call, unless a dependency breaker rejects it,
// and returns attempt's error. A nil attempt returns an error.
//
// Off, Observe, and Enforce calls that are neither rejected by a breaker nor
// retried run attempt exactly once, with ctx unless an adaptive timeout
// applies. Observe and Enforce record how the attempt ended: its latency,
// measured until attempt returns, and its outcome, which is OutcomeSuccess
// for a nil error and the call's classification of any other error. A call
// whose ctx was canceled, or whose ctx deadline expired, before attempt
// returned an error is the caller's, whatever Classify reports.
//
// In Enforce mode, while the dependency breaker of call's target is open, Do
// returns ErrBreakerOpen without running attempt. After a cooldown, one call
// at a time runs as a probe instead, and a probe is never retried.
//
// In Enforce mode, a read-only call to a target whose decision selects
// CandidateTimeout, other than a probe, has an adaptive timeout unless the
// deadline of ctx comes first. attempt then runs with a context derived from
// ctx, which Curo cancels when the timeout expires or attempt returns, and Do
// waits for attempt to return. If the timeout expired first, Do returns
// ErrTimeout and does not retry, even if attempt succeeded.
//
// attempt must therefore finish with the context it receives before it
// returns. A result that keeps using that context, such as the rows of a
// database query or the messages of a stream, must be read in full within
// attempt.
//
// In Enforce mode, a replay-safe call whose initial attempt failed in a way
// that Classify marked Retryable may run attempt a second time, after a short
// backoff that a done ctx stops, if the target's decision selects
// CandidateRetry and its retry budget allows. Do then returns the retry's
// error.
//
// Do does not recover a panic in attempt, which propagates unchanged. A nil
// ctx, a nil or zero Engine, Off mode, a closed Engine, and a self-disabled
// Engine all run attempt once with ctx, without Curo.
func (e *Engine) Do(
	ctx context.Context,
	call Call,
	attempt func(context.Context) error,
) error {
	if attempt == nil {
		return errNilAttempt
	}
	if e == nil || e.guard == nil || ctx == nil {
		return attempt(ctx)
	}

	stages := e.stages
	runPostflight := false
	var (
		authority uint64
		state     requestState
	)
	if stages != nil && !e.closed.Load() {
		authority = e.authority.Load()
		if authorityMode(authority) != Off && !e.guard.Disabled() {
			runPostflight = e.guard.Run(guard.StagePreflight, func() error {
				var err error
				state, err = stages.preflight(snapshotCall(ctx, call), authority)
				return err
			}) == guard.Completed
		}
	}

	var limit time.Duration
	if runPostflight {
		switch state.admission {
		case breaker.Reject:
			e.breakers.rejections.Add(1)
			return ErrBreakerOpen
		case breaker.Probe:
			e.breakers.probes.Add(1)
		}
		if authorityMode(authority) == Enforce {
			limit = state.timeout
		}
	}

	returned := false
	if runPostflight && state.admission == breaker.Probe {
		// A probe whose attempt panics returns no result, so settle it as
		// inconclusive. The panic keeps propagating: discard contains only a
		// failure of its own.
		defer func() {
			if !returned {
				e.discard(state)
			}
		}()
	}
	cut, err := e.invoke(ctx, attempt, limit, authority)
	returned = true
	if !runPostflight {
		return err
	}

	var grant retryGrant
	outcome := e.guard.Run(guard.StagePostflight, func() error {
		result := attemptResult{err: err, classify: call.Classify, call: true}
		if cut {
			result.timeout = limit
		}

		var stageErr error
		grant, stageErr = stages.postflight(state, result)
		return stageErr
	})
	if outcome != guard.Completed {
		e.discard(state)
	}
	if !grant.lease.Held() {
		return err
	}
	if outcome != guard.Completed || authorityMode(authority) != Enforce {
		grant.lease.Cancel()
		return err
	}

	return e.retry(ctx, call, attempt, stages, state, authority, &grant, err)
}

// retry starts at most one retry attempt for grant. Until the final commit
// check passes, every exit cancels the reservation and returns the first
// attempt's error.
func (e *Engine) retry(
	ctx context.Context,
	call Call,
	attempt func(context.Context) error,
	stages requestStages,
	state requestState,
	authority uint64,
	grant *retryGrant,
	err error,
) error {
	permitted, limit := false, time.Duration(0)
	if e.backoff(ctx, authority, grant.delay) {
		permitted, limit = e.commitRetry(
			ctx,
			stages,
			state,
			authority,
			grant.firstAttempt,
		)
	}
	if !permitted {
		grant.lease.Cancel()
		return err
	}

	grant.lease.Commit()
	e.retries.attempts.Add(1)

	cut, retryErr := e.invoke(ctx, attempt, limit, authority)
	if !cut && e.succeeded(call.Classify, retryErr) {
		e.retries.successes.Add(1)
	}

	return retryErr
}

// backoff waits delay before a retry, and reports whether the retry may still
// start.
func (e *Engine) backoff(
	ctx context.Context,
	authority uint64,
	delay time.Duration,
) bool {
	ready := false
	_ = e.guard.Run(guard.StageRetry, func() error {
		ready = e.authorized(authority) && e.wait(ctx, nil, delay)
		return nil
	})

	return ready
}

// succeeded reports whether a retry's attempt, which ended with err, served
// the call.
func (e *Engine) succeeded(
	classify func(error) Classification,
	err error,
) bool {
	if err == nil {
		return true
	}

	success := false
	_ = e.guard.Contain(guard.StageRetry, func() error {
		outcome, _ := classifyCall(classify, err)
		success = outcome == observe.OutcomeSuccess
		return nil
	})

	return success
}

// invoke runs one attempt. A positive limit bounds it with an adaptive
// timeout. invoke reports whether the timeout ended the attempt, and then
// returns ErrTimeout instead of the attempt's error.
func (e *Engine) invoke(
	ctx context.Context,
	attempt func(context.Context) error,
	limit time.Duration,
	authority uint64,
) (bool, error) {
	var timed *timeout.Attempt
	if limit > 0 {
		timed = e.startTimeout(ctx, nil, limit, authority)
	}
	if timed == nil {
		return false, attempt(ctx)
	}

	done := false
	defer func() {
		// A panicking attempt returns no result to settle. Its panic keeps
		// propagating: abandon contains only a failure of Abandon.
		if !done {
			e.abandon(timed)
		}
	}()
	//nolint:contextcheck // The timed context derives from ctx.
	err := attempt(timed.Context())
	done = true

	if !e.returned(timed) {
		e.timeouts.expired.Add(1)
		return true, ErrTimeout
	}

	_ = e.guard.Contain(guard.StageTimeout, func() error {
		timed.Release()
		return nil
	})

	return false, err
}

// classifyCall classifies how an Engine call's attempt ended with err, using
// the call's classify. It returns the observer's outcome and whether a retry
// may follow a failure.
func classifyCall(
	classify func(error) Classification,
	err error,
) (observe.Outcome, bool) {
	if err == nil {
		return observe.OutcomeSuccess, false
	}

	var classification Classification
	if classify != nil {
		classification = classify(err)
	}

	switch classification.Outcome {
	case OutcomeSuccess:
		return observe.OutcomeSuccess, false
	case OutcomeClientFailure:
		return observe.OutcomeClientFailure, false
	case OutcomeRateLimited:
		return observe.OutcomeRateLimited, false
	case OutcomeDependencyFailure:
		return observe.OutcomeDependencyFailure, classification.Retryable
	case OutcomeTransportFailure:
		return observe.OutcomeTransportFailure, classification.Retryable
	default:
		return observe.OutcomeTransportFailure, false
	}
}

func snapshotCall(ctx context.Context, call Call) requestSnapshot {
	method := observe.MethodWrite
	if call.ReadOnly {
		method = observe.MethodRead
	}

	return requestSnapshot{
		requestContext: ctx,
		call: observe.Call{
			Context:  ctx,
			Scheme:   call.Scheme,
			Hostname: call.Host,
			Port:     call.Port,
			Method:   method,
		},
		isCall:     true,
		replaySafe: call.ReplaySafe,
	}
}

// Mode returns the configured operating mode.
//
// A nil or zero Engine reports Off.
func (e *Engine) Mode() Mode {
	if e == nil {
		return Off
	}

	return authorityMode(e.authority.Load())
}

// SetMode changes the operating mode for calls that start after the change.
//
// A call never gains authority after it starts. Any mode change also
// withdraws a retry that an earlier Enforce call has not started yet, and an
// adaptive timeout that has not expired yet, even if Enforce is restored
// before either would act. A nil or zero Engine returns
// ErrUninitializedEngine.
func (e *Engine) SetMode(mode Mode) error {
	if e == nil || e.guard == nil {
		return ErrUninitializedEngine
	}

	return e.setMode(mode)
}

// Close stops the Engine from acting on calls.
//
// Close is idempotent. Calls made after Close run their attempts directly,
// and retries that have not started and adaptive timeouts that have not
// expired are withdrawn. Close does not discard the bounded observation
// snapshot. A nil or zero Engine returns ErrUninitializedEngine.
func (e *Engine) Close() error {
	if e == nil || e.guard == nil {
		return ErrUninitializedEngine
	}

	e.closed.Store(true)
	return nil
}

// Report returns a detached snapshot of the Engine's published decisions and
// recent changes, as [Transport.Report] describes for a Transport. Decisions
// are produced while Observe or Enforce calls complete.
//
// A nil or zero Engine returns an empty Report. Report remains readable after
// Close, in Off mode, and after self-disable.
func (e *Engine) Report() Report {
	if e == nil || e.guard == nil {
		return Report{}
	}

	return e.report()
}

// Stats returns a point-in-time snapshot of the Engine's counters.
//
// A nil or zero Engine returns zero Stats. Stats remains readable after Close.
func (e *Engine) Stats() Stats {
	if e == nil || e.guard == nil {
		return Stats{}
	}

	return e.stats()
}
