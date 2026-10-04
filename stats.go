package curo

// Stats is a privacy-safe aggregate snapshot of one Transport.
//
// Stats never includes target identifiers, URLs, headers, bodies, or raw
// errors. Fields are sampled independently while requests are in flight.
// OverflowRequests never exceeds ObservedRequests, RetrySuccesses never
// exceeds RetryAttempts, RetryAttempts plus RetryBudgetDenials never exceeds
// ObservedRequests, ShadowTimeouts never exceeds ObservedRequests, and
// SelfDisabled implies at least three InternalFailures.
type Stats struct {
	// ObservedRequests is the number of completed initial attempts recorded in
	// Observe or Enforce mode. Retry attempts and requests that failed fast
	// with ErrBreakerOpen are not included.
	ObservedRequests uint64

	// TrackedTargets is the current number of admitted regular targets. It does
	// not include the fixed overflow aggregate.
	TrackedTargets uint64

	// OverflowRequests is the number of observations assigned to the bounded
	// non-actionable overflow aggregate.
	OverflowRequests uint64

	// RetryAttempts is the number of retry attempts Curo started in Enforce
	// mode. Each is one additional base transport call, and a request starts
	// at most one. Retries the base transport performs internally are not
	// counted.
	RetryAttempts uint64

	// RetrySuccesses is the number of retry attempts whose base transport call
	// returned a 2xx or 3xx response without an error. Reading that response
	// body can still fail.
	RetrySuccesses uint64

	// RetryBudgetDenials is the number of Enforce retries that met every other
	// condition after the initial attempt but did not start because the target
	// or instance retry budget was empty. Observe funds retry budgets but never
	// reserves from them, so it records no denials.
	RetryBudgetDenials uint64

	// BreakerOpens is the number of times Enforce opened a closed dependency
	// breaker. A failed probe keeps a breaker open without counting another
	// open.
	BreakerOpens uint64

	// BreakerProbes is the number of requests that open dependency breakers
	// sent to the base transport as probes after a cooldown.
	BreakerProbes uint64

	// BreakerRejections is the number of requests that failed fast with
	// ErrBreakerOpen without calling the base transport.
	BreakerRejections uint64

	// Timeouts is the number of attempts, including retry attempts, that
	// Enforce ended with ErrTimeout because the base transport did not return
	// response headers within the adaptive timeout.
	Timeouts uint64

	// ShadowTimeouts is the number of Observe initial attempts that took
	// longer than the adaptive timeout Enforce would have applied to them.
	// Observe never ends an attempt.
	ShadowTimeouts uint64

	// InternalFailures is the number of contained Curo-owned failures in
	// request stages and Report. [WithLogger] offers them to its logger,
	// within the limits that it describes.
	InternalFailures uint64

	// SelfDisabled reports whether repeated internal failures permanently
	// selected direct pass-through for this Transport. [WithLogger] offers the
	// change to its logger once.
	SelfDisabled bool
}

// Stats returns a concurrency-safe aggregate snapshot.
//
// The zero value and a nil *Transport return an empty snapshot. Stats remains
// readable after Close.
func (t *Transport) Stats() Stats {
	if t == nil || t.base == nil {
		return Stats{}
	}

	selfDisabled := t.guard.Disabled()
	internalFailures := t.guard.Failures()
	retrySuccesses := t.retries.successes.Load()
	retryAttempts := t.retries.attempts.Load()
	retryBudgetDenials := t.retries.budgetDenials.Load()
	shadowTimeouts := t.timeouts.shadow.Load()
	observation := t.observer.Stats()

	return Stats{
		ObservedRequests:   observation.ObservedRequests,
		TrackedTargets:     observation.TrackedTargets,
		OverflowRequests:   observation.OverflowRequests,
		RetryAttempts:      retryAttempts,
		RetrySuccesses:     retrySuccesses,
		RetryBudgetDenials: retryBudgetDenials,
		BreakerOpens:       t.breakers.opens.Load(),
		BreakerProbes:      t.breakers.probes.Load(),
		BreakerRejections:  t.breakers.rejections.Load(),
		Timeouts:           t.timeouts.expired.Load(),
		ShadowTimeouts:     shadowTimeouts,
		InternalFailures:   internalFailures,
		SelfDisabled:       selfDisabled,
	}
}
