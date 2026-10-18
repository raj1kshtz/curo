package curo

// Stats is a privacy-safe aggregate snapshot of one Transport or Engine.
//
// Stats never includes target identifiers, URLs, headers, bodies, or raw
// errors. Fields are sampled independently while requests or calls are in
// flight. OverflowRequests never exceeds ObservedRequests, RetrySuccesses never
// exceeds RetryAttempts, RetryAttempts plus RetryBudgetDenials never exceeds
// ObservedRequests, ShadowTimeouts never exceeds ObservedRequests, and
// SelfDisabled implies at least three InternalFailures.
type Stats struct {
	// ObservedRequests is the number of completed initial attempts recorded in
	// Observe or Enforce mode. Retry attempts, and requests and calls that
	// failed fast with ErrBreakerOpen, are not included.
	ObservedRequests uint64

	// TrackedTargets is the current number of admitted regular targets. It does
	// not include the fixed overflow aggregate.
	TrackedTargets uint64

	// OverflowRequests is the number of observations assigned to the bounded
	// non-actionable overflow aggregate.
	OverflowRequests uint64

	// RetryAttempts is the number of retry attempts Curo started in Enforce
	// mode. Each is one additional base transport call or Engine attempt, and
	// a request or call starts at most one. Retries that the base transport or
	// an attempt performs internally are not counted.
	RetryAttempts uint64

	// RetrySuccesses is the number of retry attempts that succeeded: a base
	// transport call that returned a 2xx or 3xx response without an error,
	// though reading its body can still fail, or an Engine attempt that
	// returned nil or an error classified as OutcomeSuccess.
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

	// BreakerProbes is the number of requests and calls that open dependency
	// breakers let through as probes after a cooldown.
	BreakerProbes uint64

	// BreakerRejections is the number of requests and calls that failed fast
	// with ErrBreakerOpen without calling the base transport or running the
	// attempt.
	BreakerRejections uint64

	// Timeouts is the number of attempts, including retry attempts, that
	// Enforce ended with ErrTimeout because the base transport did not return
	// response headers, or an Engine attempt did not return, within the
	// adaptive timeout.
	Timeouts uint64

	// ShadowTimeouts is the number of Observe initial attempts that took
	// longer than the adaptive timeout Enforce would have applied to them.
	// Observe never ends an attempt.
	ShadowTimeouts uint64

	// InternalFailures is the number of contained Curo-owned failures in the
	// stages of requests and calls, and in Report. [WithLogger] offers them to
	// its logger, within the limits that it describes.
	InternalFailures uint64

	// SelfDisabled reports whether repeated internal failures permanently
	// selected direct pass-through for this Transport or Engine. [WithLogger]
	// offers the change to its logger once.
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

	return t.stats()
}

func (c *core) stats() Stats {
	selfDisabled := c.guard.Disabled()
	internalFailures := c.guard.Failures()
	retrySuccesses := c.retries.successes.Load()
	retryAttempts := c.retries.attempts.Load()
	retryBudgetDenials := c.retries.budgetDenials.Load()
	shadowTimeouts := c.timeouts.shadow.Load()
	observation := c.observer.Stats()

	return Stats{
		ObservedRequests:   observation.ObservedRequests,
		TrackedTargets:     observation.TrackedTargets,
		OverflowRequests:   observation.OverflowRequests,
		RetryAttempts:      retryAttempts,
		RetrySuccesses:     retrySuccesses,
		RetryBudgetDenials: retryBudgetDenials,
		BreakerOpens:       c.breakers.opens.Load(),
		BreakerProbes:      c.breakers.probes.Load(),
		BreakerRejections:  c.breakers.rejections.Load(),
		Timeouts:           c.timeouts.expired.Load(),
		ShadowTimeouts:     shadowTimeouts,
		InternalFailures:   internalFailures,
		SelfDisabled:       selfDisabled,
	}
}
