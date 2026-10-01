package curo

// Stats is a privacy-safe aggregate snapshot of one Transport.
//
// Stats never includes target identifiers, URLs, headers, bodies, or raw
// errors. Fields are sampled independently while requests are in flight.
// OverflowRequests never exceeds ObservedRequests, RetrySuccesses never
// exceeds RetryAttempts, RetryAttempts plus RetryBudgetDenials never exceeds
// ObservedRequests, and SelfDisabled implies at least three InternalFailures.
type Stats struct {
	// ObservedRequests is the number of completed initial attempts recorded in
	// Observe or Enforce mode. Retry attempts are not included.
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

	// InternalFailures is the number of contained Curo-owned failures in
	// request stages and Report.
	InternalFailures uint64

	// SelfDisabled reports whether repeated internal failures permanently
	// selected direct pass-through for this Transport.
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
	observation := t.observer.Stats()

	return Stats{
		ObservedRequests:   observation.ObservedRequests,
		TrackedTargets:     observation.TrackedTargets,
		OverflowRequests:   observation.OverflowRequests,
		RetryAttempts:      retryAttempts,
		RetrySuccesses:     retrySuccesses,
		RetryBudgetDenials: retryBudgetDenials,
		InternalFailures:   internalFailures,
		SelfDisabled:       selfDisabled,
	}
}
