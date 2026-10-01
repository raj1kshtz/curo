package curo

// Stats is a privacy-safe aggregate snapshot of one Transport.
//
// Stats never includes target identifiers, URLs, headers, bodies, or raw
// errors. Fields are sampled independently while requests are in flight.
// OverflowRequests never exceeds ObservedRequests, and SelfDisabled implies at
// least three InternalFailures.
type Stats struct {
	// ObservedRequests is the number of completed initial attempts recorded in
	// Observe or Enforce mode.
	ObservedRequests uint64

	// TrackedTargets is the current number of admitted regular targets. It does
	// not include the fixed overflow aggregate.
	TrackedTargets uint64

	// OverflowRequests is the number of observations assigned to the bounded
	// non-actionable overflow aggregate.
	OverflowRequests uint64

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
	observation := t.observer.Stats()

	return Stats{
		ObservedRequests: observation.ObservedRequests,
		TrackedTargets:   observation.TrackedTargets,
		OverflowRequests: observation.OverflowRequests,
		InternalFailures: internalFailures,
		SelfDisabled:     selfDisabled,
	}
}
