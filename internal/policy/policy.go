// Package policy maps diagnoses to bounded control candidates.
//
// Candidates describe controls that a later enforcement stage may consider.
// This package never applies a control.
package policy

import "github.com/raj1kshtz/curo/internal/diagnose"

// Version identifies the candidate rule set recorded with each Plan.
//
// Version 2 stopped selecting CandidateBreakerOpen for Saturation, which is
// now reported without a candidate.
const Version uint32 = 2

// Candidate is a set of controls eligible for later enforcement.
type Candidate uint8

const (
	// CandidateRetry marks transient failures as eligible for budgeted
	// retries of replay-safe requests.
	CandidateRetry Candidate = 1 << iota

	// CandidateBreakerOpen marks a dependency outage as eligible for opening
	// the target's dependency breaker.
	CandidateBreakerOpen
)

// Reason explains a policy outcome that diagnosis reasons do not cover.
type Reason uint8

const (
	// ReasonNone means the plan needs no policy-specific explanation.
	ReasonNone Reason = iota

	// ReasonReadinessRequired means the diagnosis maps to a candidate, but
	// readiness does not permit an autonomous control.
	ReasonReadinessRequired
)

// Plan is an immutable, expiring set of control candidates.
type Plan struct {
	ExpiresAt  int64
	Version    uint32
	Candidates Candidate
	Reason     Reason
}

// Evaluate applies the closed diagnosis-to-candidate rules.
//
// Only Ready diagnoses produce candidates. Plans expire with the diagnosis
// they were derived from.
func Evaluate(result diagnose.Result) Plan {
	plan := Plan{
		ExpiresAt: result.ExpiresAt,
		Version:   Version,
	}

	candidates := candidatesFor(result.Class)
	if candidates == 0 {
		return plan
	}
	if result.Readiness != diagnose.ReadinessReady {
		plan.Reason = ReasonReadinessRequired
		return plan
	}

	plan.Candidates = candidates
	return plan
}

func candidatesFor(class diagnose.Class) Candidate {
	switch class {
	case diagnose.ClassTransient:
		return CandidateRetry
	case diagnose.ClassDependencyDown:
		return CandidateBreakerOpen
	default:
		return 0
	}
}
