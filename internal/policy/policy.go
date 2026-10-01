// Package policy maps diagnoses to bounded control candidates.
//
// Candidates describe controls that a later enforcement stage may consider.
// This package never applies a control.
package policy

import "github.com/raj1kshtz/curo/internal/diagnose"

// Version identifies the candidate rule set recorded with each Plan.
const Version uint32 = 1

// Candidate is a set of controls eligible for later enforcement.
type Candidate uint8

const (
	// CandidateRetry marks transient failures as eligible for budgeted
	// retries of replay-safe requests.
	CandidateRetry Candidate = 1 << iota

	// CandidateBreakerOpen marks dependency failure or sustained rate limiting
	// as eligible for opening a dependency breaker.
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
	case diagnose.ClassDependencyDown, diagnose.ClassSaturation:
		return CandidateBreakerOpen
	default:
		return 0
	}
}
