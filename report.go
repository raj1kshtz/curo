package curo

import (
	"strconv"
	"strings"
	"time"

	"github.com/raj1kshtz/curo/internal/diagnose"
	"github.com/raj1kshtz/curo/internal/guard"
	"github.com/raj1kshtz/curo/internal/observe"
	"github.com/raj1kshtz/curo/internal/policy"
)

// Report is a detached snapshot of published target decisions and recent
// candidate changes for one Transport.
//
// Reports contain normalized target identities and bounded counters. They
// never include paths, queries, fragments, user information, headers, bodies,
// raw errors, or response metadata.
type Report struct {
	// Targets holds the latest decision for each tracked regular target,
	// ordered by scheme, host, port, and method class. It holds at most 128
	// entries. The overflow aggregate is never reported.
	Targets []Decision

	// Changes holds the most recent candidate changes in ascending Sequence
	// order. It holds at most 256 entries.
	Changes []Change
}

// Decision is one published evaluation of a target.
//
// A target without an evaluation has zero times, ReadinessCold, DiagnosisNone,
// no reasons, zero evidence, no candidates, and a zero PolicyVersion.
type Decision struct {
	// Target identifies the normalized dependency.
	Target Target

	// EvaluatedAt is when the evaluation ran.
	EvaluatedAt time.Time

	// ExpiresAt is when the evaluation stops describing current evidence.
	// Report never re-evaluates, so an idle target keeps its last decision
	// after ExpiresAt.
	ExpiresAt time.Time

	// Reasons explains readiness, diagnosis, and candidate selection in
	// evaluation order. It is nil when there is no reason.
	Reasons []Reason

	// Recent summarizes the recent evidence window used by the evaluation.
	Recent Evidence

	// Historical summarizes the non-overlapping historical window used by the
	// evaluation.
	Historical Evidence

	// PolicyVersion identifies the rules that selected Candidates.
	PolicyVersion uint32

	// Readiness reports whether evidence was sufficient for a diagnosis.
	Readiness Readiness

	// Diagnosis is the deterministic diagnosis class.
	Diagnosis Diagnosis

	// Candidates holds controls the policy considers eligible. Enforce mode
	// applies CandidateRetry, subject to replay safety, the caller's deadline,
	// and retry budgets. It applies CandidateBreakerOpen by opening the
	// target's dependency breaker when a request fails because of the
	// dependency.
	Candidates Candidates
}

// Change is a retained evaluation that changed a target's candidates.
//
// A change is recorded when the candidate set changes, or when the same
// non-empty set is selected for a different diagnosis. Renewing an unchanged
// decision is not a change. Changes are not a target lifecycle log: target
// eviction and evidence going stale are not recorded.
type Change struct {
	// Decision is the evaluation that produced the change.
	Decision Decision

	// Sequence orders changes within one Transport. It starts at 1 and
	// increases by one per change, so a gap between reads means older changes
	// were overwritten.
	Sequence uint64
}

// Target is a normalized dependency identity.
//
// Host comes from the request URL and can be influenced by untrusted input,
// for example when an application calls user-supplied URLs. Do not use it as
// an unbounded metric label.
type Target struct {
	// Scheme is "http" or "https".
	Scheme string

	// Host is a lowercase DNS name without a trailing dot, or a canonical IP
	// address.
	Host string

	// Port is the explicit port or the scheme default.
	Port uint16

	// Method is the request method class.
	Method MethodClass
}

// Evidence summarizes the dependency-relevant attempts in one window.
//
// Caller-owned cancellations and caller deadlines are not dependency
// relevant and are excluded from every count.
type Evidence struct {
	// Attempts is the number of dependency-relevant attempts.
	Attempts uint64

	// DependencyFailures counts transport failures and HTTP 5xx responses.
	DependencyFailures uint64

	// RateLimited counts HTTP 429 responses.
	RateLimited uint64

	// ClientFailures counts HTTP 4xx responses other than 429.
	ClientFailures uint64

	// Span is the time between the first and last counted attempts.
	Span time.Duration
}

// MethodClass is a bounded class of HTTP request methods.
type MethodClass uint8

const (
	// MethodRead covers GET, HEAD, OPTIONS, TRACE, and an empty method.
	MethodRead MethodClass = iota

	// MethodWrite covers POST, PUT, PATCH, and DELETE.
	MethodWrite

	// MethodConnect covers CONNECT.
	MethodConnect

	// MethodOther covers every other method, including non-canonical case.
	MethodOther
)

// String returns the method class name.
func (class MethodClass) String() string {
	switch class {
	case MethodRead:
		return "Read"
	case MethodWrite:
		return "Write"
	case MethodConnect:
		return "Connect"
	case MethodOther:
		return "Other"
	default:
		return unknownName("MethodClass", uint64(class))
	}
}

// Readiness reports whether retained evidence can support a diagnosis.
type Readiness uint8

const (
	// ReadinessCold means no dependency-relevant attempt has been observed.
	ReadinessCold Readiness = iota

	// ReadinessWarming means recent evidence exists but is not sufficient.
	ReadinessWarming

	// ReadinessReady means recent or historical evidence is sufficient.
	ReadinessReady

	// ReadinessStale means earlier evidence exists but no recent attempt
	// remains.
	ReadinessStale
)

// String returns the readiness name.
func (readiness Readiness) String() string {
	switch readiness {
	case ReadinessCold:
		return "Cold"
	case ReadinessWarming:
		return "Warming"
	case ReadinessReady:
		return "Ready"
	case ReadinessStale:
		return "Stale"
	default:
		return unknownName("Readiness", uint64(readiness))
	}
}

// Diagnosis is a closed diagnosis class.
type Diagnosis uint8

const (
	// DiagnosisNone means the evidence does not support a diagnosis.
	DiagnosisNone Diagnosis = iota

	// DiagnosisHealthy means ready evidence is within bounds.
	DiagnosisHealthy

	// DiagnosisTransient means isolated dependency failures are present.
	DiagnosisTransient

	// DiagnosisDependencyDown means dependency failures dominate recent
	// attempts.
	DiagnosisDependencyDown

	// DiagnosisSaturation means the dependency is persistently rate limiting.
	DiagnosisSaturation

	// DiagnosisClientError means request-side HTTP failures dominate recent
	// attempts.
	DiagnosisClientError

	// DiagnosisDegrading means recent failure rate or latency moved materially
	// above the historical baseline.
	DiagnosisDegrading
)

// String returns the diagnosis name.
func (diagnosis Diagnosis) String() string {
	switch diagnosis {
	case DiagnosisNone:
		return "None"
	case DiagnosisHealthy:
		return "Healthy"
	case DiagnosisTransient:
		return "Transient"
	case DiagnosisDependencyDown:
		return "DependencyDown"
	case DiagnosisSaturation:
		return "Saturation"
	case DiagnosisClientError:
		return "ClientError"
	case DiagnosisDegrading:
		return "Degrading"
	default:
		return unknownName("Diagnosis", uint64(diagnosis))
	}
}

// Reason is a stable explanation for one part of a Decision.
type Reason uint8

const (
	// ReasonNoEvidence means no dependency-relevant attempt has been observed.
	ReasonNoEvidence Reason = iota + 1

	// ReasonEvidenceStale means no recent dependency-relevant attempt remains.
	ReasonEvidenceStale

	// ReasonInsufficientEvidence means recent evidence has not reached the
	// readiness volume or span.
	ReasonInsufficientEvidence

	// ReasonRecentEvidenceReady means recent volume and span are sufficient.
	ReasonRecentEvidenceReady

	// ReasonHistoricalBaselineReady means historical evidence is sufficient.
	ReasonHistoricalBaselineReady

	// ReasonInsufficientRecentEvidence means recent evidence supports no
	// diagnosis class.
	ReasonInsufficientRecentEvidence

	// ReasonClientFailureRate means request-side HTTP failures dominate recent
	// attempts.
	ReasonClientFailureRate

	// ReasonRateLimitRate means HTTP 429 responses are sustained.
	ReasonRateLimitRate

	// ReasonDependencyFailureRate means dependency failures dominate recent
	// attempts.
	ReasonDependencyFailureRate

	// ReasonFailureRateIncrease means the recent dependency failure rate rose
	// materially above the historical baseline.
	ReasonFailureRateIncrease

	// ReasonLatencyIncrease means recent p95 latency rose materially above the
	// historical baseline.
	ReasonLatencyIncrease

	// ReasonIsolatedDependencyFailure means dependency failures are present
	// without dominating recent attempts.
	ReasonIsolatedDependencyFailure

	// ReasonWithinBaseline means ready evidence shows no failure pattern.
	ReasonWithinBaseline

	// ReasonReadinessRequired means the diagnosis maps to a candidate, but
	// evidence is not Ready, so no candidate was selected.
	ReasonReadinessRequired
)

// String returns the reason name.
func (reason Reason) String() string {
	switch reason {
	case ReasonNoEvidence:
		return "NoEvidence"
	case ReasonEvidenceStale:
		return "EvidenceStale"
	case ReasonInsufficientEvidence:
		return "InsufficientEvidence"
	case ReasonRecentEvidenceReady:
		return "RecentEvidenceReady"
	case ReasonHistoricalBaselineReady:
		return "HistoricalBaselineReady"
	case ReasonInsufficientRecentEvidence:
		return "InsufficientRecentEvidence"
	case ReasonClientFailureRate:
		return "ClientFailureRate"
	case ReasonRateLimitRate:
		return "RateLimitRate"
	case ReasonDependencyFailureRate:
		return "DependencyFailureRate"
	case ReasonFailureRateIncrease:
		return "FailureRateIncrease"
	case ReasonLatencyIncrease:
		return "LatencyIncrease"
	case ReasonIsolatedDependencyFailure:
		return "IsolatedDependencyFailure"
	case ReasonWithinBaseline:
		return "WithinBaseline"
	case ReasonReadinessRequired:
		return "ReadinessRequired"
	default:
		return unknownName("Reason", uint64(reason))
	}
}

// Candidates is a set of controls the policy considers eligible.
type Candidates uint8

const (
	// CandidateRetry marks transient dependency failures as eligible for
	// budgeted retries of replay-safe requests. Enforce mode applies it.
	CandidateRetry Candidates = 1 << iota

	// CandidateBreakerOpen marks a dependency that is down as eligible for
	// opening its dependency breaker. Enforce mode applies it, and requests
	// then fail fast with ErrBreakerOpen until a probe shows that the
	// dependency answers again.
	CandidateBreakerOpen
)

// Has reports whether candidates contains every control in want. It reports
// false when want is empty.
func (candidates Candidates) Has(want Candidates) bool {
	return want != 0 && candidates&want == want
}

// String returns candidate names joined by "|", or "None" for an empty set.
func (candidates Candidates) String() string {
	if candidates == 0 {
		return "None"
	}

	names := make([]string, 0, 3)
	remaining := candidates
	if remaining.Has(CandidateRetry) {
		names = append(names, "Retry")
		remaining &^= CandidateRetry
	}
	if remaining.Has(CandidateBreakerOpen) {
		names = append(names, "BreakerOpen")
		remaining &^= CandidateBreakerOpen
	}
	if remaining != 0 {
		names = append(names, unknownName("Candidates", uint64(remaining)))
	}

	return strings.Join(names, "|")
}

// Report returns a detached snapshot of published decisions and recent
// candidate changes.
//
// Decisions are produced while Observe or Enforce requests complete. Report
// only copies them and never evaluates evidence, so an idle target keeps its
// last decision; compare ExpiresAt with the current time before acting on
// one. Targets are never older than Changes in the same Report for a target
// that is still tracked. A target admitted after an idle target is replaced
// starts without evidence.
//
// Report is safe for concurrent use. The zero value and a nil *Transport
// return an empty Report. Report remains readable after Close, in Off mode,
// and after self-disable. If Curo fails while building a Report, Report
// returns an empty Report and the failure counts toward InternalFailures and
// self-disable.
func (t *Transport) Report() Report {
	if t == nil || t.base == nil || t.reports == nil {
		return Report{}
	}

	var report Report
	if t.guard.Contain(func() error {
		report = newReport(t.reports())
		return nil
	}) != guard.Completed {
		return Report{}
	}

	return report
}

func newReport(snapshot observe.Report) Report {
	var report Report
	if len(snapshot.Targets) > 0 {
		report.Targets = make([]Decision, 0, len(snapshot.Targets))
	}
	for _, decision := range snapshot.Targets {
		report.Targets = append(report.Targets, newDecision(decision))
	}

	if len(snapshot.Changes) > 0 {
		report.Changes = make([]Change, 0, len(snapshot.Changes))
	}
	for _, change := range snapshot.Changes {
		report.Changes = append(report.Changes, Change{
			Decision: newDecision(change.Decision),
			Sequence: change.Sequence,
		})
	}

	return report
}

func newDecision(decision observe.Decision) Decision {
	result := decision.Result
	return Decision{
		Target: Target{
			Scheme: decision.Identity.Scheme,
			Host:   decision.Identity.Host,
			Port:   decision.Identity.Port,
			Method: methodClassFrom(decision.Identity.Method),
		},
		EvaluatedAt:   decision.EvaluatedAt,
		ExpiresAt:     decision.ExpiresAt,
		Reasons:       reasonsFrom(result, decision.Plan.Reason),
		Recent:        evidenceFrom(result.Recent),
		Historical:    evidenceFrom(result.Historical),
		PolicyVersion: decision.Plan.Version,
		Readiness:     readinessFrom(result.Readiness),
		Diagnosis:     diagnosisFrom(result.Class),
		Candidates:    candidatesFrom(decision.Plan.Candidates),
	}
}

func evidenceFrom(evidence diagnose.Evidence) Evidence {
	return Evidence{
		Attempts:           evidence.RelevantAttempts,
		DependencyFailures: evidence.DependencyFailures,
		RateLimited:        evidence.RateLimited,
		ClientFailures:     evidence.ClientFailures,
		Span:               time.Duration(evidence.Span),
	}
}

func methodClassFrom(class observe.MethodClass) MethodClass {
	switch class {
	case observe.MethodRead:
		return MethodRead
	case observe.MethodWrite:
		return MethodWrite
	case observe.MethodConnect:
		return MethodConnect
	default:
		return MethodOther
	}
}

func readinessFrom(readiness diagnose.Readiness) Readiness {
	switch readiness {
	case diagnose.ReadinessWarming:
		return ReadinessWarming
	case diagnose.ReadinessReady:
		return ReadinessReady
	case diagnose.ReadinessStale:
		return ReadinessStale
	default:
		return ReadinessCold
	}
}

func diagnosisFrom(class diagnose.Class) Diagnosis {
	switch class {
	case diagnose.ClassHealthy:
		return DiagnosisHealthy
	case diagnose.ClassTransient:
		return DiagnosisTransient
	case diagnose.ClassDependencyDown:
		return DiagnosisDependencyDown
	case diagnose.ClassSaturation:
		return DiagnosisSaturation
	case diagnose.ClassClientError:
		return DiagnosisClientError
	case diagnose.ClassDegrading:
		return DiagnosisDegrading
	default:
		return DiagnosisNone
	}
}

func reasonsFrom(result diagnose.Result, policyReason policy.Reason) []Reason {
	count := min(int(result.ReasonCount), len(result.Reasons))
	var reasons []Reason
	for _, reason := range result.Reasons[:count] {
		if converted := reasonFrom(reason); converted != 0 {
			reasons = append(reasons, converted)
		}
	}
	if policyReason == policy.ReasonReadinessRequired {
		reasons = append(reasons, ReasonReadinessRequired)
	}

	return reasons
}

func reasonFrom(reason diagnose.Reason) Reason {
	switch reason {
	case diagnose.ReasonNoEvidence:
		return ReasonNoEvidence
	case diagnose.ReasonEvidenceStale:
		return ReasonEvidenceStale
	case diagnose.ReasonInsufficientEvidence:
		return ReasonInsufficientEvidence
	case diagnose.ReasonRecentEvidenceReady:
		return ReasonRecentEvidenceReady
	case diagnose.ReasonHistoricalBaselineReady:
		return ReasonHistoricalBaselineReady
	case diagnose.ReasonInsufficientRecentEvidence:
		return ReasonInsufficientRecentEvidence
	case diagnose.ReasonClientFailureRate:
		return ReasonClientFailureRate
	case diagnose.ReasonRateLimitRate:
		return ReasonRateLimitRate
	case diagnose.ReasonDependencyFailureRate:
		return ReasonDependencyFailureRate
	case diagnose.ReasonFailureRateIncrease:
		return ReasonFailureRateIncrease
	case diagnose.ReasonLatencyIncrease:
		return ReasonLatencyIncrease
	case diagnose.ReasonIsolatedDependencyFailure:
		return ReasonIsolatedDependencyFailure
	case diagnose.ReasonWithinBaseline:
		return ReasonWithinBaseline
	default:
		return 0
	}
}

func candidatesFrom(candidates policy.Candidate) Candidates {
	var converted Candidates
	if candidates&policy.CandidateRetry != 0 {
		converted |= CandidateRetry
	}
	if candidates&policy.CandidateBreakerOpen != 0 {
		converted |= CandidateBreakerOpen
	}

	return converted
}

func unknownName(kind string, value uint64) string {
	return kind + "(" + strconv.FormatUint(value, 10) + ")"
}
