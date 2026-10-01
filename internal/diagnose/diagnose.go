// Package diagnose evaluates bounded evidence using deterministic rules.
package diagnose

import (
	"math"
	"math/bits"
	"time"
)

const (
	recentReadySamples       = 20
	historicalReadySamples   = 20
	historicalCompareSamples = 50
	recentReadySpan          = 30 * time.Second
	historicalReadySpan      = 10 * time.Minute
	resultTTL                = 10 * time.Second
	rateScale                = 10_000
	maxReasons               = 4
)

// Readiness reports whether retained evidence can support a diagnosis.
type Readiness uint8

const (
	// ReadinessCold means no dependency-relevant observation has been retained.
	ReadinessCold Readiness = iota

	// ReadinessWarming means fresh evidence exists but is not sufficient.
	ReadinessWarming

	// ReadinessReady means recent volume or historical evidence is sufficient.
	ReadinessReady

	// ReadinessStale means prior evidence exists but no recent attempt remains.
	ReadinessStale
)

// Class is a closed diagnosis category.
type Class uint8

const (
	// ClassNone means the evidence does not support a diagnosis.
	ClassNone Class = iota

	// ClassHealthy means ready evidence remains within its historical bounds.
	ClassHealthy

	// ClassTransient means isolated dependency failures are present.
	ClassTransient

	// ClassDependencyDown means dependency failures dominate recent traffic.
	ClassDependencyDown

	// ClassSaturation means explicit rate limiting is sustained.
	ClassSaturation

	// ClassClientError means request-side HTTP failures dominate recent traffic.
	ClassClientError

	// ClassDegrading means recent failure or latency evidence moved materially.
	ClassDegrading
)

// Reason is a bounded explanation code attached to a Result.
type Reason uint8

const (
	// ReasonNone is the zero-value placeholder in an unused reason slot.
	ReasonNone Reason = iota

	// ReasonNoEvidence means no dependency-relevant observation exists.
	ReasonNoEvidence

	// ReasonEvidenceStale means retained observations are outside the recent window.
	ReasonEvidenceStale

	// ReasonInsufficientEvidence means readiness sample or span floors are unmet.
	ReasonInsufficientEvidence

	// ReasonRecentEvidenceReady means recent volume satisfied readiness.
	ReasonRecentEvidenceReady

	// ReasonHistoricalBaselineReady means historical evidence satisfied readiness.
	ReasonHistoricalBaselineReady

	// ReasonInsufficientRecentEvidence means no current class has enough evidence.
	ReasonInsufficientRecentEvidence

	// ReasonClientFailureRate means request-side failures dominate recent evidence.
	ReasonClientFailureRate

	// ReasonRateLimitRate means explicit rate limits are sustained.
	ReasonRateLimitRate

	// ReasonDependencyFailureRate means dependency failures dominate recent evidence.
	ReasonDependencyFailureRate

	// ReasonFailureRateIncrease means failures rose materially above history.
	ReasonFailureRateIncrease

	// ReasonLatencyIncrease means recent p95 moved by at least two fixed buckets.
	ReasonLatencyIncrease

	// ReasonIsolatedDependencyFailure means dependency failures are not sustained.
	ReasonIsolatedDependencyFailure

	// ReasonWithinBaseline means ready evidence did not trigger another class.
	ReasonWithinBaseline
)

// Window is a bounded aggregate over one evidence interval.
type Window struct {
	Attempts           uint64
	RelevantAttempts   uint64
	RateLimited        uint64
	ClientFailures     uint64
	DependencyFailures uint64
	LatencySamples     uint64
	FirstRelevantTick  int64
	LastRelevantTick   int64
	LastTick           int64
	P95LatencyBucket   uint8
}

// Snapshot contains the recent and non-overlapping historical evidence used
// by one deterministic evaluation.
type Snapshot struct {
	Recent          Window
	Historical      Window
	Now             int64
	RecentExpiresAt int64
	EverRelevant    bool
}

// Evidence is the bounded explanation retained from one evaluated window.
type Evidence struct {
	RelevantAttempts   uint64
	DependencyFailures uint64
	RateLimited        uint64
	ClientFailures     uint64
	Span               int64
}

// Result is an immutable, expiring diagnosis snapshot.
type Result struct {
	Recent      Evidence
	Historical  Evidence
	EvaluatedAt int64
	ExpiresAt   int64
	Readiness   Readiness
	Class       Class
	Reasons     [maxReasons]Reason
	ReasonCount uint8
}

// Evaluate applies the closed readiness and diagnosis rule order.
func Evaluate(snapshot Snapshot) Result {
	if snapshot.Now < 0 {
		snapshot.Now = 0
	}

	result := Result{
		Recent:      summarize(snapshot.Recent),
		Historical:  summarize(snapshot.Historical),
		EvaluatedAt: snapshot.Now,
		ExpiresAt:   saturatingAdd(snapshot.Now, int64(resultTTL)),
	}
	if snapshot.Recent.RelevantAttempts > 0 {
		result.ExpiresAt = minimum(
			saturatingAdd(snapshot.Now, int64(resultTTL)),
			snapshot.RecentExpiresAt,
		)
		if result.ExpiresAt < snapshot.Now {
			result.ExpiresAt = snapshot.Now
		}
	}

	historicalReady := windowReady(
		snapshot.Historical,
		historicalReadySamples,
		historicalReadySpan,
	)
	recentReady := windowReady(
		snapshot.Recent,
		recentReadySamples,
		recentReadySpan,
	)

	switch {
	case !snapshot.EverRelevant:
		result.Readiness = ReadinessCold
		result.addReason(ReasonNoEvidence)
		return result
	case snapshot.Recent.RelevantAttempts == 0:
		result.Readiness = ReadinessStale
		result.addReason(ReasonEvidenceStale)
		return result
	case historicalReady:
		result.Readiness = ReadinessReady
		result.addReason(ReasonHistoricalBaselineReady)
	case recentReady:
		result.Readiness = ReadinessReady
		result.addReason(ReasonRecentEvidenceReady)
	default:
		result.Readiness = ReadinessWarming
		result.addReason(ReasonInsufficientEvidence)
	}

	recent := snapshot.Recent
	switch {
	case recent.ClientFailures >= 3 &&
		ratioAtLeast(recent.ClientFailures, recent.RelevantAttempts, 50):
		result.Class = ClassClientError
		result.addReason(ReasonClientFailureRate)
	case recent.RateLimited >= 3 &&
		recent.RelevantAttempts >= 10 &&
		ratioAtLeast(recent.RateLimited, recent.RelevantAttempts, 20):
		result.Class = ClassSaturation
		result.addReason(ReasonRateLimitRate)
	case recent.DependencyFailures >= 5 &&
		recent.RelevantAttempts >= 10 &&
		ratioAtLeast(recent.DependencyFailures, recent.RelevantAttempts, 50):
		result.Class = ClassDependencyDown
		result.addReason(ReasonDependencyFailureRate)
	case historicalReady && failureRateIncreased(recent, snapshot.Historical):
		result.Class = ClassDegrading
		result.addReason(ReasonFailureRateIncrease)
	case historicalReady && latencyIncreased(recent, snapshot.Historical):
		result.Class = ClassDegrading
		result.addReason(ReasonLatencyIncrease)
	case recent.DependencyFailures > 0:
		result.Class = ClassTransient
		result.addReason(ReasonIsolatedDependencyFailure)
	case result.Readiness == ReadinessReady && recent.RelevantAttempts >= 5:
		result.Class = ClassHealthy
		result.addReason(ReasonWithinBaseline)
	default:
		result.addReason(ReasonInsufficientRecentEvidence)
	}

	return result
}

func windowReady(window Window, samples uint64, span time.Duration) bool {
	return window.RelevantAttempts >= samples &&
		window.LastRelevantTick >= window.FirstRelevantTick &&
		window.LastRelevantTick-window.FirstRelevantTick >= int64(span)
}

func summarize(window Window) Evidence {
	evidence := Evidence{
		RelevantAttempts:   window.RelevantAttempts,
		DependencyFailures: window.DependencyFailures,
		RateLimited:        window.RateLimited,
		ClientFailures:     window.ClientFailures,
	}
	if window.RelevantAttempts > 0 &&
		window.LastRelevantTick > window.FirstRelevantTick {
		evidence.Span = window.LastRelevantTick - window.FirstRelevantTick
		if evidence.Span < 0 {
			evidence.Span = math.MaxInt64
		}
	}

	return evidence
}

func failureRateIncreased(recent, historical Window) bool {
	if recent.RelevantAttempts < recentReadySamples ||
		historical.RelevantAttempts < historicalCompareSamples ||
		recent.DependencyFailures < 3 {
		return false
	}

	recentRate := basisPoints(
		recent.DependencyFailures,
		recent.RelevantAttempts,
	)
	historicalRate := basisPoints(
		historical.DependencyFailures,
		historical.RelevantAttempts,
	)

	return recentRate >= historicalRate+2_000
}

func latencyIncreased(recent, historical Window) bool {
	if recent.LatencySamples < recentReadySamples ||
		historical.LatencySamples < historicalCompareSamples {
		return false
	}

	return int(recent.P95LatencyBucket) >=
		int(historical.P95LatencyBucket)+2
}

func ratioAtLeast(part, total, percent uint64) bool {
	if total == 0 || part == 0 || percent > 100 {
		return false
	}

	return basisPoints(part, total) >= percent*100
}

func basisPoints(part, total uint64) uint64 {
	if total == 0 {
		return 0
	}
	if part >= total {
		return rateScale
	}

	high, low := bits.Mul64(part, rateScale)
	quotient, _ := bits.Div64(high, low, total)
	return quotient
}

func (result *Result) addReason(reason Reason) {
	if reason == ReasonNone || result.ReasonCount >= maxReasons {
		return
	}

	result.Reasons[result.ReasonCount] = reason
	result.ReasonCount++
}

func saturatingAdd(value, delta int64) int64 {
	if delta > 0 && value > math.MaxInt64-delta {
		return math.MaxInt64
	}
	if delta < 0 && value < math.MinInt64-delta {
		return math.MinInt64
	}

	return value + delta
}

func minimum(left, right int64) int64 {
	if left < right {
		return left
	}

	return right
}
