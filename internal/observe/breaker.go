package observe

import (
	"context"

	"github.com/raj1kshtz/curo/internal/breaker"
	"github.com/raj1kshtz/curo/internal/policy"
	"github.com/raj1kshtz/curo/internal/retry"
)

// Admit applies the dependency breaker of token's target to a request that
// started in Enforce mode. It returns the token to pass to Finish.
//
// Requests to the overflow aggregate always pass. While a breaker is open,
// Admit rejects requests, except that once the cooldown ends it admits one
// request as a probe. A request whose context is done or whose Cancel channel
// is closed is never chosen as a probe.
func (observer *Observer) Admit(token Token) (Token, breaker.Admission) {
	if observer == nil || token.target == nil || token.overflow {
		return token, breaker.Pass
	}
	if !token.target.engaged.Load() {
		token.admitted = true
		return token, breaker.Pass
	}

	admission, lease := token.target.admit(
		observer.tick(observer.now()),
		token.callerErr() == nil,
	)
	switch admission {
	case breaker.Pass:
		token.admitted = true
	case breaker.Probe:
		token.probe = lease
	}

	return token, admission
}

// Trip opens the dependency breaker of token's target and reports whether it
// opened. The request must have been admitted by a closed breaker, and the
// target's current plan must still select CandidateBreakerOpen.
func (observer *Observer) Trip(token Token) bool {
	if observer == nil || !token.admitted {
		return false
	}

	return token.target.trip(observer.tick(observer.now()))
}

// callerErr returns an error when the caller canceled token's request, or nil.
func (token Token) callerErr() error {
	if token.context != nil {
		if err := token.context.Err(); err != nil {
			return err
		}
	}
	if retry.Canceled(token.cancel) {
		// net/http closes the deprecated Cancel channel when an http.Client
		// timeout expires, possibly before the context reports its deadline.
		return context.Canceled
	}

	return nil
}

func (target *target) admit(
	tick int64,
	eligible bool,
) (breaker.Admission, uint64) {
	target.mu.Lock()
	defer target.mu.Unlock()

	if target.retired {
		return breaker.Pass, 0
	}

	return target.breaker.Admit(tick, eligible)
}

// settleLocked applies a probe's outcome. When the probe closes the breaker,
// everything recorded before the close leaves the recent window, so opening
// again needs fresh evidence. The immediate evaluation replaces a published
// plan that still selects CandidateBreakerOpen.
func (target *target) settleLocked(
	lease uint64,
	signal diagnosisSignal,
	tick int64,
) {
	if target.retired ||
		!target.breaker.Settle(lease, probeOutcome(signal), tick) {
		return
	}

	target.engaged.Store(false)
	fence := max(
		tick,
		target.diagnosis.lastRelevant,
		target.diagnosis.result.EvaluatedAt,
	)
	target.recentFloor = max(
		target.recentFloor,
		fence/int64(observationBucketWidth)+1,
	)
	target.evaluateDiagnosisLocked(fence)
}

// mayTripLocked reports whether an attempt that a closed breaker admitted may
// open it at tick.
func (target *target) mayTripLocked(tick int64) bool {
	return target.breaker.State() == breaker.Closed &&
		target.selectsLocked(policy.CandidateBreakerOpen, tick)
}

func (target *target) trip(tick int64) bool {
	target.mu.Lock()
	defer target.mu.Unlock()

	if !target.selectsLocked(policy.CandidateBreakerOpen, tick) ||
		!target.breaker.Trip(tick) {
		return false
	}

	target.engaged.Store(true)
	return true
}

func (target *target) retryPermitted(tick int64) bool {
	target.mu.Lock()
	defer target.mu.Unlock()

	return target.breaker.State() == breaker.Closed &&
		target.selectsLocked(policy.CandidateRetry, tick)
}

// selectsLocked reports whether the target is live and its published plan is
// an unexpired current-version plan containing candidate at tick.
func (target *target) selectsLocked(
	candidate policy.Candidate,
	tick int64,
) bool {
	plan := target.diagnosis.plan
	return !target.retired &&
		plan.Version == policy.Version &&
		plan.Candidates&candidate != 0 &&
		tick < plan.ExpiresAt
}

// probeOutcome maps a probe's diagnosis signal to breaker evidence. Any answer
// other than a rate limit or a dependency failure shows that the dependency
// is serving requests, including a client error such as 404.
func probeOutcome(signal diagnosisSignal) breaker.Outcome {
	switch signal {
	case diagnosisCallerOwned:
		return breaker.Inconclusive
	case diagnosisRateLimited, diagnosisDependencyFailure:
		return breaker.Failed
	default:
		return breaker.Healthy
	}
}
