package observe

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/raj1kshtz/curo/internal/policy"
	"github.com/raj1kshtz/curo/internal/retry"
)

func TestFinishReportsCompletion(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(7_000, 0))
	observer := NewWithClock(clock.Now, noTimeouts)
	request := testRequest("GET", "https", "api.example", "")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		context   context.Context
		name      string
		result    Result
		wantFault bool
	}{
		{
			name:   "success",
			result: Result{StatusCode: httpStatusOK, HasResponse: true},
		},
		{
			name:      "dependency status",
			result:    Result{StatusCode: httpStatusServiceUnavailable, HasResponse: true},
			wantFault: true,
		},
		{
			name:      "transport error",
			result:    Result{Err: errors.New("connection reset")},
			wantFault: true,
		},
		{
			name:    "caller cancellation",
			context: canceled,
			result:  Result{Err: errors.New("connection reset")},
		},
	}

	for _, test := range tests {
		request.Context = test.context
		token := observer.Begin(request)
		clock.Advance(15 * time.Millisecond)

		completion := observer.Finish(token, test.result)
		want := Completion{
			Latency:           15 * time.Millisecond,
			Recorded:          true,
			DependencyFailure: test.wantFault,
		}
		if completion != want {
			t.Errorf("%s completion = %+v, want %+v", test.name, completion, want)
		}
	}

	request.Context = nil
	backwards := observer.Begin(request)
	clock.Advance(-time.Second)
	if got := observer.Finish(backwards, Result{StatusCode: httpStatusOK, HasResponse: true}); got.Latency != 0 {
		t.Errorf("backwards clock latency = %v, want 0", got.Latency)
	}

	if got := (*Observer)(nil).Finish(Token{}, Result{}); got != (Completion{}) {
		t.Errorf("nil Observer Finish() = %+v, want zero", got)
	}
	if got := observer.Finish(Token{}, Result{}); got != (Completion{}) {
		t.Errorf("empty token Finish() = %+v, want zero", got)
	}
}

func TestFinishFundsRetryBudgetsOnlyForRecordedRegularAttempts(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(8_000, 0))
	observer := NewWithClock(clock.Now, noTimeouts)
	request := testRequest("GET", "https", "api.example", "")

	var regular Token
	for range 25 {
		regular = observer.Begin(request)
		observer.Finish(regular, Result{StatusCode: httpStatusOK, HasResponse: true})
	}
	for range 3 * retry.DepositsPerToken {
		overflow := observer.Begin(Request{})
		completion := observer.Finish(overflow, Result{StatusCode: httpStatusOK, HasResponse: true})
		if !completion.Recorded {
			t.Fatal("overflow completion was not recorded")
		}
	}

	stale := observer.Begin(request)
	futureTick := int64(observationBucketWidth) * observationBucketCount
	if !stale.target.record(futureTick, observation{outcome: outcomeSuccess}) {
		t.Fatal("future observation was rejected")
	}
	completion := observer.Finish(stale, Result{StatusCode: httpStatusOK, HasResponse: true})
	if completion.Recorded {
		t.Fatal("stale completion was recorded")
	}

	if got := reservableTargetTokens(&regular.target.retryBudget); got != 2 {
		t.Errorf("target retry tokens = %d, want 2 from 25 recorded attempts", got)
	}
	if got := reservableInstanceTokens(&observer.retryBudget); got != 2 {
		t.Errorf("instance retry tokens = %d, want 2 from regular attempts only", got)
	}
}

func TestReserveRetryRequiresLiveCurrentRetryPlan(t *testing.T) {
	t.Parallel()

	origin := time.Unix(9_000, 0)
	clock := newObservationClock(origin)
	observer := NewWithClock(clock.Now, noTimeouts)
	token := recordTransientTarget(t, observer, clock, "api.example")

	if !observer.RetryPermitted(token) {
		t.Fatal("RetryPermitted() on a Transient target = false, want true")
	}
	first, got := observer.ReserveRetry(token)
	if got != RetryReserved || !first.Held() {
		t.Fatalf("first ReserveRetry() = %v, held %t, want RetryReserved, true", got, first.Held())
	}
	second, got := observer.ReserveRetry(token)
	if got != RetryReserved || !second.Held() {
		t.Fatalf("second ReserveRetry() = %v, held %t, want RetryReserved, true", got, second.Held())
	}
	exhausted, got := observer.ReserveRetry(token)
	if got != RetryBudgetExhausted || exhausted.Held() {
		t.Fatalf("third ReserveRetry() = %v, held %t, want RetryBudgetExhausted, false", got, exhausted.Held())
	}

	second.Cancel()
	third, got := observer.ReserveRetry(token)
	if got != RetryReserved {
		t.Fatalf("ReserveRetry() after Cancel() = %v, want RetryReserved", got)
	}
	first.Commit()
	third.Cancel()

	token.target.mu.Lock()
	token.target.diagnosis.plan.Version = policy.Version + 1
	token.target.mu.Unlock()
	assertRetryIneligible(t, "newer plan version", observer, token)

	token.target.mu.Lock()
	token.target.diagnosis.plan.Version = policy.Version
	token.target.mu.Unlock()
	expiresAt := publishedPlan(token).ExpiresAt
	clock.Set(origin.Add(time.Duration(expiresAt)))
	assertRetryIneligible(t, "plan expiry", observer, token)

	clock.Set(origin.Add(time.Duration(expiresAt) - time.Nanosecond))
	if !observer.RetryPermitted(token) {
		t.Fatal("RetryPermitted() before plan expiry = false, want true")
	}
	token.target.retire()
	assertRetryIneligible(t, "retired target", observer, token)
}

func TestReserveRetryRejectsTargetsWithoutRetryCandidate(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(10_000, 0))
	observer := NewWithClock(clock.Now, noTimeouts)

	warming := observer.Begin(testRequest("GET", "https", "warming.example", ""))
	for range retry.DepositsPerToken {
		token := observer.Begin(testRequest("GET", "https", "warming.example", ""))
		observer.Finish(token, Result{StatusCode: httpStatusServiceUnavailable, HasResponse: true})
	}
	assertRetryIneligible(t, "warming target", observer, warming)

	down := observer.Begin(testRequest("GET", "https", "down.example", ""))
	for range 21 {
		token := observer.Begin(testRequest("GET", "https", "down.example", ""))
		observer.Finish(token, Result{StatusCode: httpStatusServiceUnavailable, HasResponse: true})
		clock.Advance(2 * time.Second)
	}
	if plan := publishedPlan(down); plan.Candidates != policy.CandidateBreakerOpen {
		t.Fatalf("down plan candidates = %v, want BreakerOpen", plan.Candidates)
	}
	assertRetryIneligible(t, "dependency-down target", observer, down)

	for range 3 * retry.DepositsPerToken {
		overflow := observer.Begin(Request{})
		observer.Finish(overflow, Result{Err: errors.New("connection reset")})
	}
	assertRetryIneligible(t, "overflow target", observer, observer.Begin(Request{}))
	assertRetryIneligible(t, "nil Observer", nil, down)
	assertRetryIneligible(t, "empty token", observer, Token{})
}

func assertRetryIneligible(t *testing.T, name string, observer *Observer, token Token) {
	t.Helper()

	if observer.RetryPermitted(token) {
		t.Errorf("%s RetryPermitted() = true, want false", name)
	}
	lease, got := observer.ReserveRetry(token)
	if got != RetryIneligible || lease.Held() {
		t.Errorf("%s ReserveRetry() = %v, held %t, want RetryIneligible, false", name, got, lease.Held())
	}
}

// recordTransientTarget records 20 successes and one dependency failure two
// seconds apart, so the target publishes a Ready Transient plan with a Retry
// candidate and 2.1 tokens in both budgets.
func recordTransientTarget(
	t *testing.T,
	observer *Observer,
	clock *observationClock,
	hostname string,
) Token {
	t.Helper()

	request := testRequest("GET", "https", hostname, "")
	var token Token
	for index := range 21 {
		if index > 0 {
			clock.Advance(2 * time.Second)
		}

		token = observer.Begin(request)
		result := Result{StatusCode: httpStatusOK, HasResponse: true}
		if index == 20 {
			result.StatusCode = httpStatusServiceUnavailable
		}
		if completion := observer.Finish(token, result); !completion.Recorded {
			t.Fatalf("attempt %d was not recorded", index)
		}
	}

	if plan := publishedPlan(token); plan.Candidates != policy.CandidateRetry {
		t.Fatalf("plan candidates = %v, want Retry", plan.Candidates)
	}

	return token
}

func publishedPlan(token Token) policy.Plan {
	current, _ := token.target.published()
	return current.plan
}

// reservableTargetTokens drains and counts the whole tokens in a target
// budget by pairing it with freshly funded instance budgets.
func reservableTargetTokens(target *retry.Budget) int {
	reserved := 0
	for {
		var scratch, instance retry.Budget
		for range retry.DepositsPerToken {
			retry.Deposit(&scratch, &instance)
		}
		lease, ok := retry.Reserve(target, &instance)
		if !ok {
			return reserved
		}
		lease.Commit()
		reserved++
	}
}

// reservableInstanceTokens drains and counts the whole tokens in an instance
// budget by pairing it with freshly funded target budgets.
func reservableInstanceTokens(instance *retry.Budget) int {
	reserved := 0
	for {
		var target, scratch retry.Budget
		for range retry.DepositsPerToken {
			retry.Deposit(&target, &scratch)
		}
		lease, ok := retry.Reserve(&target, instance)
		if !ok {
			return reserved
		}
		lease.Commit()
		reserved++
	}
}
