package observe

import (
	"errors"
	"testing"
	"time"

	"github.com/raj1kshtz/curo/internal/diagnose"
	"github.com/raj1kshtz/curo/internal/policy"
	"github.com/raj1kshtz/curo/internal/timeout"
)

var errTimeoutTestCut = errors.New("adaptive timeout")

func TestObserverSelectsTimeoutsForReadTargets(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(30_000, 0))
	observer := NewWithClock(clock.Now, timeout.DefaultBounds)
	read := testRequest("GET", "https", "api.example", "")
	write := testRequest("POST", "https", "api.example", "")

	recordLatencies(t, observer, clock, write, timeout.MinimumSamples, 0)
	clock.Advance(10 * time.Second)
	recordLatencies(t, observer, clock, write, 1, 0)
	recordLatencies(t, observer, clock, read, timeout.MinimumSamples-1, 0)
	if got := observer.Timeout(observer.Begin(read)); got != 0 {
		t.Errorf("Timeout() with %d samples = %v, want 0", timeout.MinimumSamples-1, got)
	}
	if got := observer.Timeout(observer.Begin(write)); got != 0 {
		t.Errorf("write Timeout() = %v, want 0", got)
	}

	recordLatencies(t, observer, clock, read, 1, 0)
	if got := observer.Timeout(observer.Begin(read)); got != 0 {
		t.Errorf("Timeout() before the plan renews = %v, want 0", got)
	}

	clock.Advance(10 * time.Second)
	recordLatencies(t, observer, clock, read, 1, 0)
	token := observer.Begin(read)
	if got := observer.Timeout(token); got != timeout.DefaultBounds.Minimum {
		t.Fatalf("Timeout() = %v, want %v", got, timeout.DefaultBounds.Minimum)
	}

	plan := publishedPlan(token)
	if plan.Candidates != policy.CandidateTimeout ||
		plan.Timeout != timeout.DefaultBounds.Minimum {
		t.Errorf("plan = %+v, want only CandidateTimeout of %v", plan, timeout.DefaultBounds.Minimum)
	}
	result := observer.Diagnosis(token)
	if want := (diagnose.Latency{
		Samples: timeout.MinimumSamples + 1,
		Slowest: time.Millisecond,
	}); result.Latency != want {
		t.Errorf("latency = %+v, want %+v", result.Latency, want)
	}
}

func TestObserverTimeoutIgnoresIneligibleTokens(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(31_000, 0))
	observer := NewWithClock(clock.Now, timeout.DefaultBounds)
	read := testRequest("GET", "https", "api.example", "")
	warmTimeout(t, observer, clock, read)

	probe := observer.Begin(read)
	probe.probe = 1
	overflow := observer.Begin(testRequest("GET", "ftp", "api.example", ""))
	if !overflow.overflow {
		t.Fatal("ftp request did not resolve to the overflow aggregate")
	}

	tests := map[string]struct {
		observer *Observer
		token    Token
	}{
		"nil observer": {token: observer.Begin(read)},
		"empty token":  {observer: observer},
		"overflow":     {observer: observer, token: overflow},
		"probe":        {observer: observer, token: probe},
	}
	for name, test := range tests {
		if got := test.observer.Timeout(test.token); got != 0 {
			t.Errorf("%s Timeout() = %v, want 0", name, got)
		}
	}
}

func TestObserverTimeoutEscalatesAfterCuts(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(32_000, 0))
	observer := NewWithClock(clock.Now, timeout.DefaultBounds)
	read := testRequest("GET", "https", "api.example", "")
	warmTimeout(t, observer, clock, read)

	steps := []struct {
		limit             time.Duration
		next              time.Duration
		dependencyFailure bool
	}{
		{limit: 2 * time.Second, next: 7500 * time.Millisecond},
		{limit: 7500 * time.Millisecond, next: 30 * time.Second},
		{limit: 30 * time.Second, next: 30 * time.Second, dependencyFailure: true},
	}
	for _, step := range steps {
		clock.Advance(100 * time.Millisecond)
		token := observer.Begin(read)
		if got := observer.Timeout(token); got != step.limit {
			t.Fatalf("Timeout() = %v, want %v", got, step.limit)
		}

		clock.Advance(step.limit)
		completion := observer.Finish(token, Result{Err: errTimeoutTestCut, Timeout: step.limit})
		if !completion.Recorded || completion.DependencyFailure != step.dependencyFailure {
			t.Errorf(
				"cut at %v completion = %+v, want recorded with dependency failure %t",
				step.limit,
				completion,
				step.dependencyFailure,
			)
		}
		if got := observer.Timeout(observer.Begin(read)); got != step.next {
			t.Errorf("Timeout() after a cut at %v = %v, want %v", step.limit, got, step.next)
		}
	}

	var timedOut, adaptive uint64
	state := observer.Begin(read).target
	state.mu.Lock()
	for index := range state.buckets {
		timedOut += state.buckets[index].outcomes[outcomeTimedOut]
		adaptive += state.buckets[index].timeouts[timeoutAdaptive]
	}
	state.mu.Unlock()
	if timedOut != 3 || adaptive != 3 {
		t.Errorf("recorded cuts = %d timed out, %d adaptive, want 3 and 3", timedOut, adaptive)
	}
}

func TestObserverRaisesTimeoutOnlyForSlowerSamples(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(33_000, 0))
	observer := NewWithClock(clock.Now, timeout.DefaultBounds)
	read := testRequest("GET", "https", "api.example", "")
	state := warmTimeout(t, observer, clock, read)
	before, _ := state.published()

	recordLatencies(t, observer, clock, read, 1, 400*time.Millisecond)
	if after, _ := state.published(); after.result.EvaluatedAt != before.result.EvaluatedAt {
		t.Errorf("a sample that keeps the timeout evaluated the target at %d", after.result.EvaluatedAt)
	}

	recordLatencies(t, observer, clock, read, 1, 900*time.Millisecond)
	after, _ := state.published()
	if after.result.EvaluatedAt == before.result.EvaluatedAt ||
		after.plan.Timeout != 3*time.Second {
		t.Errorf("plan after a slower sample = %+v, want a new evaluation with a 3s timeout", after.plan)
	}
	if got := observer.Timeout(observer.Begin(read)); got != 3*time.Second {
		t.Errorf("Timeout() = %v, want 3s", got)
	}
}

func TestObserverTimeoutRenewsAndLapsesWithEvidence(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(34_000, 0))
	observer := NewWithClock(clock.Now, timeout.DefaultBounds)
	read := testRequest("GET", "https", "api.example", "")
	state := warmTimeout(t, observer, clock, read)
	before, _ := state.published()

	clock.Advance(20 * time.Second)
	if got := observer.Timeout(observer.Begin(read)); got != timeout.DefaultBounds.Minimum {
		t.Errorf("Timeout() after the plan expired = %v, want %v", got, timeout.DefaultBounds.Minimum)
	}
	if after, _ := state.published(); after.result.EvaluatedAt <= before.result.EvaluatedAt {
		t.Error("Timeout() did not evaluate an expired plan")
	}

	clock.Advance(40 * time.Minute)
	if got := observer.Timeout(observer.Begin(read)); got != 0 {
		t.Errorf("Timeout() after the evidence aged out = %v, want 0", got)
	}
	if state.timeout.Load() != nil {
		t.Error("timeout mirror survived a plan without CandidateTimeout")
	}
}

func TestTimeoutEvaluationLeavesTheNextResultToTheDiagnosis(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(34_500, 0))
	observer := NewWithClock(clock.Now, timeout.DefaultBounds)
	read := testRequest("GET", "https", "api.example", "")
	state := warmTimeout(t, observer, clock, read)

	clock.Advance(20 * time.Second)
	token := observer.Begin(read)
	if got := observer.Timeout(token); got != timeout.DefaultBounds.Minimum {
		t.Fatalf("Timeout() = %v, want %v", got, timeout.DefaultBounds.Minimum)
	}
	renewed, _ := state.published()

	completion := observer.Finish(token, breakerTestFailure)
	if !completion.Recorded || !completion.DependencyFailure {
		t.Fatalf("completion = %+v, want a recorded dependency failure", completion)
	}
	after, _ := state.published()
	if after.result.Recent.DependencyFailures != 1 ||
		after.result.Class != diagnose.ClassTransient {
		t.Errorf(
			"diagnosis after the failure = %v with %d failures, want Transient with 1",
			after.result.Class,
			after.result.Recent.DependencyFailures,
		)
	}
	if after.result.EvaluatedAt != renewed.result.EvaluatedAt ||
		after.plan.Timeout != timeout.DefaultBounds.Minimum {
		t.Errorf(
			"evaluation = %d with timeout %v, want %d with %v",
			after.result.EvaluatedAt,
			after.plan.Timeout,
			renewed.result.EvaluatedAt,
			timeout.DefaultBounds.Minimum,
		)
	}

	recordLatencies(t, observer, clock, read, 1, 0)
	if latest, _ := state.published(); latest.result.EvaluatedAt != after.result.EvaluatedAt {
		t.Error("a result after the follow-up evaluation evaluated the target again")
	}
}

func TestRetiredTargetSelectsNoTimeout(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(35_000, 0))
	observer := NewWithClock(clock.Now, timeout.DefaultBounds)
	read := testRequest("GET", "https", "api.example", "")
	state := warmTimeout(t, observer, clock, read)
	token := observer.Begin(read)

	state.retire()
	if got := observer.Timeout(token); got != 0 {
		t.Errorf("retired Timeout() = %v, want 0", got)
	}
	if got := state.timeoutAt(0); got != 0 {
		t.Errorf("retired timeoutAt() = %v, want 0", got)
	}
}

func TestLatencySummaryKeepsFencedAndHistoricalSamples(t *testing.T) {
	t.Parallel()

	state := newTarget(0, true)
	slow := observation{
		outcome:       outcomeSuccess,
		latencyBucket: latencyBucket(4 * time.Second),
		hasLatency:    true,
	}
	fast := observation{
		outcome:       outcomeSuccess,
		latencyBucket: latencyBucket(time.Millisecond),
		hasLatency:    true,
	}
	for range 60 {
		state.record(0, slow)
	}
	for range 40 {
		state.record(int64(3*time.Minute), fast)
	}

	tick := int64(3*time.Minute + time.Second)
	state.mu.Lock()
	state.recentFloor = tick/int64(observationBucketWidth) + 1
	snapshot := state.snapshotLocked(tick)
	state.mu.Unlock()

	if snapshot.Recent.LatencySamples != 0 || snapshot.Historical.LatencySamples != 60 {
		t.Errorf(
			"windows hold %d recent and %d historical samples, want 0 and 60",
			snapshot.Recent.LatencySamples,
			snapshot.Historical.LatencySamples,
		)
	}
	if want := (diagnose.Latency{Samples: 100, Slowest: 5 * time.Second}); snapshot.Latency != want {
		t.Errorf("latency = %+v, want %+v", snapshot.Latency, want)
	}
	if got := (latencySummary{}).summary(); got != (diagnose.Latency{}) {
		t.Errorf("empty summary = %+v, want zero", got)
	}
	if got := bucketUpperBound(uint8(len(latencyUpperBounds))); got != time.Duration(1<<63-1) {
		t.Errorf("overflow bucket upper bound = %v, want the largest Duration", got)
	}
}

func TestCutsBelowTheCeilingAreOnlyLatencySamples(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(36_000, 0))
	observer := NewWithClock(clock.Now, timeout.DefaultBounds)
	read := testRequest("GET", "https", "api.example", "")
	state := warmTimeout(t, observer, clock, read)

	// The warm-up leaves the recent window but stays as history, which keeps
	// the timeout. Eleven requests then share the two second timeout.
	clock.Advance(3 * time.Minute)
	cuts := make([]Token, 11)
	for index := range cuts {
		cuts[index] = observer.Begin(read)
		if got := observer.Timeout(cuts[index]); got != timeout.DefaultBounds.Minimum {
			t.Fatalf("Timeout() = %v, want %v", got, timeout.DefaultBounds.Minimum)
		}
	}
	lastRelevant := diagnosisOf(state).lastRelevant

	clock.Advance(timeout.DefaultBounds.Minimum)
	for _, token := range cuts {
		completion := observer.Finish(token, Result{
			Err:     errTimeoutTestCut,
			Timeout: timeout.DefaultBounds.Minimum,
		})
		if !completion.Recorded || completion.DependencyFailure {
			t.Fatalf("cut completion = %+v, want recorded without a dependency failure", completion)
		}
	}
	// The first cut raised the timeout, but its evaluation is provisional, so
	// the next relevant result still evaluates the target.
	if diagnosis := diagnosisOf(state); diagnosis.lastRelevant != lastRelevant ||
		!diagnosis.provisional ||
		diagnosis.plan.Timeout != 7500*time.Millisecond {
		t.Errorf(
			"after the cuts: last relevant %v, provisional %t, timeout %v, want %v, true, 7.5s",
			time.Duration(diagnosis.lastRelevant),
			diagnosis.provisional,
			diagnosis.plan.Timeout,
			time.Duration(lastRelevant),
		)
	}

	for range 10 {
		clock.Advance(100 * time.Millisecond)
		observer.Finish(observer.Begin(read), breakerTestFailure)
	}
	clock.Advance(10 * time.Second)
	snapshot := state.snapshotAt(observer.tick(clock.Now()))
	if recent := snapshot.Recent; recent.Attempts != 21 ||
		recent.LatencySamples != 21 ||
		recent.RelevantAttempts != 10 ||
		recent.DependencyFailures != 10 {
		t.Errorf(
			"recent window = %+v, want 21 attempts and latency samples, 10 relevant failures",
			recent,
		)
	}
	// Counted as relevant attempts, the cuts would dilute the failures below
	// half of them.
	if result := observer.Diagnosis(observer.Begin(read)); result.Class != diagnose.ClassDependencyDown {
		t.Errorf("diagnosis = %v, want DependencyDown", result.Class)
	}
}

func diagnosisOf(state *target) diagnosisState {
	state.mu.Lock()
	defer state.mu.Unlock()

	return state.diagnosis
}

func TestCensoredAttemptsAreDependencyFailuresOnlyAtTheCeiling(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		limit  time.Duration
		bounds timeout.Bounds
		want   diagnosisSignal
	}{
		"floor":           {limit: 2 * time.Second, bounds: timeout.DefaultBounds, want: diagnosisLatencyOnly},
		"below ceiling":   {limit: 7500 * time.Millisecond, bounds: timeout.DefaultBounds, want: diagnosisLatencyOnly},
		"ceiling":         {limit: 30 * time.Second, bounds: timeout.DefaultBounds, want: diagnosisDependencyFailure},
		"disabled bounds": {limit: time.Second, want: diagnosisDependencyFailure},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			want := observation{
				outcome:       outcomeTimedOut,
				timeoutSource: timeoutAdaptive,
				latencyBucket: latencyBucket(test.limit),
				signal:        test.want,
				hasTimeout:    true,
				hasLatency:    true,
			}
			if got := censored(test.limit, test.bounds); got != want {
				t.Errorf("censored() = %+v, want %+v", got, want)
			}
		})
	}
}

func TestCandidatesChangedTracksTimeouts(t *testing.T) {
	t.Parallel()

	timeoutOnly := policy.Plan{Candidates: policy.CandidateTimeout, Timeout: 2 * time.Second}
	retryAndTimeout := policy.Plan{
		Candidates: policy.CandidateRetry | policy.CandidateTimeout,
		Timeout:    2 * time.Second,
	}
	tests := map[string]struct {
		previous diagnosisState
		plan     policy.Plan
		class    diagnose.Class
		want     bool
	}{
		"timeout renewed under a new diagnosis": {
			previous: diagnosisState{
				result: diagnose.Result{Class: diagnose.ClassHealthy},
				plan:   timeoutOnly,
			},
			class: diagnose.ClassDegrading,
			plan:  timeoutOnly,
		},
		"timeout raised": {
			previous: diagnosisState{plan: timeoutOnly},
			plan:     policy.Plan{Candidates: policy.CandidateTimeout, Timeout: 3 * time.Second},
			want:     true,
		},
		"timeout withdrawn": {
			previous: diagnosisState{plan: timeoutOnly},
			want:     true,
		},
		"diagnosis candidate under a new diagnosis": {
			previous: diagnosisState{
				result: diagnose.Result{Class: diagnose.ClassTransient},
				plan:   retryAndTimeout,
			},
			class: diagnose.ClassHealthy,
			plan:  retryAndTimeout,
			want:  true,
		},
		"renewal": {
			previous: diagnosisState{
				result: diagnose.Result{Class: diagnose.ClassTransient},
				plan:   retryAndTimeout,
			},
			class: diagnose.ClassTransient,
			plan:  retryAndTimeout,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			result := diagnose.Result{Class: test.class}
			if got := candidatesChanged(test.previous, result, test.plan); got != test.want {
				t.Errorf("candidatesChanged() = %t, want %t", got, test.want)
			}
		})
	}
}

// warmTimeout records enough fast reads, then renews the plan, so request's
// target selects a timeout at the floor of the default bounds.
func warmTimeout(
	t *testing.T,
	observer *Observer,
	clock *observationClock,
	request Request,
) *target {
	t.Helper()

	recordLatencies(t, observer, clock, request, timeout.MinimumSamples, 0)
	clock.Advance(10 * time.Second)
	recordLatencies(t, observer, clock, request, 1, 0)
	token := observer.Begin(request)
	if got := observer.Timeout(token); got != timeout.DefaultBounds.Minimum {
		t.Fatalf("warm Timeout() = %v, want %v", got, timeout.DefaultBounds.Minimum)
	}

	return token.target
}

// recordLatencies completes count successful attempts to request 100
// milliseconds apart, each taking latency on the observer clock.
func recordLatencies(
	t *testing.T,
	observer *Observer,
	clock *observationClock,
	request Request,
	count int,
	latency time.Duration,
) {
	t.Helper()

	for range count {
		clock.Advance(100 * time.Millisecond)
		token := observer.Begin(request)
		clock.Advance(latency)
		if completion := observer.Finish(token, breakerTestSuccess); !completion.Recorded {
			t.Fatalf("completion = %+v, want recorded", completion)
		}
	}
}
