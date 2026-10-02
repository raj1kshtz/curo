package observe

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raj1kshtz/curo/internal/breaker"
	"github.com/raj1kshtz/curo/internal/diagnose"
	"github.com/raj1kshtz/curo/internal/policy"
)

const (
	httpStatusNotFound        = 404
	httpStatusTooManyRequests = 429
)

var (
	breakerTestFailure = Result{
		StatusCode:  httpStatusServiceUnavailable,
		HasResponse: true,
	}
	breakerTestSuccess = Result{StatusCode: httpStatusOK, HasResponse: true}
)

func TestAdmitPassesWithoutAnOpenBreaker(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(20_000, 0))
	var reads atomic.Int64
	observer := NewWithClock(func() time.Time {
		reads.Add(1)
		return clock.Now()
	})

	if token, admission := (*Observer)(nil).Admit(Token{}); admission != breaker.Pass ||
		token != (Token{}) {
		t.Errorf("nil Observer Admit() = %+v, %v, want unchanged Pass", token, admission)
	}
	if token, admission := observer.Admit(Token{}); admission != breaker.Pass ||
		token != (Token{}) {
		t.Errorf("empty token Admit() = %+v, %v, want unchanged Pass", token, admission)
	}

	overflow := observer.Begin(Request{})
	if token, admission := observer.Admit(overflow); admission != breaker.Pass ||
		token != overflow {
		t.Errorf("overflow Admit() = %+v, %v, want unchanged Pass", token, admission)
	}

	regular := observer.Begin(testRequest("GET", "https", "api.example", ""))
	before := reads.Load()
	token, admission := observer.Admit(regular)
	if admission != breaker.Pass || !token.admitted || token.probe != 0 {
		t.Errorf(
			"closed breaker Admit() = admitted %t, probe %d, %v, want admitted Pass",
			token.admitted,
			token.probe,
			admission,
		)
	}
	if reads.Load() != before {
		t.Error("closed breaker admission read the clock")
	}
}

func TestFinishReportsTripOnlyForAdmittedDependencyFailures(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(21_000, 0))
	observer := NewWithClock(clock.Now)
	request, state := recordDownTarget(t, observer, clock, "down.example")
	clock.Advance(2 * time.Second)

	unadmitted := observer.Begin(request)
	if completion := observer.Finish(unadmitted, breakerTestFailure); completion.Trip {
		t.Error("unadmitted failure reported Trip")
	}
	if observer.Trip(unadmitted) {
		t.Error("Trip() for an unadmitted token = true, want false")
	}

	success := admit(t, observer, request, breaker.Pass)
	if completion := observer.Finish(success, breakerTestSuccess); completion.Trip {
		t.Error("admitted success reported Trip")
	}

	straggler := admit(t, observer, request, breaker.Pass)
	failure := admit(t, observer, request, breaker.Pass)
	completion := observer.Finish(failure, breakerTestFailure)
	if !completion.Trip || !completion.Recorded || !completion.DependencyFailure {
		t.Fatalf("admitted failure completion = %+v, want recorded failure with Trip", completion)
	}
	if (*Observer)(nil).Trip(failure) {
		t.Error("nil Observer Trip() = true, want false")
	}
	if !observer.Trip(failure) {
		t.Fatal("Trip() = false, want true")
	}
	if !state.engaged.Load() || breakerState(state) != breaker.Open {
		t.Fatalf(
			"after Trip() engaged = %t, state = %v, want engaged Open",
			state.engaged.Load(),
			breakerState(state),
		)
	}
	if observer.Trip(failure) {
		t.Error("second Trip() = true, want false for an open breaker")
	}
	if completion := observer.Finish(straggler, breakerTestFailure); completion.Trip {
		t.Error("failure admitted before the breaker opened reported Trip")
	}
}

func TestTripRequiresCurrentBreakerOpenPlan(t *testing.T) {
	t.Parallel()

	origin := time.Unix(22_000, 0)
	clock := newObservationClock(origin)
	observer := NewWithClock(clock.Now)

	recordTransientTarget(t, observer, clock, "transient.example")
	retrying := admit(
		t,
		observer,
		testRequest("GET", "https", "transient.example", ""),
		breaker.Pass,
	)
	if completion := observer.Finish(retrying, breakerTestFailure); completion.Trip {
		t.Error("failure on a Retry plan reported Trip")
	}
	if observer.Trip(retrying) {
		t.Error("Trip() on a Retry plan = true, want false")
	}

	request, state := recordDownTarget(t, observer, clock, "down.example")
	token := admit(t, observer, request, breaker.Pass)

	state.mu.Lock()
	state.diagnosis.plan.Version = policy.Version + 1
	state.mu.Unlock()
	if observer.Trip(token) {
		t.Error("Trip() on a newer plan version = true, want false")
	}
	state.mu.Lock()
	state.diagnosis.plan.Version = policy.Version
	state.mu.Unlock()

	expiresAt := publishedPlan(token).ExpiresAt
	clock.Set(origin.Add(time.Duration(expiresAt)))
	if observer.Trip(token) {
		t.Error("Trip() at plan expiry = true, want false")
	}

	clock.Set(origin.Add(time.Duration(expiresAt) - time.Nanosecond))
	state.retire()
	if observer.Trip(token) {
		t.Error("Trip() on a retired target = true, want false")
	}
	if state.engaged.Load() || breakerState(state) != breaker.Closed {
		t.Error("a rejected Trip() changed the breaker")
	}
}

func TestOpenBreakerAdmitsOneProbeAndClosesOnAnswer(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(23_000, 0))
	observer := NewWithClock(clock.Now)
	request, state := recordDownTarget(t, observer, clock, "down.example")
	openBreaker(t, observer, clock, request)

	rejected := admit(t, observer, request, breaker.Reject)
	if rejected.admitted || rejected.probe != 0 {
		t.Errorf(
			"rejected token = admitted %t, probe %d, want neither",
			rejected.admitted,
			rejected.probe,
		)
	}
	if observer.RetryPermitted(rejected) {
		t.Error("RetryPermitted() with an open breaker = true, want false")
	}

	clock.Advance(breaker.BaseCooldown - time.Nanosecond)
	admit(t, observer, request, breaker.Reject)

	clock.Advance(time.Nanosecond)
	probe := admit(t, observer, request, breaker.Probe)
	if probe.probe == 0 || probe.admitted {
		t.Fatalf(
			"probe token = admitted %t, probe %d, want an unadmitted lease",
			probe.admitted,
			probe.probe,
		)
	}
	admit(t, observer, request, breaker.Reject)
	if observer.RetryPermitted(probe) {
		t.Error("RetryPermitted() for a probe = true, want false")
	}

	completion := observer.Finish(probe, Result{
		StatusCode:  httpStatusNotFound,
		HasResponse: true,
	})
	if !completion.Recorded || completion.Trip {
		t.Errorf("probe completion = %+v, want recorded without Trip", completion)
	}
	if state.engaged.Load() || breakerState(state) != breaker.Closed {
		t.Fatalf(
			"after an answered probe engaged = %t, state = %v, want Closed",
			state.engaged.Load(),
			breakerState(state),
		)
	}

	current, live := state.published()
	if !live ||
		current.result.Readiness != diagnose.ReadinessStale ||
		current.plan.Candidates != 0 {
		t.Errorf(
			"plan after close = %v/%v, want Stale without candidates",
			current.result.Readiness,
			current.plan.Candidates,
		)
	}
	changes := state.changes.snapshot()
	if latest := changes[len(changes)-1]; latest.plan.Candidates != 0 ||
		latest.result.Readiness != diagnose.ReadinessStale {
		t.Errorf(
			"latest change = %v/%v, want cleared Stale plan",
			latest.result.Readiness,
			latest.plan.Candidates,
		)
	}

	after := admit(t, observer, request, breaker.Pass)
	if completion := observer.Finish(after, breakerTestFailure); completion.Trip {
		t.Error("evidence from before the close reopened the breaker")
	}

	// Fresh evidence can open the breaker again. A close less than Probation
	// earlier doubles the cooldown.
	for range 21 {
		clock.Advance(2 * time.Second)
		observer.Finish(observer.Begin(request), breakerTestFailure)
	}
	openBreaker(t, observer, clock, request)
	clock.Advance(2*breaker.BaseCooldown - time.Nanosecond)
	admit(t, observer, request, breaker.Reject)
	clock.Advance(time.Nanosecond)
	admit(t, observer, request, breaker.Probe)
}

func TestFailedProbeReopensWithLongerCooldown(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(24_000, 0))
	observer := NewWithClock(clock.Now)
	request, state := recordDownTarget(t, observer, clock, "down.example")
	openBreaker(t, observer, clock, request)

	clock.Advance(breaker.BaseCooldown)
	probe := admit(t, observer, request, breaker.Probe)
	completion := observer.Finish(probe, Result{
		StatusCode:  httpStatusTooManyRequests,
		HasResponse: true,
	})
	if completion.Trip {
		t.Error("failed probe reported Trip")
	}
	if !state.engaged.Load() || breakerState(state) != breaker.Open {
		t.Fatalf(
			"after a failed probe engaged = %t, state = %v, want engaged Open",
			state.engaged.Load(),
			breakerState(state),
		)
	}

	clock.Advance(2*breaker.BaseCooldown - time.Nanosecond)
	admit(t, observer, request, breaker.Reject)
	clock.Advance(time.Nanosecond)
	admit(t, observer, request, breaker.Probe)
}

func TestExpiredProbeLeaseAllowsAnotherProbe(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(25_000, 0))
	observer := NewWithClock(clock.Now)
	request, state := recordDownTarget(t, observer, clock, "down.example")
	openBreaker(t, observer, clock, request)

	clock.Advance(breaker.BaseCooldown)
	hung := admit(t, observer, request, breaker.Probe)
	clock.Advance(breaker.ProbeTimeout - time.Nanosecond)
	admit(t, observer, request, breaker.Reject)

	// The expired lease counts as a failed probe, which doubles the cooldown.
	clock.Advance(time.Nanosecond)
	admit(t, observer, request, breaker.Reject)
	clock.Advance(2 * breaker.BaseCooldown)
	replacement := admit(t, observer, request, breaker.Probe)
	if replacement.probe == hung.probe {
		t.Fatalf("replacement lease = %d, want a new lease", replacement.probe)
	}

	observer.Finish(hung, breakerTestFailure)
	if got := breakerState(state); got != breaker.Probing {
		t.Fatalf("state after a late failed probe = %v, want Probing", got)
	}

	observer.Finish(hung, breakerTestSuccess)
	if state.engaged.Load() || breakerState(state) != breaker.Closed {
		t.Fatal("late answered probe did not close the breaker")
	}

	observer.Finish(replacement, breakerTestFailure)
	if state.engaged.Load() || breakerState(state) != breaker.Closed {
		t.Error("probe result after the close changed the breaker")
	}
}

func TestCanceledProbeReopensWithoutEscalating(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(26_000, 0))
	observer := NewWithClock(clock.Now)
	request, state := recordDownTarget(t, observer, clock, "down.example")
	openBreaker(t, observer, clock, request)

	clock.Advance(breaker.BaseCooldown)
	probeContext, cancel := context.WithCancel(context.Background())
	canceledRequest := request
	canceledRequest.Context = probeContext
	probe := admit(t, observer, canceledRequest, breaker.Probe)
	cancel()
	observer.Finish(probe, Result{Err: context.Canceled})
	if !state.engaged.Load() || breakerState(state) != breaker.Open {
		t.Fatalf("state after a canceled probe = %v, want Open", breakerState(state))
	}

	clock.Advance(breaker.BaseCooldown - time.Nanosecond)
	admit(t, observer, request, breaker.Reject)
	clock.Advance(time.Nanosecond)
	failed := admit(t, observer, request, breaker.Probe)

	// The canceled probe did not escalate, so a failure now doubles the base
	// cooldown only once.
	observer.Finish(failed, breakerTestFailure)
	clock.Advance(2*breaker.BaseCooldown - time.Nanosecond)
	admit(t, observer, request, breaker.Reject)
	clock.Advance(time.Nanosecond)
	admit(t, observer, request, breaker.Probe)
}

func TestCanceledRequestsAreNeverProbes(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(27_000, 0))
	observer := NewWithClock(clock.Now)
	request, _ := recordDownTarget(t, observer, clock, "down.example")
	openBreaker(t, observer, clock, request)
	clock.Advance(breaker.BaseCooldown)

	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	canceled := request
	canceled.Context = canceledContext
	admit(t, observer, canceled, breaker.Reject)

	closed := make(chan struct{})
	close(closed)
	abandoned := request
	abandoned.Cancel = closed
	admit(t, observer, abandoned, breaker.Reject)

	admit(t, observer, request, breaker.Probe)
}

func TestRetiredTargetBypassesItsBreaker(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(28_000, 0))
	observer := NewWithClock(clock.Now)
	request, state := recordDownTarget(t, observer, clock, "down.example")
	openBreaker(t, observer, clock, request)
	clock.Advance(breaker.BaseCooldown)
	probe := admit(t, observer, request, breaker.Probe)

	state.retire()
	observer.Finish(probe, breakerTestSuccess)
	if !state.engaged.Load() || breakerState(state) != breaker.Probing {
		t.Error("a probe settled the breaker of a retired target")
	}

	token := admit(t, observer, request, breaker.Pass)
	if !token.admitted {
		t.Error("retired target admission was not marked admitted")
	}
	if completion := observer.Finish(token, breakerTestFailure); completion.Trip {
		t.Error("failure on a retired target reported Trip")
	}
}

func TestClosingFencesEvidenceRecordedBeforeTheClose(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(29_000, 0))
	observer := NewWithClock(clock.Now)
	request, state := recordDownTarget(t, observer, clock, "down.example")
	clock.Advance(2 * time.Second)
	straggler := admit(t, observer, request, breaker.Pass)
	openBreaker(t, observer, clock, request)

	clock.Advance(breaker.BaseCooldown)
	probe := admit(t, observer, request, breaker.Probe)
	probeTick := observer.tick(clock.Now())

	// A failure admitted before the breaker opened completes after the probe
	// read its clock but before the probe settles.
	clock.Advance(time.Second)
	if completion := observer.Finish(straggler, breakerTestFailure); completion.Trip {
		t.Error("straggler completed while probing reported Trip")
	}
	fenceTick := observer.tick(clock.Now())

	recorded, trip := state.complete(
		probeTick,
		observation{outcome: outcomeSuccess, signal: diagnosisNeutral},
		probe.probe,
		false,
	)
	if !recorded || trip {
		t.Fatalf("probe complete() = %t, %t, want recorded without trip", recorded, trip)
	}
	if breakerState(state) != breaker.Closed {
		t.Fatal("answered probe did not close the breaker")
	}

	current, _ := state.published()
	if current.result.EvaluatedAt != fenceTick ||
		current.result.Readiness != diagnose.ReadinessStale ||
		current.plan.Candidates != 0 {
		t.Errorf(
			"fenced evaluation = %v at %v, candidates %v, want Stale at %v without candidates",
			current.result.Readiness,
			time.Duration(current.result.EvaluatedAt),
			current.plan.Candidates,
			time.Duration(fenceTick),
		)
	}
	if snapshot := state.snapshotAt(fenceTick); snapshot.Recent.Attempts != 0 {
		t.Errorf("recent attempts after the fence = %d, want 0", snapshot.Recent.Attempts)
	}

	after := admit(t, observer, request, breaker.Pass)
	if completion := observer.Finish(after, breakerTestFailure); completion.Trip {
		t.Error("evidence in the fenced bucket reopened the breaker")
	}
}

func TestCallerErrIncludesTheCancelChannel(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, stop := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer stop()
	closed := make(chan struct{})
	close(closed)

	tests := []struct {
		want  error
		name  string
		token Token
	}{
		{name: "empty", token: Token{}},
		{
			name:  "open channel",
			token: Token{context: context.Background(), cancel: make(chan struct{})},
		},
		{name: "canceled context", token: Token{context: canceled}, want: context.Canceled},
		{
			name:  "expired context",
			token: Token{context: expired, cancel: closed},
			want:  context.DeadlineExceeded,
		},
		{name: "closed channel", token: Token{cancel: closed}, want: context.Canceled},
	}
	for _, test := range tests {
		if got := test.token.callerErr(); !errors.Is(got, test.want) {
			t.Errorf("%s callerErr() = %v, want %v", test.name, got, test.want)
		}
	}
}

func TestObserverClassifiesClosedCancelChannelAsCallerOwned(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(30_000, 0))
	observer := NewWithClock(clock.Now)
	closed := make(chan struct{})
	close(closed)
	token := observer.Begin(Request{
		Cancel:   closed,
		Method:   "GET",
		Scheme:   "https",
		Hostname: "example.com",
	})

	completion := observer.Finish(token, Result{Err: errors.New("request canceled")})
	if completion.DependencyFailure {
		t.Error("abandoned request counted as a dependency failure")
	}
	bucket := copyBucket(token.target, 0)
	if bucket.outcomes[outcomeCanceled] != 1 {
		t.Errorf("canceled outcomes = %d, want 1", bucket.outcomes[outcomeCanceled])
	}
}

func TestProbeOutcomeTreatsAnyAnswerAsHealthy(t *testing.T) {
	t.Parallel()

	tests := map[diagnosisSignal]breaker.Outcome{
		diagnosisNeutral:           breaker.Healthy,
		diagnosisClientFailure:     breaker.Healthy,
		diagnosisCallerOwned:       breaker.Inconclusive,
		diagnosisRateLimited:       breaker.Failed,
		diagnosisDependencyFailure: breaker.Failed,
	}
	for signal, want := range tests {
		if got := probeOutcome(signal); got != want {
			t.Errorf("probeOutcome(%d) = %v, want %v", signal, got, want)
		}
	}
}

func TestCandidatesChangedIgnoresRenewalsAndEmptySets(t *testing.T) {
	t.Parallel()

	previous := diagnosisState{
		result: diagnose.Result{Class: diagnose.ClassDependencyDown},
		plan:   policy.Plan{Candidates: policy.CandidateBreakerOpen},
	}
	tests := []struct {
		name       string
		class      diagnose.Class
		candidates policy.Candidate
		want       bool
	}{
		{
			name:       "renewal",
			class:      diagnose.ClassDependencyDown,
			candidates: policy.CandidateBreakerOpen,
		},
		{
			name:       "same set with a new class",
			class:      diagnose.ClassSaturation,
			candidates: policy.CandidateBreakerOpen,
			want:       true,
		},
		{name: "cleared", class: diagnose.ClassNone, want: true},
		{
			name:       "different set",
			class:      diagnose.ClassTransient,
			candidates: policy.CandidateRetry,
			want:       true,
		},
	}
	for _, test := range tests {
		got := candidatesChanged(
			previous,
			diagnose.Result{Class: test.class},
			policy.Plan{Candidates: test.candidates},
		)
		if got != test.want {
			t.Errorf("%s candidatesChanged() = %t, want %t", test.name, got, test.want)
		}
	}

	empty := diagnosisState{result: diagnose.Result{Class: diagnose.ClassSaturation}}
	if candidatesChanged(empty, diagnose.Result{Class: diagnose.ClassNone}, policy.Plan{}) {
		t.Error("empty plans with different classes counted as a change")
	}
}

// recordDownTarget records 21 dependency failures two seconds apart without
// admission, as Observe does, so the target publishes a Ready DependencyDown
// plan with a BreakerOpen candidate.
func recordDownTarget(
	t *testing.T,
	observer *Observer,
	clock *observationClock,
	hostname string,
) (Request, *target) {
	t.Helper()

	request := testRequest("GET", "https", hostname, "")
	var token Token
	for index := range 21 {
		if index > 0 {
			clock.Advance(2 * time.Second)
		}

		token = observer.Begin(request)
		completion := observer.Finish(token, breakerTestFailure)
		if !completion.Recorded || completion.Trip {
			t.Fatalf("unadmitted failure %d completion = %+v", index, completion)
		}
	}
	if plan := publishedPlan(token); plan.Candidates != policy.CandidateBreakerOpen {
		t.Fatalf("down plan candidates = %v, want BreakerOpen", plan.Candidates)
	}

	return request, token.target
}

// openBreaker advances two seconds and completes one admitted dependency
// failure, which opens the target's breaker.
func openBreaker(
	t *testing.T,
	observer *Observer,
	clock *observationClock,
	request Request,
) {
	t.Helper()

	clock.Advance(2 * time.Second)
	token := admit(t, observer, request, breaker.Pass)
	if completion := observer.Finish(token, breakerTestFailure); !completion.Trip {
		t.Fatal("admitted failure on a BreakerOpen plan did not report Trip")
	}
	if !observer.Trip(token) {
		t.Fatal("Trip() = false, want true")
	}
}

func admit(
	t *testing.T,
	observer *Observer,
	request Request,
	want breaker.Admission,
) Token {
	t.Helper()

	token, got := observer.Admit(observer.Begin(request))
	if got != want {
		t.Fatalf("Admit() = %v, want %v", got, want)
	}

	return token
}

func breakerState(state *target) breaker.State {
	state.mu.Lock()
	defer state.mu.Unlock()

	return state.breaker.State()
}
