package observe

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raj1kshtz/curo/internal/diagnose"
	"github.com/raj1kshtz/curo/internal/policy"
)

func TestObserverReportPublishesOrderedDetachedDecisions(t *testing.T) {
	t.Parallel()

	origin := time.Unix(7_000, 0)
	clock := newObservationClock(origin)
	var clockReads atomic.Int64
	observer := NewWithClock(func() time.Time {
		clockReads.Add(1)
		return clock.Now()
	}, noTimeouts)

	finish := func(request Request, result Result) {
		observer.Finish(observer.Begin(request), result)
	}
	failed := Result{StatusCode: httpStatusServiceUnavailable, HasResponse: true}
	succeeded := Result{StatusCode: httpStatusOK, HasResponse: true}

	down := testRequest("GET", "https", "down.example", "")
	for range 20 {
		finish(down, failed)
		clock.Advance(2 * time.Second)
	}
	finish(down, failed)
	finish(testRequest("POST", "https", "down.example", ""), succeeded)
	finish(testRequest("GET", "http", "zeta.example", "8080"), succeeded)
	finish(testRequest("GET", "HTTPS", "Alpha.Example.", "443"), succeeded)
	_ = observer.Begin(testRequest("GET", "https", "pending.example", ""))
	finish(testRequest("GET", "ftp", "overflow.example", ""), failed)

	readsBefore := clockReads.Load()
	clock.Advance(10 * time.Minute)
	report := observer.Report()
	if got := clockReads.Load(); got != readsBefore {
		t.Errorf("Report read the clock %d times, want 0", got-readsBefore)
	}

	wantTargets := []Identity{
		{Scheme: "http", Host: "zeta.example", Port: 8080, Method: MethodRead},
		{Scheme: "https", Host: "alpha.example", Port: 443, Method: MethodRead},
		{Scheme: "https", Host: "down.example", Port: 443, Method: MethodRead},
		{Scheme: "https", Host: "down.example", Port: 443, Method: MethodWrite},
		{Scheme: "https", Host: "pending.example", Port: 443, Method: MethodRead},
	}
	if len(report.Targets) != len(wantTargets) {
		t.Fatalf("targets = %d, want %d", len(report.Targets), len(wantTargets))
	}
	for index, want := range wantTargets {
		if got := report.Targets[index].Identity; got != want {
			t.Errorf("target %d identity = %+v, want %+v", index, got, want)
		}
	}

	downDecision := report.Targets[2]
	evaluatedAt := origin.Add(40 * time.Second)
	if !downDecision.EvaluatedAt.Equal(evaluatedAt) {
		t.Errorf("EvaluatedAt = %v, want %v", downDecision.EvaluatedAt, evaluatedAt)
	}
	if want := evaluatedAt.Add(10 * time.Second); !downDecision.ExpiresAt.Equal(want) {
		t.Errorf("ExpiresAt = %v, want %v", downDecision.ExpiresAt, want)
	}
	if downDecision.Result.Readiness != diagnose.ReadinessReady ||
		downDecision.Result.Class != diagnose.ClassDependencyDown {
		t.Errorf(
			"down decision = %v/%v, want Ready/DependencyDown",
			downDecision.Result.Readiness,
			downDecision.Result.Class,
		)
	}
	if downDecision.Plan.Candidates != policy.CandidateBreakerOpen ||
		downDecision.Plan.Version != policy.Version {
		t.Errorf("down plan = %+v, want versioned BreakerOpen", downDecision.Plan)
	}
	wantEvidence := diagnose.Evidence{
		RelevantAttempts:   21,
		DependencyFailures: 21,
		Span:               int64(40 * time.Second),
	}
	if downDecision.Result.Recent != wantEvidence {
		t.Errorf("recent evidence = %+v, want %+v", downDecision.Result.Recent, wantEvidence)
	}

	pending := report.Targets[4]
	if !pending.EvaluatedAt.IsZero() || !pending.ExpiresAt.IsZero() ||
		pending.Result != (diagnose.Result{}) || pending.Plan != (policy.Plan{}) {
		t.Errorf("unevaluated decision = %+v, want zero evaluation", pending)
	}

	if len(report.Changes) != 1 {
		t.Fatalf("changes = %d, want 1", len(report.Changes))
	}
	change := report.Changes[0]
	if change.Sequence != 1 || change.Decision.Identity != wantTargets[2] ||
		!change.Decision.EvaluatedAt.Equal(evaluatedAt) ||
		change.Decision.Plan.Candidates != policy.CandidateBreakerOpen {
		t.Errorf("change = %+v, want BreakerOpen for down.example at 40s", change)
	}

	report.Targets[0].Identity.Host = "mutated.example"
	report.Changes[0].Sequence = 99
	again := observer.Report()
	if again.Targets[0].Identity.Host != "zeta.example" || again.Changes[0].Sequence != 1 {
		t.Error("report mutation reached observer state")
	}
	if !again.Targets[2].EvaluatedAt.Equal(evaluatedAt) {
		t.Error("Report evaluated an idle target")
	}
}

func TestObserverReportHandlesEmptyObservers(t *testing.T) {
	t.Parallel()

	var nilObserver *Observer
	if report := nilObserver.Report(); report.Targets != nil || report.Changes != nil {
		t.Errorf("nil observer report = %+v, want empty", report)
	}
	if report := (&Observer{}).Report(); report.Targets != nil || report.Changes != nil {
		t.Errorf("zero observer report = %+v, want empty", report)
	}
	if report := New(noTimeouts).Report(); report.Targets != nil || report.Changes != nil {
		t.Errorf("new observer report = %+v, want empty", report)
	}
}

func TestObserverReportSkipsTargetsRetiredDuringCollection(t *testing.T) {
	t.Parallel()

	observer := New(noTimeouts)
	_ = observer.Begin(testRequest("GET", "https", "api.example", ""))
	targets := observer.registry.regularTargets()
	if len(targets) != 1 {
		t.Fatalf("regular targets = %d, want 1", len(targets))
	}
	targets[0].retire()

	if report := observer.Report(); len(report.Targets) != 0 {
		t.Errorf("report targets = %d, want retired target skipped", len(report.Targets))
	}
}

func TestObserverReportIsCoherentUnderConcurrentCompletion(t *testing.T) {
	t.Parallel()

	const (
		writers    = 4
		readers    = 2
		iterations = 300
	)

	clock := newObservationClock(time.Unix(8_000, 0))
	observer := NewWithClock(clock.Now, noTimeouts)
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(writers + readers)

	for writer := range writers {
		go func() {
			defer wait.Done()
			<-start

			request := testRequest("GET", "https", "api.example", "")
			if writer%2 == 1 {
				request.Method = "POST"
			}
			for iteration := range iterations {
				result := Result{StatusCode: httpStatusOK, HasResponse: true}
				if (iteration/40)%2 == 0 {
					result.StatusCode = httpStatusServiceUnavailable
				}
				observer.Finish(observer.Begin(request), result)
				clock.Advance(500 * time.Millisecond)
			}
		}()
	}

	for range readers {
		go func() {
			defer wait.Done()
			<-start

			for range iterations {
				if message := reportIncoherence(observer.Report()); message != "" {
					t.Error(message)
					return
				}
			}
		}()
	}

	close(start)
	wait.Wait()

	if message := reportIncoherence(observer.Report()); message != "" {
		t.Error(message)
	}
}

func reportIncoherence(report Report) string {
	for index := 1; index < len(report.Targets); index++ {
		previous := report.Targets[index-1].Identity
		current := report.Targets[index].Identity
		if compareIdentity(previous, current) >= 0 {
			return "targets are not strictly ordered"
		}
	}

	for index, change := range report.Changes {
		if index > 0 && change.Sequence != report.Changes[index-1].Sequence+1 {
			return "change sequences are not contiguous"
		}
		for _, decision := range report.Targets {
			if decision.Identity == change.Decision.Identity &&
				decision.EvaluatedAt.Before(change.Decision.EvaluatedAt) {
				return "target decision is older than a retained change"
			}
		}
	}

	return ""
}

func compareIdentity(left, right Identity) int {
	if result := strings.Compare(left.Scheme, right.Scheme); result != 0 {
		return result
	}
	if result := strings.Compare(left.Host, right.Host); result != 0 {
		return result
	}
	if left.Port != right.Port {
		return int(left.Port) - int(right.Port)
	}

	return int(left.Method) - int(right.Method)
}

func testRequest(method, scheme, hostname, port string) Request {
	return Request{
		Context:  context.Background(),
		Method:   method,
		Scheme:   scheme,
		Hostname: hostname,
		Port:     port,
	}
}
