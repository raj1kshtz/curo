package curo

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/raj1kshtz/curo/internal/diagnose"
	"github.com/raj1kshtz/curo/internal/observe"
	"github.com/raj1kshtz/curo/internal/policy"
)

func TestReportPublishesCandidateChanges(t *testing.T) {
	t.Parallel()

	origin := time.Unix(9_000, 0)
	clock := newReportClock(origin)
	status := http.StatusServiceUnavailable
	base := internalRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Body: http.NoBody}, nil
	})
	transport := newClockedTransport(t, base, clock.Now)
	request := newInternalReportRequest(t, "https://api.example/items/123")

	for range 11 {
		roundTripInternal(t, transport, request)
		clock.Advance(2 * time.Second)
	}

	gated := transport.Report()
	if len(gated.Changes) != 0 || len(gated.Targets) != 1 {
		t.Fatalf("readiness-gated report = %+v, want one target and no changes", gated)
	}
	assertDecision(t, "gated", gated.Targets[0], ReadinessWarming, DiagnosisDependencyDown, 0)
	wantGatedReasons := []Reason{
		ReasonInsufficientEvidence,
		ReasonDependencyFailureRate,
		ReasonReadinessRequired,
	}
	if !slices.Equal(gated.Targets[0].Reasons, wantGatedReasons) {
		t.Errorf("gated Reasons = %v, want %v", gated.Targets[0].Reasons, wantGatedReasons)
	}

	for range 10 {
		roundTripInternal(t, transport, request)
		clock.Advance(2 * time.Second)
	}

	down := transport.Report()
	if len(down.Targets) != 1 || len(down.Changes) != 1 {
		t.Fatalf("dependency-down report = %+v, want one target and one change", down)
	}
	decision := down.Targets[0]
	assertDecision(t, "down", decision, ReadinessReady, DiagnosisDependencyDown, CandidateBreakerOpen)
	wantDownReasons := []Reason{ReasonRecentEvidenceReady, ReasonDependencyFailureRate}
	if !slices.Equal(decision.Reasons, wantDownReasons) {
		t.Errorf("down Reasons = %v, want %v", decision.Reasons, wantDownReasons)
	}
	wantEvidence := Evidence{
		Attempts:           21,
		DependencyFailures: 21,
		Span:               40 * time.Second,
	}
	if decision.Recent != wantEvidence {
		t.Errorf("Recent = %+v, want %+v", decision.Recent, wantEvidence)
	}
	if want := origin.Add(40 * time.Second); !decision.EvaluatedAt.Equal(want) {
		t.Errorf("EvaluatedAt = %v, want %v", decision.EvaluatedAt, want)
	}
	if want := origin.Add(50 * time.Second); !decision.ExpiresAt.Equal(want) {
		t.Errorf("ExpiresAt = %v, want %v", decision.ExpiresAt, want)
	}
	if decision.PolicyVersion != policy.Version {
		t.Errorf("PolicyVersion = %d, want %d", decision.PolicyVersion, policy.Version)
	}
	if down.Changes[0].Sequence != 1 || !reflect.DeepEqual(down.Changes[0].Decision, decision) {
		t.Errorf("change = %+v, want sequence 1 for %+v", down.Changes[0], decision)
	}

	status = http.StatusOK
	clock.Set(origin.Add(10 * time.Minute))
	roundTripInternal(t, transport, request)

	recovered := transport.Report()
	if len(recovered.Targets) != 1 || len(recovered.Changes) != 2 {
		t.Fatalf("recovered report = %+v, want one target and two changes", recovered)
	}
	cleared := recovered.Changes[1]
	if cleared.Sequence != 2 {
		t.Errorf("cleared Sequence = %d, want 2", cleared.Sequence)
	}
	assertDecision(t, "cleared", cleared.Decision, ReadinessWarming, DiagnosisNone, 0)
	if !reflect.DeepEqual(recovered.Targets[0], cleared.Decision) {
		t.Errorf(
			"current decision = %+v, want latest change %+v",
			recovered.Targets[0],
			cleared.Decision,
		)
	}
	if stats := transport.Stats(); stats.InternalFailures != 0 {
		t.Errorf("InternalFailures = %d, want 0", stats.InternalFailures)
	}
}

func TestReportContainsInternalFailure(t *testing.T) {
	t.Parallel()

	transport := newInternalReportTransport(t)
	transport.reports = func() observe.Report {
		panic("report failure")
	}

	if report := transport.Report(); report.Targets != nil || report.Changes != nil {
		t.Errorf("failed Report() = %+v, want empty", report)
	}
	stats := transport.Stats()
	if stats.InternalFailures != 1 || stats.SelfDisabled {
		t.Errorf("Stats() = %#v, want one contained failure", stats)
	}

	roundTripInternal(t, transport, newInternalReportRequest(t, "https://api.example"))
	if got := transport.Stats().ObservedRequests; got != 1 {
		t.Errorf("ObservedRequests after Report failure = %d, want 1", got)
	}

	transport.reports = nil
	if report := transport.Report(); report.Targets != nil || report.Changes != nil {
		t.Errorf("Report() without a source = %+v, want empty", report)
	}
	if got := transport.Stats().InternalFailures; got != 1 {
		t.Errorf("InternalFailures without a source = %d, want 1", got)
	}
}

func TestReportRemainsReadableAfterSelfDisable(t *testing.T) {
	t.Parallel()

	transport := newInternalReportTransport(t)
	request := newInternalReportRequest(t, "https://api.example")
	roundTripInternal(t, transport, request)
	before := transport.Report()
	if len(before.Targets) != 1 {
		t.Fatalf("targets before self-disable = %d, want 1", len(before.Targets))
	}

	transport.stages = testRequestStages{
		before: func(requestSnapshot, Mode) error {
			return errors.New("internal failure")
		},
		after: func(attemptResult) error {
			return nil
		},
	}
	for range 3 {
		roundTripInternal(t, transport, request)
	}
	if !transport.Stats().SelfDisabled {
		t.Fatal("transport did not self-disable")
	}

	after := transport.Report()
	if !reflect.DeepEqual(after, before) {
		t.Errorf("Report() after self-disable = %+v, want %+v", after, before)
	}
}

func TestReportConversionsUseExplicitMappings(t *testing.T) {
	t.Parallel()

	methods := map[observe.MethodClass]MethodClass{
		observe.MethodRead:      MethodRead,
		observe.MethodWrite:     MethodWrite,
		observe.MethodConnect:   MethodConnect,
		observe.MethodOther:     MethodOther,
		observe.MethodClass(99): MethodOther,
	}
	for internal, want := range methods {
		if got := methodClassFrom(internal); got != want {
			t.Errorf("methodClassFrom(%d) = %v, want %v", internal, got, want)
		}
	}

	readiness := map[diagnose.Readiness]Readiness{
		diagnose.ReadinessCold:    ReadinessCold,
		diagnose.ReadinessWarming: ReadinessWarming,
		diagnose.ReadinessReady:   ReadinessReady,
		diagnose.ReadinessStale:   ReadinessStale,
		diagnose.Readiness(99):    ReadinessCold,
	}
	for internal, want := range readiness {
		if got := readinessFrom(internal); got != want {
			t.Errorf("readinessFrom(%d) = %v, want %v", internal, got, want)
		}
	}

	diagnoses := map[diagnose.Class]Diagnosis{
		diagnose.ClassNone:           DiagnosisNone,
		diagnose.ClassHealthy:        DiagnosisHealthy,
		diagnose.ClassTransient:      DiagnosisTransient,
		diagnose.ClassDependencyDown: DiagnosisDependencyDown,
		diagnose.ClassSaturation:     DiagnosisSaturation,
		diagnose.ClassClientError:    DiagnosisClientError,
		diagnose.ClassDegrading:      DiagnosisDegrading,
		diagnose.Class(99):           DiagnosisNone,
	}
	for internal, want := range diagnoses {
		if got := diagnosisFrom(internal); got != want {
			t.Errorf("diagnosisFrom(%d) = %v, want %v", internal, got, want)
		}
	}

	reasons := map[diagnose.Reason]Reason{
		diagnose.ReasonNone:                       0,
		diagnose.ReasonNoEvidence:                 ReasonNoEvidence,
		diagnose.ReasonEvidenceStale:              ReasonEvidenceStale,
		diagnose.ReasonInsufficientEvidence:       ReasonInsufficientEvidence,
		diagnose.ReasonRecentEvidenceReady:        ReasonRecentEvidenceReady,
		diagnose.ReasonHistoricalBaselineReady:    ReasonHistoricalBaselineReady,
		diagnose.ReasonInsufficientRecentEvidence: ReasonInsufficientRecentEvidence,
		diagnose.ReasonClientFailureRate:          ReasonClientFailureRate,
		diagnose.ReasonRateLimitRate:              ReasonRateLimitRate,
		diagnose.ReasonDependencyFailureRate:      ReasonDependencyFailureRate,
		diagnose.ReasonFailureRateIncrease:        ReasonFailureRateIncrease,
		diagnose.ReasonLatencyIncrease:            ReasonLatencyIncrease,
		diagnose.ReasonIsolatedDependencyFailure:  ReasonIsolatedDependencyFailure,
		diagnose.ReasonWithinBaseline:             ReasonWithinBaseline,
		diagnose.Reason(99):                       0,
	}
	for internal, want := range reasons {
		if got := reasonFrom(internal); got != want {
			t.Errorf("reasonFrom(%d) = %v, want %v", internal, got, want)
		}
	}

	candidates := map[policy.Candidate]Candidates{
		0:                           0,
		policy.CandidateRetry:       CandidateRetry,
		policy.CandidateBreakerOpen: CandidateBreakerOpen,
		policy.CandidateTimeout:     CandidateTimeout,
		policy.CandidateRetry | policy.CandidateBreakerOpen: CandidateRetry |
			CandidateBreakerOpen,
		policy.CandidateBreakerOpen | policy.CandidateTimeout: CandidateBreakerOpen |
			CandidateTimeout,
		policy.Candidate(0x80): 0,
	}
	for internal, want := range candidates {
		if got := candidatesFrom(internal); got != want {
			t.Errorf("candidatesFrom(%d) = %v, want %v", internal, got, want)
		}
	}
}

func TestReasonsFromStaysBounded(t *testing.T) {
	t.Parallel()

	result := diagnose.Result{
		Reasons: [4]diagnose.Reason{
			diagnose.ReasonNoEvidence,
			diagnose.Reason(99),
			diagnose.ReasonWithinBaseline,
			diagnose.ReasonNone,
		},
		ReasonCount: 9,
	}
	got := reasonsFrom(result, policy.ReasonReadinessRequired)
	want := []Reason{
		ReasonNoEvidence,
		ReasonWithinBaseline,
		ReasonReadinessRequired,
	}
	if !slices.Equal(got, want) {
		t.Errorf("reasonsFrom() = %v, want %v", got, want)
	}
	if got := reasonsFrom(diagnose.Result{}, policy.ReasonNone); got != nil {
		t.Errorf("reasonsFrom(empty) = %v, want nil", got)
	}
}

func newClockedTransport(
	t testing.TB,
	base http.RoundTripper,
	now func() time.Time,
	options ...Option,
) *Transport {
	t.Helper()

	transport, err := New(base, options...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	cfg, err := applyOptions(options)
	if err != nil {
		t.Fatalf("applyOptions() error = %v", err)
	}
	observer := observe.NewWithClock(now, cfg.timeouts)
	transport.observer = observer
	transport.stages = newAdaptiveStages(
		observer,
		&transport.retries,
		&transport.breakers,
		&transport.timeouts,
		transport.authorized,
	)
	transport.reports = observer.Report

	return transport
}

func newInternalReportTransport(t *testing.T) *Transport {
	t.Helper()

	base := internalRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	})
	transport, err := New(base)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	return transport
}

func newInternalReportRequest(t *testing.T, rawURL string) *http.Request {
	t.Helper()

	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		rawURL,
		http.NoBody,
	)
	if err != nil {
		t.Fatalf("NewRequestWithContext(%q) error = %v", rawURL, err)
	}

	return request
}

func roundTripInternal(t *testing.T, transport *Transport, request *http.Request) {
	t.Helper()

	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	if closeErr := response.Body.Close(); closeErr != nil {
		t.Fatalf("response Body.Close() error = %v", closeErr)
	}
}

func assertDecision(
	t *testing.T,
	name string,
	decision Decision,
	readiness Readiness,
	diagnosis Diagnosis,
	candidates Candidates,
) {
	t.Helper()

	if decision.Readiness != readiness ||
		decision.Diagnosis != diagnosis ||
		decision.Candidates != candidates {
		t.Errorf(
			"%s decision = %v/%v/%v, want %v/%v/%v",
			name,
			decision.Readiness,
			decision.Diagnosis,
			decision.Candidates,
			readiness,
			diagnosis,
			candidates,
		)
	}
}

type reportClock struct {
	now time.Time
	mu  sync.Mutex
}

func newReportClock(now time.Time) *reportClock {
	return &reportClock{now: now}
}

func (clock *reportClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()

	return clock.now
}

func (clock *reportClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()

	clock.now = clock.now.Add(duration)
}

func (clock *reportClock) Set(now time.Time) {
	clock.mu.Lock()
	defer clock.mu.Unlock()

	clock.now = now
}
