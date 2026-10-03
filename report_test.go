package curo_test

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/raj1kshtz/curo"
)

func TestReportPublishesNormalizedWarmingTarget(t *testing.T) {
	t.Parallel()

	transport := newReportTransport(t)
	request := newReportRequest(
		t,
		"https://user-secret:pass-secret@API.Example.:443/private-path?token=query-secret#fragment-secret",
	)

	before := time.Now()
	roundTripAndClose(t, transport, request)
	report := transport.Report()

	if len(report.Changes) != 0 {
		t.Errorf("changes = %d, want 0", len(report.Changes))
	}
	if len(report.Targets) != 1 {
		t.Fatalf("targets = %d, want 1", len(report.Targets))
	}

	decision := report.Targets[0]
	wantTarget := curo.Target{
		Scheme: "https",
		Host:   "api.example",
		Port:   443,
		Method: curo.MethodRead,
	}
	if decision.Target != wantTarget {
		t.Errorf("Target = %+v, want %+v", decision.Target, wantTarget)
	}
	if decision.Readiness != curo.ReadinessWarming ||
		decision.Diagnosis != curo.DiagnosisNone ||
		decision.Candidates != 0 {
		t.Errorf(
			"decision = %v/%v/%v, want Warming/None/None",
			decision.Readiness,
			decision.Diagnosis,
			decision.Candidates,
		)
	}
	wantReasons := []curo.Reason{
		curo.ReasonInsufficientEvidence,
		curo.ReasonInsufficientRecentEvidence,
	}
	if !slices.Equal(decision.Reasons, wantReasons) {
		t.Errorf("Reasons = %v, want %v", decision.Reasons, wantReasons)
	}
	if decision.PolicyVersion != 3 {
		t.Errorf("PolicyVersion = %d, want 3", decision.PolicyVersion)
	}
	if decision.Recent != (curo.Evidence{Attempts: 1}) {
		t.Errorf("Recent = %+v, want one attempt", decision.Recent)
	}
	if decision.Historical != (curo.Evidence{}) {
		t.Errorf("Historical = %+v, want empty", decision.Historical)
	}
	if decision.Latency.Samples != 1 || decision.Latency.Slowest <= 0 {
		t.Errorf("Latency = %+v, want one sample", decision.Latency)
	}
	if decision.Timeout != 0 {
		t.Errorf("Timeout = %v, want 0", decision.Timeout)
	}
	if decision.EvaluatedAt.Before(before) ||
		decision.ExpiresAt.Before(decision.EvaluatedAt) {
		t.Errorf(
			"EvaluatedAt = %v, ExpiresAt = %v, want ordered times after %v",
			decision.EvaluatedAt,
			decision.ExpiresAt,
			before,
		)
	}

	rendered := fmt.Sprintf("%+v", report)
	for _, private := range []string{"secret", "private", "API.Example"} {
		if strings.Contains(rendered, private) {
			t.Errorf("report contains %q: %s", private, rendered)
		}
	}
}

func TestReportRemainsReadableAcrossModesAndClose(t *testing.T) {
	t.Parallel()

	transport := newReportTransport(t, curo.WithMode(curo.Off))
	request := newReportRequest(t, "https://example.com/items")

	roundTripAndClose(t, transport, request)
	if report := transport.Report(); report.Targets != nil || report.Changes != nil {
		t.Fatalf("Off report = %+v, want empty", report)
	}

	if err := transport.SetMode(curo.Observe); err != nil {
		t.Fatalf("SetMode(Observe) error = %v", err)
	}
	roundTripAndClose(t, transport, request)
	observed := transport.Report()
	if len(observed.Targets) != 1 {
		t.Fatalf("Observe targets = %d, want 1", len(observed.Targets))
	}

	if err := transport.SetMode(curo.Off); err != nil {
		t.Fatalf("SetMode(Off) error = %v", err)
	}
	roundTripAndClose(t, transport, request)
	assertSameDecisions(t, "Off", transport.Report(), observed)

	if err := transport.SetMode(curo.Enforce); err != nil {
		t.Fatalf("SetMode(Enforce) error = %v", err)
	}
	if err := transport.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	roundTripAndClose(t, transport, request)
	assertSameDecisions(t, "closed", transport.Report(), observed)
}

func TestZeroAndNilTransportReportsAreEmpty(t *testing.T) {
	t.Parallel()

	var zero curo.Transport
	if report := zero.Report(); report.Targets != nil || report.Changes != nil {
		t.Errorf("zero Transport Report() = %+v, want empty", report)
	}

	var transport *curo.Transport
	if report := transport.Report(); report.Targets != nil || report.Changes != nil {
		t.Errorf("nil Transport Report() = %+v, want empty", report)
	}
}

func TestReportIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	const (
		writers    = 4
		readers    = 2
		iterations = 100
	)

	transport := newReportTransport(t)
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(writers + readers)

	for writer := range writers {
		go func() {
			defer wait.Done()
			<-start

			rawURL := fmt.Sprintf("https://api%d.example/items", writer)
			for range iterations {
				request, err := http.NewRequestWithContext(
					context.Background(),
					http.MethodGet,
					rawURL,
					nil,
				)
				if err != nil {
					t.Errorf("NewRequestWithContext() error = %v", err)
					return
				}
				response, err := transport.RoundTrip(request)
				if err != nil {
					t.Errorf("RoundTrip() error = %v", err)
					return
				}
				_ = response.Body.Close()
			}
		}()
	}
	for range readers {
		go func() {
			defer wait.Done()
			<-start

			for range iterations {
				report := transport.Report()
				if len(report.Targets) > writers {
					t.Errorf("targets = %d, want at most %d", len(report.Targets), writers)
					return
				}
			}
		}()
	}

	close(start)
	wait.Wait()

	if got := len(transport.Report().Targets); got != writers {
		t.Errorf("targets = %d, want %d", got, writers)
	}
	if got := transport.Stats().InternalFailures; got != 0 {
		t.Errorf("InternalFailures = %d, want 0", got)
	}
}

func TestReportValuesHaveStableNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		value fmt.Stringer
		want  string
	}{
		{value: curo.MethodRead, want: "Read"},
		{value: curo.MethodWrite, want: "Write"},
		{value: curo.MethodConnect, want: "Connect"},
		{value: curo.MethodOther, want: "Other"},
		{value: curo.MethodClass(9), want: "MethodClass(9)"},
		{value: curo.ReadinessCold, want: "Cold"},
		{value: curo.ReadinessWarming, want: "Warming"},
		{value: curo.ReadinessReady, want: "Ready"},
		{value: curo.ReadinessStale, want: "Stale"},
		{value: curo.Readiness(9), want: "Readiness(9)"},
		{value: curo.DiagnosisNone, want: "None"},
		{value: curo.DiagnosisHealthy, want: "Healthy"},
		{value: curo.DiagnosisTransient, want: "Transient"},
		{value: curo.DiagnosisDependencyDown, want: "DependencyDown"},
		{value: curo.DiagnosisSaturation, want: "Saturation"},
		{value: curo.DiagnosisClientError, want: "ClientError"},
		{value: curo.DiagnosisDegrading, want: "Degrading"},
		{value: curo.Diagnosis(9), want: "Diagnosis(9)"},
		{value: curo.Reason(0), want: "Reason(0)"},
		{value: curo.ReasonNoEvidence, want: "NoEvidence"},
		{value: curo.ReasonEvidenceStale, want: "EvidenceStale"},
		{value: curo.ReasonInsufficientEvidence, want: "InsufficientEvidence"},
		{value: curo.ReasonRecentEvidenceReady, want: "RecentEvidenceReady"},
		{value: curo.ReasonHistoricalBaselineReady, want: "HistoricalBaselineReady"},
		{value: curo.ReasonInsufficientRecentEvidence, want: "InsufficientRecentEvidence"},
		{value: curo.ReasonClientFailureRate, want: "ClientFailureRate"},
		{value: curo.ReasonRateLimitRate, want: "RateLimitRate"},
		{value: curo.ReasonDependencyFailureRate, want: "DependencyFailureRate"},
		{value: curo.ReasonFailureRateIncrease, want: "FailureRateIncrease"},
		{value: curo.ReasonLatencyIncrease, want: "LatencyIncrease"},
		{value: curo.ReasonIsolatedDependencyFailure, want: "IsolatedDependencyFailure"},
		{value: curo.ReasonWithinBaseline, want: "WithinBaseline"},
		{value: curo.ReasonReadinessRequired, want: "ReadinessRequired"},
		{value: curo.Reason(99), want: "Reason(99)"},
		{value: curo.Candidates(0), want: "None"},
		{value: curo.CandidateRetry, want: "Retry"},
		{value: curo.CandidateBreakerOpen, want: "BreakerOpen"},
		{value: curo.CandidateTimeout, want: "Timeout"},
		{
			value: curo.CandidateRetry | curo.CandidateBreakerOpen,
			want:  "Retry|BreakerOpen",
		},
		{
			value: curo.CandidateRetry | curo.CandidateBreakerOpen | curo.CandidateTimeout,
			want:  "Retry|BreakerOpen|Timeout",
		},
		{value: curo.CandidateTimeout | curo.Candidates(8), want: "Timeout|Candidates(8)"},
		{value: curo.Candidates(136), want: "Candidates(136)"},
	}

	for _, test := range tests {
		if got := test.value.String(); got != test.want {
			t.Errorf("%T(%v).String() = %q, want %q", test.value, test.value, got, test.want)
		}
	}
}

func TestCandidatesHas(t *testing.T) {
	t.Parallel()

	both := curo.CandidateRetry | curo.CandidateBreakerOpen
	tests := []struct {
		candidates curo.Candidates
		want       curo.Candidates
		has        bool
	}{
		{candidates: both, want: curo.CandidateRetry, has: true},
		{candidates: both, want: both, has: true},
		{candidates: curo.CandidateRetry, want: curo.CandidateBreakerOpen},
		{candidates: curo.CandidateRetry, want: both},
		{candidates: both, want: curo.CandidateTimeout},
		{candidates: both | curo.CandidateTimeout, want: curo.CandidateTimeout, has: true},
		{candidates: both, want: 0},
		{candidates: 0, want: 0},
	}

	for _, test := range tests {
		if got := test.candidates.Has(test.want); got != test.has {
			t.Errorf("%v.Has(%v) = %t, want %t", test.candidates, test.want, got, test.has)
		}
	}
}

func newReportTransport(t *testing.T, options ...curo.Option) *curo.Transport {
	t.Helper()

	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       http.NoBody,
		}, nil
	})
	transport, err := curo.New(base, options...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	return transport
}

func newReportRequest(t *testing.T, rawURL string) *http.Request {
	t.Helper()

	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		rawURL,
		nil,
	)
	if err != nil {
		t.Fatalf("NewRequestWithContext(%q) error = %v", rawURL, err)
	}

	return request
}

func assertSameDecisions(
	t *testing.T,
	state string,
	got curo.Report,
	want curo.Report,
) {
	t.Helper()

	if len(got.Targets) != len(want.Targets) || len(got.Changes) != len(want.Changes) {
		t.Fatalf("%s report = %+v, want %+v", state, got, want)
	}
	for index := range want.Targets {
		if got.Targets[index].Target != want.Targets[index].Target ||
			!got.Targets[index].EvaluatedAt.Equal(want.Targets[index].EvaluatedAt) {
			t.Errorf(
				"%s target %d = %+v, want %+v",
				state,
				index,
				got.Targets[index],
				want.Targets[index],
			)
		}
	}
}
