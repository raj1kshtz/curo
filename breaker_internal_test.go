package curo

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raj1kshtz/curo/internal/breaker"
	"github.com/raj1kshtz/curo/internal/guard"
)

// openBreaker records 21 dependency failures in Observe, selects Enforce, and
// completes one more failure, which opens the target's dependency breaker.
func (harness *retryHarness) openBreaker() {
	harness.t.Helper()

	harness.record(21, http.StatusServiceUnavailable)
	assertDecision(
		harness.t,
		"down target",
		harness.transport.Report().Targets[0],
		ReadinessReady,
		DiagnosisDependencyDown,
		CandidateBreakerOpen,
	)
	harness.enforce()
	harness.expectStatus(http.StatusServiceUnavailable, http.StatusServiceUnavailable)
	harness.assertBreakerStats("open", 1, 0, 0)
}

// expectStatus sends one request whose base attempt returns status and
// requires want.
func (harness *retryHarness) expectStatus(status, want int) {
	harness.t.Helper()

	harness.script(respondStatus(status))
	response, err := harness.transport.RoundTrip(
		newInternalReportRequest(harness.t, retryTestURL),
	)
	if err != nil || response == nil || response.StatusCode != want {
		harness.t.Fatalf("RoundTrip() = %v, %v, want status %d", response, err, want)
	}
	closeTestBody(harness.t, response)
	if requests, _ := harness.recorded(); len(requests) != 1 {
		harness.t.Fatalf("base attempts = %d, want 1", len(requests))
	}
	harness.script()
}

// assertRejected requires request to fail fast without a base attempt.
func (harness *retryHarness) assertRejected(request *http.Request) {
	harness.t.Helper()

	harness.script()
	response, err := harness.transport.RoundTrip(request)
	if response != nil || !errors.Is(err, ErrBreakerOpen) {
		closeTestBody(harness.t, response)
		harness.t.Fatalf("RoundTrip() = %v, %v, want ErrBreakerOpen", response, err)
	}
	if requests, _ := harness.recorded(); len(requests) != 0 {
		harness.t.Fatalf("rejected request reached the base transport %d times", len(requests))
	}
}

func (harness *retryHarness) assertBreakerStats(
	name string,
	opens, probes, rejections uint64,
) {
	harness.t.Helper()

	stats := harness.transport.Stats()
	if stats.BreakerOpens != opens ||
		stats.BreakerProbes != probes ||
		stats.BreakerRejections != rejections {
		harness.t.Errorf(
			"%s breaker stats = %d opens, %d probes, %d rejections, want %d, %d, %d",
			name,
			stats.BreakerOpens,
			stats.BreakerProbes,
			stats.BreakerRejections,
			opens,
			probes,
			rejections,
		)
	}
}

func TestEnforceOpensBreakerAndFailsFast(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.openBreaker()
	observed := harness.transport.Stats().ObservedRequests

	body := newTrackedBody("payload")
	request := newRetryRequest(t, context.Background(), http.MethodGet, body)
	harness.assertRejected(request)
	if got := body.closes.Load(); got != 1 {
		t.Errorf("rejected request body closes = %d, want 1", got)
	}
	harness.assertRejected(&http.Request{
		Method: http.MethodGet,
		URL:    request.URL,
		Header: http.Header{},
	})

	stats := harness.transport.Stats()
	if stats.ObservedRequests != observed || stats.InternalFailures != 0 {
		t.Errorf(
			"ObservedRequests = %d, InternalFailures = %d, want %d and 0",
			stats.ObservedRequests,
			stats.InternalFailures,
			observed,
		)
	}
	harness.assertBreakerStats("rejections", 1, 0, 2)
	assertDecision(
		t,
		"open target",
		harness.transport.Report().Targets[0],
		ReadinessReady,
		DiagnosisDependencyDown,
		CandidateBreakerOpen,
	)
}

func TestBreakerRejectionContainsBodyClosePanic(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.openBreaker()

	body := &trackedBody{Reader: strings.NewReader("payload"), panics: true}
	harness.assertRejected(newRetryRequest(t, context.Background(), http.MethodGet, body))
	if got := body.closes.Load(); got != 1 {
		t.Errorf("rejected request body closes = %d, want 1", got)
	}
	if got := harness.transport.Stats().InternalFailures; got != 1 {
		t.Errorf("InternalFailures = %d, want 1", got)
	}
	harness.assertBreakerStats("close panic", 1, 0, 1)
}

func TestBreakerProbeClosesAfterCooldown(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.openBreaker()

	harness.clock.Advance(breaker.BaseCooldown - time.Nanosecond)
	harness.assertRejected(newInternalReportRequest(t, retryTestURL))

	harness.clock.Advance(time.Nanosecond)
	harness.expectStatus(http.StatusOK, http.StatusOK)
	harness.assertBreakerStats("probe", 1, 1, 1)
	assertDecision(
		t,
		"closed target",
		harness.transport.Report().Targets[0],
		ReadinessStale,
		DiagnosisNone,
		0,
	)

	harness.expectStatus(http.StatusServiceUnavailable, http.StatusServiceUnavailable)
	harness.assertBreakerStats("after close", 1, 1, 1)
}

func TestFailedProbeKeepsBreakerOpenLonger(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.openBreaker()

	harness.clock.Advance(breaker.BaseCooldown)
	harness.script(respondWith(nil, errRetryTestTransport))
	response, err := harness.transport.RoundTrip(newInternalReportRequest(t, retryTestURL))
	if response != nil || !errors.Is(err, errRetryTestTransport) {
		closeTestBody(t, response)
		t.Fatalf("failed probe = %v, %v, want the base error", response, err)
	}
	if requests, _ := harness.recorded(); len(requests) != 1 {
		t.Fatalf("failed probe base attempts = %d, want 1", len(requests))
	}

	harness.clock.Advance(2*breaker.BaseCooldown - time.Nanosecond)
	harness.assertRejected(newInternalReportRequest(t, retryTestURL))
	harness.clock.Advance(time.Nanosecond)
	harness.expectStatus(http.StatusOK, http.StatusOK)
	harness.assertBreakerStats("failed probe", 1, 2, 1)
	harness.assertRetryStats("failed probe", 0, 0, 0, 0)
}

func TestConcurrentRequestsAdmitExactlyOneProbe(t *testing.T) {
	t.Parallel()

	const workers = 16

	harness := newRetryHarness(t)
	harness.openBreaker()
	harness.clock.Advance(breaker.BaseCooldown)

	release := make(chan struct{})
	harness.mu.Lock()
	harness.fallback = func(request *http.Request) (*http.Response, error) {
		<-release
		return respondStatus(http.StatusOK)(request)
	}
	harness.mu.Unlock()

	requests := make([]*http.Request, workers)
	for index := range requests {
		requests[index] = newInternalReportRequest(t, retryTestURL)
	}
	results := make(chan error, workers)
	var group sync.WaitGroup
	for _, request := range requests {
		group.Add(1)
		go func() {
			defer group.Done()

			response, err := harness.transport.RoundTrip(request)
			if err == nil {
				closeTestBody(t, response)
			}
			results <- err
		}()
	}

	for range workers - 1 {
		if err := <-results; !errors.Is(err, ErrBreakerOpen) {
			t.Errorf("concurrent RoundTrip() error = %v, want ErrBreakerOpen", err)
		}
	}
	close(release)
	group.Wait()
	if err := <-results; err != nil {
		t.Errorf("probe RoundTrip() error = %v", err)
	}
	if attempts, _ := harness.recorded(); len(attempts) != 1 {
		t.Errorf("base attempts = %d, want one probe", len(attempts))
	}
	harness.assertBreakerStats("concurrent probe", 1, 1, workers-1)
}

func TestObserveNeverOpensBreaker(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.record(30, http.StatusServiceUnavailable)
	harness.assertBreakerStats("observe", 0, 0, 0)

	harness.enforce()
	harness.expectStatus(http.StatusOK, http.StatusOK)
	harness.assertBreakerStats("enforce success", 0, 0, 0)
	harness.expectStatus(http.StatusServiceUnavailable, http.StatusServiceUnavailable)
	harness.assertBreakerStats("enforce failure", 1, 0, 0)
}

func TestAuthorityRevocationPreventsOpening(t *testing.T) {
	t.Parallel()

	tests := []struct {
		revoke  func(*testing.T, *retryHarness)
		name    string
		restore bool
	}{
		{
			name: "mode change",
			revoke: func(t *testing.T, harness *retryHarness) {
				setMode(Observe)(t, harness)
				setMode(Enforce)(t, harness)
			},
			restore: true,
		},
		{
			name: "close",
			revoke: func(t *testing.T, harness *retryHarness) {
				if err := harness.transport.Close(); err != nil {
					t.Errorf("Close() error = %v", err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			harness := newRetryHarness(t)
			harness.record(21, http.StatusServiceUnavailable)
			harness.enforce()
			harness.script(func(*http.Request) (*http.Response, error) {
				test.revoke(t, harness)
				return respondStatus(http.StatusServiceUnavailable)(nil)
			})
			response, err := harness.transport.RoundTrip(newInternalReportRequest(t, retryTestURL))
			if err != nil || response.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("RoundTrip() = %v, %v, want the 503", response, err)
			}
			closeTestBody(t, response)
			harness.assertBreakerStats(test.name, 0, 0, 0)

			if test.restore {
				harness.expectStatus(
					http.StatusServiceUnavailable,
					http.StatusServiceUnavailable,
				)
				harness.assertBreakerStats("restored", 1, 0, 0)
			}
		})
	}
}

func TestProbeSettlesAfterSwitchToObserve(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.openBreaker()
	harness.clock.Advance(breaker.BaseCooldown)

	harness.script(func(*http.Request) (*http.Response, error) {
		setMode(Observe)(t, harness)
		return respondStatus(http.StatusOK)(nil)
	})
	response, err := harness.transport.RoundTrip(newInternalReportRequest(t, retryTestURL))
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("probe = %v, %v, want 200", response, err)
	}
	closeTestBody(t, response)
	harness.assertBreakerStats("probe", 1, 1, 0)
	assertDecision(
		t,
		"settled target",
		harness.transport.Report().Targets[0],
		ReadinessStale,
		DiagnosisNone,
		0,
	)

	setMode(Enforce)(t, harness)
	harness.expectStatus(http.StatusOK, http.StatusOK)
	harness.assertBreakerStats("closed", 1, 1, 0)
}

func TestOpenBreakerPersistsAcrossModeChanges(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.openBreaker()

	setMode(Observe)(t, harness)
	harness.expectStatus(http.StatusServiceUnavailable, http.StatusServiceUnavailable)
	setMode(Enforce)(t, harness)
	harness.assertRejected(newInternalReportRequest(t, retryTestURL))

	harness.clock.Advance(breaker.BaseCooldown)
	harness.expectStatus(http.StatusOK, http.StatusOK)
	harness.assertBreakerStats("persisted", 1, 1, 1)
}

func TestCanceledRequestIsRejectedInsteadOfProbed(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.openBreaker()
	harness.clock.Advance(breaker.BaseCooldown)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	harness.assertRejected(newRetryRequest(t, ctx, http.MethodGet, http.NoBody))
	harness.expectStatus(http.StatusOK, http.StatusOK)
	harness.assertBreakerStats("canceled", 1, 1, 1)
}

func TestSelfDisabledTransportBypassesOpenBreaker(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.openBreaker()
	for range 3 {
		harness.transport.guard.Run(guard.StagePreflight, func() error {
			return errRetryTestTransport
		})
	}
	if !harness.transport.Stats().SelfDisabled {
		t.Fatal("transport enabled after repeated internal failures")
	}

	harness.expectStatus(http.StatusServiceUnavailable, http.StatusServiceUnavailable)
	harness.assertBreakerStats("self-disabled", 1, 0, 0)
}

func TestRetryWithdrawnWhenBreakerOpensDuringBackoff(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.warm()
	harness.mu.Lock()
	harness.fallback = respondStatus(http.StatusServiceUnavailable)
	harness.mu.Unlock()

	var opening atomic.Bool
	harness.setOnWait(func(ctx context.Context, _ <-chan struct{}) bool {
		if !opening.CompareAndSwap(false, true) {
			return false
		}

		// Later failures turn the target's Transient diagnosis into a
		// DependencyDown diagnosis, and one of them opens the breaker.
		for range 40 {
			if harness.transport.Stats().BreakerOpens > 0 {
				break
			}
			harness.clock.Advance(time.Second)
			//nolint:contextcheck // RoundTrip takes its context from the request.
			response, err := harness.transport.RoundTrip(
				newRetryRequest(t, ctx, http.MethodGet, http.NoBody),
			)
			if err != nil {
				t.Errorf("nested RoundTrip() error = %v", err)
				continue
			}
			closeTestBody(t, response)
		}

		return ctx.Err() == nil
	})

	harness.script(respondStatus(http.StatusServiceUnavailable))
	response, err := harness.transport.RoundTrip(newInternalReportRequest(t, retryTestURL))
	if err != nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("RoundTrip() = %v, %v, want the first 503", response, err)
	}
	closeTestBody(t, response)
	if !opening.Load() {
		t.Fatal("the first 503 did not start a retry backoff")
	}

	stats := harness.transport.Stats()
	if stats.BreakerOpens != 1 || stats.RetryAttempts != 0 {
		t.Errorf(
			"BreakerOpens = %d, RetryAttempts = %d, want one open and no retry",
			stats.BreakerOpens,
			stats.RetryAttempts,
		)
	}
}

func TestHTTPClientSeesErrBreakerOpen(t *testing.T) {
	t.Parallel()

	var (
		mu       sync.Mutex
		status   = http.StatusServiceUnavailable
		received int
	)
	server := httptest.NewServer(http.HandlerFunc(
		func(response http.ResponseWriter, _ *http.Request) {
			mu.Lock()
			received++
			current := status
			mu.Unlock()

			response.WriteHeader(current)
		},
	))
	t.Cleanup(server.Close)

	clock := newReportClock(time.Unix(60_000, 0))
	transport := newClockedTransport(t, server.Client().Transport, clock.Now)
	// A Client.Timeout makes net/http set the deprecated Request.Cancel
	// channel for transports it does not recognize, including Curo.
	client := &http.Client{Transport: transport, Timeout: time.Minute}
	get := func() (int, error) {
		t.Helper()

		request, newErr := http.NewRequestWithContext(
			context.Background(),
			http.MethodGet,
			server.URL+"/items",
			nil,
		)
		if newErr != nil {
			t.Fatalf("NewRequestWithContext() error = %v", newErr)
		}
		response, doErr := client.Do(request)
		if doErr != nil {
			return 0, doErr
		}
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Errorf("response Body.Close() error = %v", closeErr)
		}

		return response.StatusCode, nil
	}

	for index := range 21 {
		if index > 0 {
			clock.Advance(2 * time.Second)
		}
		if got, err := get(); err != nil || got != http.StatusServiceUnavailable {
			t.Fatalf("warm request %d = %d, %v, want 503", index, got, err)
		}
	}
	if err := transport.SetMode(Enforce); err != nil {
		t.Fatalf("SetMode(Enforce) error = %v", err)
	}
	clock.Advance(2 * time.Second)
	if got, err := get(); err != nil || got != http.StatusServiceUnavailable {
		t.Fatalf("tripping request = %d, %v, want 503", got, err)
	}

	_, err := get()
	var urlErr *url.Error
	if !errors.Is(err, ErrBreakerOpen) || !errors.As(err, &urlErr) {
		t.Fatalf("rejected Do() error = %v, want *url.Error wrapping ErrBreakerOpen", err)
	}

	mu.Lock()
	hits := received
	status = http.StatusOK
	mu.Unlock()
	if hits != 22 {
		t.Errorf("server requests = %d, want 22 without the rejected request", hits)
	}

	clock.Advance(breaker.BaseCooldown)
	for index := range 2 {
		if got, err := get(); err != nil || got != http.StatusOK {
			t.Fatalf("request %d after the cooldown = %d, %v, want 200", index, got, err)
		}
	}

	stats := transport.Stats()
	if stats.BreakerOpens != 1 ||
		stats.BreakerProbes != 1 ||
		stats.BreakerRejections != 1 ||
		stats.InternalFailures != 0 {
		t.Errorf("Stats() = %+v, want one open, probe, and rejection", stats)
	}
}

func BenchmarkBreakerRejection(b *testing.B) {
	transport, request := openBreakerTransport(b)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		response, roundTripErr := transport.RoundTrip(request)
		if response != nil || !errors.Is(roundTripErr, ErrBreakerOpen) {
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			b.Fatalf("RoundTrip() = %v, %v, want ErrBreakerOpen", response, roundTripErr)
		}
	}
}
