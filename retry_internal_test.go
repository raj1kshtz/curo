package curo

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raj1kshtz/curo/internal/retry"
)

const (
	retryTestURL   = "https://retry.example/items"
	retryTestDelay = 40 * time.Millisecond
)

var errRetryTestTransport = errors.New("connection reset")

type retryStep func(*http.Request) (*http.Response, error)

// firstAttemptStep scripts a first base attempt that needs the harness.
type firstAttemptStep func(*retryHarness) (*http.Response, error)

// retryHarness drives a Transport with a fake observer clock, a fixed retry
// delay, a recording wait, and a scripted base transport.
type retryHarness struct {
	t         *testing.T
	transport *Transport
	clock     *reportClock
	onWait    func(context.Context, <-chan struct{}) bool
	fallback  retryStep
	steps     []retryStep
	requests  []*http.Request
	waits     []time.Duration
	mu        sync.Mutex
}

func newRetryHarness(t *testing.T, options ...Option) *retryHarness {
	t.Helper()

	harness := &retryHarness{
		t:     t,
		clock: newReportClock(time.Unix(60_000, 0)),
	}
	harness.transport = newClockedTransport(
		t,
		internalRoundTripperFunc(harness.roundTrip),
		harness.clock.Now,
		options...,
	)
	stages := newAdaptiveStages(
		harness.transport.observer,
		&harness.transport.retries,
		&harness.transport.breakers,
		&harness.transport.timeouts,
		harness.transport.authorized,
	)
	stages.jitter = func() time.Duration { return retryTestDelay }
	harness.transport.stages = stages
	harness.transport.wait = harness.wait

	return harness
}

func (harness *retryHarness) roundTrip(request *http.Request) (*http.Response, error) {
	harness.mu.Lock()
	harness.requests = append(harness.requests, request)
	step := harness.fallback
	if len(harness.steps) > 0 {
		step = harness.steps[0]
		harness.steps = harness.steps[1:]
	}
	harness.mu.Unlock()

	if step == nil {
		harness.t.Error("unexpected base transport attempt")
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	}

	return step(request)
}

func (harness *retryHarness) wait(
	ctx context.Context,
	cancel <-chan struct{},
	delay time.Duration,
) bool {
	harness.mu.Lock()
	harness.waits = append(harness.waits, delay)
	onWait := harness.onWait
	harness.mu.Unlock()

	if onWait != nil {
		return onWait(ctx, cancel)
	}

	return ctx.Err() == nil && !retry.Canceled(cancel)
}

// script replaces the pending base transport steps and clears recorded
// attempts and waits.
func (harness *retryHarness) script(steps ...retryStep) {
	harness.mu.Lock()
	defer harness.mu.Unlock()

	harness.steps = steps
	harness.requests = nil
	harness.waits = nil
}

func (harness *retryHarness) setOnWait(onWait func(context.Context, <-chan struct{}) bool) {
	harness.mu.Lock()
	defer harness.mu.Unlock()

	harness.onWait = onWait
}

func (harness *retryHarness) recorded() ([]*http.Request, []time.Duration) {
	harness.mu.Lock()
	defer harness.mu.Unlock()

	return append([]*http.Request(nil), harness.requests...),
		append([]time.Duration(nil), harness.waits...)
}

// warm records 20 successes and one 503 two seconds apart in Observe mode, so
// the target publishes a Ready Transient decision with a Retry candidate and
// both retry budgets hold 2.1 tokens. It then selects Enforce.
func (harness *retryHarness) warm() {
	harness.t.Helper()

	harness.record(20, http.StatusOK)
	harness.record(1, http.StatusServiceUnavailable)
	report := harness.transport.Report()
	if len(report.Targets) != 1 {
		harness.t.Fatalf("warm report targets = %d, want 1", len(report.Targets))
	}
	assertDecision(
		harness.t,
		"warm target",
		report.Targets[0],
		ReadinessReady,
		DiagnosisTransient,
		CandidateRetry,
	)

	harness.enforce()
}

// record completes count initial attempts with status two seconds apart.
func (harness *retryHarness) record(count, status int) {
	harness.t.Helper()

	for range count {
		if harness.transport.Stats().ObservedRequests > 0 {
			harness.clock.Advance(2 * time.Second)
		}
		harness.script(respondStatus(status))
		roundTripInternal(
			harness.t,
			harness.transport,
			newInternalReportRequest(harness.t, retryTestURL),
		)
	}
	harness.script()
}

func (harness *retryHarness) enforce() {
	harness.t.Helper()

	if err := harness.transport.SetMode(Enforce); err != nil {
		harness.t.Fatalf("SetMode(Enforce) error = %v", err)
	}
	harness.clock.Advance(2 * time.Second)
}

// assertTwoRetriesFunded proves that a canceled reservation returned its
// token. After one canceled reservation the budgets hold 2.2 tokens, enough
// for two more retries. A lost token would leave enough for only one.
func (harness *retryHarness) assertTwoRetriesFunded() {
	harness.t.Helper()

	harness.setOnWait(nil)
	before := harness.transport.Stats()
	for index := range 2 {
		harness.script(
			respondStatus(http.StatusServiceUnavailable),
			respondStatus(http.StatusOK),
		)
		response, err := harness.transport.RoundTrip(
			newInternalReportRequest(harness.t, retryTestURL),
		)
		if err != nil || response == nil || response.StatusCode != http.StatusOK {
			harness.t.Fatalf("funded retry %d = %v, %v, want 200 response", index, response, err)
		}
		closeTestBody(harness.t, response)
	}

	after := harness.transport.Stats()
	if after.RetryAttempts-before.RetryAttempts != 2 ||
		after.RetryBudgetDenials != before.RetryBudgetDenials {
		harness.t.Fatalf(
			"funded retries = %d attempts, %d denials, want 2 attempts and no denials",
			after.RetryAttempts-before.RetryAttempts,
			after.RetryBudgetDenials-before.RetryBudgetDenials,
		)
	}
}

func (harness *retryHarness) assertRetryStats(
	name string,
	attempts, successes, denials, failures uint64,
) {
	harness.t.Helper()

	stats := harness.transport.Stats()
	if stats.RetryAttempts != attempts ||
		stats.RetrySuccesses != successes ||
		stats.RetryBudgetDenials != denials ||
		stats.InternalFailures != failures {
		harness.t.Errorf(
			"%s stats = %d attempts, %d successes, %d denials, %d failures, want %d, %d, %d, %d",
			name,
			stats.RetryAttempts,
			stats.RetrySuccesses,
			stats.RetryBudgetDenials,
			stats.InternalFailures,
			attempts,
			successes,
			denials,
			failures,
		)
	}
}

func respondStatus(status int) retryStep {
	return func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: http.NoBody}, nil
	}
}

func respondWith(response *http.Response, err error) retryStep {
	return func(*http.Request) (*http.Response, error) {
		return response, err
	}
}

type trackedBody struct {
	io.Reader
	closeErr error
	closes   atomic.Int32
	panics   bool
}

func newTrackedBody(content string) *trackedBody {
	return &trackedBody{Reader: strings.NewReader(content)}
}

func (body *trackedBody) Close() error {
	body.closes.Add(1)
	if body.panics {
		panic("body close panic")
	}

	return body.closeErr
}

func closeTestBody(t *testing.T, response *http.Response) {
	t.Helper()

	if response == nil || response.Body == nil {
		return
	}
	if err := response.Body.Close(); err != nil {
		t.Errorf("response Body.Close() error = %v", err)
	}
}

func newRetryRequest(
	t *testing.T,
	ctx context.Context,
	method string,
	body io.Reader,
) *http.Request {
	t.Helper()

	request, err := http.NewRequestWithContext(ctx, method, retryTestURL, body)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}

	return request
}

func TestEnforceRetriesTransientGatewayFailure(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.warm()

	firstBody := newTrackedBody("unavailable")
	first := &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Header:     http.Header{},
		Body:       firstBody,
	}
	second := &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}
	harness.script(respondWith(first, nil), respondWith(second, nil))

	request := newInternalReportRequest(t, retryTestURL)
	request.Header.Set("X-Request", "original")
	response, err := harness.transport.RoundTrip(request)
	if err != nil || response != second {
		t.Fatalf("RoundTrip() = %v, %v, want the retry response", response, err)
	}
	closeTestBody(t, response)
	if got := firstBody.closes.Load(); got != 1 {
		t.Errorf("discarded body closes = %d, want 1", got)
	}

	requests, waits := harness.recorded()
	if len(requests) != 2 || requests[0] != request {
		t.Fatalf("base attempts = %d, want the original request then one retry", len(requests))
	}
	retried := requests[1]
	if retried == request ||
		retried.Method != request.Method ||
		retried.URL.String() != request.URL.String() ||
		retried.Context() != request.Context() ||
		retried.Header.Get("X-Request") != "original" {
		t.Errorf("retry request = %+v, want a clone of the original request", retried)
	}
	retried.Header.Set("X-Request", "changed")
	if got := request.Header.Get("X-Request"); got != "original" {
		t.Errorf("original request header = %q after retry clone change, want original", got)
	}
	if len(waits) != 1 || waits[0] != retryTestDelay {
		t.Errorf("waits = %v, want one %v backoff", waits, retryTestDelay)
	}

	harness.assertRetryStats("retried request", 1, 1, 0, 0)
	if got := harness.transport.Stats().ObservedRequests; got != 22 {
		t.Errorf("ObservedRequests = %d, want 22 initial attempts only", got)
	}
}

func TestEnforceRetriesTransportError(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.warm()

	second := &http.Response{StatusCode: http.StatusServiceUnavailable, Body: http.NoBody}
	harness.script(respondWith(nil, errRetryTestTransport), respondWith(second, nil))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	response, err := harness.transport.RoundTrip(
		newRetryRequest(t, ctx, http.MethodHead, http.NoBody),
	)
	if err != nil || response != second {
		t.Fatalf("RoundTrip() = %v, %v, want the retry response", response, err)
	}
	closeTestBody(t, response)

	harness.assertRetryStats("retried transport error", 1, 0, 0, 0)
}

func TestEnforceReplaysBodyFromGetBody(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.warm()

	var reads []string
	readBody := func(status int) retryStep {
		return func(request *http.Request) (*http.Response, error) {
			content, err := io.ReadAll(request.Body)
			if err != nil {
				t.Errorf("read request body error = %v", err)
			}
			reads = append(reads, string(content))
			return respondStatus(status)(request)
		}
	}
	harness.script(readBody(http.StatusBadGateway), readBody(http.StatusOK))

	request := newRetryRequest(t, context.Background(), http.MethodGet, strings.NewReader("query"))
	getBody := request.GetBody
	var getBodyCalls atomic.Int32
	request.GetBody = func() (io.ReadCloser, error) {
		getBodyCalls.Add(1)
		return getBody()
	}

	response, err := harness.transport.RoundTrip(request)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("RoundTrip() = %v, %v, want 200 response", response, err)
	}
	closeTestBody(t, response)
	if len(reads) != 2 || reads[0] != "query" || reads[1] != "query" {
		t.Errorf("request bodies = %q, want the body twice", reads)
	}
	if got := getBodyCalls.Load(); got != 1 {
		t.Errorf("GetBody calls = %d, want 1", got)
	}
}

func TestEnforceRetriesThroughHTTPServer(t *testing.T) {
	t.Parallel()

	var (
		mu       sync.Mutex
		statuses []int
		bodies   []string
	)
	server := httptest.NewServer(http.HandlerFunc(
		func(response http.ResponseWriter, request *http.Request) {
			content, readErr := io.ReadAll(request.Body)
			if readErr != nil {
				t.Errorf("server request body read error = %v", readErr)
			}

			mu.Lock()
			bodies = append(bodies, string(content))
			status := http.StatusOK
			if len(statuses) > 0 {
				status = statuses[0]
				statuses = statuses[1:]
			}
			mu.Unlock()

			response.WriteHeader(status)
			_, _ = io.WriteString(response, http.StatusText(status))
		},
	))
	t.Cleanup(server.Close)

	clock := newReportClock(time.Unix(60_000, 0))
	transport := newClockedTransport(t, server.Client().Transport, clock.Now)
	// A Client.Timeout makes net/http set the deprecated Request.Cancel
	// channel for transports it does not recognize, including Curo.
	client := &http.Client{Transport: transport, Timeout: time.Minute}
	get := func(body string, script ...int) (int, string) {
		t.Helper()

		mu.Lock()
		statuses = script
		mu.Unlock()

		request, newErr := http.NewRequestWithContext(
			context.Background(),
			http.MethodGet,
			server.URL+"/items",
			strings.NewReader(body),
		)
		if newErr != nil {
			t.Fatalf("NewRequestWithContext() error = %v", newErr)
		}
		response, doErr := client.Do(request)
		if doErr != nil {
			t.Fatalf("Do() error = %v", doErr)
		}
		content, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			t.Errorf("response body read error = %v", readErr)
		}
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Errorf("response Body.Close() error = %v", closeErr)
		}

		return response.StatusCode, string(content)
	}

	for index := range 21 {
		if index > 0 {
			clock.Advance(2 * time.Second)
		}
		status := http.StatusOK
		if index == 20 {
			status = http.StatusServiceUnavailable
		}
		if got, _ := get("", status); got != status {
			t.Fatalf("warm request %d status = %d, want %d", index, got, status)
		}
	}
	report := transport.Report()
	if len(report.Targets) != 1 {
		t.Fatalf("report targets = %d, want 1", len(report.Targets))
	}
	assertDecision(
		t,
		"server target",
		report.Targets[0],
		ReadinessReady,
		DiagnosisTransient,
		CandidateRetry,
	)

	if err := transport.SetMode(Enforce); err != nil {
		t.Fatalf("SetMode(Enforce) error = %v", err)
	}
	clock.Advance(2 * time.Second)
	status, content := get("query", http.StatusServiceUnavailable, http.StatusOK)
	if status != http.StatusOK || content != http.StatusText(http.StatusOK) {
		t.Fatalf("retried request = %d %q, want the retry's 200 response", status, content)
	}

	mu.Lock()
	received := append([]string(nil), bodies...)
	mu.Unlock()
	if len(received) != 23 || !slices.Equal(received[21:], []string{"query", "query"}) {
		t.Errorf("server requests = %d, last bodies = %q, want 23 ending with two query bodies",
			len(received), received[max(len(received)-2, 0):])
	}

	stats := transport.Stats()
	if stats.ObservedRequests != 22 ||
		stats.RetryAttempts != 1 ||
		stats.RetrySuccesses != 1 ||
		stats.RetryBudgetDenials != 0 ||
		stats.InternalFailures != 0 {
		t.Errorf("Stats() = %+v, want 22 observed requests and one successful retry", stats)
	}
}

func TestEnforceRetryKeepsOpenCancelChannel(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.warm()
	harness.script(
		respondStatus(http.StatusServiceUnavailable),
		respondStatus(http.StatusOK),
	)

	request := newInternalReportRequest(t, retryTestURL)
	//nolint:staticcheck // net/http sets the deprecated Cancel channel for a Client.Timeout.
	request.Cancel = make(chan struct{})
	response, err := harness.transport.RoundTrip(request)
	if err != nil || response == nil || response.StatusCode != http.StatusOK {
		t.Fatalf("RoundTrip() = %v, %v, want the retry's 200 response", response, err)
	}
	closeTestBody(t, response)

	requests, _ := harness.recorded()
	if len(requests) != 2 || requests[1] == request {
		t.Fatalf("base attempts = %d, want the request and a retry clone", len(requests))
	}
	//nolint:staticcheck // The retry clone must keep observing the deprecated Cancel channel.
	if requests[1].Cancel != request.Cancel {
		t.Error("retry clone dropped the request's Cancel channel")
	}
	harness.assertRetryStats("open cancel channel", 1, 1, 0, 0)
}

func TestEnforceKeepsRetryFailureResult(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.warm()

	retryErr := errors.New("retry failed")
	harness.script(
		respondStatus(http.StatusGatewayTimeout),
		respondWith(nil, retryErr),
	)
	response, err := harness.transport.RoundTrip(newInternalReportRequest(t, retryTestURL))
	if response != nil || !errors.Is(err, retryErr) {
		t.Fatalf("RoundTrip() = %v, %v, want the retry error", response, err)
	}
	closeTestBody(t, response)

	harness.assertRetryStats("failed retry", 1, 0, 0, 0)
}

func TestEnforceReturnsFirstResultWhenRetryIsIneligible(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		request func(*testing.T) *http.Request
		first   firstAttemptStep
		setup   func(*testing.T, *retryHarness)
		name    string
	}{
		{
			name:  "observe mode",
			setup: setMode(Observe),
		},
		{
			name: "unsafe method",
			request: func(t *testing.T) *http.Request {
				return newRetryRequest(t, context.Background(), http.MethodPost, http.NoBody)
			},
		},
		{
			name: "body without GetBody",
			request: func(t *testing.T) *http.Request {
				request := newRetryRequest(t, context.Background(), http.MethodGet, strings.NewReader("query"))
				request.GetBody = nil
				return request
			},
		},
		{
			name: "closed cancel channel",
			request: func(t *testing.T) *http.Request {
				cancel := make(chan struct{})
				close(cancel)
				request := newInternalReportRequest(t, retryTestURL)
				//nolint:staticcheck // net/http sets the deprecated Cancel channel for a Client.Timeout.
				request.Cancel = cancel
				return request
			},
		},
		{
			name:  "retry after",
			first: firstStatus(http.StatusServiceUnavailable, http.Header{"Retry-After": {"2"}}),
		},
		{
			name:  "empty retry after",
			first: firstStatus(http.StatusBadGateway, http.Header{"Retry-After": {""}}),
		},
		{
			name:  "internal server error",
			first: firstStatus(http.StatusInternalServerError, nil),
		},
		{
			name:  "rate limited",
			first: firstStatus(http.StatusTooManyRequests, nil),
		},
		{
			name:  "not found",
			first: firstStatus(http.StatusNotFound, nil),
		},
		{
			name:  "success",
			first: firstStatus(http.StatusOK, nil),
		},
		{
			name: "caller canceled",
			request: func(t *testing.T) *http.Request {
				return newRetryRequest(t, canceled, http.MethodGet, http.NoBody)
			},
		},
		{
			name: "deadline shorter than the first attempt",
			request: func(t *testing.T) *http.Request {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				t.Cleanup(cancel)
				return newRetryRequest(t, ctx, http.MethodGet, http.NoBody)
			},
			first: func(harness *retryHarness) (*http.Response, error) {
				harness.clock.Advance(10 * time.Second)
				return respondStatus(http.StatusServiceUnavailable)(nil)
			},
		},
		{
			name: "nil response without error",
			first: func(*retryHarness) (*http.Response, error) {
				return nil, nil
			},
		},
		{
			name:  "enforce selected during the attempt",
			setup: setMode(Observe),
			first: func(harness *retryHarness) (*http.Response, error) {
				if err := harness.transport.SetMode(Enforce); err != nil {
					harness.t.Errorf("SetMode(Enforce) error = %v", err)
				}
				return respondStatus(http.StatusServiceUnavailable)(nil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			harness := newRetryHarness(t)
			harness.warm()
			if test.setup != nil {
				test.setup(t, harness)
			}
			request := newInternalReportRequest(t, retryTestURL)
			if test.request != nil {
				request = test.request(t)
			}
			first := test.first
			if first == nil {
				first = firstStatus(http.StatusServiceUnavailable, nil)
			}

			var firstResponse *http.Response
			var firstErr error
			harness.script(func(*http.Request) (*http.Response, error) {
				firstResponse, firstErr = first(harness)
				return firstResponse, firstErr
			})
			response, err := harness.transport.RoundTrip(request)
			if response != firstResponse || !errors.Is(err, firstErr) {
				t.Fatalf("RoundTrip() = %v, %v, want the first result", response, err)
			}
			closeTestBody(t, response)

			requests, waits := harness.recorded()
			if len(requests) != 1 || len(waits) != 0 {
				t.Errorf("base attempts = %d, waits = %d, want 1 and 0", len(requests), len(waits))
			}
			harness.assertRetryStats(test.name, 0, 0, 0, 0)
		})
	}
}

func TestEnforceDoesNotRetryWithoutRetryCandidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		prepare func(*retryHarness)
		name    string
	}{
		{
			name:    "cold target",
			prepare: (*retryHarness).enforce,
		},
		{
			name: "dependency down",
			prepare: func(harness *retryHarness) {
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
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			harness := newRetryHarness(t)
			test.prepare(harness)
			harness.script(respondStatus(http.StatusServiceUnavailable))
			response, err := harness.transport.RoundTrip(newInternalReportRequest(t, retryTestURL))
			if err != nil || response.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("RoundTrip() = %v, %v, want the first 503", response, err)
			}
			closeTestBody(t, response)

			harness.assertRetryStats(test.name, 0, 0, 0, 0)
		})
	}
}

func TestEnforceCountsRetryBudgetDenials(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.warm()

	for index := range 2 {
		harness.script(
			respondStatus(http.StatusServiceUnavailable),
			respondStatus(http.StatusOK),
		)
		response, err := harness.transport.RoundTrip(newInternalReportRequest(t, retryTestURL))
		if err != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("funded request %d = %v, %v, want a retried 200", index, response, err)
		}
		closeTestBody(t, response)
	}

	harness.script(respondStatus(http.StatusServiceUnavailable))
	response, err := harness.transport.RoundTrip(newInternalReportRequest(t, retryTestURL))
	if err != nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unfunded request = %v, %v, want the first 503", response, err)
	}
	closeTestBody(t, response)
	if _, waits := harness.recorded(); len(waits) != 0 {
		t.Errorf("unfunded request waits = %v, want none", waits)
	}

	harness.assertRetryStats("budget denial", 2, 2, 1, 0)
	if got := harness.transport.Stats().ObservedRequests; got != 24 {
		t.Errorf("ObservedRequests = %d, want 24", got)
	}
}

func TestRetryAuthorityRevocationWithdrawsPendingRetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		revoke       func(*testing.T, *retryHarness)
		name         string
		wantFailures uint64
		restore      bool
	}{
		{name: "observe", revoke: setMode(Observe), restore: true},
		{name: "off", revoke: setMode(Off), restore: true},
		{
			name: "enforce restored",
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
		{
			name: "self-disable",
			revoke: func(_ *testing.T, harness *retryHarness) {
				for range 3 {
					harness.transport.guard.Run(func() error {
						return errRetryTestTransport
					})
				}
			},
			wantFailures: 3,
		},
		{
			name: "plan expired",
			revoke: func(_ *testing.T, harness *retryHarness) {
				harness.clock.Advance(time.Minute)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			harness := newRetryHarness(t)
			harness.warm()
			firstBody := newTrackedBody("unavailable")
			first := &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header:     http.Header{},
				Body:       firstBody,
			}
			harness.script(respondWith(first, nil))
			harness.setOnWait(func(ctx context.Context, _ <-chan struct{}) bool {
				test.revoke(t, harness)
				return ctx.Err() == nil
			})

			response, err := harness.transport.RoundTrip(newInternalReportRequest(t, retryTestURL))
			if err != nil || response != first {
				t.Fatalf("RoundTrip() = %v, %v, want the first result", response, err)
			}
			if got := firstBody.closes.Load(); got != 0 {
				t.Errorf("first body closes = %d, want 0", got)
			}
			closeTestBody(t, response)
			requests, waits := harness.recorded()
			if len(requests) != 1 || len(waits) != 1 {
				t.Errorf("base attempts = %d, waits = %d, want 1 and 1", len(requests), len(waits))
			}
			harness.assertRetryStats(test.name, 0, 0, 0, test.wantFailures)

			if test.restore {
				setMode(Enforce)(t, harness)
				harness.assertTwoRetriesFunded()
			}
		})
	}
}

func TestRetryAuthorityRevokedBeforeBackoffSkipsWait(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.warm()
	harness.script(func(*http.Request) (*http.Response, error) {
		setMode(Observe)(t, harness)
		return respondStatus(http.StatusServiceUnavailable)(nil)
	})

	response, err := harness.transport.RoundTrip(newInternalReportRequest(t, retryTestURL))
	if err != nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("RoundTrip() = %v, %v, want the first 503", response, err)
	}
	closeTestBody(t, response)
	if _, waits := harness.recorded(); len(waits) != 0 {
		t.Errorf("waits = %v, want none after authority was revoked", waits)
	}

	setMode(Enforce)(t, harness)
	harness.assertTwoRetriesFunded()
}

func TestRetryStopsWhenCallerCancelsDuringBackoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		viaChannel bool
		waitResult bool
	}{
		{name: "context, wait interrupted"},
		{name: "context, wait completed", waitResult: true},
		{name: "cancel channel, wait interrupted", viaChannel: true},
		{name: "cancel channel, wait completed", viaChannel: true, waitResult: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			harness := newRetryHarness(t)
			harness.warm()
			ctx, cancelContext := context.WithCancel(context.Background())
			t.Cleanup(cancelContext)
			request := newRetryRequest(t, ctx, http.MethodGet, http.NoBody)
			cancelChannel := make(chan struct{})
			var wantCancel <-chan struct{}
			if test.viaChannel {
				//nolint:staticcheck // net/http sets the deprecated Cancel channel for a Client.Timeout.
				request.Cancel = cancelChannel
				wantCancel = cancelChannel
			}
			harness.setOnWait(func(_ context.Context, cancel <-chan struct{}) bool {
				if cancel != wantCancel {
					t.Errorf("wait cancel channel = %v, want %v", cancel, wantCancel)
				}
				if test.viaChannel {
					close(cancelChannel)
				} else {
					cancelContext()
				}
				return test.waitResult
			})
			harness.script(respondStatus(http.StatusServiceUnavailable))

			response, err := harness.transport.RoundTrip(request)
			if err != nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("RoundTrip() = %v, %v, want the first 503", response, err)
			}
			closeTestBody(t, response)
			if requests, waits := harness.recorded(); len(requests) != 1 || len(waits) != 1 {
				t.Errorf("base attempts = %d, waits = %d, want 1 and 1", len(requests), len(waits))
			}

			harness.assertRetryStats(test.name, 0, 0, 0, 0)
			harness.assertTwoRetriesFunded()
		})
	}
}

func TestRetryBodyReplayFailuresKeepFirstResult(t *testing.T) {
	t.Parallel()

	tests := []struct {
		getBody      func(*retryHarness) (io.ReadCloser, error)
		name         string
		wantFailures uint64
	}{
		{
			name: "error",
			getBody: func(*retryHarness) (io.ReadCloser, error) {
				return nil, errors.New("replay unavailable")
			},
		},
		{
			name: "nil body",
			getBody: func(*retryHarness) (io.ReadCloser, error) {
				return nil, nil
			},
		},
		{
			name: "panic",
			getBody: func(*retryHarness) (io.ReadCloser, error) {
				panic("GetBody panic")
			},
			wantFailures: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			harness := newRetryHarness(t)
			harness.warm()
			harness.script(respondStatus(http.StatusServiceUnavailable))
			request := newRetryRequest(t, context.Background(), http.MethodGet, strings.NewReader("query"))
			request.GetBody = func() (io.ReadCloser, error) {
				return test.getBody(harness)
			}

			response, err := harness.transport.RoundTrip(request)
			if err != nil || response.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("RoundTrip() = %v, %v, want the first 503", response, err)
			}
			closeTestBody(t, response)

			harness.assertRetryStats(test.name, 0, 0, 0, test.wantFailures)
			harness.assertTwoRetriesFunded()
		})
	}
}

func TestRetryWithdrawnAfterGetBodyClosesFreshBody(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.warm()
	harness.script(respondStatus(http.StatusServiceUnavailable))

	fresh := newTrackedBody("query")
	request := newRetryRequest(t, context.Background(), http.MethodGet, strings.NewReader("query"))
	request.GetBody = func() (io.ReadCloser, error) {
		setMode(Observe)(t, harness)
		return fresh, nil
	}

	response, err := harness.transport.RoundTrip(request)
	if err != nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("RoundTrip() = %v, %v, want the first 503", response, err)
	}
	closeTestBody(t, response)
	if got := fresh.closes.Load(); got != 1 {
		t.Errorf("fresh body closes = %d, want 1", got)
	}

	harness.assertRetryStats("withdrawn after GetBody", 0, 0, 0, 0)
	setMode(Enforce)(t, harness)
	harness.assertTwoRetriesFunded()
}

func TestRetryStageFailuresCancelReservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stages       func(adaptiveStages) requestStages
		name         string
		wantFailures uint64
		wantWaits    int
		observe      bool
	}{
		{
			name: "postflight error",
			stages: func(stages adaptiveStages) requestStages {
				return retryFaultStages{
					adaptiveStages: stages,
					postflightErr:  errors.New("postflight failed"),
				}
			},
			wantFailures: 1,
		},
		{
			name: "forged grant outside enforce",
			stages: func(stages adaptiveStages) requestStages {
				return retryFaultStages{adaptiveStages: stages, forge: true}
			},
			observe: true,
		},
		{
			name: "confirm rejects",
			stages: func(stages adaptiveStages) requestStages {
				return retryFaultStages{
					adaptiveStages: stages,
					confirm:        func() bool { return false },
				}
			},
			wantWaits: 1,
		},
		{
			name: "confirm panics",
			stages: func(stages adaptiveStages) requestStages {
				return retryFaultStages{
					adaptiveStages: stages,
					confirm:        func() bool { panic("confirm panic") },
				}
			},
			wantFailures: 1,
			wantWaits:    1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			harness := newRetryHarness(t)
			harness.warm()
			adaptive, ok := harness.transport.stages.(adaptiveStages)
			if !ok {
				t.Fatalf("stages = %T, want adaptiveStages", harness.transport.stages)
			}
			harness.transport.stages = test.stages(adaptive)
			if test.observe {
				setMode(Observe)(t, harness)
			}

			fresh := newTrackedBody("query")
			request := newRetryRequest(t, context.Background(), http.MethodGet, strings.NewReader("query"))
			request.GetBody = func() (io.ReadCloser, error) {
				return fresh, nil
			}
			harness.script(respondStatus(http.StatusServiceUnavailable))

			response, err := harness.transport.RoundTrip(request)
			if err != nil || response.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("RoundTrip() = %v, %v, want the first 503", response, err)
			}
			closeTestBody(t, response)
			if _, waits := harness.recorded(); len(waits) != test.wantWaits {
				t.Errorf("waits = %d, want %d", len(waits), test.wantWaits)
			}
			if got, want := fresh.closes.Load(), int32(test.wantWaits); got != want {
				t.Errorf("fresh body closes = %d, want %d", got, want)
			}
			harness.assertRetryStats(test.name, 0, 0, 0, test.wantFailures)

			harness.transport.stages = adaptive
			setMode(Enforce)(t, harness)
			harness.assertTwoRetriesFunded()
		})
	}
}

func TestRetryWaitPanicIsContained(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.warm()
	harness.setOnWait(func(context.Context, <-chan struct{}) bool {
		panic("wait panic")
	})
	harness.script(respondStatus(http.StatusServiceUnavailable))

	response, err := harness.transport.RoundTrip(newInternalReportRequest(t, retryTestURL))
	if err != nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("RoundTrip() = %v, %v, want the first 503", response, err)
	}
	closeTestBody(t, response)

	harness.assertRetryStats("wait panic", 0, 0, 0, 1)
	harness.assertTwoRetriesFunded()
}

func TestRetryClosesDiscardedBodyWithoutFailingRetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         *trackedBody
		name         string
		wantFailures uint64
	}{
		{
			name: "close error",
			body: &trackedBody{Reader: strings.NewReader(""), closeErr: errors.New("close failed")},
		},
		{
			name:         "close panic",
			body:         &trackedBody{Reader: strings.NewReader(""), panics: true},
			wantFailures: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			harness := newRetryHarness(t)
			harness.warm()
			first := &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header:     http.Header{},
				Body:       test.body,
			}
			harness.script(respondWith(first, nil), respondStatus(http.StatusOK))

			response, err := harness.transport.RoundTrip(newInternalReportRequest(t, retryTestURL))
			if err != nil || response.StatusCode != http.StatusOK {
				t.Fatalf("RoundTrip() = %v, %v, want the retry 200", response, err)
			}
			closeTestBody(t, response)
			if got := test.body.closes.Load(); got != 1 {
				t.Errorf("discarded body closes = %d, want 1", got)
			}

			harness.assertRetryStats(test.name, 1, 1, 0, test.wantFailures)
		})
	}
}

func TestRetryBaseTransportPanicPropagates(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.warm()
	firstBody := newTrackedBody("unavailable")
	harness.script(
		respondWith(&http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Header:     http.Header{},
			Body:       firstBody,
		}, nil),
		func(*http.Request) (*http.Response, error) {
			panic("base retry panic")
		},
	)

	func() {
		defer func() {
			if recovered := recover(); recovered != "base retry panic" {
				t.Errorf("recovered = %v, want the base transport panic", recovered)
			}
		}()
		response, err := harness.transport.RoundTrip(newInternalReportRequest(t, retryTestURL))
		t.Errorf("RoundTrip() = %v, %v, want a propagated panic", response, err)
		closeTestBody(t, response)
	}()

	if got := firstBody.closes.Load(); got != 1 {
		t.Errorf("discarded body closes = %d, want 1", got)
	}
	harness.assertRetryStats("base retry panic", 1, 0, 0, 0)
}

func TestSetModeAdvancesAuthorityGeneration(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	transport := harness.transport
	start := transport.authority.Load()
	if authorityMode(start) != Observe || start>>authorityGenerationShift != 0 {
		t.Fatalf("initial authority = %#x, want Observe at generation 0", start)
	}

	setMode(Observe)(t, harness)
	if got := transport.authority.Load(); got != start {
		t.Errorf("authority after an unchanged mode = %#x, want %#x", got, start)
	}

	setMode(Enforce)(t, harness)
	setMode(Observe)(t, harness)
	setMode(Enforce)(t, harness)
	got := transport.authority.Load()
	if authorityMode(got) != Enforce || got>>authorityGenerationShift != 3 {
		t.Errorf("authority after three changes = %#x, want Enforce at generation 3", got)
	}
	if transport.Mode() != Enforce {
		t.Errorf("Mode() = %v, want Enforce", transport.Mode())
	}
}

func TestConcurrentEnforceRetriesStayWithinBudget(t *testing.T) {
	t.Parallel()

	const (
		workers           = 16
		requestsPerWorker = 20
		requests          = workers * requestsPerWorker
	)

	harness := newRetryHarness(t)
	harness.warm()

	var originals sync.Map
	pending := make(chan *http.Request, requests)
	for range requests {
		request := newInternalReportRequest(t, retryTestURL)
		originals.Store(request, struct{}{})
		pending <- request
	}
	close(pending)
	harness.mu.Lock()
	harness.fallback = func(request *http.Request) (*http.Response, error) {
		if _, original := originals.Load(request); original {
			return respondStatus(http.StatusServiceUnavailable)(request)
		}
		return respondStatus(http.StatusOK)(request)
	}
	harness.mu.Unlock()

	done := make(chan struct{})
	sampled := make(chan error, 1)
	go func() {
		defer close(sampled)
		for {
			stats := harness.transport.Stats()
			if stats.RetrySuccesses > stats.RetryAttempts ||
				stats.RetryAttempts+stats.RetryBudgetDenials > stats.ObservedRequests {
				sampled <- errors.New("stats invariant violated")
				return
			}
			select {
			case <-done:
				return
			default:
			}
		}
	}()

	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for request := range pending {
				response, err := harness.transport.RoundTrip(request)
				if err != nil {
					t.Errorf("RoundTrip() error = %v", err)
					continue
				}
				closeTestBody(t, response)
			}
		}()
	}
	group.Wait()
	close(done)
	if err := <-sampled; err != nil {
		t.Fatal(err)
	}

	stats := harness.transport.Stats()
	if stats.ObservedRequests != 21+requests {
		t.Errorf("ObservedRequests = %d, want %d", stats.ObservedRequests, 21+requests)
	}
	if stats.RetryAttempts+stats.RetryBudgetDenials != requests {
		t.Errorf(
			"retries plus denials = %d, want every request to retry or be denied (%d)",
			stats.RetryAttempts+stats.RetryBudgetDenials,
			requests,
		)
	}
	if limit := uint64(21+requests) / 10; stats.RetryAttempts < 2 || stats.RetryAttempts > limit {
		t.Errorf("RetryAttempts = %d, want between 2 and %d", stats.RetryAttempts, limit)
	}
	if stats.RetrySuccesses != stats.RetryAttempts || stats.InternalFailures != 0 {
		t.Errorf(
			"RetrySuccesses = %d, InternalFailures = %d, want %d and 0",
			stats.RetrySuccesses,
			stats.InternalFailures,
			stats.RetryAttempts,
		)
	}
}

func TestConcurrentModeChangesDuringRetries(t *testing.T) {
	t.Parallel()

	harness := newRetryHarness(t)
	harness.warm()
	harness.mu.Lock()
	harness.fallback = respondStatus(http.StatusServiceUnavailable)
	harness.mu.Unlock()

	done := make(chan struct{})
	toggled := make(chan struct{})
	go func() {
		defer close(toggled)
		modes := []Mode{Observe, Enforce, Off, Enforce}
		for index := 0; ; index++ {
			select {
			case <-done:
				return
			default:
			}
			if err := harness.transport.SetMode(modes[index%len(modes)]); err != nil {
				t.Errorf("SetMode() error = %v", err)
				return
			}
		}
	}()

	pending := make(chan *http.Request, 200)
	for range cap(pending) {
		pending <- newInternalReportRequest(t, retryTestURL)
	}
	close(pending)

	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			for request := range pending {
				response, err := harness.transport.RoundTrip(request)
				if err != nil {
					t.Errorf("RoundTrip() error = %v", err)
					continue
				}
				closeTestBody(t, response)
			}
		}()
	}
	group.Wait()
	close(done)
	<-toggled

	stats := harness.transport.Stats()
	if stats.RetrySuccesses != 0 ||
		stats.RetryAttempts+stats.RetryBudgetDenials > stats.ObservedRequests ||
		stats.InternalFailures != 0 {
		t.Errorf("stats = %+v, want consistent retry counters without failures", stats)
	}
}

type retryFaultStages struct {
	postflightErr error
	confirm       func() bool
	adaptiveStages
	forge bool
}

func (stages retryFaultStages) postflight(
	state requestState,
	result attemptResult,
) (retryGrant, error) {
	grant, err := stages.adaptiveStages.postflight(state, result)
	if stages.forge && !grant.lease.Held() {
		grant.lease, _ = stages.observer.ReserveRetry(state.observation)
	}
	if stages.postflightErr != nil {
		return grant, stages.postflightErr
	}

	return grant, err
}

func (stages retryFaultStages) confirmRetry(state requestState) bool {
	if stages.confirm != nil {
		return stages.confirm()
	}

	return stages.adaptiveStages.confirmRetry(state)
}

func setMode(mode Mode) func(*testing.T, *retryHarness) {
	return func(t *testing.T, harness *retryHarness) {
		t.Helper()

		if err := harness.transport.SetMode(mode); err != nil {
			t.Errorf("SetMode(%d) error = %v", mode, err)
		}
	}
}

func firstStatus(status int, header http.Header) firstAttemptStep {
	if header == nil {
		header = http.Header{}
	}

	return func(*retryHarness) (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: header, Body: http.NoBody}, nil
	}
}
