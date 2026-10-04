package curo

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raj1kshtz/curo/internal/breaker"
	"github.com/raj1kshtz/curo/internal/guard"
	"github.com/raj1kshtz/curo/internal/timeout"
)

var (
	errBasePanic      = errors.New("base transport panic")
	errCallerCanceled = errors.New("net/http: request canceled")
)

// manualTimers replaces Transport.after, so a test decides when adaptive
// timeouts expire.
type manualTimers struct {
	timers     []*manualTimer
	mu         sync.Mutex
	stopPanics bool
}

type manualTimer struct {
	fire    func()
	limit   time.Duration
	stopped atomic.Bool
	panics  bool
}

func (timer *manualTimer) Stop() bool {
	if timer.panics {
		panic("timer stop failure")
	}

	return !timer.stopped.Swap(true)
}

func (timers *manualTimers) after(limit time.Duration, fire func()) timeout.Timer {
	timers.mu.Lock()
	defer timers.mu.Unlock()

	timer := &manualTimer{fire: fire, limit: limit, panics: timers.stopPanics}
	timers.timers = append(timers.timers, timer)

	return timer
}

func (timers *manualTimers) started() []*manualTimer {
	timers.mu.Lock()
	defer timers.mu.Unlock()

	return append([]*manualTimer(nil), timers.timers...)
}

// only returns the one timer started so far.
func (timers *manualTimers) only(t *testing.T) *manualTimer {
	t.Helper()

	started := timers.started()
	if len(started) != 1 {
		t.Fatalf("started timers = %d, want 1", len(started))
	}

	return started[0]
}

// newTimeoutHarness returns a retry harness whose adaptive timeouts expire
// only when the test fires their timers.
func newTimeoutHarness(t *testing.T, options ...Option) (*retryHarness, *manualTimers) {
	t.Helper()

	harness := newRetryHarness(t, options...)
	timers := &manualTimers{}
	harness.transport.after = timers.after

	return harness, timers
}

// warmTimeout completes timeout.MinimumSamples initial attempts with method
// and status half a second apart, then one more after the target's decision
// expired. Every sample is still recent at that last evaluation, so a read
// target publishes the adaptive timeout its latency selects.
func (harness *retryHarness) warmTimeout(method string, status int) {
	harness.t.Helper()

	for index := range timeout.MinimumSamples + 1 {
		gap := 500 * time.Millisecond
		if index == timeout.MinimumSamples {
			gap = 10 * time.Second
		}
		harness.clock.Advance(gap)
		harness.script(respondStatus(status))
		roundTripInternal(
			harness.t,
			harness.transport,
			newRetryRequest(harness.t, context.Background(), method, nil),
		)
	}
	harness.script()
}

// warmHealthyTimeout publishes a Ready Healthy read decision whose only
// candidate is a two second adaptive timeout, then selects Enforce.
func (harness *retryHarness) warmHealthyTimeout() {
	harness.t.Helper()

	harness.warmTimeout(http.MethodGet, http.StatusOK)
	decision := harness.decision(MethodRead)
	assertDecision(
		harness.t,
		"healthy target",
		decision,
		ReadinessReady,
		DiagnosisHealthy,
		CandidateTimeout,
	)
	wantLatency := Latency{Samples: timeout.MinimumSamples + 1, Slowest: time.Millisecond}
	if decision.Timeout != timeout.DefaultBounds.Minimum || decision.Latency != wantLatency {
		harness.t.Fatalf(
			"Timeout = %v, Latency = %+v, want %v and %+v",
			decision.Timeout,
			decision.Latency,
			timeout.DefaultBounds.Minimum,
			wantLatency,
		)
	}
	harness.enforce()
}

// warmRetryTimeout publishes a Ready Transient read decision with Retry and a
// two second adaptive timeout, then selects Enforce.
func (harness *retryHarness) warmRetryTimeout() {
	harness.t.Helper()

	harness.warmTimeout(http.MethodGet, http.StatusOK)
	harness.clock.Advance(10 * time.Second)
	harness.record(1, http.StatusServiceUnavailable)
	decision := harness.decision(MethodRead)
	assertDecision(
		harness.t,
		"transient target",
		decision,
		ReadinessReady,
		DiagnosisTransient,
		CandidateRetry|CandidateTimeout,
	)
	if decision.Timeout != timeout.DefaultBounds.Minimum {
		harness.t.Fatalf("Timeout = %v, want %v", decision.Timeout, timeout.DefaultBounds.Minimum)
	}
	harness.enforce()
}

// decision returns the published decision of the harness target with method.
func (harness *retryHarness) decision(method MethodClass) Decision {
	harness.t.Helper()

	for _, decision := range harness.transport.Report().Targets {
		if decision.Target.Method == method {
			return decision
		}
	}
	harness.t.Fatalf("no decision for method class %v", method)

	return Decision{}
}

func (harness *retryHarness) assertTimeoutStats(name string, timeouts, shadow uint64) {
	harness.t.Helper()

	stats := harness.transport.Stats()
	if stats.Timeouts != timeouts ||
		stats.ShadowTimeouts != shadow ||
		stats.InternalFailures != 0 {
		harness.t.Errorf(
			"%s stats = %d timeouts, %d shadow, %d failures, want %d, %d, 0",
			name,
			stats.Timeouts,
			stats.ShadowTimeouts,
			stats.InternalFailures,
			timeouts,
			shadow,
		)
	}
}

func isTimeoutCause(err error) bool {
	var timeoutErr interface{ Timeout() bool }
	return errors.As(err, &timeoutErr) && timeoutErr.Timeout()
}

func TestEnforceTimeoutEndsSlowRead(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*http.Request, io.ReadCloser) (*http.Response, error){
		"canceled attempt": func(request *http.Request, _ io.ReadCloser) (*http.Response, error) {
			return nil, request.Context().Err()
		},
		"late response": func(_ *http.Request, body io.ReadCloser) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: body}, nil
		},
	}

	for name, respond := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness, timers := newTimeoutHarness(t)
			harness.warmRetryTimeout()

			original := newInternalReportRequest(t, retryTestURL)
			late := newTrackedBody("late")
			var attempt *http.Request
			harness.script(func(request *http.Request) (*http.Response, error) {
				attempt = request
				timer := timers.only(t)
				if timer.limit != timeout.DefaultBounds.Minimum {
					t.Errorf("timer limit = %v, want %v", timer.limit, timeout.DefaultBounds.Minimum)
				}
				timer.fire()
				if err := request.Context().Err(); !errors.Is(err, context.Canceled) {
					t.Errorf("expired attempt context error = %v, want context.Canceled", err)
				}
				return respond(request, late)
			})

			response, err := harness.transport.RoundTrip(original)
			if response != nil || !errors.Is(err, ErrTimeout) {
				closeTestBody(t, response)
				t.Fatalf("RoundTrip() = %v, %v, want ErrTimeout", response, err)
			}
			if attempt == nil || attempt == original {
				t.Fatal("timed attempt did not use a copy of the caller's request")
			}
			if cause := context.Cause(attempt.Context()); !isTimeoutCause(cause) {
				t.Errorf("attempt context cause = %v, want a timeout", cause)
			}
			if err := original.Context().Err(); err != nil {
				t.Errorf("caller context error = %v, want nil", err)
			}
			wantCloses := int32(0)
			if name == "late response" {
				wantCloses = 1
			}
			if got := late.closes.Load(); got != wantCloses {
				t.Errorf("late body closes = %d, want %d", got, wantCloses)
			}
			if requests, _ := harness.recorded(); len(requests) != 1 {
				t.Errorf("base attempts = %d, want 1 without a retry", len(requests))
			}
			harness.assertRetryStats("timeout", 0, 0, 0, 0)
			harness.assertTimeoutStats("timeout", 1, 0)

			// The cut is a latency sample at the timeout, in the 2.5 second
			// bucket, so the next attempt waits three times as long.
			decision := harness.decision(MethodRead)
			if decision.Timeout != 7500*time.Millisecond ||
				decision.Latency.Slowest != 2500*time.Millisecond {
				t.Errorf(
					"escalated Timeout = %v, Latency = %+v, want 7.5s from a 2.5s bucket",
					decision.Timeout,
					decision.Latency,
				)
			}
			harness.script(respondStatus(http.StatusOK))
			roundTripInternal(t, harness.transport, newInternalReportRequest(t, retryTestURL))
			if started := timers.started(); len(started) != 2 ||
				started[1].limit != 7500*time.Millisecond {
				t.Errorf("escalated timers = %d, want a second timer of 7.5s", len(started))
			}
		})
	}
}

func TestTimeoutNeverClaimsCallerCancellation(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*testing.T) (*http.Request, func()){
		"context": func(t *testing.T) (*http.Request, func()) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			return newRetryRequest(t, ctx, http.MethodGet, nil), cancel
		},
		"cancel channel": func(t *testing.T) (*http.Request, func()) {
			request := newInternalReportRequest(t, retryTestURL)
			cancel := make(chan struct{})
			//nolint:staticcheck // net/http sets the deprecated Cancel channel for a Client.Timeout.
			request.Cancel = cancel
			return request, func() { close(cancel) }
		},
	}

	for name, prepare := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness, timers := newTimeoutHarness(t)
			harness.warmHealthyTimeout()

			request, cancel := prepare(t)
			harness.script(func(attempt *http.Request) (*http.Response, error) {
				cancel()
				timers.only(t).fire()
				if cause := context.Cause(attempt.Context()); isTimeoutCause(cause) {
					t.Errorf("attempt context cause = %v, want the caller's", cause)
				}
				return nil, errCallerCanceled
			})

			response, err := harness.transport.RoundTrip(request)
			if response != nil || !errors.Is(err, errCallerCanceled) {
				closeTestBody(t, response)
				t.Fatalf("RoundTrip() = %v, %v, want the caller's cancellation", response, err)
			}
			harness.assertTimeoutStats("caller cancellation", 0, 0)
		})
	}
}

func TestTimeoutSkipsAttemptsItMustNotBound(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		prepare func(*testing.T, *retryHarness) *http.Request
		timed   bool
	}{
		"distant deadline": {
			timed: true,
			prepare: func(t *testing.T, _ *retryHarness) *http.Request {
				ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
				t.Cleanup(cancel)
				return newRetryRequest(t, ctx, http.MethodGet, nil)
			},
		},
		"near deadline": {
			prepare: func(t *testing.T, _ *retryHarness) *http.Request {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				t.Cleanup(cancel)
				return newRetryRequest(t, ctx, http.MethodGet, nil)
			},
		},
		"canceled context": {
			prepare: func(t *testing.T, _ *retryHarness) *http.Request {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return newRetryRequest(t, ctx, http.MethodGet, nil)
			},
		},
		"closed cancel channel": {
			prepare: func(t *testing.T, _ *retryHarness) *http.Request {
				request := newInternalReportRequest(t, retryTestURL)
				closed := make(chan struct{})
				close(closed)
				//nolint:staticcheck // net/http sets the deprecated Cancel channel for a Client.Timeout.
				request.Cancel = closed
				return request
			},
		},
		"write": {
			prepare: func(t *testing.T, harness *retryHarness) *http.Request {
				harness.warmTimeout(http.MethodPost, http.StatusOK)
				decision := harness.decision(MethodWrite)
				if decision.Timeout != 0 ||
					decision.Candidates.Has(CandidateTimeout) ||
					decision.Latency.Samples < timeout.MinimumSamples {
					t.Errorf("write decision = %+v, want latency evidence without a timeout", decision)
				}
				return newRetryRequest(t, context.Background(), http.MethodPost, nil)
			},
		},
		"observe": {
			prepare: func(t *testing.T, harness *retryHarness) *http.Request {
				if err := harness.transport.SetMode(Observe); err != nil {
					t.Fatalf("SetMode(Observe) error = %v", err)
				}
				return newInternalReportRequest(t, retryTestURL)
			},
		},
		"off": {
			prepare: func(t *testing.T, harness *retryHarness) *http.Request {
				if err := harness.transport.SetMode(Off); err != nil {
					t.Fatalf("SetMode(Off) error = %v", err)
				}
				return newInternalReportRequest(t, retryTestURL)
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness, timers := newTimeoutHarness(t)
			harness.warmHealthyTimeout()
			request := test.prepare(t, harness)

			body := newTrackedBody("payload")
			harness.script(respondWith(
				&http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: body},
				nil,
			))
			response, err := harness.transport.RoundTrip(request)
			if err != nil || response == nil {
				t.Fatalf("RoundTrip() = %v, %v, want a response", response, err)
			}
			closeTestBody(t, response)

			requests, _ := harness.recorded()
			if len(requests) != 1 {
				t.Fatalf("base attempts = %d, want 1", len(requests))
			}
			started := len(timers.started())
			if test.timed {
				if requests[0] == request || started != 1 || response.Body == io.ReadCloser(body) {
					t.Errorf("timed = %t, %d timers, want a timed copy and a wrapped body", requests[0] != request, started)
				}
			} else if requests[0] != request || started != 0 || response.Body != io.ReadCloser(body) {
				t.Errorf("timed = %t, %d timers, want the caller's request and body untouched", requests[0] != request, started)
			}
			if got := body.closes.Load(); got != 1 {
				t.Errorf("body closes = %d, want 1", got)
			}
		})
	}
}

func TestTimeoutWithdrawnWhenAuthorityEnds(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*testing.T, *Transport){
		"mode change": func(t *testing.T, transport *Transport) {
			if err := transport.SetMode(Observe); err != nil {
				t.Fatalf("SetMode(Observe) error = %v", err)
			}
		},
		"restored mode": func(t *testing.T, transport *Transport) {
			if err := transport.SetMode(Observe); err != nil {
				t.Fatalf("SetMode(Observe) error = %v", err)
			}
			if err := transport.SetMode(Enforce); err != nil {
				t.Fatalf("SetMode(Enforce) error = %v", err)
			}
		},
		"close": func(t *testing.T, transport *Transport) {
			if err := transport.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
		},
		"self-disable": func(t *testing.T, transport *Transport) {
			for range 3 {
				transport.guard.Run(guard.StagePreflight, func() error {
					return errors.New("internal failure")
				})
			}
			if !transport.guard.Disabled() {
				t.Fatal("guard enabled after repeated internal failures")
			}
		},
	}

	for name, revoke := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness, timers := newTimeoutHarness(t)
			harness.warmHealthyTimeout()
			harness.script(func(request *http.Request) (*http.Response, error) {
				revoke(t, harness.transport)
				timers.only(t).fire()
				if err := request.Context().Err(); err != nil {
					t.Errorf("withdrawn timeout canceled the attempt: %v", err)
				}
				return respondStatus(http.StatusOK)(request)
			})

			response, err := harness.transport.RoundTrip(newInternalReportRequest(t, retryTestURL))
			if err != nil || response == nil || response.StatusCode != http.StatusOK {
				closeTestBody(t, response)
				t.Fatalf("RoundTrip() = %v, %v, want the base response", response, err)
			}
			closeTestBody(t, response)
			if got := harness.transport.Stats().Timeouts; got != 0 {
				t.Errorf("Timeouts = %d, want 0", got)
			}
		})
	}
}

func TestTimeoutAbandonedWhenBaseTransportPanics(t *testing.T) {
	t.Parallel()

	// A timer whose Stop fails is an internal failure. Containing it must
	// still release the attempt and keep the base transport's panic.
	tests := map[string]struct {
		failures   uint64
		stopPanics bool
	}{
		"timer stopped":     {},
		"timer stop failed": {failures: 1, stopPanics: true},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness, timers := newTimeoutHarness(t)
			timers.stopPanics = test.stopPanics
			harness.warmHealthyTimeout()

			var attempt *http.Request
			harness.script(func(request *http.Request) (*http.Response, error) {
				attempt = request
				panic(errBasePanic)
			})

			func() {
				defer func() {
					//nolint:errorlint // The recovered value is the panic value itself.
					if recovered := recover(); recovered != errBasePanic {
						t.Errorf("recovered %v, want the base transport's panic", recovered)
					}
				}()
				response, err := harness.transport.RoundTrip(newInternalReportRequest(t, retryTestURL))
				closeTestBody(t, response)
				t.Errorf("RoundTrip() = %v, %v, want the base transport's panic", response, err)
			}()

			timer := timers.only(t)
			if !test.stopPanics && !timer.stopped.Load() {
				t.Error("panicking attempt left its timer running")
			}
			if attempt == nil || attempt.Context().Err() == nil {
				t.Error("panicking attempt left its context live")
			}
			// The abandoned attempt is settled, so a late timer cannot end it.
			timer.fire()
			stats := harness.transport.Stats()
			if stats.Timeouts != 0 || stats.InternalFailures != test.failures {
				t.Errorf(
					"Stats() = %+v, want no timeouts and %d internal failures",
					stats,
					test.failures,
				)
			}
		})
	}
}

// upgradeBody is a writable response body, like the connection of a 101
// Switching Protocols response.
type upgradeBody struct {
	*trackedBody
	written strings.Builder
}

func (body *upgradeBody) Write(p []byte) (int, error) {
	return body.written.Write(p)
}

func TestTimeoutFailingToStartSendsTheRequestUntimed(t *testing.T) {
	t.Parallel()

	harness, _ := newTimeoutHarness(t)
	harness.transport.after = func(time.Duration, func()) timeout.Timer {
		panic("timer start failure")
	}
	harness.warmHealthyTimeout()

	original := newInternalReportRequest(t, retryTestURL)
	body := newTrackedBody("payload")
	harness.script(func(request *http.Request) (*http.Response, error) {
		if request != original {
			t.Error("base transport received a timed copy")
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: body}, nil
	})
	response, err := harness.transport.RoundTrip(original)
	if err != nil || response == nil || response.Body != io.ReadCloser(body) {
		closeTestBody(t, response)
		t.Fatalf("RoundTrip() = %v, %v, want the base response untouched", response, err)
	}
	closeTestBody(t, response)

	stats := harness.transport.Stats()
	if stats.InternalFailures != 1 || stats.Timeouts != 0 {
		t.Errorf("Stats() = %+v, want one internal failure and no timeouts", stats)
	}
}

func TestTimeoutFailingToSettleKeepsTheResult(t *testing.T) {
	t.Parallel()

	harness, timers := newTimeoutHarness(t)
	timers.stopPanics = true
	harness.warmHealthyTimeout()

	body := newTrackedBody("payload")
	var attempt context.Context
	harness.script(func(request *http.Request) (*http.Response, error) {
		attempt = request.Context()
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: body}, nil
	})
	response, err := harness.transport.RoundTrip(newInternalReportRequest(t, retryTestURL))
	if err != nil || response == nil || response.StatusCode != http.StatusOK {
		closeTestBody(t, response)
		t.Fatalf("RoundTrip() = %v, %v, want the base response", response, err)
	}

	// The attempt is settled, so its timer can no longer end it. Its context
	// lives until the caller closes the body.
	timers.only(t).fire()
	if attempt.Err() != nil {
		t.Errorf("settled attempt context error = %v, want nil before Close", attempt.Err())
	}
	closeTestBody(t, response)
	if attempt.Err() == nil {
		t.Error("closing the body left the attempt's context live")
	}
	if got := body.closes.Load(); got != 1 {
		t.Errorf("base body closes = %d, want 1", got)
	}
	stats := harness.transport.Stats()
	if stats.InternalFailures != 1 || stats.Timeouts != 0 {
		t.Errorf("Stats() = %+v, want one internal failure and no timeouts", stats)
	}
}

func TestTimedResponseReleasesItsAttempt(t *testing.T) {
	t.Parallel()

	type outcome struct {
		body     io.ReadCloser
		response *http.Response
		err      error
	}
	// use, when set, runs before the response is closed.
	tests := map[string]struct {
		respond func() outcome
		use     func(*testing.T, outcome, context.Context)
		live    bool
	}{
		"read to end": {
			live: true,
			respond: func() outcome {
				body := newTrackedBody("payload")
				return outcome{body: body, response: &http.Response{StatusCode: http.StatusOK, Body: body}}
			},
			use: func(t *testing.T, result outcome, attempt context.Context) {
				if _, writable := result.response.Body.(io.Writer); writable {
					t.Error("read-only body became writable")
				}
				data, err := io.ReadAll(result.response.Body)
				if err != nil || string(data) != "payload" {
					t.Errorf("ReadAll() = %q, %v, want payload", data, err)
				}
				if attempt.Err() == nil {
					t.Error("reading to the end did not release the attempt")
				}
			},
		},
		"close": {
			live: true,
			respond: func() outcome {
				body := newTrackedBody("payload")
				return outcome{body: body, response: &http.Response{StatusCode: http.StatusOK, Body: body}}
			},
		},
		"writable": {
			live: true,
			respond: func() outcome {
				body := &upgradeBody{trackedBody: newTrackedBody("upgraded")}
				return outcome{
					body:     body,
					response: &http.Response{StatusCode: http.StatusSwitchingProtocols, Body: body},
				}
			},
			use: func(t *testing.T, result outcome, attempt context.Context) {
				writer, writable := result.response.Body.(io.Writer)
				if !writable {
					t.Fatal("writable body lost io.Writer")
				}
				if _, err := io.WriteString(writer, "ping"); err != nil {
					t.Errorf("Write() error = %v", err)
				}
				if got := result.body.(*upgradeBody).written.String(); got != "ping" {
					t.Errorf("written = %q, want ping", got)
				}
				if _, err := io.ReadAll(result.response.Body); err != nil {
					t.Errorf("ReadAll() error = %v", err)
				}
				if attempt.Err() != nil {
					t.Error("read EOF released a writable body's attempt")
				}
			},
		},
		"no body": {
			respond: func() outcome {
				return outcome{
					body:     http.NoBody,
					response: &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody},
				}
			},
			use: func(t *testing.T, result outcome, _ context.Context) {
				if result.response.Body != http.NoBody {
					t.Error("NoBody was wrapped")
				}
			},
		},
		"error": {
			respond: func() outcome {
				return outcome{err: errRetryTestTransport}
			},
			use: func(t *testing.T, result outcome, _ context.Context) {
				if !errors.Is(result.err, errRetryTestTransport) || result.response != nil {
					t.Errorf("result = %v, %v, want the base error", result.response, result.err)
				}
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness, timers := newTimeoutHarness(t)
			harness.warmHealthyTimeout()

			want := test.respond()
			var attempt context.Context
			harness.script(func(request *http.Request) (*http.Response, error) {
				attempt = request.Context()
				return want.response, want.err
			})
			response, err := harness.transport.RoundTrip(newInternalReportRequest(t, retryTestURL))
			if response != nil && want.response != nil && response != want.response {
				t.Error("RoundTrip() replaced the base response")
			}
			if !timers.only(t).stopped.Load() {
				t.Error("returned attempt left its timer running")
			}
			if live := attempt.Err() == nil; live != test.live {
				t.Errorf("attempt context live = %t after RoundTrip, want %t", live, test.live)
			}
			if test.live && response.Body == want.body {
				t.Error("body was not wrapped")
			}
			if test.use != nil {
				test.use(t, outcome{body: want.body, response: response, err: err}, attempt)
			}
			closeTestBody(t, response)
			if test.live && attempt.Err() == nil {
				t.Error("closing the body did not release the attempt")
			}

			var tracked *trackedBody
			switch body := want.body.(type) {
			case *trackedBody:
				tracked = body
			case *upgradeBody:
				tracked = body.trackedBody
			}
			if tracked != nil && tracked.closes.Load() != 1 {
				t.Errorf("body closes = %d, want 1", tracked.closes.Load())
			}
			harness.assertTimeoutStats(name, 0, 0)
		})
	}
}

func TestRetryAttemptHasAdaptiveTimeout(t *testing.T) {
	t.Parallel()

	harness, timers := newTimeoutHarness(t)
	harness.warmRetryTimeout()

	firstBody := newTrackedBody("unavailable")
	original := newInternalReportRequest(t, retryTestURL)
	harness.script(
		respondWith(&http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Header:     http.Header{},
			Body:       firstBody,
		}, nil),
		func(request *http.Request) (*http.Response, error) {
			started := timers.started()
			if len(started) != 2 ||
				!started[0].stopped.Load() ||
				started[1].limit != timeout.DefaultBounds.Minimum {
				t.Fatalf("timers = %d, want the first stopped and a retry timer of 2s", len(started))
			}
			started[1].fire()
			return nil, request.Context().Err()
		},
	)

	response, err := harness.transport.RoundTrip(original)
	if response != nil || !errors.Is(err, ErrTimeout) {
		closeTestBody(t, response)
		t.Fatalf("RoundTrip() = %v, %v, want ErrTimeout from the retry", response, err)
	}
	if got := firstBody.closes.Load(); got != 1 {
		t.Errorf("discarded body closes = %d, want 1", got)
	}
	requests, _ := harness.recorded()
	if len(requests) != 2 || requests[0] == original || requests[1] == original {
		t.Fatalf("base attempts = %d, want two timed copies", len(requests))
	}
	harness.assertRetryStats("timed retry", 1, 0, 0, 0)
	harness.assertTimeoutStats("timed retry", 1, 0)
}

func TestObserveCountsShadowTimeouts(t *testing.T) {
	t.Parallel()

	harness, timers := newTimeoutHarness(t)
	harness.warmTimeout(http.MethodGet, http.StatusOK)

	for _, latency := range []time.Duration{3 * time.Second, time.Second} {
		original := newInternalReportRequest(t, retryTestURL)
		harness.script(func(request *http.Request) (*http.Response, error) {
			if request != original {
				t.Error("Observe sent a copy of the request")
			}
			harness.clock.Advance(latency)
			return respondStatus(http.StatusOK)(request)
		})
		roundTripInternal(t, harness.transport, original)
	}

	if started := len(timers.started()); started != 0 {
		t.Errorf("started timers = %d, want 0", started)
	}
	harness.assertTimeoutStats("shadow", 0, 1)
}

func TestBreakerProbeIsNeverTimed(t *testing.T) {
	t.Parallel()

	harness, timers := newTimeoutHarness(t)
	harness.warmTimeout(http.MethodGet, http.StatusServiceUnavailable)
	assertDecision(
		t,
		"down target",
		harness.decision(MethodRead),
		ReadinessReady,
		DiagnosisDependencyDown,
		CandidateBreakerOpen|CandidateTimeout,
	)
	harness.enforce()
	harness.expectStatus(http.StatusServiceUnavailable, http.StatusServiceUnavailable)
	harness.assertBreakerStats("open", 1, 0, 0)
	if started := len(timers.started()); started != 1 {
		t.Fatalf("started timers = %d, want 1 for the request that opened the breaker", started)
	}

	harness.clock.Advance(breaker.BaseCooldown)
	probe := newInternalReportRequest(t, retryTestURL)
	harness.script(func(request *http.Request) (*http.Response, error) {
		if request != probe {
			t.Error("probe was sent as a timed copy")
		}
		return respondStatus(http.StatusOK)(request)
	})
	roundTripInternal(t, harness.transport, probe)
	harness.assertBreakerStats("probe", 1, 1, 0)
	if started := len(timers.started()); started != 1 {
		t.Errorf("started timers = %d, want no timer for the probe", started)
	}
}

func TestTimeoutBoundsSelectTheTimeout(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		options []Option
		want    time.Duration
	}{
		"default": {want: 2 * time.Second},
		"custom floor": {
			options: []Option{WithTimeoutBounds(5*time.Second, 10*time.Second)},
			want:    5 * time.Second,
		},
		"disabled": {
			options: []Option{WithTimeoutBounds(0, 0)},
		},
		"last option wins": {
			options: []Option{
				WithTimeoutBounds(0, 0),
				WithTimeoutBounds(time.Millisecond, time.Minute),
			},
			want: 3 * time.Millisecond,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness, timers := newTimeoutHarness(t, test.options...)
			harness.warmTimeout(http.MethodGet, http.StatusOK)
			decision := harness.decision(MethodRead)
			if decision.Timeout != test.want ||
				decision.Candidates.Has(CandidateTimeout) != (test.want > 0) {
				t.Errorf(
					"decision Timeout = %v, Candidates = %v, want %v",
					decision.Timeout,
					decision.Candidates,
					test.want,
				)
			}

			harness.enforce()
			harness.script(respondStatus(http.StatusOK))
			roundTripInternal(t, harness.transport, newInternalReportRequest(t, retryTestURL))
			started := timers.started()
			if test.want == 0 && len(started) != 0 {
				t.Errorf("started timers = %d, want 0", len(started))
			}
			if test.want > 0 && (len(started) != 1 || started[0].limit != test.want) {
				t.Errorf("started timers = %d, want one of %v", len(started), test.want)
			}
		})
	}
}

func TestTimeoutEndsSlowReadThroughHTTPClient(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/slow" {
			select {
			case <-request.Context().Done():
			case <-time.After(10 * time.Second):
			}
			return
		}
		_, _ = io.WriteString(writer, "fast")
	}))
	defer server.Close()

	clock := newReportClock(time.Unix(60_000, 0))
	transport := newClockedTransport(
		t,
		server.Client().Transport,
		clock.Now,
		WithTimeoutBounds(50*time.Millisecond, time.Second),
	)
	client := &http.Client{Transport: transport}
	get := func(path string) (*http.Response, error) {
		request, err := http.NewRequestWithContext(
			context.Background(),
			http.MethodGet,
			server.URL+path,
			http.NoBody,
		)
		if err != nil {
			t.Fatalf("NewRequestWithContext() error = %v", err)
		}
		return client.Do(request)
	}

	for index := range timeout.MinimumSamples + 1 {
		gap := 500 * time.Millisecond
		if index == timeout.MinimumSamples {
			gap = 10 * time.Second
		}
		clock.Advance(gap)
		response, err := get("/")
		if err != nil {
			t.Fatalf("warm request error = %v", err)
		}
		if _, err := io.Copy(io.Discard, response.Body); err != nil {
			t.Fatalf("warm body error = %v", err)
		}
		closeTestBody(t, response)
	}
	if err := transport.SetMode(Enforce); err != nil {
		t.Fatalf("SetMode(Enforce) error = %v", err)
	}

	started := time.Now()
	response, err := get("/slow")
	elapsed := time.Since(started)
	if response != nil || !errors.Is(err, ErrTimeout) {
		closeTestBody(t, response)
		t.Fatalf("Do() = %v, %v, want ErrTimeout", response, err)
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Errorf("Do() error = %v, want a net.Error timeout", err)
	}
	if elapsed < 50*time.Millisecond {
		t.Errorf("request ended after %v, before its 50ms timeout", elapsed)
	}
	if got := transport.Stats().Timeouts; got != 1 {
		t.Errorf("Timeouts = %d, want 1", got)
	}
}

func TestTimeoutsAreSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	const (
		workers    = 8
		iterations = 20
	)

	base := internalRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("X-Slow") != "" {
			select {
			case <-request.Context().Done():
				return nil, request.Context().Err()
			case <-time.After(20 * time.Millisecond):
			}
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader("ok")),
		}, nil
	})
	clock := newReportClock(time.Unix(60_000, 0))
	transport := newClockedTransport(
		t,
		base,
		clock.Now,
		WithTimeoutBounds(time.Millisecond, time.Hour),
	)
	for index := range timeout.MinimumSamples + 1 {
		gap := 500 * time.Millisecond
		if index == timeout.MinimumSamples {
			gap = 10 * time.Second
		}
		clock.Advance(gap)
		roundTripInternal(t, transport, newInternalReportRequest(t, retryTestURL))
	}
	if err := transport.SetMode(Enforce); err != nil {
		t.Fatalf("SetMode(Enforce) error = %v", err)
	}

	var wg sync.WaitGroup
	failures := make(chan error, workers*iterations)
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for iteration := range iterations {
				request, err := http.NewRequestWithContext(
					context.Background(),
					http.MethodGet,
					retryTestURL,
					http.NoBody,
				)
				if err != nil {
					failures <- err
					return
				}
				if iteration%2 == 0 {
					request.Header.Set("X-Slow", "1")
				}

				response, err := transport.RoundTrip(request)
				switch {
				case err == nil:
					_, _ = io.Copy(io.Discard, response.Body)
					_ = response.Body.Close()
				case !errors.Is(err, ErrTimeout):
					failures <- err
				}

				if worker == 0 && iteration%5 == 0 {
					_ = transport.SetMode(Observe)
					_ = transport.SetMode(Enforce)
				}
			}
		}()
	}
	wg.Wait()
	close(failures)

	for err := range failures {
		t.Errorf("concurrent RoundTrip() error = %v", err)
	}
	stats := transport.Stats()
	if stats.InternalFailures != 0 || stats.Timeouts > workers*iterations {
		t.Errorf("Stats() = %+v, want no failures and bounded timeouts", stats)
	}
}

func BenchmarkTimedRoundTrip(b *testing.B) {
	transport, request := timedReadTransport(b)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		response, err := transport.RoundTrip(request)
		if err != nil {
			b.Fatalf("RoundTrip() error = %v", err)
		}
		_ = response.Body.Close()
	}
}
