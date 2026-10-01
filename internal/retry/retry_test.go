package retry

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestReplaySafe(t *testing.T) {
	t.Parallel()

	body := io.NopCloser(strings.NewReader("payload"))
	getBody := func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("payload")), nil
	}

	tests := []struct {
		request *http.Request
		name    string
		want    bool
	}{
		{name: "nil", request: nil},
		{name: "empty method", request: &http.Request{}, want: true},
		{name: "get", request: &http.Request{Method: http.MethodGet}, want: true},
		{name: "head", request: &http.Request{Method: http.MethodHead}, want: true},
		{name: "options", request: &http.Request{Method: http.MethodOptions}, want: true},
		{name: "trace", request: &http.Request{Method: http.MethodTrace}, want: true},
		{name: "lowercase get", request: &http.Request{Method: "get"}},
		{name: "post", request: &http.Request{Method: http.MethodPost}},
		{name: "put", request: &http.Request{Method: http.MethodPut}},
		{name: "patch", request: &http.Request{Method: http.MethodPatch}},
		{name: "delete", request: &http.Request{Method: http.MethodDelete}},
		{name: "connect", request: &http.Request{Method: http.MethodConnect}},
		{
			name:    "no body",
			request: &http.Request{Method: http.MethodGet, Body: http.NoBody},
			want:    true,
		},
		{
			name:    "body without GetBody",
			request: &http.Request{Method: http.MethodGet, Body: body},
		},
		{
			name: "body with GetBody",
			request: &http.Request{
				Method:  http.MethodGet,
				Body:    body,
				GetBody: getBody,
			},
			want: true,
		},
		{
			name: "cancel channel",
			request: &http.Request{
				Method: http.MethodGet,
				//nolint:staticcheck // A deprecated Cancel channel does not affect replay safety.
				Cancel: make(chan struct{}),
			},
			want: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := ReplaySafe(test.request); got != test.want {
				t.Fatalf("ReplaySafe() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestRetryable(t *testing.T) {
	t.Parallel()

	withRetryAfter := http.Header{}
	withRetryAfter.Set("Retry-After", "1")
	errTransport := errors.New("connection reset")
	unrelatedHeader := http.Header{"Content-Type": {"text/plain"}}

	tests := []struct {
		err        error
		header     http.Header
		name       string
		statusCode int
		dependency bool
		want       bool
	}{
		{name: "caller owned error", err: errTransport},
		{name: "dependency error", err: errTransport, dependency: true, want: true},
		{
			name:       "error with response",
			err:        errTransport,
			statusCode: http.StatusOK,
			dependency: true,
			want:       true,
		},
		{name: "bad gateway", statusCode: http.StatusBadGateway, dependency: true, want: true},
		{
			name:       "service unavailable",
			statusCode: http.StatusServiceUnavailable,
			dependency: true,
			want:       true,
		},
		{
			name:       "gateway timeout",
			statusCode: http.StatusGatewayTimeout,
			dependency: true,
			want:       true,
		},
		{
			name:       "unrelated header",
			statusCode: http.StatusBadGateway,
			header:     unrelatedHeader,
			dependency: true,
			want:       true,
		},
		{
			name:       "retry after",
			statusCode: http.StatusServiceUnavailable,
			header:     withRetryAfter,
			dependency: true,
		},
		{
			name:       "empty retry after",
			statusCode: http.StatusBadGateway,
			header:     http.Header{"Retry-After": {""}},
			dependency: true,
		},
		{
			name:       "retry after without values",
			statusCode: http.StatusGatewayTimeout,
			header:     http.Header{"Retry-After": nil},
			dependency: true,
		},
		{
			name:       "noncanonical retry after",
			statusCode: http.StatusServiceUnavailable,
			header:     http.Header{"retry-after": {"later"}},
			dependency: true,
		},
		{
			name:       "internal server error",
			statusCode: http.StatusInternalServerError,
			dependency: true,
		},
		{name: "not implemented", statusCode: http.StatusNotImplemented, dependency: true},
		{name: "too many requests", statusCode: http.StatusTooManyRequests},
		{name: "not found", statusCode: http.StatusNotFound},
		{name: "no response or error", dependency: true},
		{name: "neutral gateway status", statusCode: http.StatusServiceUnavailable},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := Retryable(test.dependency, test.err, test.statusCode, test.header)
			if got != test.want {
				t.Fatalf("Retryable() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestDeadlineAllows(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	roomy, cancelRoomy := context.WithTimeout(context.Background(), time.Hour)
	t.Cleanup(cancelRoomy)
	tight, cancelTight := context.WithTimeout(context.Background(), 50*time.Millisecond)
	t.Cleanup(cancelTight)

	tests := []struct {
		ctx          context.Context
		name         string
		delay        time.Duration
		firstAttempt time.Duration
		want         bool
	}{
		{name: "nil context"},
		{name: "canceled", ctx: canceled},
		{name: "no deadline", ctx: context.Background(), delay: MaxBackoff, want: true},
		{
			name:         "roomy deadline",
			ctx:          roomy,
			delay:        MaxBackoff,
			firstAttempt: time.Second,
			want:         true,
		},
		{name: "delay exceeds deadline", ctx: tight, delay: time.Second},
		{
			name:         "first attempt exceeds remaining time",
			ctx:          roomy,
			delay:        MaxBackoff,
			firstAttempt: 2 * time.Hour,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := DeadlineAllows(test.ctx, test.delay, test.firstAttempt); got != test.want {
				t.Fatalf("DeadlineAllows() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestCanceled(t *testing.T) {
	t.Parallel()

	closed := make(chan struct{})
	close(closed)

	tests := []struct {
		cancel <-chan struct{}
		name   string
		want   bool
	}{
		{name: "nil"},
		{name: "open", cancel: make(chan struct{})},
		{name: "closed", cancel: closed, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := Canceled(test.cancel); got != test.want {
				t.Fatalf("Canceled() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestJitterStaysWithinBounds(t *testing.T) {
	t.Parallel()

	wantSpan := int64(MaxBackoff-MinBackoff) + 1
	lowest := jitter(func(span int64) int64 {
		if span != wantSpan {
			t.Errorf("random span = %d, want %d", span, wantSpan)
		}
		return 0
	})
	if lowest != MinBackoff {
		t.Errorf("lowest jitter = %v, want %v", lowest, MinBackoff)
	}
	if highest := jitter(func(span int64) int64 { return span - 1 }); highest != MaxBackoff {
		t.Errorf("highest jitter = %v, want %v", highest, MaxBackoff)
	}

	for range 1_000 {
		if delay := Jitter(); delay < MinBackoff || delay > MaxBackoff {
			t.Fatalf("Jitter() = %v, want within [%v, %v]", delay, MinBackoff, MaxBackoff)
		}
	}
}

func TestWaitHonorsDelayAndCancellation(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	closed := make(chan struct{})
	close(closed)

	tests := []struct {
		ctx    context.Context
		cancel <-chan struct{}
		name   string
		delay  time.Duration
		want   bool
	}{
		{name: "elapsed delay", ctx: context.Background(), delay: time.Nanosecond, want: true},
		{
			name:   "elapsed delay with open cancel channel",
			ctx:    context.Background(),
			cancel: make(chan struct{}),
			delay:  time.Nanosecond,
			want:   true,
		},
		{name: "canceled context", ctx: canceled, delay: time.Hour},
		{
			name:  "context canceled during the delay",
			ctx:   newCancelDuringWaitContext(),
			delay: time.Hour,
		},
		{name: "closed cancel channel", ctx: context.Background(), cancel: closed, delay: time.Hour},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := Wait(test.ctx, test.cancel, test.delay); got != test.want {
				t.Fatalf("Wait() = %t, want %t", got, test.want)
			}
		})
	}
}

// TestWaitRechecksCancellationWhenDelayElapses gives Wait an elapsed delay and
// a visible cancellation together. select may pick either ready case, so the
// timer path must check cancellation again.
func TestWaitRechecksCancellationWhenDelayElapses(t *testing.T) {
	t.Parallel()

	closed := make(chan struct{})
	close(closed)

	for range 100 {
		if Wait(context.Background(), closed, 0) {
			t.Fatal("Wait() with a closed cancel channel = true, want false")
		}
		if Wait(newCancelDuringWaitContext(), nil, 0) {
			t.Fatal("Wait() with a context canceled during the delay = true, want false")
		}
	}
}

// cancelDuringWaitContext is active when Wait starts and done once Wait
// selects, so the cancellation path is deterministic.
type cancelDuringWaitContext struct {
	context.Context
	done   chan struct{}
	checks int
}

func newCancelDuringWaitContext() *cancelDuringWaitContext {
	done := make(chan struct{})
	close(done)

	return &cancelDuringWaitContext{Context: context.Background(), done: done}
}

func (ctx *cancelDuringWaitContext) Done() <-chan struct{} {
	return ctx.done
}

func (ctx *cancelDuringWaitContext) Err() error {
	ctx.checks++
	if ctx.checks == 1 {
		return nil
	}

	return context.Canceled
}
