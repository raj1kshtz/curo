// Package retry contains Curo's fixed retry rules and lock-free retry budgets.
//
// The package never starts an attempt. Callers combine replay safety, outcome
// rules, cancellation and deadline checks, and budget reservations before
// starting at most one retry attempt for an initial request.
package retry

import (
	"context"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"
)

const (
	// MinBackoff is the shortest delay before a retry attempt starts.
	MinBackoff = 25 * time.Millisecond

	// MaxBackoff is the longest delay before a retry attempt starts.
	MaxBackoff = 100 * time.Millisecond
)

// ReplaySafe reports whether request can be replayed by a retry without
// application cooperation beyond Request.GetBody.
//
// Only GET, HEAD, OPTIONS, TRACE, and the empty method, which net/http sends
// as GET, are replay-safe. A body must be absent or reproducible through
// GetBody.
func ReplaySafe(request *http.Request) bool {
	if request == nil {
		return false
	}

	switch request.Method {
	case "", http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
	default:
		return false
	}

	return request.Body == nil ||
		request.Body == http.NoBody ||
		request.GetBody != nil
}

// Retryable reports whether a dependency-owned initial attempt failure may be
// retried.
//
// Transport errors qualify. Responses qualify only for 502, 503, and 504
// without a Retry-After header field. Any Retry-After field, including an
// empty or malformed one, means the dependency asked callers to wait.
// Caller-owned cancellation and deadlines, 429, other 4xx, and other 5xx
// responses never qualify.
func Retryable(
	dependencyFailure bool,
	err error,
	statusCode int,
	header http.Header,
) bool {
	if !dependencyFailure {
		return false
	}
	if err != nil {
		return true
	}

	switch statusCode {
	case http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return !hasRetryAfter(header)
	default:
		return false
	}
}

// hasRetryAfter reports whether header holds a Retry-After field under any
// key casing, so a field set without canonicalization still counts.
func hasRetryAfter(header http.Header) bool {
	for key := range header {
		if strings.EqualFold(key, "Retry-After") {
			return true
		}
	}

	return false
}

// DeadlineAllows reports whether ctx is active and leaves time for a retry
// that waits delay and then takes as long as the first attempt.
func DeadlineAllows(
	ctx context.Context,
	delay time.Duration,
	firstAttempt time.Duration,
) bool {
	if ctx == nil || ctx.Err() != nil {
		return false
	}

	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		return true
	}

	remaining := time.Until(deadline)
	return remaining > delay && remaining-delay > firstAttempt
}

// Canceled reports whether cancel, a request's deprecated Cancel channel, is
// closed. A nil channel is never closed. net/http still sets the channel when
// an http.Client with a Timeout uses a transport it does not recognize, so a
// retry must treat its closure like context cancellation.
func Canceled(cancel <-chan struct{}) bool {
	select {
	case <-cancel:
		return true
	default:
		return false
	}
}

// Jitter returns a uniformly distributed retry delay in
// [MinBackoff, MaxBackoff].
func Jitter() time.Duration {
	return jitter(rand.Int64N)
}

func jitter(random func(int64) int64) time.Duration {
	return MinBackoff + time.Duration(random(int64(MaxBackoff-MinBackoff)+1))
}

// Wait blocks for delay, or until ctx is done or cancel is closed, without
// holding a lock or starting a goroutine. A nil cancel channel is ignored. Wait
// reports whether delay elapsed while ctx remained active and cancel open.
func Wait(ctx context.Context, cancel <-chan struct{}, delay time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-cancel:
		return false
	case <-timer.C:
		return ctx.Err() == nil && !Canceled(cancel)
	}
}
