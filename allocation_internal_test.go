package curo

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/raj1kshtz/curo/internal/timeout"
)

// allocationRuns is how many requests each allocation gate averages.
const allocationRuns = 200

// TestRoundTripAllocations gates the per-request allocations that engine.md
// section 19 documents. Every count is exact, so a change in either direction
// updates the gate and the documentation together. The test must not run in
// parallel, because AllocsPerRun counts every allocation in the process. The
// race detector and coverage instrumentation change allocation counts, so it
// skips under either.
func TestRoundTripAllocations(t *testing.T) {
	if raceEnabled || testing.CoverMode() != "" {
		t.Skip("allocation counts need a build without the race detector or coverage")
	}

	tests := []struct {
		setup  func(testing.TB) (*Transport, *http.Request)
		want   error
		name   string
		allocs float64
	}{
		{name: "off", setup: warmAllocationTransport(Off)},
		{name: "observe", setup: warmAllocationTransport(Observe)},
		{name: "enforce without action", setup: warmAllocationTransport(Enforce)},
		{name: "breaker rejection", setup: openBreakerTransport, want: ErrBreakerOpen},
		{name: "overflow aggregate", setup: overflowTransport},
		{name: "timed read", setup: timedReadTransport, allocs: 6},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport, request := test.setup(t)

			var (
				got      error
				mismatch bool
			)
			allocs := testing.AllocsPerRun(allocationRuns, func() {
				response, err := transport.RoundTrip(request)
				if response != nil {
					_ = response.Body.Close()
				}
				if !errors.Is(err, test.want) {
					got, mismatch = err, true
				}
			})
			if mismatch {
				t.Fatalf("RoundTrip() error = %v, want %v", got, test.want)
			}
			if allocs != test.allocs {
				t.Errorf("RoundTrip() allocates %v times per request, want %v", allocs, test.allocs)
			}
		})
	}
}

// allocationBase answers every request with the same response, so the base
// transport allocates nothing. The response has no body, so Curo never wraps
// it.
func allocationBase() http.RoundTripper {
	response := &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}
	return internalRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return response, nil
	})
}

func newAllocationRequest(tb testing.TB, rawURL string) *http.Request {
	tb.Helper()

	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		rawURL,
		nil,
	)
	if err != nil {
		tb.Fatalf("NewRequestWithContext(%q) error = %v", rawURL, err)
	}

	return request
}

// warmAllocationTransport returns a setup for a transport in mode whose
// target is already admitted. Its clock never moves, so the target never
// renews its plan and selects no adaptive timeout.
func warmAllocationTransport(mode Mode) func(testing.TB) (*Transport, *http.Request) {
	return func(tb testing.TB) (*Transport, *http.Request) {
		tb.Helper()

		clock := newReportClock(time.Unix(60_000, 0))
		transport := newClockedTransport(tb, allocationBase(), clock.Now, WithMode(mode))
		request := newAllocationRequest(tb, retryTestURL)
		roundTripInternal(tb, transport, request)

		return transport, request
	}
}

// overflowTransport returns an Enforce transport whose registry is full, so
// request's target is the overflow aggregate.
func overflowTransport(tb testing.TB) (*Transport, *http.Request) {
	tb.Helper()

	clock := newReportClock(time.Unix(60_000, 0))
	transport := newClockedTransport(tb, allocationBase(), clock.Now, WithMode(Enforce))
	for index := range reportTargets {
		rawURL := fmt.Sprintf("https://host-%03d.example/items", index)
		roundTripInternal(tb, transport, newAllocationRequest(tb, rawURL))
	}
	request := newAllocationRequest(tb, "https://overflow.example/items")
	roundTripInternal(tb, transport, request)
	if stats := transport.Stats(); stats.TrackedTargets != reportTargets ||
		stats.OverflowRequests != 1 {
		tb.Fatalf("Stats() = %+v, want %d targets and one overflow request", stats, reportTargets)
	}

	return transport, request
}

// openBreakerTransport returns an Enforce transport whose dependency breaker
// for request's target is open, so request fails fast without an attempt.
func openBreakerTransport(tb testing.TB) (*Transport, *http.Request) {
	tb.Helper()

	clock := newReportClock(time.Unix(60_000, 0))
	base := internalRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Header:     http.Header{},
			Body:       http.NoBody,
		}, nil
	})
	transport := newClockedTransport(tb, base, clock.Now)
	request := newAllocationRequest(tb, retryTestURL)
	for index := range 21 {
		if index > 0 {
			clock.Advance(2 * time.Second)
		}
		roundTripInternal(tb, transport, request)
	}
	if err := transport.SetMode(Enforce); err != nil {
		tb.Fatalf("SetMode(Enforce) error = %v", err)
	}
	clock.Advance(2 * time.Second)
	roundTripInternal(tb, transport, request)
	if opens := transport.Stats().BreakerOpens; opens != 1 {
		tb.Fatalf("BreakerOpens = %d, want 1", opens)
	}

	return transport, request
}

// timedReadTransport returns an Enforce transport whose target selected the
// minimum adaptive timeout, so every request is timed. Its clock stops after
// the warm-up, so the plan never expires.
func timedReadTransport(tb testing.TB) (*Transport, *http.Request) {
	tb.Helper()

	clock := newReportClock(time.Unix(60_000, 0))
	transport := newClockedTransport(tb, allocationBase(), clock.Now)
	request := newAllocationRequest(tb, retryTestURL)
	for index := range timeout.MinimumSamples + 1 {
		gap := 500 * time.Millisecond
		if index == timeout.MinimumSamples {
			gap = 10 * time.Second
		}
		clock.Advance(gap)
		roundTripInternal(tb, transport, request)
	}
	if err := transport.SetMode(Enforce); err != nil {
		tb.Fatalf("SetMode(Enforce) error = %v", err)
	}
	if got := transport.Report().Targets[0].Timeout; got != timeout.DefaultBounds.Minimum {
		tb.Fatalf("Timeout = %v, want %v", got, timeout.DefaultBounds.Minimum)
	}

	return transport, request
}
