package curo_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raj1kshtz/curo"
)

func TestStatsReportsBoundedObservation(t *testing.T) {
	t.Parallel()

	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       http.NoBody,
		}, nil
	})
	transport, err := curo.New(base)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	urls := []string{
		"https://EXAMPLE.com/items/123?token=secret",
		"https://example.com/orders/456?session=private",
	}
	for _, rawURL := range urls {
		request, requestErr := http.NewRequestWithContext(
			context.Background(),
			http.MethodGet,
			rawURL,
			nil,
		)
		if requestErr != nil {
			t.Fatalf("NewRequestWithContext(%q) error = %v", rawURL, requestErr)
		}

		response, roundTripErr := transport.RoundTrip(request)
		if roundTripErr != nil {
			t.Fatalf("RoundTrip(%q) error = %v", rawURL, roundTripErr)
		}
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Fatalf("response Body.Close() error = %v", closeErr)
		}
	}

	stats := transport.Stats()
	if stats.ObservedRequests != 2 {
		t.Errorf("ObservedRequests = %d, want 2", stats.ObservedRequests)
	}
	if stats.TrackedTargets != 1 {
		t.Errorf("TrackedTargets = %d, want 1", stats.TrackedTargets)
	}
	if stats.OverflowRequests != 0 {
		t.Errorf("OverflowRequests = %d, want 0", stats.OverflowRequests)
	}
	if stats.InternalFailures != 0 {
		t.Errorf("InternalFailures = %d, want 0", stats.InternalFailures)
	}
	if stats.SelfDisabled {
		t.Error("SelfDisabled = true, want false")
	}
}

func TestStatsFollowModeTransitions(t *testing.T) {
	t.Parallel()

	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       http.NoBody,
		}, nil
	})
	transport, err := curo.New(base, curo.WithMode(curo.Off))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		"https://example.com",
		nil,
	)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}

	roundTripAndClose(t, transport, request)
	if got := transport.Stats().ObservedRequests; got != 0 {
		t.Errorf("Off ObservedRequests = %d, want 0", got)
	}

	if err := transport.SetMode(curo.Observe); err != nil {
		t.Fatalf("SetMode(Observe) error = %v", err)
	}
	roundTripAndClose(t, transport, request)
	if got := transport.Stats().ObservedRequests; got != 1 {
		t.Errorf("Observe ObservedRequests = %d, want 1", got)
	}

	if err := transport.SetMode(curo.Off); err != nil {
		t.Fatalf("SetMode(Off) error = %v", err)
	}
	roundTripAndClose(t, transport, request)
	if got := transport.Stats().ObservedRequests; got != 1 {
		t.Errorf("second Off ObservedRequests = %d, want 1", got)
	}
}

func TestStatsRemainReadableAndFrozenAfterClose(t *testing.T) {
	t.Parallel()

	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       http.NoBody,
		}, nil
	})
	transport, err := curo.New(base)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		"https://example.com",
		nil,
	)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}

	roundTripAndClose(t, transport, request)
	beforeClose := transport.Stats()
	if err := transport.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	roundTripAndClose(t, transport, request)

	if got := transport.Stats(); got != beforeClose {
		t.Errorf("Stats() after Close = %#v, want %#v", got, beforeClose)
	}
}

func TestInFlightObservationUsesStartState(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*curo.Transport) error{
		"mode changes to Off": func(transport *curo.Transport) error {
			return transport.SetMode(curo.Off)
		},
		"transport closes": func(transport *curo.Transport) error {
			return transport.Close()
		},
	}

	for name, changeState := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			enteredBase := make(chan struct{}, 1)
			releaseBase := make(chan struct{})
			var calls atomic.Int32
			base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
				if calls.Add(1) == 1 {
					enteredBase <- struct{}{}
					<-releaseBase
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       http.NoBody,
				}, nil
			})

			transport, err := curo.New(base)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			request, err := http.NewRequestWithContext(
				context.Background(),
				http.MethodGet,
				"https://example.com",
				nil,
			)
			if err != nil {
				t.Fatalf("NewRequestWithContext() error = %v", err)
			}

			completed := make(chan error, 1)
			go func() {
				response, roundTripErr := transport.RoundTrip(request)
				if roundTripErr == nil {
					roundTripErr = response.Body.Close()
				}
				completed <- roundTripErr
			}()

			<-enteredBase
			if err := changeState(transport); err != nil {
				t.Fatalf("change state error = %v", err)
			}
			close(releaseBase)
			if err := <-completed; err != nil {
				t.Fatalf("in-flight RoundTrip() error = %v", err)
			}

			if got := transport.Stats().ObservedRequests; got != 1 {
				t.Errorf("in-flight ObservedRequests = %d, want 1", got)
			}
			roundTripAndClose(t, transport, request)
			if got := transport.Stats().ObservedRequests; got != 1 {
				t.Errorf("later ObservedRequests = %d, want 1", got)
			}
		})
	}
}

func TestStatsUseBoundedOverflowAggregate(t *testing.T) {
	t.Parallel()

	const requests = 129

	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       http.NoBody,
		}, nil
	})
	transport, err := curo.New(base)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	for index := 0; index < requests; index++ {
		request, requestErr := http.NewRequestWithContext(
			context.Background(),
			http.MethodGet,
			fmt.Sprintf("https://host-%03d.example/path", index),
			nil,
		)
		if requestErr != nil {
			t.Fatalf("NewRequestWithContext(%d) error = %v", index, requestErr)
		}
		roundTripAndClose(t, transport, request)
	}

	stats := transport.Stats()
	if stats.ObservedRequests != requests {
		t.Errorf("ObservedRequests = %d, want %d", stats.ObservedRequests, requests)
	}
	if stats.TrackedTargets != 128 {
		t.Errorf("TrackedTargets = %d, want 128", stats.TrackedTargets)
	}
	if stats.OverflowRequests != 1 {
		t.Errorf("OverflowRequests = %d, want 1", stats.OverflowRequests)
	}
}

func TestNilRequestsDoNotTripObservationGuard(t *testing.T) {
	t.Parallel()

	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       http.NoBody,
		}, nil
	})
	transport, err := curo.New(base)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	for range 4 {
		roundTripAndClose(t, transport, nil)
	}

	stats := transport.Stats()
	if stats.ObservedRequests != 4 || stats.OverflowRequests != 4 {
		t.Errorf("nil-request Stats() = %#v, want four overflow observations", stats)
	}
	if stats.InternalFailures != 0 {
		t.Errorf("InternalFailures = %d, want 0", stats.InternalFailures)
	}
	if stats.SelfDisabled {
		t.Error("SelfDisabled = true, want false")
	}
}

func TestMalformedAuthorityUsesOverflowWithoutFailure(t *testing.T) {
	t.Parallel()

	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       http.NoBody,
		}, nil
	})
	transport, err := curo.New(base)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	request := &http.Request{
		Method: http.MethodGet,
		URL: &url.URL{
			Scheme: "https",
			Host:   "api.example:bad",
		},
	}

	roundTripAndClose(t, transport, request)
	stats := transport.Stats()
	if stats.ObservedRequests != 1 ||
		stats.OverflowRequests != 1 ||
		stats.TrackedTargets != 0 {
		t.Errorf("malformed-authority Stats() = %#v, want one overflow request", stats)
	}
	if stats.InternalFailures != 0 || stats.SelfDisabled {
		t.Errorf("malformed-authority failure Stats() = %#v", stats)
	}
}

func TestCyclicTransportErrorCannotWedgeObservation(t *testing.T) {
	t.Parallel()

	wantError := &cyclicTransportError{}
	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, wantError
	})
	transport, err := curo.New(base)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	completed := make(chan error, 1)
	go func() {
		response, roundTripErr := transport.RoundTrip(nil)
		if response != nil {
			_ = response.Body.Close()
		}
		completed <- roundTripErr
	}()

	select {
	case gotError := <-completed:
		//nolint:errorlint // Exact identity proves the original cyclic error survived.
		if gotError != wantError {
			t.Errorf("RoundTrip() error = %v, want original cyclic error", gotError)
		}
	case <-time.After(time.Second):
		t.Fatal("RoundTrip() blocked while classifying cyclic transport error")
	}

	stats := transport.Stats()
	if stats.ObservedRequests != 1 ||
		stats.InternalFailures != 0 ||
		stats.SelfDisabled {
		t.Errorf("cyclic-error Stats() = %#v, want one contained observation", stats)
	}
}

func TestPanickingErrorMatcherIsContained(t *testing.T) {
	t.Parallel()

	wantError := &panickingMatcherError{}
	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, wantError
	})
	transport, err := curo.New(base)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	response, gotError := transport.RoundTrip(nil)
	if response != nil {
		_ = response.Body.Close()
	}
	//nolint:errorlint // Exact identity proves containment preserved the base error.
	if gotError != wantError {
		t.Errorf("RoundTrip() error = %v, want original matcher error", gotError)
	}

	stats := transport.Stats()
	if stats.ObservedRequests != 0 ||
		stats.InternalFailures != 1 ||
		stats.SelfDisabled {
		t.Errorf("panicking-matcher Stats() = %#v, want one internal failure", stats)
	}
}

func TestZeroAndNilTransportStatsAreEmpty(t *testing.T) {
	t.Parallel()

	var zero curo.Transport
	if got := zero.Stats(); got != (curo.Stats{}) {
		t.Errorf("zero Transport Stats() = %#v, want empty", got)
	}

	var transport *curo.Transport
	if got := transport.Stats(); got != (curo.Stats{}) {
		t.Errorf("nil Transport Stats() = %#v, want empty", got)
	}
}

type cyclicTransportError struct{}

func (*cyclicTransportError) Error() string {
	return "cyclic transport error"
}

func (err *cyclicTransportError) Unwrap() error {
	return err
}

type panickingMatcherError struct{}

func (*panickingMatcherError) Error() string {
	return "panicking matcher"
}

func (*panickingMatcherError) Is(error) bool {
	panic("matcher failure")
}

func roundTripAndClose(
	t *testing.T,
	transport *curo.Transport,
	request *http.Request,
) {
	t.Helper()

	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	if closeErr := response.Body.Close(); closeErr != nil {
		t.Fatalf("response Body.Close() error = %v", closeErr)
	}
}
