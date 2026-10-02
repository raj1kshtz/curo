package curo

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

func TestPreflightFailureFallsBackToOriginalRequestOnce(t *testing.T) {
	t.Parallel()

	tests := map[string]func() error{
		"error": func() error {
			return errors.New("preflight failure")
		},
		"panic": func() error {
			panic("preflight failure")
		},
	}

	for name, fail := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request, err := http.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				"https://user:secret@EXAMPLE.com:8443/items?token=private",
				nil,
			)
			if err != nil {
				t.Fatalf("NewRequest() error = %v", err)
			}
			request.Header.Set("X-Test", "original")

			wantResponse := &http.Response{
				StatusCode: http.StatusAccepted,
				Body:       http.NoBody,
			}
			var attempts atomic.Int32
			base := internalRoundTripperFunc(func(got *http.Request) (*http.Response, error) {
				attempts.Add(1)
				if got != request {
					t.Errorf("base request = %p, want original %p", got, request)
				}
				if got.Method != http.MethodPost {
					t.Errorf("base request method = %q, want POST", got.Method)
				}
				if value := got.Header.Get("X-Test"); value != "original" {
					t.Errorf("base request header = %q, want original", value)
				}
				return wantResponse, nil
			})

			transport, err := New(base, WithMode(Enforce))
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			var postflightCalls atomic.Int32
			transport.stages = testRequestStages{
				before: func(snapshot requestSnapshot, mode Mode) error {
					if snapshot.method != http.MethodPost {
						t.Errorf("preflight method = %q, want POST", snapshot.method)
					}
					if snapshot.scheme != "https" {
						t.Errorf("preflight scheme = %q, want https", snapshot.scheme)
					}
					if snapshot.hostname != "EXAMPLE.com" {
						t.Errorf(
							"preflight hostname = %q, want EXAMPLE.com",
							snapshot.hostname,
						)
					}
					if snapshot.port != "8443" {
						t.Errorf("preflight port = %q, want 8443", snapshot.port)
					}
					if mode != Enforce {
						t.Errorf("preflight mode = %v, want Enforce", mode)
					}
					snapshot.method = http.MethodDelete
					return fail()
				},
				after: func(attemptResult) error {
					postflightCalls.Add(1)
					return nil
				},
			}

			response, roundTripErr := transport.RoundTrip(request)
			if roundTripErr != nil {
				t.Fatalf("RoundTrip() error = %v", roundTripErr)
			}
			if response != wantResponse {
				t.Errorf("RoundTrip() response = %p, want %p", response, wantResponse)
			}
			if closeErr := response.Body.Close(); closeErr != nil {
				t.Errorf("response Body.Close() error = %v", closeErr)
			}
			if got := attempts.Load(); got != 1 {
				t.Errorf("base attempts = %d, want 1", got)
			}
			if got := postflightCalls.Load(); got != 0 {
				t.Errorf("postflight calls = %d, want 0", got)
			}
			stats := transport.Stats()
			if stats.InternalFailures != 1 || stats.SelfDisabled {
				t.Errorf("Stats() = %#v, want one non-disabling internal failure", stats)
			}
		})
	}
}

func TestPostflightFailurePreservesCapturedResult(t *testing.T) {
	t.Parallel()

	tests := map[string]func() error{
		"error": func() error {
			return errors.New("postflight failure")
		},
		"panic": func() error {
			panic("postflight failure")
		},
	}

	for name, fail := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			wantError := errors.New("base result")
			wantResponse := &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(strings.NewReader("base body")),
			}
			var attempts atomic.Int32
			base := internalRoundTripperFunc(func(*http.Request) (*http.Response, error) {
				attempts.Add(1)
				return wantResponse, wantError
			})

			transport, err := New(base, WithMode(Observe))
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			transport.stages = testRequestStages{
				before: func(requestSnapshot, Mode) error {
					return nil
				},
				after: func(result attemptResult) error {
					if result.statusCode != http.StatusServiceUnavailable {
						t.Errorf(
							"postflight status = %d, want %d",
							result.statusCode,
							http.StatusServiceUnavailable,
						)
					}
					if !errors.Is(result.err, wantError) {
						t.Errorf("postflight error = %v, want %v", result.err, wantError)
					}
					return fail()
				},
			}

			response, roundTripErr := transport.RoundTrip(nil)
			if response != wantResponse {
				t.Fatalf("RoundTrip() response = %p, want %p", response, wantResponse)
			}
			if !errors.Is(roundTripErr, wantError) {
				t.Errorf("RoundTrip() error = %v, want %v", roundTripErr, wantError)
			}
			if got := attempts.Load(); got != 1 {
				t.Errorf("base attempts = %d, want 1", got)
			}
			stats := transport.Stats()
			if stats.InternalFailures != 1 || stats.SelfDisabled {
				t.Errorf("Stats() = %#v, want one non-disabling internal failure", stats)
			}

			body, readErr := io.ReadAll(response.Body)
			if readErr != nil {
				t.Fatalf("ReadAll() error = %v", readErr)
			}
			if closeErr := response.Body.Close(); closeErr != nil {
				t.Fatalf("response Body.Close() error = %v", closeErr)
			}
			if got := string(body); got != "base body" {
				t.Errorf("body = %q, want base body", got)
			}
		})
	}
}

func TestRepeatedInternalFailuresSelfDisableStages(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	base := internalRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		attempts.Add(1)
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       http.NoBody,
		}, nil
	})

	transport, err := New(base, WithMode(Enforce))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var preflightCalls atomic.Int32
	var postflightCalls atomic.Int32
	transport.stages = testRequestStages{
		before: func(requestSnapshot, Mode) error {
			preflightCalls.Add(1)
			return errors.New("internal failure")
		},
		after: func(attemptResult) error {
			postflightCalls.Add(1)
			return nil
		},
	}

	for attempt := 0; attempt < 4; attempt++ {
		response, roundTripErr := transport.RoundTrip(nil)
		if roundTripErr != nil {
			t.Fatalf("RoundTrip() attempt %d error = %v", attempt+1, roundTripErr)
		}
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Fatalf("response Body.Close() attempt %d error = %v", attempt+1, closeErr)
		}
	}

	if !transport.guard.Disabled() {
		t.Fatal("guard enabled after repeated internal failures")
	}
	if got := preflightCalls.Load(); got != 3 {
		t.Errorf("preflight calls = %d, want 3", got)
	}
	if got := postflightCalls.Load(); got != 0 {
		t.Errorf("postflight calls = %d, want 0", got)
	}
	if got := attempts.Load(); got != 4 {
		t.Errorf("base attempts = %d, want 4", got)
	}
	stats := transport.Stats()
	if stats.InternalFailures != 3 || !stats.SelfDisabled {
		t.Errorf("Stats() = %#v, want three failures and self-disable", stats)
	}

	if setModeErr := transport.SetMode(Observe); setModeErr != nil {
		t.Fatalf("SetMode(Observe) error = %v", setModeErr)
	}
	response, roundTripErr := transport.RoundTrip(nil)
	if roundTripErr != nil {
		t.Fatalf("RoundTrip() after mode change error = %v", roundTripErr)
	}
	if closeErr := response.Body.Close(); closeErr != nil {
		t.Fatalf("response Body.Close() after mode change error = %v", closeErr)
	}
	if got := preflightCalls.Load(); got != 3 {
		t.Errorf("preflight calls after mode change = %d, want 3", got)
	}
	if got := attempts.Load(); got != 5 {
		t.Errorf("base attempts after mode change = %d, want 5", got)
	}
}

func TestOffAndClosedTransportsBypassInternalStages(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*Transport) error{
		"off": func(*Transport) error {
			return nil
		},
		"closed": func(transport *Transport) error {
			return transport.Close()
		},
	}

	for name, prepare := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mode := Off
			if name == "closed" {
				mode = Enforce
			}

			var attempts atomic.Int32
			base := internalRoundTripperFunc(func(*http.Request) (*http.Response, error) {
				attempts.Add(1)
				return &http.Response{
					StatusCode: http.StatusNoContent,
					Body:       http.NoBody,
				}, nil
			})

			transport, err := New(base, WithMode(mode))
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			transport.stages = testRequestStages{
				before: func(requestSnapshot, Mode) error {
					t.Error("preflight called on direct pass-through path")
					return nil
				},
				after: func(attemptResult) error {
					t.Error("postflight called on direct pass-through path")
					return nil
				},
			}
			if err := prepare(transport); err != nil {
				t.Fatalf("prepare transport error = %v", err)
			}

			response, roundTripErr := transport.RoundTrip(nil)
			if roundTripErr != nil {
				t.Fatalf("RoundTrip() error = %v", roundTripErr)
			}
			if closeErr := response.Body.Close(); closeErr != nil {
				t.Fatalf("response Body.Close() error = %v", closeErr)
			}
			if got := attempts.Load(); got != 1 {
				t.Errorf("base attempts = %d, want 1", got)
			}
		})
	}
}

func TestBasePanicPropagatesOutsideActiveGuard(t *testing.T) {
	t.Parallel()

	panicValue := &struct{ source string }{source: "base"}
	base := internalRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		panic(panicValue)
	})

	transport, err := New(base, WithMode(Observe))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var preflightCalls atomic.Int32
	var postflightCalls atomic.Int32
	transport.stages = testRequestStages{
		before: func(requestSnapshot, Mode) error {
			preflightCalls.Add(1)
			return nil
		},
		after: func(attemptResult) error {
			postflightCalls.Add(1)
			return nil
		},
	}

	defer func() {
		if recovered := recover(); recovered != panicValue {
			t.Errorf("recovered value = %v, want %v", recovered, panicValue)
		}
		if got := preflightCalls.Load(); got != 1 {
			t.Errorf("preflight calls = %d, want 1", got)
		}
		if got := postflightCalls.Load(); got != 0 {
			t.Errorf("postflight calls = %d, want 0", got)
		}
		stats := transport.Stats()
		if stats.InternalFailures != 0 || stats.SelfDisabled {
			t.Errorf("Stats() after base panic = %#v, want no internal failure", stats)
		}
	}()

	response, roundTripErr := transport.RoundTrip(nil)
	if response != nil {
		_ = response.Body.Close()
	}
	_ = roundTripErr
}

func TestRequestSnapshotHandlesNilURL(t *testing.T) {
	t.Parallel()

	snapshot := snapshotRequest(&http.Request{Method: http.MethodPatch})
	if snapshot.method != http.MethodPatch {
		t.Errorf("snapshot method = %q, want PATCH", snapshot.method)
	}
	if snapshot.requestContext == nil {
		t.Fatal("snapshot context = nil, want background context")
	}
	if snapshot.scheme != "" || snapshot.hostname != "" || snapshot.port != "" {
		t.Errorf("snapshot URL fields = %#v, want empty", snapshot)
	}
}

func TestSnapshotAuthorityValidation(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		authority    string
		wantHostname string
		wantPort     string
	}{
		"hostname": {
			authority:    "example.com",
			wantHostname: "example.com",
		},
		"hostname and port": {
			authority:    "example.com:8443",
			wantHostname: "example.com",
			wantPort:     "8443",
		},
		"ipv6": {
			authority:    "[2001:db8::1]",
			wantHostname: "2001:db8::1",
		},
		"ipv6 and port": {
			authority:    "[2001:db8::1]:8443",
			wantHostname: "2001:db8::1",
			wantPort:     "8443",
		},
		"empty": {},
		"manual user info": {
			authority: "user@example.com",
		},
		"invalid port": {
			authority: "example.com:bad",
		},
		"empty port": {
			authority: "example.com:",
		},
		"unbracketed ipv6": {
			authority: "2001:db8::1",
		},
		"unclosed ipv6": {
			authority: "[2001:db8::1",
		},
		"invalid bracket suffix": {
			authority: "[2001:db8::1]suffix",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			hostname, port := snapshotAuthority(&url.URL{Host: test.authority})
			if hostname != test.wantHostname || port != test.wantPort {
				t.Errorf(
					"snapshotAuthority(%q) = (%q, %q), want (%q, %q)",
					test.authority,
					hostname,
					port,
					test.wantHostname,
					test.wantPort,
				)
			}
		})
	}
}

type testRequestStages struct {
	before func(requestSnapshot, Mode) error
	after  func(attemptResult) error
}

func (s testRequestStages) preflight(
	snapshot requestSnapshot,
	authority uint64,
) (requestState, error) {
	return requestState{}, s.before(snapshot, authorityMode(authority))
}

func (s testRequestStages) postflight(
	_ requestState,
	result attemptResult,
) (retryGrant, error) {
	return retryGrant{}, s.after(result)
}

func (testRequestStages) confirmRetry(requestState) bool {
	return false
}

type internalRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f internalRoundTripperFunc) RoundTrip(
	request *http.Request,
) (*http.Response, error) {
	return f(request)
}
