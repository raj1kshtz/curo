package curo_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/raj1kshtz/curo"
)

var (
	_ http.RoundTripper = (*curo.Transport)(nil)
	_ io.Closer         = (*curo.Transport)(nil)

	_ interface{ CloseIdleConnections() } = (*curo.Transport)(nil)
)

func TestNewRejectsNilBaseTransport(t *testing.T) {
	t.Parallel()

	transport, err := curo.New(nil)
	if !errors.Is(err, curo.ErrNilBaseTransport) {
		t.Fatalf("New(nil) error = %v, want ErrNilBaseTransport", err)
	}
	if transport != nil {
		t.Fatalf("New(nil) transport = %v, want nil", transport)
	}
}

func TestNewRejectsNilOption(t *testing.T) {
	t.Parallel()

	var option curo.Option
	transport, err := curo.New(roundTripperFunc(nil), option)
	if err == nil {
		t.Fatal("New with nil option error = nil, want an error")
	}
	if transport != nil {
		t.Fatalf("New with nil option transport = %v, want nil", transport)
	}
}

func TestTransportWorksWithHTTPClient(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("X-Curo-Test", "pass-through")
		response.WriteHeader(http.StatusCreated)
		_, _ = response.Write([]byte("ok"))
	}))
	defer server.Close()

	baseClient := server.Client()
	transport, err := curo.New(baseClient.Transport)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	client := &http.Client{Transport: transport}
	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		server.URL,
		nil,
	)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}

	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("client.Do() error = %v", err)
	}
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Errorf("response Body.Close() error = %v", closeErr)
		}
	}()

	if response.StatusCode != http.StatusCreated {
		t.Errorf("status = %d, want %d", response.StatusCode, http.StatusCreated)
	}
	if got := response.Header.Get("X-Curo-Test"); got != "pass-through" {
		t.Errorf("X-Curo-Test = %q, want pass-through", got)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if got := string(body); got != "ok" {
		t.Errorf("body = %q, want ok", got)
	}
}

func TestRoundTripPreservesRequestAndResult(t *testing.T) {
	t.Parallel()

	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"https://example.com/items",
		nil,
	)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	request.Header.Set("X-Test", "original")

	wantResponse := &http.Response{
		StatusCode: http.StatusAccepted,
		Body:       http.NoBody,
	}
	var attempts atomic.Int32

	base := roundTripperFunc(func(got *http.Request) (*http.Response, error) {
		attempts.Add(1)
		if got != request {
			t.Errorf("RoundTrip request = %p, want original %p", got, request)
		}
		if value := got.Header.Get("X-Test"); value != "original" {
			t.Errorf("RoundTrip header = %q, want original", value)
		}
		return wantResponse, nil
	})

	transport, err := curo.New(base)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	gotResponse, gotError := transport.RoundTrip(request)
	if gotResponse == nil {
		t.Fatal("RoundTrip response = nil, want base response")
	}
	defer func() {
		if closeErr := gotResponse.Body.Close(); closeErr != nil {
			t.Errorf("response Body.Close() error = %v", closeErr)
		}
	}()
	if gotResponse != wantResponse {
		t.Errorf("RoundTrip response = %p, want %p", gotResponse, wantResponse)
	}
	if gotError != nil {
		t.Errorf("RoundTrip error = %v, want nil", gotError)
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("base attempts = %d, want 1", got)
	}
}

func TestRoundTripPreservesBaseError(t *testing.T) {
	t.Parallel()

	wantError := errors.New("base result")
	var attempts atomic.Int32
	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		attempts.Add(1)
		return nil, wantError
	})

	transport, err := curo.New(base)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	response, gotError := transport.RoundTrip(nil)
	if response != nil {
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Errorf("response Body.Close() error = %v", closeErr)
		}
		t.Errorf("RoundTrip response = %p, want nil", response)
	}
	if !errors.Is(gotError, wantError) {
		t.Errorf("RoundTrip error = %v, want %v", gotError, wantError)
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("base attempts = %d, want 1", got)
	}
}

func TestRoundTripPreservesBasePanic(t *testing.T) {
	t.Parallel()

	panicValue := &struct{ name string }{name: "base panic"}
	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		panic(panicValue)
	})

	transport, err := curo.New(base)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	defer func() {
		if recovered := recover(); recovered != panicValue {
			t.Errorf("recovered value = %v, want %v", recovered, panicValue)
		}
		stats := transport.Stats()
		if stats.ObservedRequests != 0 ||
			stats.InternalFailures != 0 ||
			stats.SelfDisabled {
			t.Errorf("Stats() after base panic = %#v, want no completed observation", stats)
		}
	}()

	response, roundTripErr := transport.RoundTrip(nil)
	if response != nil {
		_ = response.Body.Close()
	}
	_ = roundTripErr
}

func TestCloseIsIdempotentAndKeepsPassThrough(t *testing.T) {
	t.Parallel()

	wantResponse := &http.Response{
		StatusCode: http.StatusNoContent,
		Body:       http.NoBody,
	}
	base := &closableRoundTripper{response: wantResponse}

	transport, err := curo.New(base, curo.WithMode(curo.Enforce))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if closeErr := transport.Close(); closeErr != nil {
		t.Fatalf("first Close() error = %v", closeErr)
	}
	if closeErr := transport.Close(); closeErr != nil {
		t.Fatalf("second Close() error = %v", closeErr)
	}
	if got := base.closes.Load(); got != 0 {
		t.Errorf("base Close calls = %d, want 0", got)
	}
	if setModeErr := transport.SetMode(curo.Off); !errors.Is(setModeErr, curo.ErrClosed) {
		t.Errorf("SetMode() after Close error = %v, want ErrClosed", setModeErr)
	}
	if setModeErr := transport.SetMode(curo.Mode(99)); !errors.Is(setModeErr, curo.ErrClosed) {
		t.Errorf("SetMode(invalid) after Close error = %v, want ErrClosed", setModeErr)
	}
	if got := transport.Mode(); got != curo.Enforce {
		t.Errorf("Mode() after Close = %v, want Enforce", got)
	}

	response, err := transport.RoundTrip(nil)
	if err != nil {
		t.Fatalf("RoundTrip() after Close error = %v", err)
	}
	if response == nil {
		t.Fatal("RoundTrip() after Close response = nil, want base response")
	}
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Errorf("response Body.Close() error = %v", closeErr)
		}
	}()
	if response != wantResponse {
		t.Errorf("RoundTrip() after Close response = %p, want %p", response, wantResponse)
	}
	if got := base.attempts.Load(); got != 1 {
		t.Errorf("base attempts = %d, want 1", got)
	}
}

func TestZeroValueReturnsExplicitErrors(t *testing.T) {
	t.Parallel()

	var transport curo.Transport

	if got := transport.Mode(); got != curo.Off {
		t.Errorf("zero-value Mode() = %v, want Off", got)
	}
	response, roundTripErr := transport.RoundTrip(nil)
	if response != nil {
		_ = response.Body.Close()
	}
	if !errors.Is(roundTripErr, curo.ErrNilBaseTransport) {
		t.Errorf(
			"zero-value RoundTrip() error = %v, want ErrNilBaseTransport",
			roundTripErr,
		)
	}
	assertRoundTripClosesBody(t, &transport)
	if err := transport.SetMode(curo.Observe); !errors.Is(err, curo.ErrNilBaseTransport) {
		t.Errorf("zero-value SetMode() error = %v, want ErrNilBaseTransport", err)
	}
	if err := transport.Close(); !errors.Is(err, curo.ErrNilBaseTransport) {
		t.Errorf("zero-value Close() error = %v, want ErrNilBaseTransport", err)
	}
	transport.CloseIdleConnections()
}

func TestNilTransportReturnsExplicitErrors(t *testing.T) {
	t.Parallel()

	var transport *curo.Transport

	if got := transport.Mode(); got != curo.Off {
		t.Errorf("nil Mode() = %v, want Off", got)
	}
	response, roundTripErr := transport.RoundTrip(nil)
	if response != nil {
		_ = response.Body.Close()
	}
	if !errors.Is(roundTripErr, curo.ErrNilBaseTransport) {
		t.Errorf("nil RoundTrip() error = %v, want ErrNilBaseTransport", roundTripErr)
	}
	assertRoundTripClosesBody(t, transport)
	if err := transport.SetMode(curo.Observe); !errors.Is(err, curo.ErrNilBaseTransport) {
		t.Errorf("nil SetMode() error = %v, want ErrNilBaseTransport", err)
	}
	if err := transport.Close(); !errors.Is(err, curo.ErrNilBaseTransport) {
		t.Errorf("nil Close() error = %v, want ErrNilBaseTransport", err)
	}
	transport.CloseIdleConnections()
}

// assertRoundTripClosesBody checks that an uninitialized transport closes the
// request body before it returns ErrNilBaseTransport, as http.RoundTripper
// requires, and contains a panic from Close.
func assertRoundTripClosesBody(t *testing.T, transport *curo.Transport) {
	t.Helper()

	counting := &closeCountingBody{}
	for _, body := range []io.ReadCloser{counting, panickingCloseBody{}} {
		request, err := http.NewRequestWithContext(
			context.Background(),
			http.MethodPost,
			"https://api.example/items",
			body,
		)
		if err != nil {
			t.Fatalf("NewRequestWithContext() error = %v", err)
		}

		response, roundTripErr := transport.RoundTrip(request)
		if response != nil {
			_ = response.Body.Close()
		}
		if !errors.Is(roundTripErr, curo.ErrNilBaseTransport) {
			t.Errorf("RoundTrip() with a %T body error = %v, want ErrNilBaseTransport",
				body, roundTripErr)
		}
	}
	if got := counting.closes.Load(); got != 1 {
		t.Errorf("request body closes = %d, want 1", got)
	}
}

func TestCloseIdleConnectionsReachesBase(t *testing.T) {
	t.Parallel()

	base := &idleClosingRoundTripper{}
	transport, err := curo.New(base)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	client := &http.Client{Transport: transport}

	client.CloseIdleConnections()
	if got := base.idleCloses.Load(); got != 1 {
		t.Errorf("base CloseIdleConnections calls = %d, want 1", got)
	}

	if err := transport.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := base.idleCloses.Load(); got != 1 {
		t.Errorf("base CloseIdleConnections calls after Close = %d, want 1", got)
	}
	client.CloseIdleConnections()
	if got := base.idleCloses.Load(); got != 2 {
		t.Errorf("base CloseIdleConnections calls after a closed call = %d, want 2", got)
	}
}

func TestCloseIdleConnectionsIgnoresBaseWithoutIt(t *testing.T) {
	t.Parallel()

	transport, err := curo.New(roundTripperFunc(nil))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	transport.CloseIdleConnections()
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type closableRoundTripper struct {
	response *http.Response
	attempts atomic.Int32
	closes   atomic.Int32
}

func (t *closableRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	t.attempts.Add(1)
	return t.response, nil
}

func (t *closableRoundTripper) Close() error {
	t.closes.Add(1)
	return nil
}

type idleClosingRoundTripper struct {
	idleCloses atomic.Int32
}

func (*idleClosingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
}

func (t *idleClosingRoundTripper) CloseIdleConnections() {
	t.idleCloses.Add(1)
}

type closeCountingBody struct {
	closes atomic.Int32
}

func (*closeCountingBody) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (body *closeCountingBody) Close() error {
	body.closes.Add(1)
	return nil
}

type panickingCloseBody struct{}

func (panickingCloseBody) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (panickingCloseBody) Close() error {
	panic("request body Close")
}
