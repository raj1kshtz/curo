package curo

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
)

// testProtocol is a wire protocol that the end-to-end tests run over. Curo
// wraps the base transport above the protocol, so its retries, breakers, and
// timeouts must behave the same whichever protocol the base transport speaks.
type testProtocol struct {
	start func(*testing.T, http.Handler) (*httptest.Server, http.RoundTripper)
	name  string
	major int
	tls   bool
}

func testProtocols() []testProtocol {
	protocols := []testProtocol{
		{name: "http1", start: startHTTP1Server, major: 1},
		{name: "https", start: startHTTPSServer, major: 1, tls: true},
		{name: "http2", start: startHTTP2Server, major: 2, tls: true},
	}

	return append(protocols, h2cTestProtocols()...)
}

// forEachProtocol runs test as a parallel subtest for each protocol.
func forEachProtocol(t *testing.T, test func(*testing.T, testProtocol)) {
	t.Helper()

	for _, protocol := range testProtocols() {
		t.Run(protocol.name, func(t *testing.T) {
			t.Parallel()
			test(t, protocol)
		})
	}
}

// serve starts a server for handler and returns it with a base transport that
// speaks the protocol. The server fails the test when a request arrives over
// any other protocol, so a silent fallback to HTTP/1.1 cannot pass.
func (protocol testProtocol) serve(
	t *testing.T,
	handler http.Handler,
) (*httptest.Server, http.RoundTripper) {
	t.Helper()

	checked := http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.ProtoMajor != protocol.major || (request.TLS != nil) != protocol.tls {
			t.Errorf(
				"server request = %s with TLS %t, want HTTP/%d with TLS %t",
				request.Proto,
				request.TLS != nil,
				protocol.major,
				protocol.tls,
			)
		}
		handler.ServeHTTP(response, request)
	})
	server, base := protocol.start(t, checked)
	t.Cleanup(server.Close)

	return server, base
}

func startHTTP1Server(_ *testing.T, handler http.Handler) (*httptest.Server, http.RoundTripper) {
	server := httptest.NewServer(handler)

	return server, server.Client().Transport
}

func startHTTPSServer(_ *testing.T, handler http.Handler) (*httptest.Server, http.RoundTripper) {
	server := httptest.NewTLSServer(handler)

	return server, server.Client().Transport
}

// startHTTP2Server serves HTTP/2 over TLS, which the base transport
// negotiates through ALPN, as http.DefaultTransport does.
func startHTTP2Server(_ *testing.T, handler http.Handler) (*httptest.Server, http.RoundTripper) {
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.StartTLS()

	return server, server.Client().Transport
}

func TestTransportPassesEveryProtocolThrough(t *testing.T) {
	t.Parallel()

	forEachProtocol(t, func(t *testing.T, protocol testProtocol) {
		server, base := protocol.serve(t, http.HandlerFunc(
			func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("X-Curo-Test", "pass-through")
				response.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(response, "ok")
			},
		))
		transport, err := New(base)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		t.Cleanup(func() {
			if closeErr := transport.Close(); closeErr != nil {
				t.Errorf("Close() error = %v", closeErr)
			}
		})
		client := &http.Client{Transport: transport}

		request, err := http.NewRequestWithContext(
			context.Background(),
			http.MethodGet,
			server.URL+"/items",
			http.NoBody,
		)
		if err != nil {
			t.Fatalf("NewRequestWithContext() error = %v", err)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatalf("Do() error = %v", err)
		}
		content, readErr := io.ReadAll(response.Body)
		closeTestBody(t, response)
		if readErr != nil {
			t.Fatalf("response body read error = %v", readErr)
		}
		if response.StatusCode != http.StatusCreated ||
			response.Header.Get("X-Curo-Test") != "pass-through" ||
			string(content) != "ok" {
			t.Errorf("response = %d %q with body %q, want the server's 201 response",
				response.StatusCode, response.Header.Get("X-Curo-Test"), content)
		}
		if response.ProtoMajor != protocol.major || (response.TLS != nil) != protocol.tls {
			t.Errorf("response = %s with TLS %t, want HTTP/%d with TLS %t",
				response.Proto, response.TLS != nil, protocol.major, protocol.tls)
		}

		serverURL, err := url.Parse(server.URL)
		if err != nil {
			t.Fatalf("url.Parse(%q) error = %v", server.URL, err)
		}
		port, err := strconv.ParseUint(serverURL.Port(), 10, 16)
		if err != nil {
			t.Fatalf("server port %q: %v", serverURL.Port(), err)
		}
		want := Target{Scheme: "http", Host: serverURL.Hostname(), Port: uint16(port), Method: MethodRead}
		if protocol.tls {
			want.Scheme = "https"
		}
		report := transport.Report()
		if len(report.Targets) != 1 || report.Targets[0].Target != want {
			t.Errorf("report targets = %+v, want only %+v", report.Targets, want)
		}
		if got := transport.Stats().ObservedRequests; got != 1 {
			t.Errorf("ObservedRequests = %d, want 1", got)
		}
	})
}

// TestUnverifiedCertificateIsDependencyFailure pins the documented rule that a
// certificate the client cannot verify counts as a dependency failure, while
// the caller still receives the verification error.
func TestUnverifiedCertificateIsDependencyFailure(t *testing.T) {
	t.Parallel()

	server := httptest.NewUnstartedServer(http.HandlerFunc(
		func(http.ResponseWriter, *http.Request) {
			t.Error("handler ran for a client that rejected the certificate")
		},
	))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	t.Cleanup(server.Close)

	base := &http.Transport{}
	t.Cleanup(base.CloseIdleConnections)
	transport, err := New(base)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := transport.Close(); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	})

	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		server.URL,
		http.NoBody,
	)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	response, err := (&http.Client{Transport: transport}).Do(request)
	if err == nil {
		closeTestBody(t, response)
		t.Fatal("Do() error = nil, want a certificate verification error")
	}
	var verification *tls.CertificateVerificationError
	if !errors.As(err, &verification) {
		t.Fatalf("Do() error = %v, want a *tls.CertificateVerificationError", err)
	}

	report := transport.Report()
	if len(report.Targets) != 1 {
		t.Fatalf("report targets = %+v, want one", report.Targets)
	}
	if recent := report.Targets[0].Recent; recent.Attempts != 1 ||
		recent.DependencyFailures != 1 {
		t.Errorf("recent evidence = %+v, want one attempt that is a dependency failure", recent)
	}
}
