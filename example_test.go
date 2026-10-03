package curo_test

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	"github.com/raj1kshtz/curo"
)

func ExampleNew() {
	transport, err := curo.New(
		http.DefaultTransport,
		curo.WithMode(curo.Observe),
	)
	if err != nil {
		fmt.Println("setup failed")
		return
	}
	defer func() {
		_ = transport.Close()
	}()

	client := &http.Client{Transport: transport}

	fmt.Println(transport.Mode() == curo.Observe)
	fmt.Println(client.Transport == transport)
	fmt.Println(transport.Stats().ObservedRequests)
	// Output:
	// true
	// true
	// 0
}

func ExampleWithTimeoutBounds() {
	// Keep the ceiling below the client's timeout. A hung read then ends at
	// the ceiling, which counts as a dependency failure and can open the
	// breaker, rather than at the client's deadline, which counts as the
	// caller giving up.
	transport, err := curo.New(
		http.DefaultTransport,
		curo.WithMode(curo.Enforce),
		curo.WithTimeoutBounds(2*time.Second, 9*time.Second),
	)
	if err != nil {
		fmt.Println("setup failed")
		return
	}
	defer func() {
		_ = transport.Close()
	}()

	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}

	fmt.Println(client.Transport == transport)
	fmt.Println(client.Timeout)
	fmt.Println(transport.Stats().Timeouts)

	_, err = curo.New(
		http.DefaultTransport,
		curo.WithTimeoutBounds(10*time.Second, time.Second),
	)
	fmt.Println(err != nil)
	// Output:
	// true
	// 10s
	// 0
	// true
}

func ExampleTransport_SetMode() {
	transport, err := curo.New(http.DefaultTransport)
	if err != nil {
		fmt.Println("setup failed")
		return
	}
	defer func() {
		_ = transport.Close()
	}()

	fmt.Println(transport.Mode())

	// Connect SetMode to a runtime control, such as an admin endpoint, so
	// that a rollout or a rollback needs no deploy. Each change applies to
	// requests that start after it. UnmarshalText accepts a mode name in any
	// letter case and rejects any other text with ErrInvalidMode.
	for _, name := range []string{"Enforce", "observe", "on", "OFF"} {
		var mode curo.Mode
		err = mode.UnmarshalText([]byte(name))
		if err != nil {
			fmt.Println("unknown mode:", name)
			continue
		}
		err = transport.SetMode(mode)
		if err != nil {
			fmt.Println("mode change failed")
			return
		}
		fmt.Println(transport.Mode())
	}

	_ = transport.Close()
	fmt.Println(errors.Is(transport.SetMode(curo.Enforce), curo.ErrClosed))
	// Output:
	// Observe
	// Enforce
	// Observe
	// unknown mode: on
	// Off
	// true
}

func ExampleTransport_Stats() {
	server := httptest.NewServer(http.HandlerFunc(
		func(response http.ResponseWriter, _ *http.Request) {
			response.WriteHeader(http.StatusNoContent)
		},
	))
	defer server.Close()

	transport, err := curo.New(server.Client().Transport)
	if err != nil {
		fmt.Println("setup failed")
		return
	}
	defer func() {
		_ = transport.Close()
	}()

	client := &http.Client{Transport: transport}
	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		server.URL,
		nil,
	)
	if err != nil {
		fmt.Println("request setup failed")
		return
	}
	response, err := client.Do(request)
	if err != nil {
		fmt.Println("request failed")
		return
	}
	_ = response.Body.Close()

	// expvar.Publish("curo", stats) serves the snapshot as JSON at
	// /debug/vars. The counters are cumulative, so alert on their rates.
	stats := expvar.Func(func() any {
		return transport.Stats()
	})
	fmt.Println(stats.String())
	// Output:
	// {"ObservedRequests":1,"TrackedTargets":1,"OverflowRequests":0,"RetryAttempts":0,"RetrySuccesses":0,"RetryBudgetDenials":0,"BreakerOpens":0,"BreakerProbes":0,"BreakerRejections":0,"Timeouts":0,"ShadowTimeouts":0,"InternalFailures":0,"SelfDisabled":false}
}

func Example_errorHandling() {
	describe := func(err error) string {
		switch {
		case err == nil:
			return "succeeded"
		case errors.Is(err, curo.ErrBreakerOpen):
			return "not sent: the dependency is down, so use a fallback"
		case errors.Is(err, curo.ErrTimeout):
			return "ended by Curo: the dependency may have received it"
		case errors.Is(err, context.DeadlineExceeded):
			return "the caller's deadline passed"
		default:
			return "failed"
		}
	}

	// http.Client wraps transport errors in *url.Error, which errors.Is
	// unwraps. ErrTimeout also matches context.DeadlineExceeded, so check it
	// first.
	for _, cause := range []error{
		curo.ErrBreakerOpen,
		curo.ErrTimeout,
		context.DeadlineExceeded,
	} {
		err := &url.Error{
			Op:  "Get",
			URL: "https://api.example/orders",
			Err: cause,
		}
		fmt.Println(describe(err))
	}
	// Output:
	// not sent: the dependency is down, so use a fallback
	// ended by Curo: the dependency may have received it
	// the caller's deadline passed
}

func ExampleTransport_Report() {
	server := httptest.NewServer(http.HandlerFunc(
		func(response http.ResponseWriter, _ *http.Request) {
			response.WriteHeader(http.StatusNoContent)
		},
	))
	defer server.Close()

	transport, err := curo.New(server.Client().Transport)
	if err != nil {
		fmt.Println("setup failed")
		return
	}
	defer func() {
		_ = transport.Close()
	}()

	client := &http.Client{Transport: transport}
	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		server.URL+"/orders/42?token=secret",
		nil,
	)
	if err != nil {
		fmt.Println("request setup failed")
		return
	}
	response, err := client.Do(request)
	if err != nil {
		fmt.Println("request failed")
		return
	}
	_ = response.Body.Close()

	for _, decision := range transport.Report().Targets {
		fmt.Printf(
			"%s %s: readiness=%s diagnosis=%s candidates=%s\n",
			decision.Target.Scheme,
			decision.Target.Method,
			decision.Readiness,
			decision.Diagnosis,
			decision.Candidates,
		)
	}
	// Output:
	// http Read: readiness=Warming diagnosis=None candidates=None
}
