package curo_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	transport, err := curo.New(
		http.DefaultTransport,
		curo.WithMode(curo.Enforce),
		curo.WithTimeoutBounds(time.Second, 10*time.Second),
	)
	if err != nil {
		fmt.Println("setup failed")
		return
	}
	defer func() {
		_ = transport.Close()
	}()

	fmt.Println(transport.Mode() == curo.Enforce)
	fmt.Println(transport.Stats().Timeouts)

	_, err = curo.New(
		http.DefaultTransport,
		curo.WithTimeoutBounds(10*time.Second, time.Second),
	)
	fmt.Println(err != nil)
	// Output:
	// true
	// 0
	// true
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
