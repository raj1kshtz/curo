package curo_test

import (
	"fmt"
	"net/http"

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
