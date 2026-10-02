package curo

import "errors"

var (
	// ErrNilBaseTransport indicates that a Transport has no base RoundTripper.
	ErrNilBaseTransport = errors.New("curo: base transport is nil")

	// ErrInvalidMode indicates that a Mode is not one of Off, Observe, or Enforce.
	ErrInvalidMode = errors.New("curo: invalid mode")

	// ErrClosed indicates that an operation requires an open Transport.
	ErrClosed = errors.New("curo: transport is closed")

	// ErrBreakerOpen indicates that Enforce failed a request fast because the
	// dependency breaker of the request's target is open. The base transport
	// was not called, so the request was not sent.
	ErrBreakerOpen = errors.New("curo: dependency breaker is open")
)
