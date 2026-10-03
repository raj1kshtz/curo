package curo

import (
	"context"
	"errors"
)

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

	// ErrTimeout indicates that Enforce ended a request because the base
	// transport did not return response headers within the target's adaptive
	// timeout. The request may have reached the dependency.
	//
	// Like the timeout error of an http.Client, ErrTimeout reports true from
	// Timeout, and errors.Is(ErrTimeout, context.DeadlineExceeded) is true.
	ErrTimeout error = &timeoutError{}
)

type timeoutError struct{}

func (*timeoutError) Error() string {
	return "curo: adaptive timeout awaiting response headers"
}

func (*timeoutError) Timeout() bool {
	return true
}

func (*timeoutError) Temporary() bool {
	return true
}

func (*timeoutError) Is(target error) bool {
	return target == context.DeadlineExceeded
}
