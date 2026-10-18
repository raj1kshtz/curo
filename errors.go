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

	// ErrUninitializedEngine indicates that an Engine is nil or was not
	// created by NewEngine.
	ErrUninitializedEngine = errors.New("curo: engine is not initialized")

	// ErrClosed indicates that an operation requires an open Transport or
	// Engine.
	ErrClosed = errors.New("curo: closed")

	// ErrBreakerOpen indicates that Enforce failed a request or call fast
	// because the dependency breaker of its target is open. Neither the base
	// transport nor the call's attempt ran, so nothing was sent.
	ErrBreakerOpen = errors.New("curo: dependency breaker is open")

	// ErrTimeout indicates that Enforce ended a request because the base
	// transport did not return response headers within the target's adaptive
	// timeout, or ended an Engine call's attempt that did not return within
	// it. The request or call may have reached the dependency.
	//
	// Like the timeout error of an http.Client, ErrTimeout reports true from
	// Timeout, and errors.Is(ErrTimeout, context.DeadlineExceeded) is true.
	ErrTimeout error = &timeoutError{}

	errNilAttempt = errors.New("curo: nil attempt")
)

type timeoutError struct{}

func (*timeoutError) Error() string {
	return "curo: adaptive timeout expired"
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
