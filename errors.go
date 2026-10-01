package curo

import "errors"

var (
	// ErrNilBaseTransport indicates that a Transport has no base RoundTripper.
	ErrNilBaseTransport = errors.New("curo: base transport is nil")

	// ErrInvalidMode indicates that a Mode is not one of Off, Observe, or Enforce.
	ErrInvalidMode = errors.New("curo: invalid mode")

	// ErrClosed indicates that an operation requires an open Transport.
	ErrClosed = errors.New("curo: transport is closed")
)
