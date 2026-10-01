package curo

import (
	"net/http"
	"sync/atomic"
)

// Transport is an explicit, concurrency-safe wrapper around a host-owned
// http.RoundTripper.
//
// The current implementation delegates every request to the base transport
// exactly once without mutation. Adaptive observation and mitigation will be
// added behind this API in later releases.
//
// A Transport must not be copied after first use.
type Transport struct {
	base   http.RoundTripper
	mode   atomic.Uint32
	closed atomic.Bool
}

// New constructs a Transport around base.
//
// New does not take ownership of base. Closing the returned Transport never
// closes base. A nil base is rejected so fallback ownership remains explicit.
func New(base http.RoundTripper, options ...Option) (*Transport, error) {
	if base == nil {
		return nil, ErrNilBaseTransport
	}

	cfg, err := applyOptions(options)
	if err != nil {
		return nil, err
	}

	transport := &Transport{base: base}
	transport.mode.Store(uint32(cfg.mode))

	return transport, nil
}

// RoundTrip delegates req to the base transport without mutation.
//
// RoundTrip remains available after Close and preserves the base transport's
// response, error, and panic behavior.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t == nil || t.base == nil {
		return nil, ErrNilBaseTransport
	}

	return t.base.RoundTrip(req)
}

// Mode returns the configured operating mode.
//
// The zero value and a nil *Transport report Off.
func (t *Transport) Mode() Mode {
	if t == nil {
		return Off
	}

	return Mode(t.mode.Load())
}

// SetMode changes the operating mode for requests that start after the change.
func (t *Transport) SetMode(mode Mode) error {
	if t == nil || t.base == nil {
		return ErrNilBaseTransport
	}
	if t.closed.Load() {
		return ErrClosed
	}
	if !mode.valid() {
		return invalidModeError(mode)
	}

	t.mode.Store(uint32(mode))
	return nil
}

// Close releases resources owned by the Transport.
//
// Close is idempotent. Requests made after Close continue to delegate directly
// to the base transport. Close does not close the base transport.
func (t *Transport) Close() error {
	if t == nil || t.base == nil {
		return ErrNilBaseTransport
	}

	t.closed.Store(true)
	return nil
}
