package curo

import (
	"net/http"
	"sync/atomic"

	"github.com/raj1kshtz/curo/internal/guard"
)

// Transport is an explicit, concurrency-safe wrapper around a host-owned
// http.RoundTripper.
//
// The current implementation delegates every request to the base transport
// exactly once without mutation. It also provides narrow failure boundaries
// for future Curo-owned request stages without wrapping the host-owned
// transport call. Adaptive observation and mitigation will be added behind
// this API in later releases.
//
// A Transport must not be copied after first use.
type Transport struct {
	base   http.RoundTripper
	guard  *guard.Guard
	stages requestStages

	mode   atomic.Uint32
	closed atomic.Bool
}

type requestStages interface {
	preflight(requestSnapshot, Mode) error
	postflight(attemptResult) error
}

type requestSnapshot struct {
	method string
	scheme string
	host   string
	path   string
}

type attemptResult struct {
	err        error
	statusCode int
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

	transport := &Transport{
		base:  base,
		guard: guard.New(),
	}
	transport.mode.Store(uint32(cfg.mode))

	return transport, nil
}

// RoundTrip delegates req to the base transport without mutation.
//
// Curo-owned stages run only before an attempt starts or after its result has
// been captured. The base transport call remains outside Curo's recovery
// boundary, so RoundTrip preserves its response, error, and panic behavior.
// RoundTrip remains available after Close.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t == nil || t.base == nil {
		return nil, ErrNilBaseTransport
	}

	stages := t.stages
	runPostflight := false
	if stages != nil && !t.closed.Load() {
		mode := Mode(t.mode.Load())
		if mode != Off && !t.guard.Disabled() {
			runPostflight = t.guard.Run(func() error {
				return stages.preflight(snapshotRequest(req), mode)
			}) == guard.Completed
		}
	}

	response, err := t.base.RoundTrip(req)

	if runPostflight {
		result := attemptResult{err: err}
		if response != nil {
			result.statusCode = response.StatusCode
		}

		_ = t.guard.Run(func() error {
			return stages.postflight(result)
		})
	}

	return response, err
}

func snapshotRequest(request *http.Request) requestSnapshot {
	if request == nil {
		return requestSnapshot{}
	}

	snapshot := requestSnapshot{method: request.Method}
	if request.URL != nil {
		snapshot.scheme = request.URL.Scheme
		snapshot.host = request.URL.Host
		snapshot.path = request.URL.Path
	}

	return snapshot
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
