package curo

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/raj1kshtz/curo/internal/guard"
	"github.com/raj1kshtz/curo/internal/observe"
)

// Transport is an explicit, concurrency-safe wrapper around a host-owned
// http.RoundTripper.
//
// The current implementation delegates every request to the base transport
// exactly once without mutation. Observe and Enforce collect bounded result
// evidence behind narrow failure boundaries without wrapping the host-owned
// transport call, evaluate deterministic diagnoses and control candidates,
// and publish them through Report. Mitigation is not implemented yet, so
// candidates are never applied.
//
// A Transport must not be copied after first use.
type Transport struct {
	base     http.RoundTripper
	guard    *guard.Guard
	observer *observe.Observer
	stages   requestStages
	reports  func() observe.Report

	mode   atomic.Uint32
	closed atomic.Bool
}

type requestStages interface {
	preflight(requestSnapshot, Mode) (requestState, error)
	postflight(requestState, attemptResult) error
}

type requestState struct {
	observation observe.Token
}

type requestSnapshot struct {
	requestContext context.Context
	method         string
	scheme         string
	hostname       string
	port           string
}

type attemptResult struct {
	err         error
	statusCode  int
	hasResponse bool
}

type observationStages struct {
	observer *observe.Observer
}

func (stages observationStages) preflight(
	snapshot requestSnapshot,
	_ Mode,
) (requestState, error) {
	token := stages.observer.Begin(observe.Request{
		Context:  snapshot.requestContext,
		Method:   snapshot.method,
		Scheme:   snapshot.scheme,
		Hostname: snapshot.hostname,
		Port:     snapshot.port,
	})

	return requestState{observation: token}, nil
}

func (stages observationStages) postflight(
	state requestState,
	result attemptResult,
) error {
	stages.observer.Finish(state.observation, observe.Result{
		Err:         result.err,
		StatusCode:  result.statusCode,
		HasResponse: result.hasResponse,
	})

	return nil
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

	observer := observe.New()
	transport := &Transport{
		base:     base,
		guard:    guard.New(),
		observer: observer,
		stages:   observationStages{observer: observer},
		reports:  observer.Report,
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
	var state requestState
	if stages != nil && !t.closed.Load() {
		mode := Mode(t.mode.Load())
		if mode != Off && !t.guard.Disabled() {
			runPostflight = t.guard.Run(func() error {
				var err error
				state, err = stages.preflight(snapshotRequest(req), mode)
				return err
			}) == guard.Completed
		}
	}

	response, err := t.base.RoundTrip(req)

	if runPostflight {
		result := attemptResult{
			err:         err,
			hasResponse: response != nil,
		}
		if response != nil {
			result.statusCode = response.StatusCode
		}

		_ = t.guard.Run(func() error {
			return stages.postflight(state, result)
		})
	}

	return response, err
}

func snapshotRequest(request *http.Request) requestSnapshot {
	if request == nil {
		return requestSnapshot{requestContext: context.Background()}
	}

	snapshot := requestSnapshot{
		requestContext: request.Context(),
		method:         request.Method,
	}
	if request.URL != nil {
		snapshot.scheme = request.URL.Scheme
		snapshot.hostname, snapshot.port = snapshotAuthority(request.URL)
	}

	return snapshot
}

func snapshotAuthority(address *url.URL) (string, string) {
	rawAuthority := address.Host
	if rawAuthority == "" || strings.Contains(rawAuthority, "@") {
		return "", ""
	}

	port := address.Port()
	if strings.HasPrefix(rawAuthority, "[") {
		closingBracket := strings.LastIndex(rawAuthority, "]")
		if closingBracket < 0 {
			return "", ""
		}

		suffix := rawAuthority[closingBracket+1:]
		if suffix != "" && (len(suffix) < 2 || suffix[0] != ':' || port == "") {
			return "", ""
		}
	} else {
		switch strings.Count(rawAuthority, ":") {
		case 0:
		case 1:
			if port == "" {
				return "", ""
			}
		default:
			return "", ""
		}
	}

	return address.Hostname(), port
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
// to the base transport. Close does not close the base transport or discard
// the bounded observation snapshot.
func (t *Transport) Close() error {
	if t == nil || t.base == nil {
		return ErrNilBaseTransport
	}

	t.closed.Store(true)
	return nil
}
