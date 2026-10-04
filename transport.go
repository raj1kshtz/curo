package curo

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/raj1kshtz/curo/internal/breaker"
	"github.com/raj1kshtz/curo/internal/guard"
	"github.com/raj1kshtz/curo/internal/observe"
	"github.com/raj1kshtz/curo/internal/retry"
	"github.com/raj1kshtz/curo/internal/timeout"
)

// Transport is an explicit, concurrency-safe wrapper around a host-owned
// http.RoundTripper.
//
// Off delegates every request to the base transport exactly once without
// mutation. Observe and Enforce collect bounded result evidence behind narrow
// failure boundaries without wrapping the host-owned transport call, evaluate
// deterministic diagnoses and control candidates, and publish them through
// Report. Enforce also applies three candidates. For Retry, when every retry
// condition holds, it starts one budgeted retry of a replay-safe request with
// a clone of the original request. For BreakerOpen, it opens the target's
// dependency breaker, so later requests to that target fail fast with
// ErrBreakerOpen until a probe request gets an answer from the dependency.
// For Timeout, it ends a read request that waits longer than the target's
// adaptive timeout for response headers, and returns ErrTimeout.
//
// A Transport must not be copied after first use.
type Transport struct {
	base     http.RoundTripper
	guard    *guard.Guard
	observer *observe.Observer
	stages   requestStages
	reports  func() observe.Report
	wait     func(context.Context, <-chan struct{}, time.Duration) bool
	after    timeout.AfterFunc

	retries   retryCounters
	breakers  breakerCounters
	timeouts  timeoutCounters
	authority atomic.Uint64
	closed    atomic.Bool
}

// The authority word holds the operating mode in its low 32 bits and a
// generation in its high 32 bits. Every mode change advances the generation,
// so a pending retry detects any change made after its request started, even
// one that later restored Enforce.
const (
	authorityGenerationShift = 32
	authorityModeMask        = 1<<authorityGenerationShift - 1
)

type requestStages interface {
	preflight(requestSnapshot, uint64) (requestState, error)
	postflight(requestState, attemptResult) (retryGrant, error)
	confirmRetry(requestState) bool
}

type requestState struct {
	requestContext context.Context
	cancel         <-chan struct{}
	observation    observe.Token
	authority      uint64

	// timeout is the adaptive timeout for the request's attempts, or zero.
	// Enforce applies it. Observe compares the initial attempt's latency
	// with it.
	timeout    time.Duration
	admission  breaker.Admission
	replaySafe bool
}

type requestSnapshot struct {
	requestContext context.Context
	cancel         <-chan struct{}
	method         string
	scheme         string
	hostname       string
	port           string
	replaySafe     bool
}

type attemptResult struct {
	err        error
	header     http.Header
	statusCode int

	// timeout is the adaptive timeout that ended the attempt, or zero.
	timeout     time.Duration
	hasResponse bool
}

// retryGrant holds the budget reservation for one retry attempt that has not
// started yet, the backoff before it, and the first attempt's latency.
type retryGrant struct {
	lease        retry.Lease
	delay        time.Duration
	firstAttempt time.Duration
}

type retryCounters struct {
	attempts      atomic.Uint64
	successes     atomic.Uint64
	budgetDenials atomic.Uint64
}

type breakerCounters struct {
	opens      atomic.Uint64
	probes     atomic.Uint64
	rejections atomic.Uint64
}

type timeoutCounters struct {
	expired atomic.Uint64
	shadow  atomic.Uint64
}

type adaptiveStages struct {
	observer   *observe.Observer
	retries    *retryCounters
	breakers   *breakerCounters
	timeouts   *timeoutCounters
	authorized func(uint64) bool
	jitter     func() time.Duration
}

func newAdaptiveStages(
	observer *observe.Observer,
	retries *retryCounters,
	breakers *breakerCounters,
	timeouts *timeoutCounters,
	authorized func(uint64) bool,
) adaptiveStages {
	return adaptiveStages{
		observer:   observer,
		retries:    retries,
		breakers:   breakers,
		timeouts:   timeouts,
		authorized: authorized,
		jitter:     retry.Jitter,
	}
}

// preflight resolves the request's target. In Enforce mode it also asks the
// target's dependency breaker to admit the request. A request that passes
// gets the target's adaptive timeout, unless the caller already canceled it
// or its deadline is no later than the timeout would be.
func (stages adaptiveStages) preflight(
	snapshot requestSnapshot,
	authority uint64,
) (requestState, error) {
	token := stages.observer.Begin(observe.Request{
		Context:  snapshot.requestContext,
		Cancel:   snapshot.cancel,
		Method:   snapshot.method,
		Scheme:   snapshot.scheme,
		Hostname: snapshot.hostname,
		Port:     snapshot.port,
	})
	admission := breaker.Pass
	if authorityMode(authority) == Enforce {
		token, admission = stages.observer.Admit(token)
	}

	state := requestState{
		requestContext: snapshot.requestContext,
		cancel:         snapshot.cancel,
		observation:    token,
		authority:      authority,
		admission:      admission,
		replaySafe:     snapshot.replaySafe,
	}
	if admission == breaker.Pass {
		state.timeout = timeoutWithin(
			snapshot.requestContext,
			snapshot.cancel,
			stages.observer.Timeout(token),
		)
	}

	return state, nil
}

// timeoutWithin returns limit when a request with ctx and cancel can still
// run for longer than limit, and zero otherwise.
func timeoutWithin(
	ctx context.Context,
	cancel <-chan struct{},
	limit time.Duration,
) time.Duration {
	if limit <= 0 ||
		retry.Canceled(cancel) ||
		!retry.DeadlineAllows(ctx, 0, limit) {
		return 0
	}

	return limit
}

// postflight records the initial attempt and settles a breaker probe. In
// Enforce mode, a dependency failure opens the target's dependency breaker
// when the target's plan selects BreakerOpen and the request's authority is
// still current. When every retry condition holds, its last step reserves
// retry budget for one retry. An attempt that the adaptive timeout ended is
// never retried.
func (stages adaptiveStages) postflight(
	state requestState,
	result attemptResult,
) (retryGrant, error) {
	completion := stages.observer.Finish(state.observation, observe.Result{
		Err:         result.err,
		Timeout:     result.timeout,
		StatusCode:  result.statusCode,
		HasResponse: result.hasResponse,
	})
	if authorityMode(state.authority) == Observe &&
		state.timeout > 0 &&
		completion.Recorded &&
		completion.Latency > state.timeout {
		stages.timeouts.shadow.Add(1)
	}
	if completion.Trip &&
		stages.authorized(state.authority) &&
		stages.observer.Trip(state.observation) {
		stages.breakers.opens.Add(1)
	}
	if authorityMode(state.authority) != Enforce ||
		!state.replaySafe ||
		!completion.Recorded ||
		result.timeout > 0 ||
		!retry.Retryable(
			completion.DependencyFailure,
			result.err,
			result.statusCode,
			result.header,
		) {
		return retryGrant{}, nil
	}

	delay := stages.jitter()
	if retry.Canceled(state.cancel) ||
		!retry.DeadlineAllows(state.requestContext, delay, completion.Latency) {
		return retryGrant{}, nil
	}

	lease, reservation := stages.observer.ReserveRetry(state.observation)
	if reservation == observe.RetryBudgetExhausted {
		stages.retries.budgetDenials.Add(1)
	}

	return retryGrant{
		lease:        lease,
		delay:        delay,
		firstAttempt: completion.Latency,
	}, nil
}

// confirmRetry reports whether the target's published plan still permits the
// retry after the backoff.
func (stages adaptiveStages) confirmRetry(state requestState) bool {
	return stages.observer.RetryPermitted(state.observation)
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

	observer := observe.New(cfg.timeouts)
	transport := &Transport{
		base:     base,
		guard:    guard.New(failureReporter(cfg.logger)),
		observer: observer,
		reports:  observer.Report,
		wait:     retry.Wait,
		after:    timeout.SystemAfterFunc,
	}
	transport.stages = newAdaptiveStages(
		observer,
		&transport.retries,
		&transport.breakers,
		&transport.timeouts,
		transport.authorized,
	)
	transport.authority.Store(uint64(cfg.mode))

	return transport, nil
}

// RoundTrip delegates req to the base transport, unless a dependency breaker
// rejects it.
//
// Off, Observe, and Enforce requests that are neither rejected by a breaker
// nor retried reach the base transport exactly once, unmodified unless an
// adaptive timeout applies. In Enforce mode, a replay-safe request whose
// initial attempt failed in a retryable way may reach the base transport a
// second time, as a clone with a body from GetBody, after a short cancellable
// backoff. The retry's result is returned and the discarded first response
// body is closed without being drained. If the retry does not start, the
// first result is returned untouched. A done request context, or a closed
// deprecated Cancel channel, stops a retry that has not started.
//
// In Enforce mode, while the dependency breaker of req's target is open,
// RoundTrip closes req.Body, if any, and returns ErrBreakerOpen without calling
// the base transport. After a cooldown, one request at a time is sent as a
// probe instead, and a probe is never retried.
//
// In Enforce mode, a read request to a target whose decision selects
// CandidateTimeout, other than a probe, has an adaptive timeout unless its
// own deadline comes first. Each attempt reaches the base transport as a
// shallow copy of the request whose context Curo cancels if response headers
// do not arrive within the timeout. RoundTrip then returns ErrTimeout, closes
// any response that arrives late, and does not retry. Otherwise the response
// body is wrapped, so its concrete type is not preserved, and closing it or
// reading it to the end releases the timeout. A wrapped body keeps io.Writer
// when the original body has it, as for 101 Switching Protocols.
//
// Curo-owned stages run only before an attempt starts or after its result has
// been captured, except that an expiring adaptive timeout cancels the
// attempt's context from the timer's goroutine. Base transport calls remain
// outside Curo's recovery boundary, so RoundTrip preserves their error and
// panic behavior, and their response apart from a timed body. RoundTrip
// remains available after Close.
//
// The zero value and a nil *Transport close req.Body, if any, and return
// ErrNilBaseTransport.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t == nil || t.base == nil {
		if req != nil && req.Body != nil {
			closeUnsentBody(req.Body)
		}
		return nil, ErrNilBaseTransport
	}

	stages := t.stages
	runPostflight := false
	var (
		authority uint64
		state     requestState
	)
	if stages != nil && !t.closed.Load() {
		authority = t.authority.Load()
		if authorityMode(authority) != Off && !t.guard.Disabled() {
			runPostflight = t.guard.Run(guard.StagePreflight, func() error {
				var err error
				state, err = stages.preflight(snapshotRequest(req), authority)
				return err
			}) == guard.Completed
		}
	}

	var limit time.Duration
	if runPostflight {
		switch state.admission {
		case breaker.Reject:
			return nil, t.reject(req)
		case breaker.Probe:
			t.breakers.probes.Add(1)
		}
		if authorityMode(authority) == Enforce {
			limit = state.timeout
		}
	}

	response, cut, err := t.send(req, state.cancel, limit, authority)
	if !runPostflight {
		return response, err
	}

	var grant retryGrant
	outcome := t.guard.Run(guard.StagePostflight, func() error {
		result := captureAttempt(response, err)
		if cut {
			result.timeout = limit
		}

		var stageErr error
		grant, stageErr = stages.postflight(state, result)
		return stageErr
	})
	if !grant.lease.Held() {
		return response, err
	}
	if outcome != guard.Completed || authorityMode(authority) != Enforce {
		grant.lease.Cancel()
		return response, err
	}

	return t.retry(req, stages, state, authority, &grant, response, err)
}

func captureAttempt(response *http.Response, err error) attemptResult {
	result := attemptResult{err: err, hasResponse: response != nil}
	if response != nil {
		result.statusCode = response.StatusCode
		result.header = response.Header
	}

	return result
}

// retry starts at most one retry attempt for grant. Until the final commit
// check passes, every exit cancels the reservation and returns the first
// result untouched.
func (t *Transport) retry(
	req *http.Request,
	stages requestStages,
	state requestState,
	authority uint64,
	grant *retryGrant,
	response *http.Response,
	err error,
) (*http.Response, error) {
	next, body := t.prepareRetry(req, state.cancel, authority, grant.delay)
	permitted, limit := false, time.Duration(0)
	if next != nil {
		permitted, limit = t.commitRetry(req, stages, state, authority, grant.firstAttempt)
	}
	if !permitted {
		grant.lease.Cancel()
		if body != nil {
			t.closeBody(body)
		}
		return response, err
	}

	grant.lease.Commit()
	if response != nil && response.Body != nil {
		t.closeBody(response.Body)
	}
	t.retries.attempts.Add(1)

	retryResponse, _, retryErr := t.send(next, state.cancel, limit, authority)
	if retryErr == nil &&
		retryResponse != nil &&
		retryResponse.StatusCode >= http.StatusOK &&
		retryResponse.StatusCode < http.StatusBadRequest {
		t.retries.successes.Add(1)
	}

	return retryResponse, retryErr
}

// prepareRetry waits for the backoff and clones req for the retry attempt. It
// returns a nil request when the retry must not start, along with any fresh
// body it obtained from GetBody so the caller can close it.
func (t *Transport) prepareRetry(
	req *http.Request,
	cancel <-chan struct{},
	authority uint64,
	delay time.Duration,
) (*http.Request, io.ReadCloser) {
	var (
		next *http.Request
		body io.ReadCloser
	)
	_ = t.guard.Run(guard.StageRetry, func() error {
		ctx := req.Context()
		if !t.authorized(authority) || !t.wait(ctx, cancel, delay) {
			return nil
		}

		clone := req.Clone(ctx)
		if req.Body != nil && req.Body != http.NoBody {
			body = replayBody(req)
			if body == nil {
				return nil
			}
			clone.Body = body
		}
		next = clone

		return nil
	})

	return next, body
}

// replayBody returns a fresh copy of req's body from GetBody, or nil if
// GetBody fails. A GetBody error belongs to the application, so it prevents
// the retry without counting as a Curo failure.
func replayBody(req *http.Request) io.ReadCloser {
	body, err := req.GetBody()
	if err != nil {
		return nil
	}

	return body
}

// commitRetry is the final check before a retry starts. Authority, the caller's
// cancellation and deadline, and the target's published Retry plan must all
// still hold after the backoff. It also returns the request's adaptive
// timeout for the retry, or zero when the caller's deadline now comes first.
func (t *Transport) commitRetry(
	req *http.Request,
	stages requestStages,
	state requestState,
	authority uint64,
	firstAttempt time.Duration,
) (bool, time.Duration) {
	var (
		permitted bool
		limit     time.Duration
	)
	_ = t.guard.Run(guard.StageRetry, func() error {
		permitted = t.authorized(authority) &&
			!retry.Canceled(state.cancel) &&
			retry.DeadlineAllows(req.Context(), 0, firstAttempt) &&
			stages.confirmRetry(state)
		if permitted {
			limit = timeoutWithin(req.Context(), state.cancel, state.timeout)
		}
		return nil
	})

	return permitted, limit
}

// authorized reports whether the authority a request started with is still
// current. Any mode change or Close after the request started revokes it.
// Self-disable revokes it through the guard, which skips later stages. Opening
// a dependency breaker, starting a retry, and ending an attempt with an
// adaptive timeout all take effect only after this check passes.
func (t *Transport) authorized(authority uint64) bool {
	return t.authority.Load() == authority && !t.closed.Load()
}

// send makes one base transport call. A positive limit bounds the call with
// an adaptive timeout, and cancel is the request's deprecated Cancel channel.
// send reports whether the timeout ended the call. It then returns ErrTimeout
// and closes any response that arrived too late.
func (t *Transport) send(
	req *http.Request,
	cancel <-chan struct{},
	limit time.Duration,
	authority uint64,
) (*http.Response, bool, error) {
	var (
		attempt *timeout.Attempt
		timed   *http.Request
	)
	if limit > 0 {
		attempt, timed = t.arm(req, cancel, limit, authority)
	}
	if attempt == nil {
		response, err := t.base.RoundTrip(req)
		return response, false, err
	}

	returned := false
	defer func() {
		// A panicking base transport returns no result to settle. Its panic
		// keeps propagating: Contain recovers only a failure of Abandon.
		if !returned {
			_ = t.guard.Contain(guard.StageTimeout, func() error {
				attempt.Abandon()
				return nil
			})
		}
	}()
	//nolint:contextcheck // RoundTrip takes its context from the request.
	response, err := t.base.RoundTrip(timed)
	returned = true

	if !t.settle(attempt, response, err) {
		return response, false, err
	}

	t.timeouts.expired.Add(1)
	return nil, true, ErrTimeout
}

// arm starts an adaptive timeout of limit for req. It returns the attempt and
// a shallow copy of req that uses the attempt's context, or nil when the
// timeout did not start.
func (t *Transport) arm(
	req *http.Request,
	cancel <-chan struct{},
	limit time.Duration,
	authority uint64,
) (*timeout.Attempt, *http.Request) {
	var (
		attempt *timeout.Attempt
		timed   *http.Request
	)
	if t.guard.Run(guard.StageTimeout, func() error {
		armed := timeout.New(req.Context(), cancel, limit)
		attempt = armed
		timed = req.WithContext(armed.Context())
		armed.Arm(t.after, func() {
			t.expire(armed, authority)
		})
		return nil
	}) == guard.Completed {
		return attempt, timed
	}

	if attempt != nil {
		_ = t.guard.Contain(guard.StageTimeout, func() error {
			attempt.Abandon()
			return nil
		})
	}
	return nil, nil
}

// expire runs on the timer's goroutine when an attempt's adaptive timeout
// elapses. Like a retry that has not started, the timeout is withdrawn when
// the request's authority was revoked or Curo disabled itself.
func (t *Transport) expire(attempt *timeout.Attempt, authority uint64) {
	_ = t.guard.Run(guard.StageTimeout, func() error {
		if t.authorized(authority) {
			attempt.Expire()
		}
		return nil
	})
}

// settle ends a timed attempt after the base transport returned, and reports
// whether the timeout ended the attempt first. That result is discarded, so
// its body is closed. Otherwise the attempt's context is released once nothing
// uses it: at once for a result without a body, or when the caller closes the
// body or reads it to the end.
func (t *Transport) settle(
	attempt *timeout.Attempt,
	response *http.Response,
	err error,
) bool {
	// Settle decides the attempt before it stops the timer, the only step
	// that can fail, so an attempt whose Settle failed still returned.
	returned := true
	_ = t.guard.Contain(guard.StageTimeout, func() error {
		returned = attempt.Settle()
		return nil
	})
	if !returned {
		if response != nil && response.Body != nil {
			t.closeBody(response.Body)
		}
		return true
	}

	_ = t.guard.Contain(guard.StageTimeout, func() error {
		if err != nil ||
			response == nil ||
			response.Body == nil ||
			response.Body == http.NoBody {
			attempt.Release()
		} else {
			response.Body = attempt.Body(response.Body)
		}
		return nil
	})

	return false
}

// reject fails a request fast because its target's dependency breaker is open.
// The base transport is not called, so RoundTrip closes req.Body as a base
// transport would.
func (t *Transport) reject(req *http.Request) error {
	t.breakers.rejections.Add(1)
	if req != nil && req.Body != nil {
		t.closeBody(req.Body)
	}

	return ErrBreakerOpen
}

// closeBody closes a request body that Curo did not send or a response body
// that Curo discarded. The close error is ignored because the body is no
// longer part of any result.
func (t *Transport) closeBody(body io.Closer) {
	_ = t.guard.Contain(guard.StageBody, func() error {
		_ = body.Close()
		return nil
	})
}

// closeUnsentBody closes the body of a request that an uninitialized Transport
// rejects. Such a Transport has no guard, so a new one contains a panic from
// Close, as closeBody does.
func closeUnsentBody(body io.Closer) {
	_ = guard.New(nil).Contain(guard.StageBody, func() error {
		_ = body.Close()
		return nil
	})
}

func authorityMode(authority uint64) Mode {
	return Mode(authority & authorityModeMask)
}

func snapshotRequest(request *http.Request) requestSnapshot {
	if request == nil {
		return requestSnapshot{requestContext: context.Background()}
	}

	snapshot := requestSnapshot{
		requestContext: request.Context(),
		method:         request.Method,
		replaySafe:     retry.ReplaySafe(request),
	}
	//nolint:staticcheck // net/http sets the deprecated Cancel channel when an http.Client with a Timeout wraps an unrecognized transport.
	snapshot.cancel = request.Cancel
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

	return authorityMode(t.authority.Load())
}

// SetMode changes the operating mode for requests that start after the change.
//
// A request never gains authority after it starts. Any mode change also
// withdraws a retry that an earlier Enforce request has not started yet, and
// an adaptive timeout that has not expired yet, even if Enforce is restored
// before either would act.
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

	for {
		current := t.authority.Load()
		if authorityMode(current) == mode {
			return nil
		}

		generation := current>>authorityGenerationShift + 1
		next := generation<<authorityGenerationShift | uint64(mode)
		if t.authority.CompareAndSwap(current, next) {
			return nil
		}
	}
}

// Close releases resources owned by the Transport.
//
// Close is idempotent. Requests made after Close continue to delegate directly
// to the base transport, and retries that have not started and adaptive
// timeouts that have not expired are withdrawn. Close does not close the base
// transport or discard the bounded observation snapshot.
func (t *Transport) Close() error {
	if t == nil || t.base == nil {
		return ErrNilBaseTransport
	}

	t.closed.Store(true)
	return nil
}

// CloseIdleConnections calls the base transport's CloseIdleConnections
// method, if it has one, so that http.Client.CloseIdleConnections reaches the
// base transport through Curo. Otherwise it does nothing.
//
// CloseIdleConnections remains available after Close. The zero value and a
// nil *Transport do nothing.
func (t *Transport) CloseIdleConnections() {
	if t == nil || t.base == nil {
		return
	}

	type closeIdler interface {
		CloseIdleConnections()
	}
	if base, ok := t.base.(closeIdler); ok {
		base.CloseIdleConnections()
	}
}
