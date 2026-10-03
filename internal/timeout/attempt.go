package timeout

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

// Timer is a started timer. Stop prevents it from firing if it has not fired
// yet.
type Timer interface {
	Stop() bool
}

// AfterFunc starts a Timer that calls fire on its own goroutine after limit.
type AfterFunc func(limit time.Duration, fire func()) Timer

// SystemAfterFunc is the AfterFunc backed by time.AfterFunc.
func SystemAfterFunc(limit time.Duration, fire func()) Timer {
	return time.AfterFunc(limit, fire)
}

type state uint8

const (
	pending state = iota
	settled
	expired
)

// Attempt bounds how long one attempt may wait for response headers.
//
// Exactly one of two things ends a pending Attempt. Settle wins when the
// attempt returns first. Expire wins when the timer fires first and its own
// cancellation is what ended the attempt's context. A caller cancellation that
// reached the context first leaves the Attempt to Settle, so a caller's
// cancellation is never reported as a timeout.
//
// The timeout is cooperative: Expire cancels the attempt's context, and the
// attempt ends when the transport using that context returns.
type Attempt struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	legacy <-chan struct{}
	timer  Timer
	body   io.ReadCloser
	cause  expiry
	mu     sync.Mutex
	state  state
}

// expiry is the cancellation cause of one Attempt's timer. Each Attempt owns
// its cause, so a parent context canceled with any other error, including
// another Attempt's cause, is never mistaken for this Attempt's timeout.
type expiry struct {
	limit time.Duration
}

func (cause *expiry) Error() string {
	return "curo: adaptive timeout of " + cause.limit.String() +
		" awaiting response headers"
}

// Timeout reports that the cause is a timeout.
func (*expiry) Timeout() bool {
	return true
}

// New derives an attempt's context from parent. legacy is the request's
// deprecated Cancel channel, or nil. Closing it before the timer fires keeps
// the timer from ending the attempt. The Attempt does nothing until Arm starts
// its timer.
func New(
	parent context.Context,
	legacy <-chan struct{},
	limit time.Duration,
) *Attempt {
	ctx, cancel := context.WithCancelCause(parent)

	return &Attempt{
		ctx:    ctx,
		cancel: cancel,
		legacy: legacy,
		cause:  expiry{limit: limit},
	}
}

// Context returns the context the attempt must use.
func (attempt *Attempt) Context() context.Context {
	return attempt.ctx
}

// Arm starts the timer. When the limit passed to New elapses, fire runs on the
// timer's goroutine. fire decides whether the attempt may still be ended, and
// if so calls Expire. Arm must be called once, by the goroutine that later
// calls Settle.
func (attempt *Attempt) Arm(after AfterFunc, fire func()) {
	attempt.timer = after(attempt.cause.limit, fire)
}

// Expire ends a pending attempt by canceling its context, and reports whether
// the timeout ended it. It does nothing after Settle, after the deprecated
// Cancel channel closed, or after the parent context was canceled.
func (attempt *Attempt) Expire() bool {
	attempt.mu.Lock()
	defer attempt.mu.Unlock()

	if attempt.state != pending || canceled(attempt.legacy) {
		return false
	}

	cause := error(&attempt.cause)
	attempt.cancel(cause)
	//nolint:errorlint // Identity shows which cancellation reached the context first.
	if context.Cause(attempt.ctx) != cause {
		return false
	}

	attempt.state = expired
	return true
}

// Settle stops the timer of an attempt that returned, and reports whether the
// attempt returned before Expire ended it. Settle records that decision
// before it stops the timer, so a Timer whose Stop fails cannot change it.
// Settle is idempotent.
func (attempt *Attempt) Settle() bool {
	attempt.mu.Lock()
	defer attempt.mu.Unlock()

	switch attempt.state {
	case expired:
		return false
	case pending:
		attempt.state = settled
		if attempt.timer != nil {
			attempt.timer.Stop()
		}
	}

	return true
}

// Release cancels the attempt's context. Call it once nothing uses the
// context, such as when the attempt failed or its response has no body.
func (attempt *Attempt) Release() {
	attempt.cancel(nil)
}

// Abandon settles the attempt and releases its context, even when stopping
// the timer fails. It is for exits that return no result, such as a
// panicking transport.
func (attempt *Attempt) Abandon() {
	defer attempt.Release()

	attempt.Settle()
}

// Body returns body wrapped so that closing it releases the attempt's
// context, and so does reading it to EOF unless body is writable. A writable
// body, such as the connection of a 101 Switching Protocols response, stays
// an io.ReadWriteCloser and is released only on Close, because its write side
// can outlive read EOF. Body must be called once, after Settle reported true.
func (attempt *Attempt) Body(body io.ReadCloser) io.ReadCloser {
	attempt.body = body
	if _, writable := body.(io.Writer); writable {
		return (*writableBody)(attempt)
	}

	return (*readBody)(attempt)
}

// readBody is an Attempt viewed as its response body, so wrapping the body
// allocates nothing.
type readBody Attempt

func (view *readBody) Read(p []byte) (int, error) {
	n, err := view.body.Read(p)
	if errors.Is(err, io.EOF) {
		(*Attempt)(view).Release()
	}

	return n, err
}

func (view *readBody) Close() error {
	defer (*Attempt)(view).Release()

	return view.body.Close()
}

// writableBody is an Attempt viewed as its writable response body.
type writableBody Attempt

func (view *writableBody) Read(p []byte) (int, error) {
	return view.body.Read(p)
}

// Write writes to the wrapped body, which Body checked is an io.Writer.
func (view *writableBody) Write(p []byte) (int, error) {
	return view.body.(io.Writer).Write(p)
}

func (view *writableBody) Close() error {
	defer (*Attempt)(view).Release()

	return view.body.Close()
}

func canceled(legacy <-chan struct{}) bool {
	select {
	case <-legacy:
		return true
	default:
		return false
	}
}
