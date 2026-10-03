package timeout

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

type manualTimer struct {
	fire    func()
	limit   time.Duration
	stopped bool
}

func (timer *manualTimer) Stop() bool {
	wasActive := !timer.stopped
	timer.stopped = true
	return wasActive
}

func (timer *manualTimer) after(limit time.Duration, fire func()) Timer {
	timer.limit = limit
	timer.fire = fire
	return timer
}

func armed(
	t *testing.T,
	parent context.Context,
	legacy <-chan struct{},
) (*Attempt, *manualTimer) {
	t.Helper()

	attempt := New(parent, legacy, 2*time.Second)
	timer := &manualTimer{}
	attempt.Arm(timer.after, func() { attempt.Expire() })
	if timer.limit != 2*time.Second {
		t.Fatalf("timer limit = %v, want 2s", timer.limit)
	}

	return attempt, timer
}

func TestSettleBeforeExpiryKeepsTheContextUntilRelease(t *testing.T) {
	t.Parallel()

	attempt, timer := armed(t, context.Background(), nil)
	if !attempt.Settle() {
		t.Fatal("Settle() = false before expiry")
	}
	if !timer.stopped {
		t.Error("Settle() did not stop the timer")
	}
	if !attempt.Settle() {
		t.Error("second Settle() = false")
	}
	if attempt.Expire() {
		t.Error("Expire() after Settle() = true")
	}
	if err := attempt.Context().Err(); err != nil {
		t.Fatalf("context error before Release = %v", err)
	}

	attempt.Release()
	if !errors.Is(attempt.Context().Err(), context.Canceled) {
		t.Errorf("context error after Release = %v, want Canceled", attempt.Context().Err())
	}
	if cause := context.Cause(attempt.Context()); !errors.Is(cause, context.Canceled) {
		t.Errorf("cause after Release = %v, want Canceled", cause)
	}
}

func TestExpiryCancelsWithItsOwnCause(t *testing.T) {
	t.Parallel()

	attempt, timer := armed(t, context.Background(), nil)
	timer.fire()

	cause := context.Cause(attempt.Context())
	var timeout interface{ Timeout() bool }
	if !errors.As(cause, &timeout) || !timeout.Timeout() {
		t.Fatalf("cause = %v, want a timeout", cause)
	}
	if want := "curo: adaptive timeout of 2s awaiting response headers"; cause.Error() != want {
		t.Errorf("cause = %q, want %q", cause.Error(), want)
	}
	if attempt.Settle() {
		t.Error("Settle() after expiry = true")
	}
	if attempt.Expire() {
		t.Error("second Expire() = true")
	}
	if timer.stopped {
		t.Error("Settle() after expiry stopped the timer")
	}
}

func TestCallerCancellationIsNeverATimeout(t *testing.T) {
	t.Parallel()

	t.Run("context", func(t *testing.T) {
		t.Parallel()

		parent, cancel := context.WithCancel(context.Background())
		attempt, timer := armed(t, parent, nil)
		cancel()
		timer.fire()

		if !attempt.Settle() {
			t.Error("Settle() after a caller cancellation = false")
		}
		if cause := context.Cause(attempt.Context()); !errors.Is(cause, context.Canceled) {
			t.Errorf("cause = %v, want the caller's cancellation", cause)
		}
	})

	t.Run("another attempt's timeout", func(t *testing.T) {
		t.Parallel()

		outer, outerTimer := armed(t, context.Background(), nil)
		inner, innerTimer := armed(t, outer.Context(), nil)
		outerTimer.fire()
		innerTimer.fire()

		if !inner.Settle() {
			t.Error("inner Settle() after the outer timeout = false")
		}
		if outer.Settle() {
			t.Error("outer Settle() after its timeout = true")
		}
	})

	t.Run("deprecated cancel channel", func(t *testing.T) {
		t.Parallel()

		legacy := make(chan struct{})
		attempt, timer := armed(t, context.Background(), legacy)
		close(legacy)
		timer.fire()

		if !attempt.Settle() {
			t.Error("Settle() after closing Cancel = false")
		}
		if err := attempt.Context().Err(); err != nil {
			t.Errorf("context error = %v, want an active context", err)
		}
	})
}

func TestExactlyOneOfExpireAndSettleWins(t *testing.T) {
	t.Parallel()

	for range 1000 {
		attempt := New(context.Background(), nil, time.Second)
		start := make(chan struct{})
		expired := make(chan bool, 1)
		go func() {
			<-start
			expired <- attempt.Expire()
		}()
		close(start)
		settled := attempt.Settle()
		won := <-expired

		if settled == won {
			t.Fatalf("Settle() = %v and Expire() = %v, want exactly one winner", settled, won)
		}
		if canceled := attempt.Context().Err() != nil; canceled != won {
			t.Fatalf("context canceled = %v after Expire() = %v", canceled, won)
		}
		if again := attempt.Settle(); again != settled {
			t.Fatalf("second Settle() = %v, want %v", again, settled)
		}
		attempt.Release()
	}
}

func TestAbandonStopsTheTimerAndReleases(t *testing.T) {
	t.Parallel()

	attempt, timer := armed(t, context.Background(), nil)
	attempt.Abandon()
	if !timer.stopped {
		t.Error("Abandon() did not stop the timer")
	}
	if attempt.Context().Err() == nil {
		t.Error("Abandon() did not release the context")
	}

	expiredAttempt, expiredTimer := armed(t, context.Background(), nil)
	expiredTimer.fire()
	expiredAttempt.Abandon()
	if expiredAttempt.Settle() {
		t.Error("Settle() after expiry and Abandon() = true")
	}

	unarmed := New(context.Background(), nil, time.Second)
	unarmed.Abandon()
	if unarmed.Context().Err() == nil {
		t.Error("Abandon() of an unarmed attempt did not release the context")
	}
}

// failingTimer is a Timer whose Stop panics.
type failingTimer struct{}

func (failingTimer) Stop() bool {
	panic("stop")
}

func TestFailingStopKeepsTheAttemptSettled(t *testing.T) {
	t.Parallel()

	attempt := New(context.Background(), nil, time.Second)
	attempt.Arm(func(time.Duration, func()) Timer { return failingTimer{} }, func() {})
	func() {
		defer func() {
			if recovered := recover(); recovered != "stop" {
				t.Errorf("recovered %v, want the timer's panic", recovered)
			}
		}()
		attempt.Abandon()
	}()

	if attempt.Context().Err() == nil {
		t.Error("Abandon() with a failing Stop did not release the context")
	}
	if !attempt.Settle() || attempt.Expire() {
		t.Error("a failing Stop let the timeout end a settled attempt")
	}
}

type closeRecorder struct {
	io.Reader
	closed bool
}

func (body *closeRecorder) Close() error {
	body.closed = true
	return errors.New("close failed")
}

func TestReadBodyReleasesOnEOFOrClose(t *testing.T) {
	t.Parallel()

	t.Run("EOF", func(t *testing.T) {
		t.Parallel()

		attempt, _ := armed(t, context.Background(), nil)
		attempt.Settle()
		body := attempt.Body(&closeRecorder{Reader: strings.NewReader("ok")})
		if _, writable := body.(io.Writer); writable {
			t.Fatal("read body implements io.Writer")
		}

		buffer := make([]byte, 2)
		if n, err := body.Read(buffer); n != 2 || err != nil {
			t.Fatalf("Read() = %d, %v", n, err)
		}
		if attempt.Context().Err() != nil {
			t.Fatal("context released before EOF")
		}
		if _, err := body.Read(buffer); !errors.Is(err, io.EOF) {
			t.Fatalf("Read() error = %v, want EOF", err)
		}
		if attempt.Context().Err() == nil {
			t.Error("context not released at EOF")
		}
	})

	t.Run("close", func(t *testing.T) {
		t.Parallel()

		attempt, _ := armed(t, context.Background(), nil)
		attempt.Settle()
		recorder := &closeRecorder{Reader: strings.NewReader("ok")}
		body := attempt.Body(recorder)
		if err := body.Close(); err == nil || err.Error() != "close failed" {
			t.Errorf("Close() = %v, want the body's error", err)
		}
		if !recorder.closed || attempt.Context().Err() == nil {
			t.Error("Close() did not close the body and release the context")
		}
	})
}

type panickingCloser struct {
	io.Reader
}

func (panickingCloser) Close() error {
	panic("close")
}

func TestBodyCloseReleasesWhenTheBodyPanics(t *testing.T) {
	t.Parallel()

	attempt, _ := armed(t, context.Background(), nil)
	attempt.Settle()
	body := attempt.Body(panickingCloser{Reader: strings.NewReader("")})

	defer func() {
		if recovered := recover(); recovered != "close" {
			t.Errorf("recovered %v, want the body's panic", recovered)
		}
		if attempt.Context().Err() == nil {
			t.Error("context not released after a panicking Close")
		}
	}()
	_ = body.Close()
}

type connection struct {
	*bytes.Buffer
	written bytes.Buffer
	closed  bool
}

func (conn *connection) Write(p []byte) (int, error) {
	return conn.written.Write(p)
}

func (conn *connection) Close() error {
	conn.closed = true
	return nil
}

func TestWritableBodyReleasesOnlyOnClose(t *testing.T) {
	t.Parallel()

	attempt, _ := armed(t, context.Background(), nil)
	attempt.Settle()
	conn := &connection{Buffer: bytes.NewBufferString("hi")}
	body := attempt.Body(conn)

	writer, writable := body.(io.ReadWriteCloser)
	if !writable {
		t.Fatal("writable body does not implement io.ReadWriteCloser")
	}
	if got, err := io.ReadAll(writer); string(got) != "hi" || err != nil {
		t.Fatalf("ReadAll() = %q, %v", got, err)
	}
	if attempt.Context().Err() != nil {
		t.Fatal("writable body released at read EOF")
	}
	if n, err := writer.Write([]byte("back")); n != 4 || err != nil {
		t.Fatalf("Write() = %d, %v", n, err)
	}
	if conn.written.String() != "back" {
		t.Errorf("written = %q, want back", conn.written.String())
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if !conn.closed || attempt.Context().Err() == nil {
		t.Error("Close() did not close the connection and release the context")
	}
}

func TestSystemAfterFunc(t *testing.T) {
	t.Parallel()

	fired := make(chan struct{})
	SystemAfterFunc(time.Nanosecond, func() { close(fired) })
	<-fired

	timer := SystemAfterFunc(time.Hour, func() { t.Error("stopped timer fired") })
	if !timer.Stop() {
		t.Error("Stop() = false for a pending timer")
	}
}
