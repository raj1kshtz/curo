package guard

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunContainsPanicWithoutHoldingFailureLock(t *testing.T) {
	t.Parallel()

	state := New()
	if outcome := state.Run(func() error {
		panic("internal failure")
	}); outcome != Failed {
		t.Fatalf("panic outcome = %v, want Failed", outcome)
	}
	if state.Disabled() {
		t.Fatal("guard disabled after one failure, want enabled")
	}
	if got := state.Failures(); got != 1 {
		t.Errorf("Failures() = %d, want 1", got)
	}

	done := make(chan Outcome, 1)
	go func() {
		done <- state.Run(func() error {
			return nil
		})
	}()

	select {
	case outcome := <-done:
		if outcome != Completed {
			t.Errorf("second outcome = %v, want Completed", outcome)
		}
	case <-time.After(time.Second):
		t.Fatal("second guarded operation blocked after recovered panic")
	}
}

func TestRunCountsReturnedErrors(t *testing.T) {
	t.Parallel()

	state := New()
	if outcome := state.Run(func() error {
		return errors.New("internal failure")
	}); outcome != Failed {
		t.Fatalf("error outcome = %v, want Failed", outcome)
	}
	if state.Disabled() {
		t.Fatal("guard disabled after one failure, want enabled")
	}
}

func TestGuardDisablesAtFailureThreshold(t *testing.T) {
	t.Parallel()

	state := New()
	var calls atomic.Int32

	for attempt := 1; attempt <= failureThreshold; attempt++ {
		outcome := state.Run(func() error {
			calls.Add(1)
			return errors.New("internal failure")
		})
		if outcome != Failed {
			t.Fatalf("attempt %d outcome = %v, want Failed", attempt, outcome)
		}

		wantDisabled := attempt == failureThreshold
		if got := state.Disabled(); got != wantDisabled {
			t.Fatalf(
				"attempt %d Disabled() = %t, want %t",
				attempt,
				got,
				wantDisabled,
			)
		}
	}

	if outcome := state.Run(func() error {
		calls.Add(1)
		return nil
	}); outcome != Skipped {
		t.Fatalf("post-disable outcome = %v, want Skipped", outcome)
	}
	if got := calls.Load(); got != failureThreshold {
		t.Errorf("operation calls = %d, want %d", got, failureThreshold)
	}

	trippedAt := state.trippedAt
	state.recordFailure()
	if got := state.failureCount; got != failureThreshold {
		t.Errorf("failure count after disable = %d, want %d", got, failureThreshold)
	}
	if got := state.Failures(); got != failureThreshold+1 {
		t.Errorf("total failures after disable = %d, want %d", got, failureThreshold+1)
	}
	if !state.trippedAt.Equal(trippedAt) {
		t.Errorf("trip time changed after disable: got %v, want %v", state.trippedAt, trippedAt)
	}
}

func TestFailureWindowExpiresOldFailures(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(time.Unix(1_000, 0))
	state := newGuard(clock.Now)

	for range failureThreshold - 1 {
		if outcome := state.Run(returnInternalError); outcome != Failed {
			t.Fatalf("initial outcome = %v, want Failed", outcome)
		}
	}

	clock.Advance(failureWindow + time.Nanosecond)
	if outcome := state.Run(returnInternalError); outcome != Failed {
		t.Fatalf("expired-window outcome = %v, want Failed", outcome)
	}
	if state.Disabled() {
		t.Fatal("guard disabled using failures outside the rolling window")
	}

	for range failureThreshold - 1 {
		if outcome := state.Run(returnInternalError); outcome != Failed {
			t.Fatalf("replacement outcome = %v, want Failed", outcome)
		}
	}
	if !state.Disabled() {
		t.Fatal("guard enabled after three replacement failures, want disabled")
	}
}

func TestFailureAtWindowBoundaryCounts(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(time.Unix(2_000, 0))
	state := newGuard(clock.Now)

	for range failureThreshold - 1 {
		if outcome := state.Run(returnInternalError); outcome != Failed {
			t.Fatalf("initial outcome = %v, want Failed", outcome)
		}
	}

	clock.Advance(failureWindow)
	if outcome := state.Run(returnInternalError); outcome != Failed {
		t.Fatalf("boundary outcome = %v, want Failed", outcome)
	}
	if !state.Disabled() {
		t.Fatal("guard enabled when three failures occurred at the window boundary")
	}
}

func TestGuardIsSafeUnderConcurrentFailures(t *testing.T) {
	t.Parallel()

	const workers = 64

	state := New()
	start := make(chan struct{})
	var failed atomic.Int32
	var skipped atomic.Int32
	var wait sync.WaitGroup
	wait.Add(workers)

	for range workers {
		go func() {
			defer wait.Done()
			<-start

			switch state.Run(returnInternalError) {
			case Failed:
				failed.Add(1)
			case Skipped:
				skipped.Add(1)
			default:
				t.Error("guarded failure completed successfully")
			}
		}()
	}

	close(start)
	wait.Wait()

	if !state.Disabled() {
		t.Fatal("guard enabled after concurrent failures, want disabled")
	}
	if got := failed.Load(); got < failureThreshold {
		t.Errorf("failed outcomes = %d, want at least %d", got, failureThreshold)
	}
	if got := failed.Load() + skipped.Load(); got != workers {
		t.Errorf("recorded outcomes = %d, want %d", got, workers)
	}
	if got, want := state.Failures(), uint64(failed.Load()); got != want {
		t.Errorf("Failures() = %d, want %d", got, want)
	}
}

func TestNilGuardFailsSafe(t *testing.T) {
	t.Parallel()

	var state *Guard
	var called atomic.Bool

	if !state.Disabled() {
		t.Fatal("nil Guard Disabled() = false, want true")
	}
	if outcome := state.Run(func() error {
		called.Store(true)
		return nil
	}); outcome != Skipped {
		t.Fatalf("nil Guard outcome = %v, want Skipped", outcome)
	}
	if called.Load() {
		t.Fatal("nil Guard ran operation")
	}
	if got := state.Failures(); got != 0 {
		t.Errorf("nil Guard Failures() = %d, want 0", got)
	}
}

func TestContainRunsAfterDisableAndCountsFailures(t *testing.T) {
	t.Parallel()

	state := New()
	for range failureThreshold {
		if outcome := state.Run(returnInternalError); outcome != Failed {
			t.Fatalf("disabling outcome = %v, want Failed", outcome)
		}
	}
	if !state.Disabled() {
		t.Fatal("guard enabled after failure threshold, want disabled")
	}

	var calls atomic.Int32
	if outcome := state.Contain(func() error {
		calls.Add(1)
		return nil
	}); outcome != Completed {
		t.Fatalf("disabled Contain() outcome = %v, want Completed", outcome)
	}
	if outcome := state.Contain(func() error {
		calls.Add(1)
		panic("read failure")
	}); outcome != Failed {
		t.Fatalf("disabled Contain() panic outcome = %v, want Failed", outcome)
	}
	if outcome := state.Contain(func() error {
		calls.Add(1)
		return errors.New("read failure")
	}); outcome != Failed {
		t.Fatalf("disabled Contain() error outcome = %v, want Failed", outcome)
	}

	if got := calls.Load(); got != 3 {
		t.Errorf("Contain() calls = %d, want 3", got)
	}
	if got := state.Failures(); got != failureThreshold+2 {
		t.Errorf("Failures() = %d, want %d", got, failureThreshold+2)
	}

	var nilGuard *Guard
	if outcome := nilGuard.Contain(func() error {
		calls.Add(1)
		return nil
	}); outcome != Skipped {
		t.Fatalf("nil Guard Contain() outcome = %v, want Skipped", outcome)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("nil Guard Contain() ran operation; calls = %d, want 3", got)
	}
}

func TestZeroValueGuardContainsFailure(t *testing.T) {
	t.Parallel()

	var state Guard
	if outcome := state.Run(returnInternalError); outcome != Failed {
		t.Fatalf("zero-value Guard outcome = %v, want Failed", outcome)
	}
	if state.Disabled() {
		t.Fatal("zero-value Guard disabled after one failure, want enabled")
	}
}

func TestNewGuardAcceptsNilClock(t *testing.T) {
	t.Parallel()

	state := newGuard(nil)
	if state.now == nil {
		t.Fatal("newGuard(nil) clock = nil, want system clock")
	}
	if outcome := state.Run(func() error {
		return nil
	}); outcome != Completed {
		t.Fatalf("newGuard(nil) outcome = %v, want Completed", outcome)
	}
}

func returnInternalError() error {
	return errors.New("internal failure")
}

type fakeClock struct {
	now time.Time
	mu  sync.Mutex
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(duration)
}
