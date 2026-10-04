package guard

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunContainsPanicWithoutHoldingFailureLock(t *testing.T) {
	t.Parallel()

	state := New(nil)
	if outcome := state.Run(StagePreflight, func() error {
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
		done <- state.Run(StagePreflight, func() error {
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

	state := New(nil)
	if outcome := state.Run(StagePreflight, func() error {
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

	state := New(nil)
	var calls atomic.Int32

	for attempt := 1; attempt <= failureThreshold; attempt++ {
		outcome := state.Run(StagePreflight, func() error {
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

	if outcome := state.Run(StagePreflight, func() error {
		calls.Add(1)
		return nil
	}); outcome != Skipped {
		t.Fatalf("post-disable outcome = %v, want Skipped", outcome)
	}
	if got := calls.Load(); got != failureThreshold {
		t.Errorf("operation calls = %d, want %d", got, failureThreshold)
	}

	trippedAt := state.trippedAt
	state.recordFailure(StageReport, nil, errors.New("internal failure"))
	if got := state.failures.count; got != failureThreshold {
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
	state := newGuard(clock.Now, nil)

	for range failureThreshold - 1 {
		if outcome := state.Run(StagePreflight, returnInternalError); outcome != Failed {
			t.Fatalf("initial outcome = %v, want Failed", outcome)
		}
	}

	clock.Advance(failureWindow + time.Nanosecond)
	if outcome := state.Run(StagePreflight, returnInternalError); outcome != Failed {
		t.Fatalf("expired-window outcome = %v, want Failed", outcome)
	}
	if state.Disabled() {
		t.Fatal("guard disabled using failures outside the rolling window")
	}

	for range failureThreshold - 1 {
		if outcome := state.Run(StagePreflight, returnInternalError); outcome != Failed {
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
	state := newGuard(clock.Now, nil)

	for range failureThreshold - 1 {
		if outcome := state.Run(StagePreflight, returnInternalError); outcome != Failed {
			t.Fatalf("initial outcome = %v, want Failed", outcome)
		}
	}

	clock.Advance(failureWindow)
	if outcome := state.Run(StagePreflight, returnInternalError); outcome != Failed {
		t.Fatalf("boundary outcome = %v, want Failed", outcome)
	}
	if !state.Disabled() {
		t.Fatal("guard enabled when three failures occurred at the window boundary")
	}
}

func TestGuardIsSafeUnderConcurrentFailures(t *testing.T) {
	t.Parallel()

	const workers = 64

	reporter := &recordingReporter{}
	state := New(reporter)
	start := make(chan struct{})
	var failed atomic.Int32
	var skipped atomic.Int32
	var wait sync.WaitGroup
	wait.Add(workers)

	for range workers {
		go func() {
			defer wait.Done()
			<-start

			switch state.Run(StagePreflight, returnInternalError) {
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

	failures, disabled := reporter.snapshot()
	if got := len(failures); got < 1 || got > failureThreshold {
		t.Errorf("reported failures = %d, want 1 to %d", got, failureThreshold)
	}
	var previous uint64
	for _, failure := range failures {
		if failure.Failures != previous+failure.Skipped+1 {
			t.Errorf("failure %d skipped %d after failure %d, want consecutive accounting", failure.Failures, failure.Skipped, previous)
		}
		previous = failure.Failures
	}
	if len(disabled) != 1 || disabled[0] != failureThreshold {
		t.Errorf("disable reports = %v, want [%d]", disabled, failureThreshold)
	}
}

func TestNilGuardFailsSafe(t *testing.T) {
	t.Parallel()

	var state *Guard
	var called atomic.Bool

	if !state.Disabled() {
		t.Fatal("nil Guard Disabled() = false, want true")
	}
	if outcome := state.Run(StagePreflight, func() error {
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

	state := New(nil)
	for range failureThreshold {
		if outcome := state.Run(StagePreflight, returnInternalError); outcome != Failed {
			t.Fatalf("disabling outcome = %v, want Failed", outcome)
		}
	}
	if !state.Disabled() {
		t.Fatal("guard enabled after failure threshold, want disabled")
	}

	var calls atomic.Int32
	if outcome := state.Contain(StageReport, func() error {
		calls.Add(1)
		return nil
	}); outcome != Completed {
		t.Fatalf("disabled Contain() outcome = %v, want Completed", outcome)
	}
	if outcome := state.Contain(StageReport, func() error {
		calls.Add(1)
		panic("read failure")
	}); outcome != Failed {
		t.Fatalf("disabled Contain() panic outcome = %v, want Failed", outcome)
	}
	if outcome := state.Contain(StageReport, func() error {
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
	if outcome := nilGuard.Contain(StageReport, func() error {
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
	if outcome := state.Run(StagePreflight, returnInternalError); outcome != Failed {
		t.Fatalf("zero-value Guard outcome = %v, want Failed", outcome)
	}
	if state.Disabled() {
		t.Fatal("zero-value Guard disabled after one failure, want enabled")
	}
}

func TestNewGuardAcceptsNilClock(t *testing.T) {
	t.Parallel()

	state := newGuard(nil, nil)
	if state.now == nil {
		t.Fatal("newGuard(nil, nil) clock = nil, want system clock")
	}
	if outcome := state.Run(StagePreflight, func() error {
		return nil
	}); outcome != Completed {
		t.Fatalf("newGuard(nil, nil) outcome = %v, want Completed", outcome)
	}
}

func TestStageString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want  string
		stage Stage
	}{
		{stage: StagePreflight, want: "preflight"},
		{stage: StagePostflight, want: "postflight"},
		{stage: StageRetry, want: "retry"},
		{stage: StageTimeout, want: "timeout"},
		{stage: StageBody, want: "body"},
		{stage: StageReport, want: "report"},
		{stage: 0, want: "unknown"},
		{stage: StageReport + 1, want: "unknown"},
		{stage: 255, want: "unknown"},
	}
	for _, test := range tests {
		if got := test.stage.String(); got != test.want {
			t.Errorf("Stage(%d).String() = %q, want %q", test.stage, got, test.want)
		}
	}
}

func TestReporterReceivesFailuresAndDisable(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	state := New(reporter)
	errFailed := errors.New("internal failure")

	if outcome := state.Run(StagePreflight, func() error {
		panic("preflight failure")
	}); outcome != Failed {
		t.Fatalf("panic outcome = %v, want Failed", outcome)
	}
	if outcome := state.Run(StagePostflight, func() error {
		return errFailed
	}); outcome != Failed {
		t.Fatalf("error outcome = %v, want Failed", outcome)
	}
	if _, disabled := reporter.snapshot(); len(disabled) != 0 {
		t.Fatalf("disable reports before the threshold = %v, want none", disabled)
	}
	if outcome := state.Contain(StageBody, func() error {
		panic(errFailed)
	}); outcome != Failed {
		t.Fatalf("disabling outcome = %v, want Failed", outcome)
	}
	if !state.Disabled() {
		t.Fatal("guard enabled after failure threshold, want disabled")
	}

	failures, disabled := reporter.snapshot()
	want := []Failure{
		{Recovered: "preflight failure", Failures: 1, Stage: StagePreflight},
		{Err: errFailed, Failures: 2, Stage: StagePostflight},
		{Recovered: errFailed, Failures: 3, Stage: StageBody},
	}
	if len(failures) != len(want) {
		t.Fatalf("reported failures = %d, want %d", len(failures), len(want))
	}
	for index := range want {
		if failures[index] != want[index] {
			t.Errorf("failure %d = %+v, want %+v", index, failures[index], want[index])
		}
	}
	if len(disabled) != 1 || disabled[0] != failureThreshold {
		t.Errorf("disable reports = %v, want [%d]", disabled, failureThreshold)
	}
}

func TestReportLimitSkipsFailuresAfterDisable(t *testing.T) {
	t.Parallel()

	const skipped = 4

	clock := newFakeClock(time.Unix(3_000, 0))
	reporter := &recordingReporter{}
	state := newGuard(clock.Now, reporter)

	for range failureThreshold {
		if outcome := state.Run(StageRetry, returnInternalError); outcome != Failed {
			t.Fatalf("disabling outcome = %v, want Failed", outcome)
		}
	}
	for range skipped {
		if outcome := state.Contain(StageBody, returnInternalError); outcome != Failed {
			t.Fatalf("disabled Contain() outcome = %v, want Failed", outcome)
		}
	}
	if failures, _ := reporter.snapshot(); len(failures) != failureThreshold {
		t.Fatalf("reported failures within the window = %d, want %d", len(failures), failureThreshold)
	}

	clock.Advance(failureWindow)
	if outcome := state.Contain(StageBody, returnInternalError); outcome != Failed {
		t.Fatalf("boundary Contain() outcome = %v, want Failed", outcome)
	}
	if failures, _ := reporter.snapshot(); len(failures) != failureThreshold {
		t.Fatalf("reported failures at the window boundary = %d, want %d", len(failures), failureThreshold)
	}

	clock.Advance(time.Nanosecond)
	if outcome := state.Contain(StageReport, returnInternalError); outcome != Failed {
		t.Fatalf("expired-window Contain() outcome = %v, want Failed", outcome)
	}
	failures, disabled := reporter.snapshot()
	if len(failures) != failureThreshold+1 {
		t.Fatalf("reported failures after the window = %d, want %d", len(failures), failureThreshold+1)
	}
	got := failures[failureThreshold]
	want := Failure{
		Err:      got.Err,
		Failures: failureThreshold + skipped + 2,
		Skipped:  skipped + 1,
		Stage:    StageReport,
	}
	if got != want || got.Err == nil {
		t.Errorf("failure after the window = %+v, want %+v with an error", got, want)
	}
	if len(disabled) != 1 {
		t.Errorf("disable reports = %v, want one", disabled)
	}

	if outcome := state.Contain(StageBody, returnInternalError); outcome != Failed {
		t.Fatalf("next Contain() outcome = %v, want Failed", outcome)
	}
	failures, _ = reporter.snapshot()
	if got := failures[len(failures)-1].Skipped; got != 0 {
		t.Errorf("skipped after a report = %d, want 0", got)
	}
}

func TestReporterPanicsAreContained(t *testing.T) {
	t.Parallel()

	reporter := &panickingReporter{}
	state := New(reporter)

	if outcome := state.Run(StagePreflight, func() error {
		panic("internal failure")
	}); outcome != Failed {
		t.Fatalf("panic outcome = %v, want Failed", outcome)
	}
	for range failureThreshold - 1 {
		if outcome := state.Run(StagePreflight, returnInternalError); outcome != Failed {
			t.Fatalf("error outcome = %v, want Failed", outcome)
		}
	}

	if !state.Disabled() {
		t.Fatal("guard enabled after failure threshold, want disabled")
	}
	if got := state.Failures(); got != failureThreshold {
		t.Errorf("Failures() = %d, want %d without reporter panics", got, failureThreshold)
	}
	if got := reporter.calls.Load(); got != failureThreshold+1 {
		t.Errorf("reporter calls = %d, want %d", got, failureThreshold+1)
	}
}

func TestReporterIsNotReentered(t *testing.T) {
	t.Parallel()

	reporter := &reentrantReporter{}
	state := New(reporter)
	reporter.state = state

	done := make(chan Outcome, 1)
	go func() {
		done <- state.Run(StagePreflight, returnInternalError)
	}()

	select {
	case outcome := <-done:
		if outcome != Failed {
			t.Fatalf("outcome = %v, want Failed", outcome)
		}
	case <-time.After(time.Second):
		t.Fatal("reporter blocked on the guard's lock")
	}
	if !state.Disabled() {
		t.Fatal("guard enabled after the reporter caused the failure threshold")
	}
	if reporter.reentered.Load() {
		t.Fatal("reporter was called while it was reporting")
	}

	if outcome := state.Contain(StageBody, returnInternalError); outcome != Failed {
		t.Fatalf("Contain() outcome after the report = %v, want Failed", outcome)
	}
	if got := state.Failures(); got != failureThreshold+1 {
		t.Errorf("Failures() = %d, want %d", got, failureThreshold+1)
	}
	failures, disabled := reporter.snapshot()
	want := []Failure{
		{Failures: 1, Stage: StagePreflight},
		{Failures: failureThreshold + 1, Skipped: failureThreshold - 1, Stage: StageBody},
	}
	if len(failures) != len(want) {
		t.Fatalf("reported failures = %d, want %d", len(failures), len(want))
	}
	for index := range want {
		got := failures[index]
		got.Err = nil
		if got != want[index] {
			t.Errorf("failure %d = %+v, want %+v", index, got, want[index])
		}
	}
	if len(disabled) != 1 || disabled[0] != failureThreshold {
		t.Errorf("disable reports = %v, want [%d]", disabled, failureThreshold)
	}
}

func TestFailuresDuringAReportAreSkipped(t *testing.T) {
	t.Parallel()

	reporter := &blockingReporter{
		entered: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	state := New(reporter)

	// fail records a failure on another goroutine, which must not wait for
	// the report in progress.
	fail := func() {
		t.Helper()

		done := make(chan Outcome, 1)
		go func() {
			done <- state.Contain(StageBody, returnInternalError)
		}()
		select {
		case outcome := <-done:
			if outcome != Failed {
				t.Fatalf("Contain() outcome = %v, want Failed", outcome)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Contain() waited for the report of another failure")
		}
	}

	reported := make(chan Outcome, 1)
	go func() {
		reported <- state.Run(StagePreflight, returnInternalError)
	}()
	<-reporter.entered

	for range failureThreshold - 1 {
		fail()
	}
	if !state.Disabled() {
		t.Fatal("guard enabled after failures during a report")
	}
	if failures, disabled := reporter.snapshot(); len(failures) != 1 || len(disabled) != 0 {
		t.Fatalf("reports during Failure() = %d failures and %v, want 1 and none", len(failures), disabled)
	}

	reporter.release <- struct{}{}
	<-reporter.entered
	fail()
	if failures, disabled := reporter.snapshot(); len(failures) != 1 || len(disabled) != 1 || disabled[0] != failureThreshold {
		t.Fatalf("reports during Disabled() = %d failures and %v, want 1 and [%d]", len(failures), disabled, failureThreshold)
	}

	reporter.release <- struct{}{}
	if outcome := <-reported; outcome != Failed {
		t.Fatalf("reported Run() outcome = %v, want Failed", outcome)
	}

	fail()
	failures, _ := reporter.snapshot()
	if len(failures) != 2 {
		t.Fatalf("reported failures = %d, want 2", len(failures))
	}
	if got := failures[1]; got.Failures != failureThreshold+2 || got.Skipped != failureThreshold {
		t.Errorf("failure after the report = %+v, want number %d with %d skipped", got, failureThreshold+2, failureThreshold)
	}
}

func TestReporterGoexitEndsItsReport(t *testing.T) {
	t.Parallel()

	for _, method := range []string{"Failure", "Disabled"} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()

			clock := newFakeClock(time.Unix(4_000, 0))
			reporter := &exitingReporter{method: method}
			state := newGuard(clock.Now, reporter)

			// Each failure runs on its own goroutine, which the reporter
			// can end.
			fail := func() {
				done := make(chan struct{})
				go func() {
					defer close(done)
					state.Contain(StageBody, returnInternalError)
				}()
				<-done
			}
			for range failureThreshold {
				fail()
			}
			clock.Advance(failureWindow + time.Nanosecond)
			fail()

			failures, disabled := reporter.snapshot()
			if len(failures) != failureThreshold+1 {
				t.Errorf("reported failures = %d, want %d", len(failures), failureThreshold+1)
			}
			if len(disabled) != 1 || disabled[0] != failureThreshold {
				t.Errorf("disable reports = %v, want [%d]", disabled, failureThreshold)
			}
		})
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

type recordingReporter struct {
	failures []Failure
	disabled []uint64
	mu       sync.Mutex
}

func (r *recordingReporter) Failure(failure Failure) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.failures = append(r.failures, failure)
}

func (r *recordingReporter) Disabled(failures uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.disabled = append(r.disabled, failures)
}

func (r *recordingReporter) snapshot() ([]Failure, []uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]Failure(nil), r.failures...), append([]uint64(nil), r.disabled...)
}

type panickingReporter struct {
	calls atomic.Int32
}

func (r *panickingReporter) Failure(Failure) {
	r.calls.Add(1)
	panic("reporter failure")
}

func (r *panickingReporter) Disabled(uint64) {
	r.calls.Add(1)
	panic("reporter failure")
}

// reentrantReporter causes enough failures to disable its Guard while it
// reports the first, and records whether it is called while it reports.
type reentrantReporter struct {
	state *Guard
	recordingReporter
	nested    atomic.Bool
	active    atomic.Bool
	reentered atomic.Bool
}

func (r *reentrantReporter) Failure(failure Failure) {
	if !r.enter() {
		return
	}
	defer r.active.Store(false)

	r.recordingReporter.Failure(failure)
	if r.nested.CompareAndSwap(false, true) {
		for range failureThreshold - 1 {
			r.state.Contain(StageReport, returnInternalError)
		}
	}
}

func (r *reentrantReporter) Disabled(failures uint64) {
	if !r.enter() {
		return
	}
	defer r.active.Store(false)

	r.recordingReporter.Disabled(failures)
}

func (r *reentrantReporter) enter() bool {
	if r.active.CompareAndSwap(false, true) {
		return true
	}
	r.reentered.Store(true)

	return false
}

// blockingReporter blocks in its first Failure call and in Disabled until the
// test releases each.
type blockingReporter struct {
	entered chan struct{}
	release chan struct{}
	recordingReporter
	blocked atomic.Bool
}

func (r *blockingReporter) Failure(failure Failure) {
	r.recordingReporter.Failure(failure)
	if r.blocked.CompareAndSwap(false, true) {
		r.block()
	}
}

func (r *blockingReporter) Disabled(failures uint64) {
	r.recordingReporter.Disabled(failures)
	r.block()
}

func (r *blockingReporter) block() {
	r.entered <- struct{}{}
	<-r.release
}

// exitingReporter ends the goroutine of the first call to its method, as a
// test's handler can by calling FailNow.
type exitingReporter struct {
	method string
	recordingReporter
	exited atomic.Bool
}

func (r *exitingReporter) Failure(failure Failure) {
	r.recordingReporter.Failure(failure)
	r.exit("Failure")
}

func (r *exitingReporter) Disabled(failures uint64) {
	r.recordingReporter.Disabled(failures)
	r.exit("Disabled")
}

func (r *exitingReporter) exit(method string) {
	if method == r.method && r.exited.CompareAndSwap(false, true) {
		runtime.Goexit()
	}
}
