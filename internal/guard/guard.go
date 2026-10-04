// Package guard contains the failure boundary for Curo-owned work.
package guard

import (
	"sync"
	"sync/atomic"
	"time"
)

// failureThreshold failures within failureWindow disable a Guard. The same
// numbers limit failure reports, so the limit skips only failures after
// self-disable.
const (
	failureThreshold = 3
	failureWindow    = time.Minute
)

// Outcome describes how a guarded operation finished.
type Outcome uint8

const (
	// Skipped means the guard was already disabled and did not run the operation.
	Skipped Outcome = iota

	// Completed means the operation returned without an internal failure.
	Completed

	// Failed means the operation returned an error or raised a recovered panic.
	Failed
)

// Stage names the Curo-owned work that a guarded operation performs.
type Stage uint8

const (
	// StagePreflight is the work before a request's first attempt.
	StagePreflight Stage = iota + 1

	// StagePostflight is the work after an attempt's result is captured.
	StagePostflight

	// StageRetry prepares and commits a retry.
	StageRetry

	// StageTimeout starts, expires, and settles adaptive timeouts.
	StageTimeout

	// StageBody closes a body that Curo did not send or discarded.
	StageBody

	// StageReport builds a report.
	StageReport
)

var stageNames = [...]string{
	StagePreflight:  "preflight",
	StagePostflight: "postflight",
	StageRetry:      "retry",
	StageTimeout:    "timeout",
	StageBody:       "body",
	StageReport:     "report",
}

// String returns the stage's name, or "unknown" for an undefined stage.
func (s Stage) String() string {
	if int(s) < len(stageNames) && stageNames[s] != "" {
		return stageNames[s]
	}

	return "unknown"
}

// Failure describes one contained failure.
type Failure struct {
	// Recovered is the recovered panic value, or nil when the operation
	// returned Err.
	Recovered any

	// Err is the error that the operation returned, or nil after a panic.
	Err error

	// Failures is the number of contained failures, including this one.
	Failures uint64

	// Skipped is the number of failures since the previous report that were
	// not reported.
	Skipped uint64

	// Stage names the work that failed.
	Stage Stage
}

// Reporter receives contained failures and the self-disable event.
//
// A Guard calls its methods synchronously, outside its lock, and one at a
// time. A failure that happens while one is running, including a failure that
// the Reporter causes, is not reported, so a Reporter is never called
// reentrantly. A panic from either method is recovered and ignored, and a
// method that calls runtime.Goexit ends only its goroutine: later failures
// are still reported.
type Reporter interface {
	// Failure reports one contained failure, on the goroutine where it
	// happened.
	Failure(failure Failure)

	// Disabled reports that failures disabled the Guard. It is called once,
	// after a Failure call returns and on the same goroutine: the call for the
	// failure that disabled the Guard, or the call that was running when it
	// did.
	Disabled(failures uint64)
}

// Guard contains failures from Curo-owned work and permanently disables itself
// after repeated failures. A Guard must not be copied after first use.
type Guard struct {
	reporter  Reporter
	now       func() time.Time
	trippedAt time.Time
	failures  recentTimes
	reports   recentTimes

	// mu numbers failures and guards failures, reports, trippedAt, skipped,
	// pendingDisabled, and reporting.
	mu      sync.Mutex
	skipped uint64
	total   atomic.Uint64

	// pendingDisabled is the number of the failure that disabled the Guard
	// until the self-disable event is reported, and zero otherwise.
	pendingDisabled uint64

	disabled atomic.Bool

	// reporting is set while a goroutine reports.
	reporting bool
}

// New constructs an enabled Guard. A non-nil reporter receives contained
// failures, one at a time and at most three per minute, and the self-disable
// event.
func New(reporter Reporter) *Guard {
	return newGuard(time.Now, reporter)
}

func newGuard(now func() time.Time, reporter Reporter) *Guard {
	if now == nil {
		now = time.Now
	}

	return &Guard{now: now, reporter: reporter}
}

// Disabled reports whether the guard has permanently selected pass-through.
//
// A nil Guard is disabled so an incomplete owner fails safe.
func (g *Guard) Disabled() bool {
	return g == nil || g.disabled.Load()
}

// Failures returns the number of contained operation failures.
func (g *Guard) Failures() uint64 {
	if g == nil {
		return 0
	}

	return g.total.Load()
}

// Run executes operation, the work of stage, inside a narrow recovery
// boundary.
//
// Run never holds the failure-window lock while operation executes. Returned
// errors and recovered panics both contribute to the same bounded failure
// signal. A disabled or nil Guard skips operation.
func (g *Guard) Run(stage Stage, operation func() error) Outcome {
	if g == nil || g.disabled.Load() {
		return Skipped
	}

	return g.Contain(stage, operation)
}

// Contain executes operation, the work of stage, inside the same recovery
// boundary as Run, even after the Guard has disabled adaptive stages.
//
// Contain is for Curo-owned work that must still run after self-disable, such
// as building a report or closing a body Curo discarded. Returned errors and
// recovered panics are still counted. A nil Guard skips operation.
func (g *Guard) Contain(stage Stage, operation func() error) (outcome Outcome) {
	if g == nil {
		return Skipped
	}

	outcome = Completed
	defer func() {
		if recovered := recover(); recovered != nil {
			g.recordFailure(stage, recovered, nil)
			outcome = Failed
		}
	}()

	if err := operation(); err != nil {
		g.recordFailure(stage, nil, err)
		return Failed
	}

	return outcome
}

// recordFailure counts a failure of stage, which panicked with recovered or
// returned err. If it is this failure's turn to be reported, it reports it
// outside the lock, followed by a pending self-disable event. After a panic,
// it runs in the deferred call that recovered it, so the panicking frames are
// still on the stack while the reporter runs.
func (g *Guard) recordFailure(stage Stage, recovered any, err error) {
	failures, skipped, report := g.track(g.clock())
	if !report {
		return
	}
	defer g.finishReporting()

	g.reportFailure(Failure{
		Recovered: recovered,
		Err:       err,
		Failures:  failures,
		Skipped:   skipped,
		Stage:     stage,
	})
}

// track numbers a failure at now and records it. It reports whether the
// caller reports the failure, and if so, how many failures were skipped since
// the previous report. The caller must then call finishReporting.
//
// A failure is skipped while another is being reported, so a Reporter is
// never called reentrantly, and when the report limit is reached. A failure
// that disables the Guard leaves the self-disable event pending for the
// goroutine that reports.
func (g *Guard) track(now time.Time) (failures, skipped uint64, report bool) {
	cutoff := now.Add(-failureWindow)

	g.mu.Lock()
	defer g.mu.Unlock()

	// Numbering failures under the lock gives them the order in which they
	// fill the windows, so the failure that disables the Guard is the
	// failureThreshold-th of its window.
	failures = g.total.Add(1)

	// An enabled Guard holds fewer than failureThreshold recent failures,
	// because the last one disables it, so there is room to add this one.
	if !g.disabled.Load() {
		g.failures.prune(cutoff)
		g.failures.add(now)
		if g.failures.full() {
			g.trippedAt = now
			g.disabled.Store(true)
			g.pendingDisabled = failures
		}
	}

	if g.reporter == nil {
		return failures, 0, false
	}

	// Until the Guard disables itself, the report window holds no more times
	// than the failure window, so the limit skips no failure before then.
	g.reports.prune(cutoff)
	if g.reporting || g.reports.full() {
		g.skipped++
		return failures, 0, false
	}
	g.reporting = true
	g.reports.add(now)
	skipped, g.skipped = g.skipped, 0

	return failures, skipped, true
}

// finishReporting reports a pending self-disable event and ends the caller's
// turn to report. Without an event, it checks for one and ends the turn under
// one lock, so an event that a concurrent failure leaves pending is never
// lost. A Guard disables itself once, so no event is left after it reports
// one, and a deferred call ends the turn even if the Reporter calls
// runtime.Goexit.
func (g *Guard) finishReporting() {
	g.mu.Lock()
	failures := g.pendingDisabled
	g.pendingDisabled = 0
	if failures == 0 {
		g.reporting = false
	}
	g.mu.Unlock()

	if failures == 0 {
		return
	}
	defer g.endReporting()
	g.reportDisabled(failures)
}

// endReporting ends the caller's turn to report.
func (g *Guard) endReporting() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.reporting = false
}

func (g *Guard) clock() time.Time {
	if g.now != nil {
		return g.now()
	}

	return time.Now()
}

// reportFailure delivers failure to the reporter. A panic from the reporter is
// recovered and ignored. It is not a Curo failure, and counting it could
// report failures without end.
func (g *Guard) reportFailure(failure Failure) {
	defer func() {
		_ = recover()
	}()

	g.reporter.Failure(failure)
}

// reportDisabled delivers the self-disable event to the reporter, with the
// same containment as reportFailure.
func (g *Guard) reportDisabled(failures uint64) {
	defer func() {
		_ = recover()
	}()

	g.reporter.Disabled(failures)
}

// recentTimes holds the times of up to failureThreshold events within a
// rolling window.
type recentTimes struct {
	times [failureThreshold]time.Time
	count int
}

// prune drops times before cutoff and keeps the rest in order.
func (r *recentTimes) prune(cutoff time.Time) {
	retained := 0
	for index := 0; index < r.count; index++ {
		if r.times[index].Before(cutoff) {
			continue
		}

		r.times[retained] = r.times[index]
		retained++
	}

	for index := retained; index < r.count; index++ {
		r.times[index] = time.Time{}
	}
	r.count = retained
}

// full reports whether every slot holds a time.
func (r *recentTimes) full() bool {
	return r.count == len(r.times)
}

// add records now. The caller must check that r is not full.
func (r *recentTimes) add(now time.Time) {
	r.times[r.count] = now
	r.count++
}
