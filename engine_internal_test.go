package curo

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/raj1kshtz/curo/internal/breaker"
	"github.com/raj1kshtz/curo/internal/guard"
	"github.com/raj1kshtz/curo/internal/observe"
	"github.com/raj1kshtz/curo/internal/timeout"
)

// Errors that classifyEngineTestError classifies. Every classification but
// errEngineInternal's is marked retryable, so the outcome alone decides
// whether Enforce may retry the error.
var (
	errEngineUnavailable = errors.New("engine test: unavailable")
	errEngineReset       = errors.New("engine test: connection reset")
	errEngineInternal    = errors.New("engine test: internal error")
	errEngineInvalid     = errors.New("engine test: invalid argument")
	errEngineThrottled   = errors.New("engine test: quota exceeded")
	errEngineNotFound    = errors.New("engine test: not found")
	errEngineUnknown     = errors.New("engine test: unknown outcome")
	errEngineUnspecified = errors.New("engine test: unspecified outcome")
)

var engineTestClassifications = map[error]Classification{
	errEngineUnavailable: {Outcome: OutcomeDependencyFailure, Retryable: true},
	errEngineReset:       {Outcome: OutcomeTransportFailure, Retryable: true},
	errEngineInternal:    {Outcome: OutcomeDependencyFailure},
	errEngineInvalid:     {Outcome: OutcomeClientFailure, Retryable: true},
	errEngineThrottled:   {Outcome: OutcomeRateLimited, Retryable: true},
	errEngineNotFound:    {Outcome: OutcomeSuccess, Retryable: true},
	errEngineUnknown:     {Outcome: OutcomeTransportFailure + 1, Retryable: true},
	errEngineUnspecified: {Outcome: OutcomeUnspecified, Retryable: true},
}

func classifyEngineTestError(err error) Classification {
	return engineTestClassifications[err]
}

// engineTestCall returns a read-only, replay-safe call to the harness target.
func engineTestCall() Call {
	return Call{
		Classify:   classifyEngineTestError,
		Scheme:     "grpc",
		Host:       "engine.example",
		Port:       443,
		ReadOnly:   true,
		ReplaySafe: true,
	}
}

type engineTestKey struct{}

// engineStep scripts one attempt of an Engine call.
type engineStep func(context.Context) error

// engineReturns returns a step whose attempt returns err.
func engineReturns(err error) engineStep {
	return func(context.Context) error {
		return err
	}
}

// engineHarness drives an Engine with a fake observer clock, a fixed retry
// delay, a recording wait, manual adaptive timers, and scripted attempts.
type engineHarness struct {
	t        *testing.T
	engine   *Engine
	clock    *reportClock
	timers   *manualTimers
	onWait   func(context.Context) bool
	steps    []engineStep
	contexts []context.Context
	waits    []time.Duration
	mu       sync.Mutex
}

func newEngineHarness(t *testing.T, options ...Option) *engineHarness {
	t.Helper()

	engine, err := NewEngine(options...)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	cfg, err := applyOptions(options)
	if err != nil {
		t.Fatalf("applyOptions() error = %v", err)
	}

	harness := &engineHarness{
		t:      t,
		engine: engine,
		clock:  newReportClock(time.Unix(90_000, 0)),
		timers: &manualTimers{},
	}
	observer := observe.NewWithClock(harness.clock.Now, cfg.timeouts)
	stages := newAdaptiveStages(
		observer,
		&engine.retries,
		&engine.breakers,
		&engine.timeouts,
		engine.authorized,
	)
	stages.jitter = func() time.Duration { return retryTestDelay }
	engine.observer = observer
	engine.stages = stages
	engine.reports = observer.Report
	engine.wait = harness.wait
	engine.after = harness.timers.after

	return harness
}

// newClockedEngine returns an Engine whose observer reads now. Unlike the
// engine harness, it keeps the Engine's real timers and backoff.
func newClockedEngine(tb testing.TB, now func() time.Time, options ...Option) *Engine {
	tb.Helper()

	engine, err := NewEngine(options...)
	if err != nil {
		tb.Fatalf("NewEngine() error = %v", err)
	}
	cfg, err := applyOptions(options)
	if err != nil {
		tb.Fatalf("applyOptions() error = %v", err)
	}
	observer := observe.NewWithClock(now, cfg.timeouts)
	engine.observer = observer
	engine.stages = newAdaptiveStages(
		observer,
		&engine.retries,
		&engine.breakers,
		&engine.timeouts,
		engine.authorized,
	)
	engine.reports = observer.Report

	return engine
}

// doInternal makes one call whose attempt returns err, and fails tb unless Do
// returns err.
func doInternal(tb testing.TB, engine *Engine, call Call, err error) {
	tb.Helper()

	got := engine.Do(context.Background(), call, func(context.Context) error {
		return err
	})
	if !errors.Is(got, err) {
		tb.Fatalf("Do() error = %v, want %v", got, err)
	}
}

func (harness *engineHarness) wait(
	ctx context.Context,
	_ <-chan struct{},
	delay time.Duration,
) bool {
	harness.mu.Lock()
	harness.waits = append(harness.waits, delay)
	onWait := harness.onWait
	harness.mu.Unlock()

	if onWait != nil {
		return onWait(ctx)
	}

	return ctx.Err() == nil
}

func (harness *engineHarness) setOnWait(onWait func(context.Context) bool) {
	harness.mu.Lock()
	defer harness.mu.Unlock()

	harness.onWait = onWait
}

// do runs call with ctx. Its attempts follow steps, and the recorded attempts
// and waits start empty.
func (harness *engineHarness) do(
	ctx context.Context,
	call Call,
	steps ...engineStep,
) error {
	harness.mu.Lock()
	harness.steps = steps
	harness.contexts = nil
	harness.waits = nil
	harness.mu.Unlock()

	return harness.engine.Do(ctx, call, harness.attempt)
}

func (harness *engineHarness) attempt(ctx context.Context) error {
	harness.mu.Lock()
	harness.contexts = append(harness.contexts, ctx)
	var step engineStep
	if len(harness.steps) > 0 {
		step = harness.steps[0]
		harness.steps = harness.steps[1:]
	}
	harness.mu.Unlock()

	if step == nil {
		harness.t.Error("unexpected attempt")
		return nil
	}

	return step(ctx)
}

// recorded returns the contexts of the last call's attempts and its backoff
// delays.
func (harness *engineHarness) recorded() ([]context.Context, []time.Duration) {
	harness.mu.Lock()
	defer harness.mu.Unlock()

	return append([]context.Context(nil), harness.contexts...),
		append([]time.Duration(nil), harness.waits...)
}

// expect runs call with a background context, and requires Do to return want
// after one attempt for each step.
func (harness *engineHarness) expect(call Call, want error, steps ...engineStep) {
	harness.t.Helper()

	if err := harness.do(context.Background(), call, steps...); !errors.Is(err, want) {
		harness.t.Fatalf("Do() error = %v, want %v", err, want)
	}
	if contexts, _ := harness.recorded(); len(contexts) != len(steps) {
		harness.t.Fatalf("attempts = %d, want %d", len(contexts), len(steps))
	}
}

// record completes count calls of call, whose attempts return err, two
// seconds apart.
func (harness *engineHarness) record(count int, call Call, err error) {
	harness.t.Helper()

	for range count {
		if harness.engine.Stats().ObservedRequests > 0 {
			harness.clock.Advance(2 * time.Second)
		}
		harness.expect(call, err, engineReturns(err))
	}
}

func (harness *engineHarness) enforce() {
	harness.t.Helper()

	if err := harness.engine.SetMode(Enforce); err != nil {
		harness.t.Fatalf("SetMode(Enforce) error = %v", err)
	}
	harness.clock.Advance(2 * time.Second)
}

// decision returns the published decision of the harness target with method.
func (harness *engineHarness) decision(method MethodClass) Decision {
	harness.t.Helper()

	for _, decision := range harness.engine.Report().Targets {
		if decision.Target.Method == method {
			return decision
		}
	}
	harness.t.Fatalf("no decision for method class %v", method)

	return Decision{}
}

// warm records 20 successes and one retryable dependency failure two seconds
// apart in Observe mode, so the read target publishes a Ready Transient
// decision with a Retry candidate and both retry budgets hold 2.1 tokens. It
// then selects Enforce.
func (harness *engineHarness) warm() {
	harness.t.Helper()

	harness.record(20, engineTestCall(), nil)
	harness.record(1, engineTestCall(), errEngineUnavailable)
	assertDecision(
		harness.t,
		"warm target",
		harness.decision(MethodRead),
		ReadinessReady,
		DiagnosisTransient,
		CandidateRetry,
	)
	harness.enforce()
}

// openBreaker records 21 dependency failures in Observe, selects Enforce, and
// completes one more failure, which opens the read target's dependency
// breaker.
func (harness *engineHarness) openBreaker() {
	harness.t.Helper()

	harness.record(21, engineTestCall(), errEngineInternal)
	assertDecision(
		harness.t,
		"down target",
		harness.decision(MethodRead),
		ReadinessReady,
		DiagnosisDependencyDown,
		CandidateBreakerOpen,
	)
	harness.enforce()
	harness.expect(engineTestCall(), errEngineInternal, engineReturns(errEngineInternal))
	harness.assertStats("open", Stats{BreakerOpens: 1})
}

// warmTimeout completes timeout.MinimumSamples calls of call, whose attempts
// return err, half a second apart, then one more after the target's decision
// expired. A read target then publishes the adaptive timeout its latency
// selects.
func (harness *engineHarness) warmTimeout(call Call, err error) {
	harness.t.Helper()

	for index := range timeout.MinimumSamples + 1 {
		gap := 500 * time.Millisecond
		if index == timeout.MinimumSamples {
			gap = 10 * time.Second
		}
		harness.clock.Advance(gap)
		harness.expect(call, err, engineReturns(err))
	}
}

// observeTimeout publishes, in Observe mode, a Ready Healthy read decision
// whose only candidate is a two second adaptive timeout.
func (harness *engineHarness) observeTimeout() {
	harness.t.Helper()

	harness.warmTimeout(engineTestCall(), nil)
	decision := harness.decision(MethodRead)
	assertDecision(
		harness.t,
		"healthy target",
		decision,
		ReadinessReady,
		DiagnosisHealthy,
		CandidateTimeout,
	)
	if decision.Timeout != timeout.DefaultBounds.Minimum {
		harness.t.Fatalf("Timeout = %v, want %v", decision.Timeout, timeout.DefaultBounds.Minimum)
	}
}

// warmHealthyTimeout publishes a Ready Healthy read decision whose only
// candidate is a two second adaptive timeout, then selects Enforce.
func (harness *engineHarness) warmHealthyTimeout() {
	harness.t.Helper()

	harness.observeTimeout()
	harness.enforce()
}

// warmRetryTimeout publishes a Ready Transient read decision with Retry and a
// two second adaptive timeout, then selects Enforce.
func (harness *engineHarness) warmRetryTimeout() {
	harness.t.Helper()

	harness.warmTimeout(engineTestCall(), nil)
	harness.clock.Advance(10 * time.Second)
	harness.record(1, engineTestCall(), errEngineUnavailable)
	decision := harness.decision(MethodRead)
	assertDecision(
		harness.t,
		"transient target",
		decision,
		ReadinessReady,
		DiagnosisTransient,
		CandidateRetry|CandidateTimeout,
	)
	if decision.Timeout != timeout.DefaultBounds.Minimum {
		harness.t.Fatalf("Timeout = %v, want %v", decision.Timeout, timeout.DefaultBounds.Minimum)
	}
	harness.enforce()
}

// assertRejected requires a call to fail fast with ErrBreakerOpen without an
// attempt.
func (harness *engineHarness) assertRejected() {
	harness.t.Helper()

	harness.expect(engineTestCall(), ErrBreakerOpen)
}

// assertStats requires the Engine's action and failure counters to equal
// want's. It ignores the observation counters.
func (harness *engineHarness) assertStats(name string, want Stats) {
	harness.t.Helper()

	got := harness.engine.Stats()
	got.ObservedRequests, got.TrackedTargets, got.OverflowRequests = 0, 0, 0
	if got != want {
		harness.t.Errorf("%s Stats() = %+v, want %+v", name, got, want)
	}
}

// assertTwoRetriesFunded proves that a canceled reservation returned its
// token. After one canceled reservation the budgets hold 2.2 tokens, enough
// for two more retries. A lost token would leave enough for only one.
func (harness *engineHarness) assertTwoRetriesFunded() {
	harness.t.Helper()

	harness.setOnWait(nil)
	before := harness.engine.Stats()
	for range 2 {
		harness.expect(
			engineTestCall(),
			nil,
			engineReturns(errEngineUnavailable),
			engineReturns(nil),
		)
	}

	after := harness.engine.Stats()
	if after.RetryAttempts-before.RetryAttempts != 2 ||
		after.RetryBudgetDenials != before.RetryBudgetDenials {
		harness.t.Fatalf(
			"funded retries = %d attempts, %d denials, want 2 attempts and no denials",
			after.RetryAttempts-before.RetryAttempts,
			after.RetryBudgetDenials-before.RetryBudgetDenials,
		)
	}
}

// selfDisable fails the guard of engine until it disables itself.
func selfDisable(t *testing.T, engine *Engine) {
	t.Helper()

	for range 3 {
		engine.guard.Run(guard.StagePreflight, func() error {
			return errEngineInternal
		})
	}
	if !engine.guard.Disabled() {
		t.Fatal("guard enabled after repeated internal failures")
	}
}

func TestEngineDoRequiresAnAttempt(t *testing.T) {
	t.Parallel()

	harness := newEngineHarness(t)
	engines := map[string]*Engine{"nil": nil, "zero": {}, "initialized": harness.engine}
	for name, engine := range engines {
		err := engine.Do(context.Background(), engineTestCall(), nil)
		if !errors.Is(err, errNilAttempt) {
			t.Errorf("%s Engine Do(nil attempt) error = %v, want errNilAttempt", name, err)
		}
	}
	if got := harness.engine.Stats().ObservedRequests; got != 0 {
		t.Errorf("ObservedRequests = %d, want 0", got)
	}
}

func TestEngineRunsAttemptsDirectlyWithoutAuthority(t *testing.T) {
	t.Parallel()

	background := context.Background()
	tests := map[string]func(*testing.T, *Engine) (*Engine, context.Context){
		"nil engine": func(*testing.T, *Engine) (*Engine, context.Context) {
			return nil, background
		},
		"zero engine": func(*testing.T, *Engine) (*Engine, context.Context) {
			return &Engine{}, background
		},
		"nil context": func(_ *testing.T, engine *Engine) (*Engine, context.Context) {
			return engine, nil
		},
		"off": func(t *testing.T, engine *Engine) (*Engine, context.Context) {
			if err := engine.SetMode(Off); err != nil {
				t.Fatalf("SetMode(Off) error = %v", err)
			}
			return engine, background
		},
		"closed": func(t *testing.T, engine *Engine) (*Engine, context.Context) {
			if err := engine.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			return engine, background
		},
		"self-disabled": func(t *testing.T, engine *Engine) (*Engine, context.Context) {
			selfDisable(t, engine)
			return engine, background
		},
	}

	for name, prepare := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// The breaker of the call's target is open, so a call over which
			// the Engine had authority would be rejected.
			harness := newEngineHarness(t)
			harness.openBreaker()
			before := harness.engine.Stats()
			engine, ctx := prepare(t, harness.engine)

			call := engineTestCall()
			call.Classify = func(error) Classification {
				t.Error("Classify ran without authority")
				return Classification{}
			}
			var attempts []context.Context
			err := engine.Do(ctx, call, func(attempt context.Context) error {
				attempts = append(attempts, attempt)
				return errEngineUnavailable
			})
			if !errors.Is(err, errEngineUnavailable) {
				t.Errorf("Do() error = %v, want the attempt's error", err)
			}
			if len(attempts) != 1 || attempts[0] != ctx {
				t.Errorf("attempts = %d, want one with the caller's context", len(attempts))
			}
			after := harness.engine.Stats()
			if after.ObservedRequests != before.ObservedRequests ||
				after.BreakerProbes != before.BreakerProbes ||
				after.BreakerRejections != before.BreakerRejections {
				t.Errorf("Stats() = %+v, want no observation or breaker action since %+v", after, before)
			}
		})
	}
}

func TestEngineObservesCallTargets(t *testing.T) {
	t.Parallel()

	harness := newEngineHarness(t)
	write := engineTestCall()
	write.ReadOnly = false
	alias := engineTestCall()
	alias.Scheme, alias.Host = "GRPC", "Engine.Example."
	overflow := engineTestCall()
	overflow.Port = 0
	for _, call := range []Call{engineTestCall(), write, alias, overflow} {
		harness.expect(call, nil, engineReturns(nil))
	}

	stats := harness.engine.Stats()
	if stats.ObservedRequests != 4 || stats.TrackedTargets != 2 || stats.OverflowRequests != 1 {
		t.Errorf("Stats() = %+v, want 4 observed, 2 tracked, and 1 overflow", stats)
	}
	var targets []Target
	for _, decision := range harness.engine.Report().Targets {
		targets = append(targets, decision.Target)
	}
	want := []Target{
		{Scheme: "grpc", Host: "engine.example", Port: 443, Method: MethodRead},
		{Scheme: "grpc", Host: "engine.example", Port: 443, Method: MethodWrite},
	}
	if !slices.Equal(targets, want) {
		t.Errorf("Report() targets = %+v, want %+v", targets, want)
	}
}

func TestEngineClassificationsBecomeEvidence(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		err        error
		recent     Evidence
		diagnosis  Diagnosis
		candidates Candidates
		noClassify bool
	}{
		"successes": {
			diagnosis: DiagnosisHealthy,
		},
		"errors classified as successes": {
			err:       errEngineNotFound,
			diagnosis: DiagnosisHealthy,
		},
		"client failures": {
			err:       errEngineInvalid,
			recent:    Evidence{ClientFailures: 21},
			diagnosis: DiagnosisClientError,
		},
		"rate limiting": {
			err:       errEngineThrottled,
			recent:    Evidence{RateLimited: 21},
			diagnosis: DiagnosisSaturation,
		},
		"dependency failures": {
			err:        errEngineInternal,
			recent:     Evidence{DependencyFailures: 21},
			diagnosis:  DiagnosisDependencyDown,
			candidates: CandidateBreakerOpen,
		},
		"transport failures": {
			err:        errEngineReset,
			recent:     Evidence{DependencyFailures: 21},
			diagnosis:  DiagnosisDependencyDown,
			candidates: CandidateBreakerOpen,
		},
		"unknown outcomes": {
			err:        errEngineUnknown,
			recent:     Evidence{DependencyFailures: 21},
			diagnosis:  DiagnosisDependencyDown,
			candidates: CandidateBreakerOpen,
		},
		"unspecified outcomes": {
			err:        errEngineUnspecified,
			recent:     Evidence{DependencyFailures: 21},
			diagnosis:  DiagnosisDependencyDown,
			candidates: CandidateBreakerOpen,
		},
		"errors without Classify": {
			err:        errEngineInvalid,
			recent:     Evidence{DependencyFailures: 21},
			diagnosis:  DiagnosisDependencyDown,
			candidates: CandidateBreakerOpen,
			noClassify: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newEngineHarness(t)
			call := engineTestCall()
			if test.noClassify {
				call.Classify = nil
			}
			harness.record(21, call, test.err)

			decision := harness.decision(MethodRead)
			assertDecision(t, name, decision, ReadinessReady, test.diagnosis, test.candidates)
			want := test.recent
			want.Attempts, want.Span = 21, 40*time.Second
			if decision.Recent != want {
				t.Errorf("Recent = %+v, want %+v", decision.Recent, want)
			}
		})
	}
}

func TestEngineCallerOwnsCanceledCalls(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		attempt   func(context.CancelFunc) engineStep
		classify  func(context.CancelFunc) func(error) Classification
		diagnosis Diagnosis
	}{
		"canceled before the attempt returned": {
			attempt: func(cancel context.CancelFunc) engineStep {
				return func(context.Context) error {
					cancel()
					return errEngineUnavailable
				}
			},
			diagnosis: DiagnosisHealthy,
		},
		"canceled while Classify ran": {
			classify: func(cancel context.CancelFunc) func(error) Classification {
				return func(err error) Classification {
					cancel()
					return classifyEngineTestError(err)
				}
			},
			diagnosis: DiagnosisTransient,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newEngineHarness(t)
			harness.record(20, engineTestCall(), nil)
			harness.clock.Advance(2 * time.Second)

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			call := engineTestCall()
			step := engineReturns(errEngineUnavailable)
			if test.attempt != nil {
				step = test.attempt(cancel)
			}
			if test.classify != nil {
				call.Classify = test.classify(cancel)
			}
			if err := harness.do(ctx, call, step); !errors.Is(err, errEngineUnavailable) {
				t.Fatalf("Do() error = %v, want the attempt's error", err)
			}

			// A call the caller owns is not evidence, so only the next call
			// evaluates the target again.
			harness.record(1, engineTestCall(), nil)
			if got := harness.decision(MethodRead).Diagnosis; got != test.diagnosis {
				t.Errorf("Diagnosis = %v, want %v", got, test.diagnosis)
			}
		})
	}
}

func TestEngineRetriesReplaySafeCalls(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		prepare    func(*Call)
		want       error
		steps      []engineStep
		classified int
		success    bool
	}{
		"retry returns no error": {
			steps:      []engineStep{engineReturns(errEngineUnavailable), engineReturns(nil)},
			classified: 1,
			success:    true,
		},
		"retry error classified as a success": {
			want:       errEngineNotFound,
			steps:      []engineStep{engineReturns(errEngineUnavailable), engineReturns(errEngineNotFound)},
			classified: 2,
			success:    true,
		},
		"retry fails": {
			want:       errEngineInternal,
			steps:      []engineStep{engineReturns(errEngineUnavailable), engineReturns(errEngineInternal)},
			classified: 2,
		},
		"retryable transport failure": {
			steps:      []engineStep{engineReturns(errEngineReset), engineReturns(nil)},
			classified: 1,
			success:    true,
		},
		"call not replay-safe": {
			prepare:    func(call *Call) { call.ReplaySafe = false },
			want:       errEngineUnavailable,
			steps:      []engineStep{engineReturns(errEngineUnavailable)},
			classified: 1,
		},
		"failure not marked retryable": {
			want:       errEngineInternal,
			steps:      []engineStep{engineReturns(errEngineInternal)},
			classified: 1,
		},
		"retryable client failure": {
			want:       errEngineInvalid,
			steps:      []engineStep{engineReturns(errEngineInvalid)},
			classified: 1,
		},
		"retryable rate limiting": {
			want:       errEngineThrottled,
			steps:      []engineStep{engineReturns(errEngineThrottled)},
			classified: 1,
		},
		"retryable success": {
			want:       errEngineNotFound,
			steps:      []engineStep{engineReturns(errEngineNotFound)},
			classified: 1,
		},
		"retryable unknown outcome": {
			want:       errEngineUnknown,
			steps:      []engineStep{engineReturns(errEngineUnknown)},
			classified: 1,
		},
		"retryable unspecified outcome": {
			want:       errEngineUnspecified,
			steps:      []engineStep{engineReturns(errEngineUnspecified)},
			classified: 1,
		},
		"no Classify": {
			prepare: func(call *Call) { call.Classify = nil },
			want:    errEngineUnavailable,
			steps:   []engineStep{engineReturns(errEngineUnavailable)},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newEngineHarness(t)
			harness.warm()

			classified := 0
			call := engineTestCall()
			call.Classify = func(err error) Classification {
				classified++
				return classifyEngineTestError(err)
			}
			if test.prepare != nil {
				test.prepare(&call)
			}
			ctx := context.WithValue(context.Background(), engineTestKey{}, name)
			if err := harness.do(ctx, call, test.steps...); !errors.Is(err, test.want) {
				t.Fatalf("Do() error = %v, want %v", err, test.want)
			}

			contexts, waits := harness.recorded()
			if len(contexts) != len(test.steps) {
				t.Fatalf("attempts = %d, want %d", len(contexts), len(test.steps))
			}
			for index, attempt := range contexts {
				if attempt != ctx {
					t.Errorf("attempt %d did not use the caller's context", index)
				}
			}
			var (
				want      Stats
				wantWaits []time.Duration
			)
			if len(test.steps) == 2 {
				want.RetryAttempts = 1
				wantWaits = []time.Duration{retryTestDelay}
			}
			if test.success {
				want.RetrySuccesses = 1
			}
			if !slices.Equal(waits, wantWaits) {
				t.Errorf("waits = %v, want %v", waits, wantWaits)
			}
			if classified != test.classified {
				t.Errorf("Classify calls = %d, want %d", classified, test.classified)
			}
			harness.assertStats(name, want)
		})
	}
}

func TestEngineRetryWithdrawnBeforeItStarts(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		onWait   func(*testing.T, *Engine, context.CancelFunc) bool
		failures uint64
		closed   bool
	}{
		"backoff stopped": {
			onWait: func(*testing.T, *Engine, context.CancelFunc) bool {
				return false
			},
		},
		"backoff panics": {
			onWait: func(*testing.T, *Engine, context.CancelFunc) bool {
				panic("wait panic")
			},
			failures: 1,
		},
		"mode changed": {
			onWait: func(t *testing.T, engine *Engine, _ context.CancelFunc) bool {
				if err := engine.SetMode(Observe); err != nil {
					t.Errorf("SetMode(Observe) error = %v", err)
				}
				return true
			},
		},
		"mode restored": {
			onWait: func(t *testing.T, engine *Engine, _ context.CancelFunc) bool {
				for _, mode := range []Mode{Observe, Enforce} {
					if err := engine.SetMode(mode); err != nil {
						t.Errorf("SetMode(%v) error = %v", mode, err)
					}
				}
				return true
			},
		},
		"closed": {
			onWait: func(t *testing.T, engine *Engine, _ context.CancelFunc) bool {
				if err := engine.Close(); err != nil {
					t.Errorf("Close() error = %v", err)
				}
				return true
			},
			closed: true,
		},
		"caller canceled": {
			onWait: func(_ *testing.T, _ *Engine, cancel context.CancelFunc) bool {
				cancel()
				return true
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newEngineHarness(t)
			harness.warm()
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			harness.setOnWait(func(context.Context) bool {
				return test.onWait(t, harness.engine, cancel)
			})

			err := harness.do(ctx, engineTestCall(), engineReturns(errEngineUnavailable))
			if !errors.Is(err, errEngineUnavailable) {
				t.Fatalf("Do() error = %v, want the first attempt's error", err)
			}
			contexts, waits := harness.recorded()
			if len(contexts) != 1 || !slices.Equal(waits, []time.Duration{retryTestDelay}) {
				t.Errorf("attempts = %d, waits = %v, want one attempt after one wait", len(contexts), waits)
			}
			harness.assertStats(name, Stats{InternalFailures: test.failures})
			if test.closed {
				return
			}

			if modeErr := harness.engine.SetMode(Enforce); modeErr != nil {
				t.Fatalf("SetMode(Enforce) error = %v", modeErr)
			}
			harness.assertTwoRetriesFunded()
		})
	}
}

func TestEngineCancelsAGrantItCannotUse(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		stages   func(adaptiveStages) requestStages
		failures uint64
		observe  bool
	}{
		"postflight error": {
			stages: func(stages adaptiveStages) requestStages {
				return retryFaultStages{
					adaptiveStages: stages,
					postflightErr:  errors.New("postflight failed"),
				}
			},
			failures: 1,
		},
		"forged grant outside enforce": {
			stages: func(stages adaptiveStages) requestStages {
				return retryFaultStages{adaptiveStages: stages, forge: true}
			},
			observe: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newEngineHarness(t)
			harness.warm()
			adaptive, ok := harness.engine.stages.(adaptiveStages)
			if !ok {
				t.Fatalf("stages = %T, want adaptiveStages", harness.engine.stages)
			}
			harness.engine.stages = test.stages(adaptive)
			if test.observe {
				if err := harness.engine.SetMode(Observe); err != nil {
					t.Fatalf("SetMode(Observe) error = %v", err)
				}
			}

			harness.expect(engineTestCall(), errEngineUnavailable, engineReturns(errEngineUnavailable))
			if _, waits := harness.recorded(); len(waits) != 0 {
				t.Errorf("waits = %d, want 0", len(waits))
			}
			harness.assertStats(name, Stats{InternalFailures: test.failures})

			harness.engine.stages = adaptive
			if err := harness.engine.SetMode(Enforce); err != nil {
				t.Fatalf("SetMode(Enforce) error = %v", err)
			}
			harness.assertTwoRetriesFunded()
		})
	}
}

func TestEngineClassifyPanicIsContained(t *testing.T) {
	t.Parallel()

	errCorrupt := errors.New("engine test: corrupt")
	tests := map[string]struct {
		steps []engineStep
		want  Stats
	}{
		"initial attempt": {
			steps: []engineStep{engineReturns(errCorrupt)},
			want:  Stats{InternalFailures: 1},
		},
		"retry attempt": {
			steps: []engineStep{engineReturns(errEngineUnavailable), engineReturns(errCorrupt)},
			want:  Stats{RetryAttempts: 1, InternalFailures: 1},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newEngineHarness(t)
			harness.warm()
			call := engineTestCall()
			call.Classify = func(err error) Classification {
				if errors.Is(err, errCorrupt) {
					panic("classify panic")
				}
				return classifyEngineTestError(err)
			}

			harness.expect(call, errCorrupt, test.steps...)
			harness.assertStats(name, test.want)
		})
	}
}

func TestEngineBreakerFailsFastAndProbes(t *testing.T) {
	t.Parallel()

	harness := newEngineHarness(t)
	harness.openBreaker()
	observed := harness.engine.Stats().ObservedRequests

	harness.assertRejected()
	harness.clock.Advance(breaker.BaseCooldown - time.Nanosecond)
	harness.assertRejected()
	if got := harness.engine.Stats().ObservedRequests; got != observed {
		t.Errorf("ObservedRequests = %d, want %d without the rejected calls", got, observed)
	}
	harness.assertStats("rejections", Stats{BreakerOpens: 1, BreakerRejections: 2})

	// A write call is a target of its own, with its own breaker.
	write := engineTestCall()
	write.ReadOnly = false
	harness.expect(write, nil, engineReturns(nil))

	harness.clock.Advance(time.Nanosecond)
	harness.expect(engineTestCall(), nil, engineReturns(nil))
	harness.assertStats("probe", Stats{BreakerOpens: 1, BreakerProbes: 1, BreakerRejections: 2})
	harness.expect(engineTestCall(), errEngineInternal, engineReturns(errEngineInternal))
	harness.assertStats("closed", Stats{BreakerOpens: 1, BreakerProbes: 1, BreakerRejections: 2})
}

func TestEngineProbeEndsTheCooldown(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		classify func(error) Classification
		cooldown time.Duration
		failures uint64
	}{
		// A failed probe doubles the cooldown.
		"failed probe": {
			classify: classifyEngineTestError,
			cooldown: 2 * breaker.BaseCooldown,
		},
		// A probe whose result Curo failed to record is inconclusive, so the
		// breaker reopens for the base cooldown.
		"unrecorded probe": {
			classify: func(error) Classification {
				panic("classify panic")
			},
			cooldown: breaker.BaseCooldown,
			failures: 1,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newEngineHarness(t)
			harness.openBreaker()
			harness.clock.Advance(breaker.BaseCooldown)
			call := engineTestCall()
			call.Classify = test.classify
			harness.expect(call, errEngineInternal, engineReturns(errEngineInternal))

			harness.clock.Advance(test.cooldown - time.Nanosecond)
			harness.assertRejected()
			harness.clock.Advance(time.Nanosecond)
			harness.expect(engineTestCall(), nil, engineReturns(nil))
			harness.assertStats(name, Stats{
				BreakerOpens:      1,
				BreakerProbes:     2,
				BreakerRejections: 1,
				InternalFailures:  test.failures,
			})
		})
	}
}

func TestEngineProbeWhoseAttemptPanicsIsInconclusive(t *testing.T) {
	t.Parallel()

	harness := newEngineHarness(t)
	harness.openBreaker()
	harness.clock.Advance(breaker.BaseCooldown)
	func() {
		defer func() {
			//nolint:errorlint // The recovered value is the panic value itself.
			if recovered := recover(); recovered != errBasePanic {
				t.Errorf("recovered %v, want the attempt's panic", recovered)
			}
		}()
		err := harness.do(context.Background(), engineTestCall(), func(context.Context) error {
			panic(errBasePanic)
		})
		t.Errorf("Do() error = %v, want the attempt's panic", err)
	}()

	// The probe returned no result, so the breaker reopens for the base
	// cooldown instead of waiting for the probe's lease to expire.
	harness.clock.Advance(breaker.BaseCooldown - time.Nanosecond)
	harness.assertRejected()
	harness.clock.Advance(time.Nanosecond)
	harness.expect(engineTestCall(), nil, engineReturns(nil))
	harness.assertStats("panicking probe", Stats{
		BreakerOpens:      1,
		BreakerProbes:     2,
		BreakerRejections: 1,
	})
}

func TestEngineTimeoutEndsSlowReadOnlyCall(t *testing.T) {
	t.Parallel()

	tests := map[string]engineStep{
		"canceled attempt": func(ctx context.Context) error { return ctx.Err() },
		"late success":     engineReturns(nil),
		"late failure":     engineReturns(errEngineUnavailable),
	}

	for name, respond := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newEngineHarness(t)
			harness.warmRetryTimeout()

			call := engineTestCall()
			call.Classify = func(error) Classification {
				t.Error("Classify ran for an attempt that the timeout ended")
				return Classification{}
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			err := harness.do(ctx, call, func(attempt context.Context) error {
				timer := harness.timers.only(t)
				if timer.limit != timeout.DefaultBounds.Minimum {
					t.Errorf("timer limit = %v, want %v", timer.limit, timeout.DefaultBounds.Minimum)
				}
				timer.fire()
				if cause := context.Cause(attempt); !isTimeoutCause(cause) {
					t.Errorf("attempt context cause = %v, want a timeout", cause)
				}
				return respond(attempt)
			})
			if !errors.Is(err, ErrTimeout) {
				t.Fatalf("Do() error = %v, want ErrTimeout", err)
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				t.Errorf("caller context error = %v, want nil", ctxErr)
			}
			if contexts, waits := harness.recorded(); len(contexts) != 1 || len(waits) != 0 {
				t.Errorf("attempts = %d, waits = %d, want one attempt without a retry", len(contexts), len(waits))
			}
			harness.assertStats(name, Stats{Timeouts: 1})
		})
	}
}

func TestEngineTimeoutNeverClaimsCallerCancellation(t *testing.T) {
	t.Parallel()

	harness := newEngineHarness(t)
	harness.warmHealthyTimeout()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	err := harness.do(ctx, engineTestCall(), func(attempt context.Context) error {
		cancel()
		harness.timers.only(t).fire()
		if cause := context.Cause(attempt); isTimeoutCause(cause) {
			t.Errorf("attempt context cause = %v, want the caller's", cause)
		}
		return attempt.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Do() error = %v, want the caller's cancellation", err)
	}
	harness.assertStats("caller cancellation", Stats{})
}

func TestEngineTimeoutSkipsCallsItMustNotBound(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		prepare func(*testing.T, *engineHarness) (context.Context, Call)
		timed   bool
	}{
		"distant deadline": {
			prepare: func(t *testing.T, _ *engineHarness) (context.Context, Call) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
				t.Cleanup(cancel)
				return ctx, engineTestCall()
			},
			timed: true,
		},
		"near deadline": {
			prepare: func(t *testing.T, _ *engineHarness) (context.Context, Call) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				t.Cleanup(cancel)
				return ctx, engineTestCall()
			},
		},
		"canceled context": {
			prepare: func(*testing.T, *engineHarness) (context.Context, Call) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, engineTestCall()
			},
		},
		"write": {
			prepare: func(t *testing.T, harness *engineHarness) (context.Context, Call) {
				write := engineTestCall()
				write.ReadOnly = false
				harness.warmTimeout(write, nil)
				decision := harness.decision(MethodWrite)
				if decision.Timeout != 0 ||
					decision.Candidates.Has(CandidateTimeout) ||
					decision.Latency.Samples < timeout.MinimumSamples {
					t.Errorf("write decision = %+v, want latency evidence without a timeout", decision)
				}
				return context.Background(), write
			},
		},
		"observe": {
			prepare: func(t *testing.T, harness *engineHarness) (context.Context, Call) {
				if err := harness.engine.SetMode(Observe); err != nil {
					t.Fatalf("SetMode(Observe) error = %v", err)
				}
				return context.Background(), engineTestCall()
			},
		},
		"off": {
			prepare: func(t *testing.T, harness *engineHarness) (context.Context, Call) {
				if err := harness.engine.SetMode(Off); err != nil {
					t.Fatalf("SetMode(Off) error = %v", err)
				}
				return context.Background(), engineTestCall()
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newEngineHarness(t)
			harness.warmHealthyTimeout()
			ctx, call := test.prepare(t, harness)
			before := len(harness.timers.started())

			if err := harness.do(ctx, call, engineReturns(nil)); err != nil {
				t.Fatalf("Do() error = %v, want nil", err)
			}
			contexts, _ := harness.recorded()
			if len(contexts) != 1 {
				t.Fatalf("attempts = %d, want 1", len(contexts))
			}
			wantTimers := 0
			if test.timed {
				wantTimers = 1
			}
			timed, timers := contexts[0] != ctx, len(harness.timers.started())-before
			if timed != test.timed || timers != wantTimers {
				t.Errorf("timed = %t with %d timers, want %t with %d", timed, timers, test.timed, wantTimers)
			}
		})
	}
}

func TestEngineTimeoutWithdrawnWhenAuthorityEnds(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*testing.T, *Engine){
		"mode change": func(t *testing.T, engine *Engine) {
			if err := engine.SetMode(Observe); err != nil {
				t.Fatalf("SetMode(Observe) error = %v", err)
			}
		},
		"restored mode": func(t *testing.T, engine *Engine) {
			for _, mode := range []Mode{Observe, Enforce} {
				if err := engine.SetMode(mode); err != nil {
					t.Fatalf("SetMode(%v) error = %v", mode, err)
				}
			}
		},
		"close": func(t *testing.T, engine *Engine) {
			if err := engine.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
		},
		"self-disable": selfDisable,
	}

	for name, revoke := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newEngineHarness(t)
			harness.warmHealthyTimeout()
			harness.expect(engineTestCall(), nil, func(ctx context.Context) error {
				revoke(t, harness.engine)
				harness.timers.only(t).fire()
				if err := ctx.Err(); err != nil {
					t.Errorf("withdrawn timeout canceled the attempt: %v", err)
				}
				return nil
			})
			if got := harness.engine.Stats().Timeouts; got != 0 {
				t.Errorf("Timeouts = %d, want 0", got)
			}
		})
	}
}

func TestEngineTimeoutReleasesAReturnedAttempt(t *testing.T) {
	t.Parallel()

	// A timer whose Stop fails is an internal failure. The attempt still
	// returned before its timeout, so Do keeps its result.
	tests := map[string]struct {
		failures   uint64
		stopPanics bool
	}{
		"timer stopped":     {},
		"timer stop failed": {failures: 1, stopPanics: true},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newEngineHarness(t)
			harness.timers.stopPanics = test.stopPanics
			harness.warmHealthyTimeout()

			var attempt context.Context
			harness.expect(engineTestCall(), errEngineInternal, func(ctx context.Context) error {
				attempt = ctx
				return errEngineInternal
			})

			timer := harness.timers.only(t)
			if !test.stopPanics && !timer.stopped.Load() {
				t.Error("returned attempt left its timer running")
			}
			if attempt.Err() == nil {
				t.Error("returned attempt left its context live")
			}
			// The attempt is settled, so a late timer cannot end it.
			timer.fire()
			harness.assertStats(name, Stats{InternalFailures: test.failures})
		})
	}
}

func TestEngineTimeoutAbandonedWhenAttemptPanics(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		failures   uint64
		stopPanics bool
	}{
		"timer stopped":     {},
		"timer stop failed": {failures: 1, stopPanics: true},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newEngineHarness(t)
			harness.timers.stopPanics = test.stopPanics
			harness.warmHealthyTimeout()

			caller := context.Background()
			var attempt context.Context
			func() {
				defer func() {
					//nolint:errorlint // The recovered value is the panic value itself.
					if recovered := recover(); recovered != errBasePanic {
						t.Errorf("recovered %v, want the attempt's panic", recovered)
					}
				}()
				err := harness.do(caller, engineTestCall(), func(ctx context.Context) error {
					attempt = ctx
					panic(errBasePanic)
				})
				t.Errorf("Do() error = %v, want the attempt's panic", err)
			}()

			timer := harness.timers.only(t)
			if !test.stopPanics && !timer.stopped.Load() {
				t.Error("panicking attempt left its timer running")
			}
			if attempt == nil || attempt.Err() == nil {
				t.Error("panicking attempt left its context live")
			}
			// The abandoned attempt is settled, so a late timer cannot end it.
			timer.fire()
			harness.assertStats(name, Stats{InternalFailures: test.failures})
		})
	}
}

func TestEngineTimeoutFailingToStartRunsTheAttemptUntimed(t *testing.T) {
	t.Parallel()

	harness := newEngineHarness(t)
	harness.engine.after = func(time.Duration, func()) timeout.Timer {
		panic("timer start failure")
	}
	harness.warmHealthyTimeout()

	ctx := context.WithValue(context.Background(), engineTestKey{}, "untimed")
	if err := harness.do(ctx, engineTestCall(), engineReturns(nil)); err != nil {
		t.Fatalf("Do() error = %v, want nil", err)
	}
	if contexts, _ := harness.recorded(); len(contexts) != 1 || contexts[0] != ctx {
		t.Error("attempt did not use the caller's context")
	}
	harness.assertStats("timer start failure", Stats{InternalFailures: 1})
}

func TestEngineRetryAttemptHasAdaptiveTimeout(t *testing.T) {
	t.Parallel()

	harness := newEngineHarness(t)
	harness.warmRetryTimeout()

	harness.expect(
		engineTestCall(),
		ErrTimeout,
		engineReturns(errEngineUnavailable),
		func(ctx context.Context) error {
			started := harness.timers.started()
			if len(started) != 2 ||
				!started[0].stopped.Load() ||
				started[1].limit != timeout.DefaultBounds.Minimum {
				t.Fatalf("timers = %d, want the first stopped and a retry timer of 2s", len(started))
			}
			started[1].fire()
			return ctx.Err()
		},
	)
	harness.assertStats("timed retry", Stats{RetryAttempts: 1, Timeouts: 1})
}

func TestEngineLatencyEndsWhenTheAttemptReturns(t *testing.T) {
	t.Parallel()

	harness := newEngineHarness(t)
	harness.observeTimeout()

	// Observe compares each attempt's latency with the two second timeout
	// that Enforce would apply. The time Classify takes does not count.
	call := engineTestCall()
	call.Classify = func(err error) Classification {
		harness.clock.Advance(3 * time.Second)
		return classifyEngineTestError(err)
	}
	harness.expect(call, errEngineNotFound, engineReturns(errEngineNotFound))
	harness.assertStats("slow Classify", Stats{})

	harness.expect(call, nil, func(context.Context) error {
		harness.clock.Advance(3 * time.Second)
		return nil
	})
	harness.assertStats("slow attempt", Stats{ShadowTimeouts: 1})
	if started := len(harness.timers.started()); started != 0 {
		t.Errorf("started timers = %d, want 0", started)
	}
}

func TestEngineProbeIsNeverTimed(t *testing.T) {
	t.Parallel()

	harness := newEngineHarness(t)
	harness.warmTimeout(engineTestCall(), errEngineInternal)
	assertDecision(
		t,
		"down target",
		harness.decision(MethodRead),
		ReadinessReady,
		DiagnosisDependencyDown,
		CandidateBreakerOpen|CandidateTimeout,
	)
	harness.enforce()
	harness.expect(engineTestCall(), errEngineInternal, engineReturns(errEngineInternal))
	harness.assertStats("open", Stats{BreakerOpens: 1})
	if started := len(harness.timers.started()); started != 1 {
		t.Fatalf("started timers = %d, want 1 for the call that opened the breaker", started)
	}

	harness.clock.Advance(breaker.BaseCooldown)
	ctx := context.WithValue(context.Background(), engineTestKey{}, "probe")
	if err := harness.do(ctx, engineTestCall(), engineReturns(nil)); err != nil {
		t.Fatalf("probe error = %v, want nil", err)
	}
	if contexts, _ := harness.recorded(); len(contexts) != 1 || contexts[0] != ctx {
		t.Error("probe did not use the caller's context")
	}
	harness.assertStats("probe", Stats{BreakerOpens: 1, BreakerProbes: 1})
	if started := len(harness.timers.started()); started != 1 {
		t.Errorf("started timers = %d, want no timer for the probe", started)
	}
}

func TestEngineIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	harness := newEngineHarness(t)
	harness.warm()

	const workers, calls = 8, 50
	var group sync.WaitGroup
	for worker := range workers {
		group.Add(1)
		go func() {
			defer group.Done()

			for index := range calls {
				err := harness.engine.Do(
					context.Background(),
					engineTestCall(),
					func(context.Context) error {
						if (worker+index)%3 == 0 {
							return errEngineUnavailable
						}
						return nil
					},
				)
				if err != nil && !errors.Is(err, errEngineUnavailable) {
					t.Errorf("Do() error = %v", err)
				}
			}
		}()
	}
	group.Add(1)
	go func() {
		defer group.Done()

		for index := range calls {
			mode := Enforce
			if index%2 == 0 {
				mode = Observe
			}
			if err := harness.engine.SetMode(mode); err != nil {
				t.Errorf("SetMode(%v) error = %v", mode, err)
			}
			_ = harness.engine.Report()
		}
	}()
	group.Wait()

	stats := harness.engine.Stats()
	if stats.RetrySuccesses > stats.RetryAttempts ||
		stats.RetryAttempts+stats.RetryBudgetDenials > stats.ObservedRequests ||
		stats.InternalFailures != 0 {
		t.Errorf("Stats() = %+v, want consistent retry counters and no failures", stats)
	}
}

func BenchmarkTimedEngineDo(b *testing.B) {
	engine, call := timedReadEngine(b)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := engine.Do(ctx, call, allocationAttempt); err != nil {
			b.Fatalf("Do() error = %v", err)
		}
	}
}

func BenchmarkEngineBreakerRejection(b *testing.B) {
	engine, call := openBreakerEngine(b)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := engine.Do(ctx, call, allocationAttempt); !errors.Is(err, ErrBreakerOpen) {
			b.Fatalf("Do() error = %v, want ErrBreakerOpen", err)
		}
	}
}
