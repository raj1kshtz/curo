package observe

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raj1kshtz/curo/internal/diagnose"
)

func TestObserverRecordsBoundedResultEvidence(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(1_000, 0))
	observer := newObserver(clock.Now)
	request := Request{
		Context:  context.Background(),
		Method:   "GET",
		Scheme:   "https",
		Hostname: "example.com",
	}

	token := observer.Begin(request)
	if token.target == nil {
		t.Fatal("Begin() target = nil")
	}
	if token.overflow {
		t.Fatal("valid target used overflow")
	}
	if !token.target.actionable {
		t.Fatal("regular target is not actionable")
	}

	clock.Advance(20 * time.Millisecond)
	observer.Finish(token, Result{
		StatusCode:  httpStatusOK,
		HasResponse: true,
	})

	stats := observer.Stats()
	if stats.ObservedRequests != 1 {
		t.Errorf("ObservedRequests = %d, want 1", stats.ObservedRequests)
	}
	if stats.TrackedTargets != 1 {
		t.Errorf("TrackedTargets = %d, want 1", stats.TrackedTargets)
	}
	if stats.OverflowRequests != 0 {
		t.Errorf("OverflowRequests = %d, want 0", stats.OverflowRequests)
	}

	bucket := copyBucket(token.target, 0)
	if bucket.outcomes[outcomeSuccess] != 1 {
		t.Errorf("success outcomes = %d, want 1", bucket.outcomes[outcomeSuccess])
	}
	if bucket.latency[3] != 1 {
		t.Errorf("20ms latency observations = %d, want 1", bucket.latency[3])
	}

	reused := observer.Begin(request)
	if reused.target != token.target {
		t.Fatal("same normalized target was not reused")
	}
}

func TestObserverUsesOverflowForInvalidIdentity(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(2_000, 0))
	observer := newObserver(clock.Now)

	token := observer.Begin(Request{})
	if !token.overflow {
		t.Fatal("invalid identity did not use overflow")
	}
	if token.target != observer.registry.overflow {
		t.Fatal("invalid identity did not use fixed overflow target")
	}
	if token.context == nil {
		t.Fatal("nil request context was not replaced")
	}

	observer.Finish(token, Result{Err: errors.New("transport failure")})
	stats := observer.Stats()
	if stats.ObservedRequests != 1 || stats.OverflowRequests != 1 {
		t.Errorf("overflow stats = %#v, want one observed overflow request", stats)
	}
	if stats.TrackedTargets != 0 {
		t.Errorf("TrackedTargets = %d, want 0", stats.TrackedTargets)
	}
	if got := observer.Diagnosis(token); got != (diagnose.Result{}) {
		t.Errorf("overflow diagnosis = %#v, want zero", got)
	}
}

func TestObserverUsesCapturedContextForCancellation(t *testing.T) {
	t.Parallel()

	clock := newObservationClock(time.Unix(3_000, 0))
	observer := newObserver(clock.Now)
	requestContext, cancel := context.WithCancel(context.Background())
	token := observer.Begin(Request{
		Context:  requestContext,
		Method:   "GET",
		Scheme:   "https",
		Hostname: "example.com",
	})

	cancel()
	observer.Finish(token, Result{Err: errors.New("transport stopped")})

	bucket := copyBucket(token.target, 0)
	if bucket.outcomes[outcomeCanceled] != 1 {
		t.Errorf("canceled outcomes = %d, want 1", bucket.outcomes[outcomeCanceled])
	}
	if bucket.outcomes[outcomeTransportFailure] != 0 {
		t.Errorf(
			"transport failures = %d, want 0",
			bucket.outcomes[outcomeTransportFailure],
		)
	}
}

func TestObserverClampsBackwardClock(t *testing.T) {
	t.Parallel()

	origin := time.Unix(4_000, 0)
	clock := newObservationClock(origin)
	observer := newObserver(clock.Now)

	clock.Set(origin.Add(-time.Second))
	token := observer.Begin(Request{
		Scheme:   "https",
		Hostname: "example.com",
	})
	clock.Set(origin.Add(-2 * time.Second))
	observer.Finish(token, Result{
		StatusCode:  httpStatusOK,
		HasResponse: true,
	})

	if got := observer.tick(origin.Add(-time.Second)); got != 0 {
		t.Errorf("backward tick = %d, want 0", got)
	}
	bucket := copyBucket(token.target, 0)
	if bucket.attempts != 1 || bucket.latency[0] != 1 {
		t.Errorf("backward-clock bucket = %#v, want one clamped observation", bucket)
	}
}

func TestObserverDropsOutOfOrderCompletion(t *testing.T) {
	t.Parallel()

	origin := time.Unix(4_500, 0)
	clock := newObservationClock(origin)
	observer := newObserver(clock.Now)
	token := observer.Begin(Request{
		Scheme:   "https",
		Hostname: "example.com",
	})

	futureTick := int64(observationBucketWidth) * observationBucketCount
	if !token.target.record(futureTick, observation{outcome: outcomeSuccess}) {
		t.Fatal("future observation was rejected")
	}

	observer.Finish(token, Result{
		StatusCode:  httpStatusOK,
		HasResponse: true,
	})
	if got := observer.Stats().ObservedRequests; got != 0 {
		t.Errorf("ObservedRequests = %d, want 0 after stale completion", got)
	}
}

func TestObserverHandlesNilReceiversAndTokens(t *testing.T) {
	t.Parallel()

	var observer *Observer
	token := observer.Begin(Request{})
	if token.target != nil {
		t.Fatal("nil Observer returned a target")
	}
	observer.Finish(token, Result{})
	if got := observer.Diagnosis(token); got != (diagnose.Result{}) {
		t.Errorf("nil Observer Diagnosis() = %#v, want zero", got)
	}
	if got := observer.Stats(); got != (Stats{}) {
		t.Errorf("nil Observer Stats() = %#v, want zero", got)
	}

	observer = newObserver(nil)
	if observer.now == nil {
		t.Fatal("newObserver(nil) clock = nil")
	}
	observer.Finish(Token{}, Result{})
	if got := observer.Stats().ObservedRequests; got != 0 {
		t.Errorf("empty token observed requests = %d, want 0", got)
	}
}

func TestObserverStatsMaintainAggregateRelationUnderConcurrency(t *testing.T) {
	t.Parallel()

	const workers = 128

	observer := New()
	start := make(chan struct{})
	done := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(workers)

	var relationBroken atomic.Bool
	go func() {
		defer close(done)
		for {
			stats := observer.Stats()
			if stats.OverflowRequests > stats.ObservedRequests {
				relationBroken.Store(true)
				return
			}
			if stats.ObservedRequests == workers {
				return
			}
		}
	}()

	for range workers {
		go func() {
			defer wait.Done()
			<-start

			token := observer.Begin(Request{})
			observer.Finish(token, Result{
				StatusCode:  httpStatusOK,
				HasResponse: true,
			})
		}()
	}

	close(start)
	wait.Wait()
	<-done

	if relationBroken.Load() {
		t.Fatal("OverflowRequests exceeded ObservedRequests")
	}
	stats := observer.Stats()
	if stats.ObservedRequests != workers || stats.OverflowRequests != workers {
		t.Errorf("concurrent stats = %#v, want %d observed overflow requests", stats, workers)
	}
}

func TestObserverProducesDeterministicTargetDiagnosis(t *testing.T) {
	t.Parallel()

	origin := time.Unix(5_000, 0)
	clock := newObservationClock(origin)
	observer := newObserver(clock.Now)
	request := Request{
		Context:  context.Background(),
		Method:   "GET",
		Scheme:   "https",
		Hostname: "example.com",
	}

	var lastToken Token
	for index := 0; index < 20; index++ {
		token := observer.Begin(request)
		lastToken = token

		statusCode := httpStatusOK
		if index >= 10 {
			statusCode = httpStatusServiceUnavailable
		}
		observer.Finish(token, Result{
			StatusCode:  statusCode,
			HasResponse: true,
		})
		clock.Advance(2 * time.Second)
	}

	result := observer.Diagnosis(lastToken)
	if result.Readiness != diagnose.ReadinessReady {
		t.Errorf("readiness = %v, want Ready", result.Readiness)
	}
	if result.Class != diagnose.ClassDependencyDown {
		t.Errorf("class = %v, want DependencyDown", result.Class)
	}
	if got := observer.Stats().ObservedRequests; got != 20 {
		t.Errorf("ObservedRequests = %d, want 20", got)
	}
}

func TestObserverExcludesCallerCancellationFromReadiness(t *testing.T) {
	t.Parallel()

	origin := time.Unix(6_000, 0)
	clock := newObservationClock(origin)
	observer := newObserver(clock.Now)
	requestContext, cancel := context.WithCancel(context.Background())
	cancel()
	request := Request{
		Context:  requestContext,
		Method:   "GET",
		Scheme:   "https",
		Hostname: "example.com",
	}

	var lastToken Token
	for range 20 {
		token := observer.Begin(request)
		lastToken = token
		observer.Finish(token, Result{Err: context.Canceled})
		clock.Advance(2 * time.Second)
	}

	result := observer.Diagnosis(lastToken)
	if result.Readiness != diagnose.ReadinessCold {
		t.Errorf("readiness = %v, want Cold", result.Readiness)
	}
	if result.Class != diagnose.ClassNone {
		t.Errorf("class = %v, want None", result.Class)
	}
}

type observationClock struct {
	now time.Time
	mu  sync.Mutex
}

func newObservationClock(now time.Time) *observationClock {
	return &observationClock{now: now}
}

func (clock *observationClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()

	return clock.now
}

func (clock *observationClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()

	clock.now = clock.now.Add(duration)
}

func (clock *observationClock) Set(now time.Time) {
	clock.mu.Lock()
	defer clock.mu.Unlock()

	clock.now = now
}
