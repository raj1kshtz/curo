package observe

import (
	"context"
	"math"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raj1kshtz/curo/internal/breaker"
	"github.com/raj1kshtz/curo/internal/retry"
	"github.com/raj1kshtz/curo/internal/timeout"
)

const (
	observationBucketCount = 12
	observationBucketWidth = 10 * time.Second
	uninitializedEpoch     = int64(-1)
)

var latencyUpperBounds = [...]time.Duration{
	time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2500 * time.Millisecond,
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
}

type outcome uint8

const (
	outcomeSuccess outcome = iota
	outcomeHTTPFailure
	outcomeTransportFailure
	outcomeCanceled
	outcomeTimedOut
	outcomeCount
)

type timeoutSource uint8

const (
	timeoutCaller timeoutSource = iota
	timeoutTransport
	timeoutAdaptive
	timeoutSourceCount
)

const statusClassCount = 6
const maxErrorNodes = 32

type diagnosisSignal uint8

const (
	diagnosisNeutral diagnosisSignal = iota
	diagnosisCallerOwned
	diagnosisRateLimited
	diagnosisClientFailure
	diagnosisDependencyFailure

	// diagnosisLatencyOnly is an attempt that Curo's adaptive timeout ended
	// below the target's ceiling. Its latency is evidence, but its outcome
	// says nothing about the dependency's health.
	diagnosisLatencyOnly
	diagnosisSignalCount
)

// relevant reports whether an attempt with signal is evidence about the
// dependency's health. Caller cancellations and latency-only samples are not.
func (signal diagnosisSignal) relevant() bool {
	return signal != diagnosisCallerOwned && signal != diagnosisLatencyOnly
}

type observation struct {
	outcome       outcome
	timeoutSource timeoutSource
	statusClass   uint8
	latencyBucket uint8
	signal        diagnosisSignal
	hasTimeout    bool
	hasStatus     bool
	hasLatency    bool
}

type evidenceBucket struct {
	epoch int64

	attempts uint64
	outcomes [outcomeCount]uint64
	timeouts [timeoutSourceCount]uint64
	statuses [statusClassCount]uint64
	latency  [len(latencyUpperBounds) + 1]uint64
	signals  [diagnosisSignalCount]uint64

	firstTick         int64
	lastTick          int64
	firstRelevantTick int64
	lastRelevantTick  int64
	hasTick           bool
	hasRelevantTick   bool
}

type target struct {
	changes *journal

	// timeout mirrors the published plan's adaptive timeout and expiry, or
	// is nil when the plan selects none, so requests skip the lock.
	timeout atomic.Pointer[publishedTimeout]
	key     targetKey

	buckets  [observationBucketCount]evidenceBucket
	baseline [baselineBucketCount]baselineBucket

	diagnosis   diagnosisState
	breaker     breaker.Breaker
	timeouts    timeout.Bounds
	recentFloor int64
	mu          sync.Mutex
	lastSeen    atomic.Int64
	retryBudget retry.Budget

	// engaged mirrors breaker.State() != breaker.Closed, so requests to a
	// target with a closed breaker skip the lock.
	engaged atomic.Bool

	actionable bool
	retired    bool
}

type publishedTimeout struct {
	limit     time.Duration
	expiresAt int64
}

func newTarget(tick int64, actionable bool) *target {
	state := &target{actionable: actionable}
	for index := range state.buckets {
		state.buckets[index].epoch = uninitializedEpoch
	}
	for index := range state.baseline {
		state.baseline[index].epoch = uninitializedEpoch
	}
	state.lastSeen.Store(tick)

	return state
}

func (target *target) touch(tick int64) {
	for {
		current := target.lastSeen.Load()
		if tick <= current || target.lastSeen.CompareAndSwap(current, tick) {
			return
		}
	}
}

// retire marks a target replaced in the registry. In-flight tokens may still
// record evidence, but a retired target never evaluates or records changes,
// and it no longer selects a timeout.
func (target *target) retire() {
	target.mu.Lock()
	defer target.mu.Unlock()

	target.retired = true
	target.timeout.Store(nil)
}

func (target *target) record(tick int64, value observation) bool {
	if target == nil {
		return false
	}

	recorded, _ := target.complete(tick, value, 0, false)
	return recorded
}

// complete records one finished attempt and applies its breaker effects under
// one lock. A non-zero probe lease is settled with the attempt's outcome, even
// when the evidence itself arrived too late to record. A probe that closes
// the breaker evaluates the target after the close, so an attempt evaluates
// the target at most once. For a dependency failure that a closed breaker
// admitted, complete reports whether the breaker may open.
func (target *target) complete(
	tick int64,
	value observation,
	probe uint64,
	admitted bool,
) (bool, bool) {
	if tick < 0 {
		tick = 0
	}

	target.mu.Lock()
	defer target.mu.Unlock()

	recorded, evaluate := target.recordLocked(tick, value)
	if probe != 0 && target.settleLocked(probe, value.signal, tick) {
		evaluate = false
	}
	if evaluate {
		target.evaluateRecordedLocked(tick, value)
	}
	trip := admitted &&
		value.signal == diagnosisDependencyFailure &&
		target.mayTripLocked(tick)

	return recorded, trip
}

// recordLocked records value and reports whether it was recorded and whether
// the target must evaluate it.
func (target *target) recordLocked(
	tick int64,
	value observation,
) (bool, bool) {
	epoch := tick / int64(observationBucketWidth)
	index := int(epoch % observationBucketCount)

	bucket := &target.buckets[index]
	if bucket.epoch > epoch {
		return false, false
	}
	if bucket.epoch < epoch {
		*bucket = evidenceBucket{epoch: epoch}
	}

	recordEvidence(bucket, tick, value)
	return true, target.recordDiagnosisLocked(tick, value)
}

func recordEvidence(
	bucket *evidenceBucket,
	tick int64,
	value observation,
) {
	increment(&bucket.attempts)
	increment(&bucket.outcomes[value.outcome])
	increment(&bucket.signals[value.signal])
	if value.hasTimeout {
		increment(&bucket.timeouts[value.timeoutSource])
	}
	if value.hasStatus {
		increment(&bucket.statuses[value.statusClass])
	}
	if value.hasLatency {
		increment(&bucket.latency[value.latencyBucket])
	}

	recordTick(
		tick,
		&bucket.firstTick,
		&bucket.lastTick,
		&bucket.hasTick,
	)
	if value.signal.relevant() {
		recordTick(
			tick,
			&bucket.firstRelevantTick,
			&bucket.lastRelevantTick,
			&bucket.hasRelevantTick,
		)
	}
}

func classify(result Result, contextErr error, latency time.Duration) observation {
	value := observation{}
	resultSignals := inspectError(result.Err)
	contextSignals := inspectError(contextErr)

	if result.HasResponse {
		value.hasStatus = true
		value.statusClass = classifyStatus(result.StatusCode)
	}

	switch {
	case result.Err == nil && result.HasResponse &&
		result.StatusCode >= 200 && result.StatusCode < 400:
		value.outcome = outcomeSuccess
	case result.Err == nil && result.HasResponse:
		value.outcome = outcomeHTTPFailure
	case result.Err == nil:
		value.outcome = outcomeTransportFailure
	case resultSignals.deadline:
		value.outcome = outcomeTimedOut
		value.hasTimeout = true
		if contextSignals.deadline {
			value.timeoutSource = timeoutCaller
		} else {
			value.timeoutSource = timeoutTransport
		}
	case resultSignals.canceled:
		value.outcome = outcomeCanceled
	case contextSignals.deadline:
		value.outcome = outcomeTimedOut
		value.timeoutSource = timeoutCaller
		value.hasTimeout = true
	case contextSignals.canceled:
		value.outcome = outcomeCanceled
	default:
		value.outcome = outcomeTransportFailure
	}

	if result.Err == nil && result.HasResponse {
		if latency < 0 {
			latency = 0
		}
		value.hasLatency = true
		value.latencyBucket = latencyBucket(latency)
	}
	value.signal = classifyDiagnosisSignal(value, result, contextSignals)

	return value
}

// censored classifies an attempt that Curo's adaptive timeout ended. Its true
// latency is at least limit, so it is recorded as a sample in limit's bucket,
// which raises the next timeout. Only a cut at the target's ceiling is a
// dependency failure. Below the ceiling, the timeout was too short to judge
// the dependency, so the cut is only a latency sample and leaves the
// target's failure rates unchanged.
func censored(limit time.Duration, bounds timeout.Bounds) observation {
	value := observation{
		outcome:       outcomeTimedOut,
		timeoutSource: timeoutAdaptive,
		latencyBucket: latencyBucket(limit),
		signal:        diagnosisLatencyOnly,
		hasTimeout:    true,
		hasLatency:    true,
	}
	if limit >= bounds.Maximum {
		value.signal = diagnosisDependencyFailure
	}

	return value
}

func classifyDiagnosisSignal(
	value observation,
	result Result,
	contextSignals errorSignals,
) diagnosisSignal {
	switch {
	case value.outcome == outcomeCanceled && contextSignals.canceled:
		return diagnosisCallerOwned
	case value.outcome == outcomeTimedOut &&
		value.timeoutSource == timeoutCaller:
		return diagnosisCallerOwned
	case result.Err != nil:
		return diagnosisDependencyFailure
	case value.outcome == outcomeTransportFailure:
		return diagnosisDependencyFailure
	case result.HasResponse && result.StatusCode == 429:
		return diagnosisRateLimited
	case result.HasResponse &&
		result.StatusCode >= 400 && result.StatusCode < 500:
		return diagnosisClientFailure
	case result.HasResponse &&
		result.StatusCode >= 500 && result.StatusCode < 600:
		return diagnosisDependencyFailure
	default:
		return diagnosisNeutral
	}
}

type errorSignals struct {
	canceled bool
	deadline bool
}

//nolint:errorlint // Standard traversal is unbounded; this walker deliberately limits host errors.
func inspectError(root error) errorSignals {
	if root == nil {
		return errorSignals{}
	}

	var (
		pending      [maxErrorNodes]error
		seen         [maxErrorNodes]error
		pendingCount = 1
		seenCount    int
		visited      int
		signals      errorSignals
	)
	pending[0] = root

	for pendingCount > 0 && visited < maxErrorNodes {
		pendingCount--
		current := pending[pendingCount]
		if current == nil || containsComparableError(seen[:seenCount], current) {
			continue
		}

		if reflect.ValueOf(current).Comparable() {
			seen[seenCount] = current
			seenCount++
		}
		visited++

		switch current {
		case context.Canceled:
			signals.canceled = true
		case context.DeadlineExceeded, os.ErrDeadlineExceeded:
			signals.deadline = true
		}
		if matcher, ok := current.(interface{ Is(error) bool }); ok {
			if matcher.Is(context.Canceled) {
				signals.canceled = true
			}
			if matcher.Is(context.DeadlineExceeded) ||
				matcher.Is(os.ErrDeadlineExceeded) {
				signals.deadline = true
			}
		}
		if timeout, ok := current.(interface{ Timeout() bool }); ok &&
			timeout.Timeout() {
			signals.deadline = true
		}
		if signals.canceled && signals.deadline {
			return signals
		}

		switch wrapped := current.(type) {
		case interface{ Unwrap() []error }:
			children := wrapped.Unwrap()
			for index := len(children) - 1; index >= 0 &&
				pendingCount < maxErrorNodes; index-- {
				pending[pendingCount] = children[index]
				pendingCount++
			}
		case interface{ Unwrap() error }:
			if pendingCount < maxErrorNodes {
				pending[pendingCount] = wrapped.Unwrap()
				pendingCount++
			}
		}
	}

	return signals
}

//nolint:errorlint // Cycle detection compares exact nodes instead of traversing them.
func containsComparableError(seen []error, candidate error) bool {
	candidateType := reflect.TypeOf(candidate)
	if !reflect.ValueOf(candidate).Comparable() {
		return false
	}

	for _, previous := range seen {
		if reflect.TypeOf(previous) == candidateType && previous == candidate {
			return true
		}
	}

	return false
}

func classifyStatus(statusCode int) uint8 {
	class := statusCode / 100
	if class < 1 || class > 5 {
		return statusClassCount - 1
	}

	return uint8(class - 1)
}

func latencyBucket(latency time.Duration) uint8 {
	for index, upperBound := range latencyUpperBounds {
		if latency <= upperBound {
			return uint8(index)
		}
	}

	return uint8(len(latencyUpperBounds))
}

// bucketUpperBound returns the largest latency in bucket, or the largest
// Duration for the bucket above every bound.
func bucketUpperBound(bucket uint8) time.Duration {
	if int(bucket) < len(latencyUpperBounds) {
		return latencyUpperBounds[bucket]
	}

	return math.MaxInt64
}

func increment(value *uint64) {
	if *value < math.MaxUint64 {
		*value++
	}
}

func recordTick(
	tick int64,
	first *int64,
	last *int64,
	recorded *bool,
) {
	if !*recorded {
		*first = tick
		*last = tick
		*recorded = true
		return
	}
	if tick < *first {
		*first = tick
	}
	if tick > *last {
		*last = tick
	}
}
