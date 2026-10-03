package observe

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/raj1kshtz/curo/internal/breaker"
	"github.com/raj1kshtz/curo/internal/diagnose"
	"github.com/raj1kshtz/curo/internal/policy"
)

func TestClassifyUsesExclusiveOutcomes(t *testing.T) {
	t.Parallel()

	genericError := errors.New("transport failure")
	tests := map[string]struct {
		contextErr    error
		result        Result
		wantOutcome   outcome
		wantTimeout   bool
		wantSource    timeoutSource
		wantStatus    bool
		wantClass     uint8
		wantLatency   bool
		wantLatencyAt uint8
		wantSignal    diagnosisSignal
	}{
		"success": {
			result: Result{
				StatusCode:  httpStatusOK,
				HasResponse: true,
			},
			wantOutcome:   outcomeSuccess,
			wantStatus:    true,
			wantClass:     1,
			wantLatency:   true,
			wantLatencyAt: 3,
			wantSignal:    diagnosisNeutral,
		},
		"http failure": {
			result: Result{
				StatusCode:  httpStatusServiceUnavailable,
				HasResponse: true,
			},
			wantOutcome:   outcomeHTTPFailure,
			wantStatus:    true,
			wantClass:     4,
			wantLatency:   true,
			wantLatencyAt: 3,
			wantSignal:    diagnosisDependencyFailure,
		},
		"missing result": {
			result:      Result{},
			wantOutcome: outcomeTransportFailure,
			wantSignal:  diagnosisDependencyFailure,
		},
		"canceled": {
			result:      Result{Err: context.Canceled},
			contextErr:  context.Canceled,
			wantOutcome: outcomeCanceled,
			wantSignal:  diagnosisCallerOwned,
		},
		"transport cancellation": {
			result:      Result{Err: context.Canceled},
			wantOutcome: outcomeCanceled,
			wantSignal:  diagnosisDependencyFailure,
		},
		"caller deadline": {
			result:      Result{Err: context.DeadlineExceeded},
			contextErr:  context.DeadlineExceeded,
			wantOutcome: outcomeTimedOut,
			wantTimeout: true,
			wantSource:  timeoutCaller,
			wantSignal:  diagnosisCallerOwned,
		},
		"transport deadline": {
			result:      Result{Err: context.DeadlineExceeded},
			wantOutcome: outcomeTimedOut,
			wantTimeout: true,
			wantSource:  timeoutTransport,
			wantSignal:  diagnosisDependencyFailure,
		},
		"caller deadline with wrapped transport error": {
			result:      Result{Err: genericError},
			contextErr:  context.DeadlineExceeded,
			wantOutcome: outcomeTimedOut,
			wantTimeout: true,
			wantSource:  timeoutCaller,
			wantSignal:  diagnosisCallerOwned,
		},
		"caller cancellation with transport error": {
			result:      Result{Err: genericError},
			contextErr:  context.Canceled,
			wantOutcome: outcomeCanceled,
			wantSignal:  diagnosisCallerOwned,
		},
		"transport timeout": {
			result:      Result{Err: os.ErrDeadlineExceeded},
			wantOutcome: outcomeTimedOut,
			wantTimeout: true,
			wantSource:  timeoutTransport,
			wantSignal:  diagnosisDependencyFailure,
		},
		"transport error": {
			result:      Result{Err: genericError},
			wantOutcome: outcomeTransportFailure,
			wantSignal:  diagnosisDependencyFailure,
		},
		"rate limited": {
			result: Result{
				StatusCode:  429,
				HasResponse: true,
			},
			wantOutcome:   outcomeHTTPFailure,
			wantStatus:    true,
			wantClass:     3,
			wantLatency:   true,
			wantLatencyAt: 3,
			wantSignal:    diagnosisRateLimited,
		},
		"client failure": {
			result: Result{
				StatusCode:  404,
				HasResponse: true,
			},
			wantOutcome:   outcomeHTTPFailure,
			wantStatus:    true,
			wantClass:     3,
			wantLatency:   true,
			wantLatencyAt: 3,
			wantSignal:    diagnosisClientFailure,
		},
		"status alongside error": {
			result: Result{
				Err:         genericError,
				StatusCode:  429,
				HasResponse: true,
			},
			wantOutcome: outcomeTransportFailure,
			wantStatus:  true,
			wantClass:   3,
			wantSignal:  diagnosisDependencyFailure,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := classify(test.result, test.contextErr, 20*time.Millisecond)
			if got.outcome != test.wantOutcome {
				t.Errorf("outcome = %v, want %v", got.outcome, test.wantOutcome)
			}
			if got.hasTimeout != test.wantTimeout {
				t.Errorf("has timeout = %t, want %t", got.hasTimeout, test.wantTimeout)
			}
			if got.timeoutSource != test.wantSource {
				t.Errorf("timeout source = %v, want %v", got.timeoutSource, test.wantSource)
			}
			if got.hasStatus != test.wantStatus {
				t.Errorf("has status = %t, want %t", got.hasStatus, test.wantStatus)
			}
			if got.statusClass != test.wantClass {
				t.Errorf("status class = %d, want %d", got.statusClass, test.wantClass)
			}
			if got.hasLatency != test.wantLatency {
				t.Errorf("has latency = %t, want %t", got.hasLatency, test.wantLatency)
			}
			if got.latencyBucket != test.wantLatencyAt {
				t.Errorf(
					"latency bucket = %d, want %d",
					got.latencyBucket,
					test.wantLatencyAt,
				)
			}
			if got.signal != test.wantSignal {
				t.Errorf("signal = %v, want %v", got.signal, test.wantSignal)
			}
		})
	}
}

func TestInspectErrorUsesBoundedCycleAwareTraversal(t *testing.T) {
	t.Parallel()

	if got := inspectError(nil); got != (errorSignals{}) {
		t.Errorf("inspectError(nil) = %#v, want empty", got)
	}

	wrappedCanceled := fmt.Errorf("wrapped: %w", context.Canceled)
	if got := inspectError(wrappedCanceled); !got.canceled || got.deadline {
		t.Errorf("wrapped cancellation signals = %#v", got)
	}

	joined := errors.Join(context.Canceled, context.DeadlineExceeded)
	if got := inspectError(joined); !got.canceled || !got.deadline {
		t.Errorf("joined signals = %#v, want both", got)
	}

	if got := inspectError(semanticDeadlineTestError{}); !got.deadline {
		t.Errorf("semantic deadline signals = %#v, want deadline", got)
	}
	if got := inspectError(semanticCanceledTestError{}); !got.canceled {
		t.Errorf("semantic cancellation signals = %#v, want cancellation", got)
	}
	if got := inspectError(timeoutOnlyTestError{}); !got.deadline {
		t.Errorf("timeout-only signals = %#v, want deadline", got)
	}

	cycle := &cyclicTestError{}
	if got := inspectError(cycle); got != (errorSignals{}) {
		t.Errorf("cyclic error signals = %#v, want empty", got)
	}

	deep := context.Canceled
	for range maxErrorNodes + 1 {
		deep = wrappedTestError{next: deep}
	}
	if got := inspectError(deep); got != (errorSignals{}) {
		t.Errorf("over-budget signals = %#v, want empty", got)
	}

	uncomparable := sliceTestError(nil)
	uncomparable = append(uncomparable, uncomparable)
	if got := inspectError(uncomparable); got != (errorSignals{}) {
		t.Errorf("uncomparable cycle signals = %#v, want empty", got)
	}
}

func TestClassifyClampsNegativeLatency(t *testing.T) {
	t.Parallel()

	got := classify(
		Result{StatusCode: httpStatusOK, HasResponse: true},
		nil,
		-time.Second,
	)
	if !got.hasLatency || got.latencyBucket != 0 {
		t.Errorf("negative latency classification = %#v, want first bucket", got)
	}
}

func TestStatusAndLatencyBucketsAreBounded(t *testing.T) {
	t.Parallel()

	statusTests := map[int]uint8{
		0:   5,
		100: 0,
		200: 1,
		300: 2,
		400: 3,
		500: 4,
		600: 5,
	}
	for statusCode, want := range statusTests {
		if got := classifyStatus(statusCode); got != want {
			t.Errorf("classifyStatus(%d) = %d, want %d", statusCode, got, want)
		}
	}

	for index, boundary := range latencyUpperBounds {
		if got := latencyBucket(boundary); got != uint8(index) {
			t.Errorf("latencyBucket(%v) = %d, want %d", boundary, got, index)
		}
	}
	if got := latencyBucket(latencyUpperBounds[len(latencyUpperBounds)-1] + 1); got !=
		uint8(len(latencyUpperBounds)) {
		t.Errorf("overflow latency bucket = %d, want %d", got, len(latencyUpperBounds))
	}
}

func TestTargetRecordsAndRotatesFixedBuckets(t *testing.T) {
	t.Parallel()

	state := newTarget(0, true)
	value := observation{
		outcome:       outcomeTimedOut,
		timeoutSource: timeoutCaller,
		statusClass:   4,
		latencyBucket: 2,
		signal:        diagnosisCallerOwned,
		hasTimeout:    true,
		hasStatus:     true,
		hasLatency:    true,
	}

	if !state.record(0, value) {
		t.Fatal("initial observation was rejected")
	}
	bucket := copyBucket(state, 0)
	if bucket.attempts != 1 {
		t.Errorf("attempts = %d, want 1", bucket.attempts)
	}
	if bucket.outcomes[outcomeTimedOut] != 1 {
		t.Errorf("timed-out outcomes = %d, want 1", bucket.outcomes[outcomeTimedOut])
	}
	if bucket.timeouts[timeoutCaller] != 1 {
		t.Errorf("caller timeouts = %d, want 1", bucket.timeouts[timeoutCaller])
	}
	if bucket.statuses[4] != 1 {
		t.Errorf("5xx statuses = %d, want 1", bucket.statuses[4])
	}
	if bucket.latency[2] != 1 {
		t.Errorf("latency bucket = %d, want 1", bucket.latency[2])
	}
	if bucket.signals[diagnosisCallerOwned] != 1 {
		t.Errorf(
			"caller-owned signals = %d, want 1",
			bucket.signals[diagnosisCallerOwned],
		)
	}

	nextGeneration := int64(observationBucketWidth) * observationBucketCount
	if !state.record(nextGeneration, observation{outcome: outcomeSuccess}) {
		t.Fatal("next-generation observation was rejected")
	}
	bucket = copyBucket(state, 0)
	if bucket.attempts != 1 || bucket.outcomes[outcomeSuccess] != 1 {
		t.Errorf("rotated bucket = %#v, want one success", bucket)
	}
	if bucket.outcomes[outcomeTimedOut] != 0 {
		t.Errorf("rotated bucket retained old timeout count")
	}

	if state.record(0, observation{outcome: outcomeSuccess}) {
		t.Fatal("out-of-order observation replaced a newer bucket generation")
	}
}

func TestTargetHandlesNilNegativeAndSaturatedState(t *testing.T) {
	t.Parallel()

	var state *target
	if state.record(0, observation{}) {
		t.Fatal("nil target accepted observation")
	}

	state = newTarget(0, false)
	if state.actionable {
		t.Fatal("non-actionable target reported actionable")
	}
	if !state.record(-1, observation{outcome: outcomeSuccess}) {
		t.Fatal("negative tick observation was rejected")
	}

	state.mu.Lock()
	state.buckets[0].attempts = math.MaxUint64
	state.buckets[0].outcomes[outcomeSuccess] = math.MaxUint64
	state.mu.Unlock()

	if !state.record(0, observation{outcome: outcomeSuccess}) {
		t.Fatal("saturated observation was rejected")
	}
	bucket := copyBucket(state, 0)
	if bucket.attempts != math.MaxUint64 ||
		bucket.outcomes[outcomeSuccess] != math.MaxUint64 {
		t.Fatal("saturated counters wrapped")
	}
}

func TestRecordTickExpandsBothBounds(t *testing.T) {
	t.Parallel()

	first := int64(10)
	last := int64(10)
	recorded := true
	recordTick(9, &first, &last, &recorded)
	recordTick(11, &first, &last, &recorded)
	if first != 9 || last != 11 {
		t.Errorf("tick bounds = [%d, %d], want [9, 11]", first, last)
	}
}

func TestEvidenceStateCannotRetainSensitiveValues(t *testing.T) {
	t.Parallel()

	// The normalized hostname is the only retained text. It is bounded to 253
	// bytes and cloned at admission.
	allowedStrings := map[string]int{"targetKey.host": 0}
	// atomic.Pointer holds its value in an unsafe.Pointer. The only one is the
	// target's timeout mirror, so its pointee is inspected instead.
	timeoutMirror := reflect.TypeOf(atomic.Pointer[publishedTimeout]{}).Name() + ".v"
	visited := make(map[reflect.Type]bool)

	var inspect func(reflect.Type)
	inspect = func(valueType reflect.Type) {
		if visited[valueType] {
			return
		}
		visited[valueType] = true

		switch valueType.Kind() {
		case reflect.Pointer, reflect.Array:
			inspect(valueType.Elem())
		case reflect.Struct:
			for index := 0; index < valueType.NumField(); index++ {
				field := valueType.Field(index)
				name := valueType.Name() + "." + field.Name
				switch field.Type.Kind() {
				case reflect.String:
					if _, allowed := allowedStrings[name]; !allowed {
						t.Errorf("%s can retain sensitive text", name)
						continue
					}
					allowedStrings[name]++
				case reflect.UnsafePointer:
					if name != timeoutMirror {
						t.Errorf("%s can retain unbounded or sensitive values", name)
						continue
					}
					inspect(reflect.TypeOf(publishedTimeout{}))
				case reflect.Interface,
					reflect.Map,
					reflect.Slice,
					reflect.Func,
					reflect.Chan:
					t.Errorf("%s can retain unbounded or sensitive values", name)
				default:
					inspect(field.Type)
				}
			}
		}
	}
	inspect(reflect.TypeOf(target{}))

	for name, visits := range allowedStrings {
		if visits != 1 {
			t.Errorf("%s inspected %d times, want 1", name, visits)
		}
	}
	for _, valueType := range []reflect.Type{
		reflect.TypeOf(journal{}),
		reflect.TypeOf(change{}),
		reflect.TypeOf(diagnose.Result{}),
		reflect.TypeOf(policy.Plan{}),
		reflect.TypeOf(breaker.Breaker{}),
		reflect.TypeOf(publishedTimeout{}),
	} {
		if !visited[valueType] {
			t.Errorf("%s was not inspected", valueType)
		}
	}

	const maximumTargetBytes = 12 * 1024
	if got := unsafe.Sizeof(target{}); got > maximumTargetBytes {
		t.Errorf("target size = %d bytes, want at most %d", got, maximumTargetBytes)
	}
	if unsafe.Sizeof(uintptr(0)) == 8 {
		const (
			documentedTargetBytes  = 10_000
			documentedJournalBytes = 45_072
		)
		if got := unsafe.Sizeof(target{}); got != documentedTargetBytes {
			t.Errorf(
				"64-bit target size = %d bytes, documented as %d",
				got,
				documentedTargetBytes,
			)
		}
		if got := unsafe.Sizeof(journal{}); got != documentedJournalBytes {
			t.Errorf(
				"64-bit journal size = %d bytes, documented as %d",
				got,
				documentedJournalBytes,
			)
		}
	}
}

func copyBucket(state *target, index int) evidenceBucket {
	state.mu.Lock()
	defer state.mu.Unlock()

	return state.buckets[index]
}

type cyclicTestError struct{}

func (*cyclicTestError) Error() string {
	return "cycle"
}

func (err *cyclicTestError) Unwrap() error {
	return err
}

type semanticDeadlineTestError struct{}

func (semanticDeadlineTestError) Error() string {
	return "semantic deadline"
}

func (semanticDeadlineTestError) Is(target error) bool {
	return target == context.DeadlineExceeded
}

type semanticCanceledTestError struct{}

func (semanticCanceledTestError) Error() string {
	return "semantic cancellation"
}

func (semanticCanceledTestError) Is(target error) bool {
	return target == context.Canceled
}

type timeoutOnlyTestError struct{}

func (timeoutOnlyTestError) Error() string {
	return "timeout"
}

func (timeoutOnlyTestError) Timeout() bool {
	return true
}

type wrappedTestError struct {
	next error
}

func (wrappedTestError) Error() string {
	return "wrapped"
}

func (err wrappedTestError) Unwrap() error {
	return err.next
}

type sliceTestError []error

func (sliceTestError) Error() string {
	return "slice"
}

func (err sliceTestError) Unwrap() []error {
	return err
}

const (
	httpStatusOK                 = 200
	httpStatusServiceUnavailable = 503
)
