package curo

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raj1kshtz/curo/internal/guard"
	"github.com/raj1kshtz/curo/internal/observe"
	"github.com/raj1kshtz/curo/internal/timeout"
)

func TestFailureLogNamesTheFailedStage(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		fail func(*testing.T, *Transport)
		want []string
	}{
		"preflight": {
			fail: func(t *testing.T, transport *Transport) {
				transport.stages = testRequestStages{
					before: func(requestSnapshot, Mode) error {
						panic("preflight failure")
					},
					after: func(attemptResult) error {
						return nil
					},
				}
				roundTripInternal(t, transport, newLoggerRequest(t))
			},
			want: []string{"preflight"},
		},
		"postflight": {
			fail: func(t *testing.T, transport *Transport) {
				transport.stages = testRequestStages{
					before: func(requestSnapshot, Mode) error {
						return nil
					},
					after: func(attemptResult) error {
						panic("postflight failure")
					},
				}
				roundTripInternal(t, transport, newLoggerRequest(t))
			},
			want: []string{"postflight"},
		},
		"retry backoff": {
			fail: func(t *testing.T, transport *Transport) {
				transport.wait = func(context.Context, <-chan struct{}, time.Duration) bool {
					panic("wait failure")
				}
				next, body := transport.prepareRetry(
					newLoggerRequest(t),
					nil,
					transport.authority.Load(),
					time.Millisecond,
				)
				if next != nil || body != nil {
					t.Errorf("prepareRetry() = %v, %v, want no retry", next, body)
				}
			},
			want: []string{"retry"},
		},
		"retry commit": {
			fail: func(t *testing.T, transport *Transport) {
				permitted, _ := transport.commitRetry(
					newLoggerRequest(t),
					panickingConfirmStages{},
					requestState{},
					transport.authority.Load(),
					0,
				)
				if permitted {
					t.Error("commitRetry() permitted a retry whose check failed")
				}
			},
			want: []string{"retry"},
		},
		"timeout start": {
			fail: func(t *testing.T, transport *Transport) {
				transport.after = func(time.Duration, func()) timeout.Timer {
					panic("timer start failure")
				}
				attempt, timed := transport.arm(
					newLoggerRequest(t),
					nil,
					time.Second,
					transport.authority.Load(),
				)
				if attempt != nil || timed != nil {
					t.Errorf("arm() = %v, %v, want no timeout", attempt, timed)
				}
			},
			want: []string{"timeout"},
		},
		"timeout expiry": {
			fail: func(_ *testing.T, transport *Transport) {
				transport.expire(nil, transport.authority.Load())
			},
			want: []string{"timeout"},
		},
		"timeout settle": {
			fail: func(t *testing.T, transport *Transport) {
				if transport.settle(nil, nil, errors.New("attempt failure")) {
					t.Error("settle() = true, want false when Settle fails")
				}
			},
			want: []string{"timeout", "timeout"},
		},
		"timeout abandon": {
			fail: func(t *testing.T, transport *Transport) {
				timers := &manualTimers{stopPanics: true}
				transport.after = timers.after
				transport.base = internalRoundTripperFunc(
					func(*http.Request) (*http.Response, error) {
						panic("base failure")
					},
				)
				defer func() {
					if recovered := recover(); recovered != "base failure" {
						t.Errorf("send() panic = %v, want the base transport's", recovered)
					}
				}()

				response, _, err := transport.send(
					newLoggerRequest(t),
					nil,
					time.Second,
					transport.authority.Load(),
				)
				closeTestBody(t, response)
				t.Errorf("send() error = %v, want the base transport's panic", err)
			},
			want: []string{"timeout"},
		},
		"body": {
			fail: func(_ *testing.T, transport *Transport) {
				body := newTrackedBody("")
				body.panics = true
				transport.closeBody(body)
			},
			want: []string{"body"},
		},
		"report": {
			fail: func(_ *testing.T, transport *Transport) {
				transport.reports = func() observe.Report {
					panic("report failure")
				}
				_ = transport.Report()
			},
			want: []string{"report"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			transport, recorder := newLoggedTransport(t, slog.LevelWarn)
			test.fail(t, transport)

			var stages []string
			for _, record := range recorder.snapshot() {
				stage, _ := attrValue(record, "stage")
				stages = append(stages, stage.String())
			}
			if !slices.Equal(stages, test.want) {
				t.Errorf("logged stages = %v, want %v", stages, test.want)
			}
			if got := transport.Stats().InternalFailures; got != uint64(len(test.want)) {
				t.Errorf("InternalFailures = %d, want %d", got, len(test.want))
			}
		})
	}
}

func TestFailureLogDescribesTheFailure(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		operation func() error
		kind      string
		valueType string
		message   string
		frame     string
	}{
		"panic value": {
			operation: func() error {
				panicWith("secret value")
				return nil
			},
			kind:      "panic",
			valueType: "string",
			frame:     "curo.panicWith(",
		},
		"argument values": {
			operation: func() error {
				panicAfterCall(secretNumber)
				return nil
			},
			kind:      "panic",
			valueType: "string",
			frame:     "curo.panicAfterCall(",
		},
		"panic in a deferred call": {
			operation: func() error {
				panicWhilePanicking()
				return nil
			},
			kind:      "panic",
			valueType: "string",
			frame:     "curo.panicWhilePanicking.func1(",
		},
		"index out of range": {
			operation: func() error {
				_ = elementAt(nil, secretNumber)
				return nil
			},
			kind:      "panic",
			valueType: "runtime.boundsError",
			frame:     "curo.elementAt(",
		},
		"nil pointer dereference": {
			operation: func() error {
				_ = dereference(nil)
				return nil
			},
			kind:      "panic",
			valueType: "runtime.errorString",
			message:   "runtime error: invalid memory address or nil pointer dereference",
			frame:     "curo.dereference(",
		},
		"nil map assignment": {
			operation: func() error {
				assignEntry(nil)
				return nil
			},
			kind:      "panic",
			valueType: "runtime.plainError",
			message:   "assignment to entry in nil map",
			frame:     "curo.assignEntry(",
		},
		"failed type assertion": {
			operation: func() error {
				_ = assertInt("secret value")
				return nil
			},
			kind:      "panic",
			valueType: "*runtime.TypeAssertionError",
			frame:     "curo.assertInt(",
		},
		"built type": {
			operation: func() error {
				panicWith(builtValue())
				return nil
			},
			kind:      "panic",
			valueType: "struct",
			frame:     "curo.panicWith(",
		},
		"failed type assertion of a built type": {
			operation: func() error {
				_ = assertInt(builtValue())
				return nil
			},
			kind:      "panic",
			valueType: "*runtime.TypeAssertionError",
			frame:     "curo.assertInt(",
		},
		"unhashable built type": {
			operation: func() error {
				_ = addKey(builtValue())
				return nil
			},
			kind:      "panic",
			valueType: "runtime.errorString",
			frame:     "curo.addKey(",
		},
		"built array type": {
			operation: func() error {
				panicWith(builtArray())
				return nil
			},
			kind:      "panic",
			valueType: "*[...]uint8",
			frame:     "curo.panicWith(",
		},
		"integer divide by zero": {
			operation: func() error {
				_ = divide(secretNumber, 0)
				return nil
			},
			kind:      "panic",
			valueType: "runtime.errorString",
			message:   "runtime error: integer divide by zero",
			frame:     "curo.divide(",
		},
		"closed channel": {
			operation: func() error {
				channel := make(chan int)
				closeChannel(channel)
				closeChannel(channel)
				return nil
			},
			kind:      "panic",
			valueType: "runtime.plainError",
			message:   "close of closed channel",
			frame:     "curo.closeChannel(",
		},
		"nil panic value": {
			operation: func() error {
				panicWith(nil)
				return nil
			},
			kind:      "panic",
			valueType: "*runtime.PanicNilError",
			// Go 1.27 adds the "runtime error: " prefix.
			message: new(runtime.PanicNilError).Error(),
			frame:   "curo.panicWith(",
		},
		"nil runtime error pointer": {
			operation: func() error {
				panicWith((*runtime.TypeAssertionError)(nil))
				return nil
			},
			kind:      "panic",
			valueType: "*runtime.TypeAssertionError",
			frame:     "curo.panicWith(",
		},
		"zero runtime error": {
			operation: func() error {
				panicWith(&runtime.TypeAssertionError{})
				return nil
			},
			kind:      "panic",
			valueType: "*runtime.TypeAssertionError",
			frame:     "curo.panicWith(",
		},
		"application runtime error": {
			operation: func() error {
				panicWith(applicationRuntimeError{})
				return nil
			},
			kind:      "panic",
			valueType: "curo.applicationRuntimeError",
			frame:     "curo.panicWith(",
		},
		"application runtime error pointer": {
			operation: func() error {
				panicWith(&applicationRuntimeError{})
				return nil
			},
			kind:      "panic",
			valueType: "*curo.applicationRuntimeError",
			frame:     "curo.panicWith(",
		},
		"returned error": {
			operation: func() error {
				return errors.New("secret value")
			},
			kind:      "error",
			valueType: "*errors.errorString",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			recorder := &logRecorder{}
			state := guard.New(failureLog{logger: slog.New(recorder)})
			if outcome := state.Run(guard.StagePostflight, test.operation); outcome != guard.Failed {
				t.Fatalf("Run() outcome = %v, want Failed", outcome)
			}

			records := recorder.snapshot()
			if len(records) != 1 {
				t.Fatalf("records = %d, want 1", len(records))
			}
			record := records[0]
			if record.Level != slog.LevelWarn || record.Message != failureMessage {
				t.Errorf("record = %v %q, want WARN %q", record.Level, record.Message, failureMessage)
			}

			wantKeys := []string{"stage", "kind", "type"}
			if test.message != "" {
				wantKeys = append(wantKeys, "error")
			}
			wantKeys = append(wantKeys, "failures")
			if test.frame != "" {
				wantKeys = append(wantKeys, "stack")
			}
			if got := attrKeys(record); !slices.Equal(got, wantKeys) {
				t.Fatalf("attribute keys = %v, want %v", got, wantKeys)
			}

			wantValues := map[string]string{
				"stage":    "postflight",
				"kind":     test.kind,
				"type":     test.valueType,
				"failures": "1",
			}
			if test.message != "" {
				wantValues["error"] = test.message
			}
			for key, want := range wantValues {
				if value, _ := attrValue(record, key); value.String() != want {
					t.Errorf("%s = %q, want %q", key, value.String(), want)
				}
			}
			if test.frame != "" {
				stack, _ := attrValue(record, "stack")
				trace := stack.String()
				if !strings.HasPrefix(trace, panicFunction+"(...)\n") {
					t.Errorf("stack does not start at the panic:\n%s", trace)
				}
				if !strings.Contains(trace, test.frame) {
					t.Errorf("stack lacks the panicking frame %q:\n%s", test.frame, trace)
				}
				if strings.Contains(trace, "reportFailure") || strings.HasSuffix(trace, elidedFrames) {
					t.Errorf("stack holds reporting frames or an elision line:\n%s", trace)
				}
			}
			secrets := []string{
				"secret value",
				strconv.Itoa(secretNumber),
				strconv.FormatInt(secretNumber, 16),
			}
			record.Attrs(func(attr slog.Attr) bool {
				value := strings.ToLower(attr.Value.String())
				for _, secret := range secrets {
					if strings.Contains(value, secret) {
						t.Errorf("%s logs application data %q: %q", attr.Key, secret, attr.Value)
					}
				}
				return true
			})
		})
	}
}

func TestFailureLogCallsNoApplicationErrorMethod(t *testing.T) {
	t.Parallel()

	value := &countingRuntimeError{}
	recorder := &logRecorder{}
	failureLog{logger: slog.New(recorder)}.Failure(guard.Failure{
		Recovered: value,
		Failures:  1,
		Stage:     guard.StageBody,
	})

	if calls := value.calls.Load(); calls != 0 {
		t.Errorf("Error() calls = %d, want 0", calls)
	}
	records := recorder.snapshot()
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	if _, ok := attrValue(records[0], "error"); ok {
		t.Error("record holds the message of an application error")
	}
}

func TestFailureTypeOmitsBuiltParts(t *testing.T) {
	t.Parallel()

	built := reflect.TypeOf(builtValue())
	wide := wideType(6)
	if len(wide.String()) <= 512 {
		t.Fatalf("wide type name has %d bytes, want more than 512", len(wide.String()))
	}
	tests := map[string]struct {
		value any
		want  string
	}{
		"interface element": {
			value: []any(nil),
			want:  "[]interface {}",
		},
		"built slice": {
			value: reflect.Zero(reflect.SliceOf(built)).Interface(),
			want:  "[]struct",
		},
		"built map": {
			value: reflect.Zero(reflect.MapOf(reflect.TypeFor[string](), built)).Interface(),
			want:  "map[string]struct",
		},
		"built function": {
			value: reflect.Zero(reflect.FuncOf([]reflect.Type{built}, nil, false)).Interface(),
			want:  "func",
		},
		"built channel": {
			value: reflect.Zero(reflect.ChanOf(reflect.BothDir, built)).Interface(),
			want:  "chan",
		},
		"declared struct": {
			value: struct{ Value string }{},
			want:  "struct",
		},
		// WithLogger documents the limits of 16 levels of nested types and
		// 512 bytes.
		"deep type": {
			value: reflect.Zero(deepType(1_000)).Interface(),
			want:  strings.Repeat("[]", 16) + "...",
		},
		"deep map key": {
			value: reflect.Zero(reflect.MapOf(reflect.PointerTo(deepType(16)), reflect.TypeFor[string]())).Interface(),
			want:  "map[*" + strings.Repeat("[]", 14) + "...",
		},
		"long name": {
			value: reflect.Zero(wide).Interface(),
			want:  wide.String()[:512] + "...",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := typeName(reflect.TypeOf(test.value)); got != test.want {
				t.Errorf("typeName() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestFailureTypeNameCut(t *testing.T) {
	t.Parallel()

	prefix := strings.Repeat("x", maxTypeName-1)
	tests := map[string]struct {
		next   string
		want   string
		elided bool
	}{
		"last byte": {
			next: "y",
			want: prefix + "y",
		},
		"split rune": {
			next:   "\u00f6",
			want:   prefix,
			elided: true,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var w typeNameWriter
			w.write(prefix)
			w.write(test.next)
			if got := w.name.String(); got != test.want || w.elided != test.elided {
				t.Errorf("name = %d bytes, elided %t, want %d bytes, elided %t", len(got), w.elided, len(test.want), test.elided)
			}
		})
	}
}

func TestFailureLogTruncatesTheStack(t *testing.T) {
	t.Parallel()

	depths := map[string]int{
		"within the frame limit": 200,
		"beyond the frame limit": 1_000,
	}
	for name, depth := range depths {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			recorder := &logRecorder{}
			state := guard.New(failureLog{logger: slog.New(recorder)})
			state.Run(guard.StagePreflight, func() error {
				panicAtDepth(depth)
				return nil
			})

			records := recorder.snapshot()
			if len(records) != 1 {
				t.Fatalf("records = %d, want 1", len(records))
			}
			stack, _ := attrValue(records[0], "stack")
			trace := stack.String()

			// WithLogger documents the 8 KiB limit. Whole frames fill most
			// of it.
			if got := len(trace); got > 8<<10 || got < 7<<10 {
				t.Errorf("stack length = %d, want 7 KiB to 8 KiB", got)
			}
			frames, elided := strings.CutSuffix(trace, elidedFrames)
			if !elided {
				t.Error("truncated stack lacks the elision line")
			}
			lines := strings.Split(strings.TrimSuffix(frames, "\n"), "\n")
			if len(lines)%2 != 0 {
				t.Fatalf("truncated stack has %d lines, want whole frames", len(lines))
			}
			for index := 0; index < len(lines); index += 2 {
				if !strings.HasSuffix(lines[index], "(...)") || !strings.HasPrefix(lines[index+1], "\t") {
					t.Fatalf("frame %d = %q, %q, want a function and a source line", index/2, lines[index], lines[index+1])
				}
			}
			if lines[0] != panicFunction+"(...)" || lines[2] != "github.com/raj1kshtz/curo.panicAtDepth(...)" {
				t.Errorf("truncated stack starts %q, %q, want the panic and the panicking frame", lines[0], lines[2])
			}
		})
	}
}

func TestFailureLogReportsSelfDisableOnceAndLimitsRecords(t *testing.T) {
	t.Parallel()

	transport, recorder := newLoggedTransport(t, slog.LevelWarn)
	failPreflight(t, transport, 3)

	transport.reports = func() observe.Report {
		panic("report failure")
	}
	for range 4 {
		_ = transport.Report()
	}

	records := recorder.snapshot()
	if len(records) != 4 {
		t.Fatalf("records = %d, want 3 failures and 1 self-disable", len(records))
	}
	for index, record := range records[:3] {
		failures, _ := attrValue(record, "failures")
		if record.Level != slog.LevelWarn ||
			record.Message != failureMessage ||
			failures.Uint64() != uint64(index+1) {
			t.Errorf("record %d = %v %q failures=%v, want a failure", index, record.Level, record.Message, failures)
		}
	}

	disabled := records[3]
	failures, _ := attrValue(disabled, "failures")
	if disabled.Level != slog.LevelError ||
		disabled.Message != disabledMessage ||
		!slices.Equal(attrKeys(disabled), []string{"failures"}) ||
		failures.Uint64() != 3 {
		t.Errorf(
			"self-disable record = %v %q %v failures=%v, want ERROR %q with 3 failures",
			disabled.Level,
			disabled.Message,
			attrKeys(disabled),
			failures,
			disabledMessage,
		)
	}

	stats := transport.Stats()
	if stats.InternalFailures != 7 || !stats.SelfDisabled {
		t.Errorf("Stats() = %#v, want 7 failures and self-disable", stats)
	}
}

func TestFailureLogHonorsTheLoggerLevel(t *testing.T) {
	t.Parallel()

	transport, recorder := newLoggedTransport(t, slog.LevelError)
	failPreflight(t, transport, 3)

	records := recorder.snapshot()
	if len(records) != 1 ||
		records[0].Level != slog.LevelError ||
		records[0].Message != disabledMessage {
		t.Errorf("records = %v, want only the self-disable record", records)
	}
}

func TestFailureLogAllocatesNothingBelowWarn(t *testing.T) {
	if raceEnabled || testing.CoverMode() != "" {
		t.Skip("allocation counts need a build without the race detector or coverage")
	}

	reporter := failureLog{logger: slog.New(&logRecorder{level: slog.LevelError})}
	failure := guard.Failure{Recovered: "failure", Failures: 1, Stage: guard.StagePreflight}
	allocs := testing.AllocsPerRun(allocationRuns, func() {
		reporter.Failure(failure)
	})
	if allocs != 0 {
		t.Errorf("Failure() allocates %v times below Warn, want 0", allocs)
	}
}

func TestFailureLogReportsSkippedFailures(t *testing.T) {
	t.Parallel()

	recorder := &logRecorder{}
	failureLog{logger: slog.New(recorder)}.Failure(guard.Failure{
		Err:      errors.New("body failure"),
		Failures: 9,
		Skipped:  4,
		Stage:    guard.StageBody,
	})

	records := recorder.snapshot()
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	wantKeys := []string{"stage", "kind", "type", "failures", "skipped"}
	if got := attrKeys(records[0]); !slices.Equal(got, wantKeys) {
		t.Errorf("attribute keys = %v, want %v", got, wantKeys)
	}
	failures, _ := attrValue(records[0], "failures")
	skipped, _ := attrValue(records[0], "skipped")
	if failures.Uint64() != 9 || skipped.Uint64() != 4 {
		t.Errorf("failures, skipped = %v, %v, want 9, 4", failures, skipped)
	}
}

func TestFailureLogContainsHandlerPanics(t *testing.T) {
	t.Parallel()

	handlers := map[string]slog.Handler{
		"Enabled": panickingHandler{enabled: true},
		"Handle":  panickingHandler{},
	}
	for name, handler := range handlers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			transport, err := New(
				noContentRoundTripper(),
				WithMode(Enforce),
				WithLogger(slog.New(handler)),
			)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			failPreflight(t, transport, 3)

			stats := transport.Stats()
			if stats.InternalFailures != 3 || !stats.SelfDisabled {
				t.Errorf("Stats() = %#v, want 3 failures and self-disable", stats)
			}
		})
	}
}

func TestFailureLogDoesNotReenterTheHandler(t *testing.T) {
	t.Parallel()

	handler := &lockingHandler{}
	transport, err := New(
		noContentRoundTripper(),
		WithMode(Enforce),
		WithLogger(slog.New(handler)),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	handler.transport = transport
	transport.reports = func() observe.Report {
		panic("report failure")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = transport.Report()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Report() deadlocked in a handler that calls the Transport")
	}

	if got := len(handler.snapshot()); got != 1 {
		t.Errorf("records = %d, want 1 for the failure that the handler did not cause", got)
	}
	if got := transport.Stats().InternalFailures; got != 2 {
		t.Errorf("InternalFailures = %d, want 2", got)
	}
}

func TestFailureReporterWithoutLoggerIsNil(t *testing.T) {
	t.Parallel()

	if reporter := failureReporter(nil); reporter != nil {
		t.Errorf("failureReporter(nil) = %#v, want nil", reporter)
	}
}

// newLoggedTransport returns an Enforce Transport that logs records at level
// and above to the returned recorder.
func newLoggedTransport(t *testing.T, level slog.Level) (*Transport, *logRecorder) {
	t.Helper()

	recorder := &logRecorder{level: level}
	transport, err := New(
		noContentRoundTripper(),
		WithMode(Enforce),
		WithLogger(slog.New(recorder)),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	return transport, recorder
}

func noContentRoundTripper() http.RoundTripper {
	return internalRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
	})
}

func newLoggerRequest(t *testing.T) *http.Request {
	t.Helper()

	return newInternalReportRequest(t, "http://logger.test/")
}

// failPreflight sends requests whose preflight panics.
func failPreflight(t *testing.T, transport *Transport, requests int) {
	t.Helper()

	transport.stages = testRequestStages{
		before: func(requestSnapshot, Mode) error {
			panic("preflight failure")
		},
		after: func(attemptResult) error {
			return nil
		},
	}
	for range requests {
		roundTripInternal(t, transport, newLoggerRequest(t))
	}
}

func panicWith(value any) {
	panic(value)
}

// secretNumber stands for application data that a failure record must not
// hold.
const secretNumber = 0x5EC7E7

// panicAfterCall keeps value live across a call, so a Go stack trace would
// print it as an argument.
//
//go:noinline
func panicAfterCall(value int) {
	pause()
	if value != 0 {
		panic("argument failure")
	}
}

//go:noinline
func pause() {}

// panicWhilePanicking panics again in a deferred call.
func panicWhilePanicking() {
	defer func() {
		panic("second failure")
	}()
	panic("first failure")
}

func elementAt(values []int, index int) int {
	return values[index]
}

func dereference(value *int) int {
	return *value
}

func assignEntry(values map[string]int) {
	values["key"] = 1
}

func assertInt(value any) int {
	return value.(int)
}

// builtValue returns a value of a struct type built at run time, whose field
// tag holds application data. Its field is a slice, so the type is not
// comparable.
func builtValue() any {
	fields := []reflect.StructField{{
		Name: "Value",
		Type: reflect.TypeFor[[]string](),
		Tag:  `json:"secret value"`,
	}}

	return reflect.Zero(reflect.StructOf(fields)).Interface()
}

// builtArray returns a nil pointer to an array type built at run time, whose
// length holds application data.
func builtArray() any {
	array := reflect.ArrayOf(secretNumber, reflect.TypeFor[byte]())

	return reflect.Zero(reflect.PointerTo(array)).Interface()
}

// deepType returns a type of levels nested slices, as package reflect can
// build without limit.
func deepType(levels int) reflect.Type {
	typ := reflect.TypeFor[byte]()
	for range levels {
		typ = reflect.SliceOf(typ)
	}

	return typ
}

// wideType returns a map type whose name more than doubles in length with
// each level. Its deepest type is nested 2*levels levels deep.
func wideType(levels int) reflect.Type {
	typ := reflect.TypeFor[string]()
	for range levels {
		typ = reflect.MapOf(reflect.PointerTo(typ), typ)
	}

	return typ
}

func addKey(key any) int {
	keys := map[any]bool{}
	keys[key] = true

	return len(keys)
}

func divide(dividend, divisor int) int {
	return dividend / divisor
}

func closeChannel(channel chan int) {
	close(channel)
}

func panicAtDepth(depth int) {
	if depth == 0 {
		panic("deep failure")
	}
	panicAtDepth(depth - 1)
}

// applicationRuntimeError is an application type that implements
// runtime.Error. Its message is application data.
type applicationRuntimeError struct{}

func (applicationRuntimeError) Error() string {
	return "secret value"
}

func (applicationRuntimeError) RuntimeError() {}

// countingRuntimeError is an application type that implements runtime.Error
// with the message of a runtime error. It counts the calls to its Error
// method.
type countingRuntimeError struct {
	calls atomic.Int32
}

func (e *countingRuntimeError) Error() string {
	e.calls.Add(1)
	return "close of closed channel"
}

func (*countingRuntimeError) RuntimeError() {}

// panickingConfirmStages fails the final check before a retry.
type panickingConfirmStages struct {
	testRequestStages
}

func (panickingConfirmStages) confirmRetry(requestState) bool {
	panic("confirm failure")
}

// logRecorder is a slog.Handler that keeps the records it handles.
type logRecorder struct {
	records []slog.Record
	mu      sync.Mutex
	level   slog.Level
}

func (r *logRecorder) Enabled(_ context.Context, level slog.Level) bool {
	return level >= r.level
}

func (r *logRecorder) Handle(_ context.Context, record slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.records = append(r.records, record.Clone())
	return nil
}

func (r *logRecorder) WithAttrs([]slog.Attr) slog.Handler {
	return r
}

func (r *logRecorder) WithGroup(string) slog.Handler {
	return r
}

func (r *logRecorder) snapshot() []slog.Record {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.records)
}

// lockingHandler is a slog.Handler that calls its Transport while it holds its
// lock, as a handler that adds the Transport's state to records might.
type lockingHandler struct {
	transport *Transport
	records   []slog.Record
	mu        sync.Mutex
}

func (*lockingHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *lockingHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	_ = h.transport.Report()
	h.records = append(h.records, record.Clone())
	return nil
}

func (h *lockingHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *lockingHandler) WithGroup(string) slog.Handler {
	return h
}

func (h *lockingHandler) snapshot() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()

	return slices.Clone(h.records)
}

// panickingHandler is a slog.Handler whose Enabled or Handle method panics.
type panickingHandler struct {
	enabled bool
}

func (h panickingHandler) Enabled(context.Context, slog.Level) bool {
	if h.enabled {
		panic("enabled failure")
	}

	return true
}

func (panickingHandler) Handle(context.Context, slog.Record) error {
	panic("handle failure")
}

func (h panickingHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h panickingHandler) WithGroup(string) slog.Handler {
	return h
}

func attrKeys(record slog.Record) []string {
	keys := make([]string, 0, record.NumAttrs())
	record.Attrs(func(attr slog.Attr) bool {
		keys = append(keys, attr.Key)
		return true
	})

	return keys
}

func attrValue(record slog.Record, key string) (slog.Value, bool) {
	var (
		value slog.Value
		found bool
	)
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key != key {
			return true
		}

		value, found = attr.Value, true
		return false
	})

	return value, found
}
