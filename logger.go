package curo

import (
	"context"
	"log/slog"
	"reflect"
	"runtime"
	"strconv"
	"strings"

	"github.com/raj1kshtz/curo/internal/guard"
)

const (
	failureMessage  = "curo contained an internal failure"
	disabledMessage = "curo disabled itself after repeated internal failures"

	// maxFailureStack is the size limit, in bytes, of a logged stack trace.
	maxFailureStack = 8 << 10

	// maxFailureFrames limits the frames that a stack trace is taken from. It
	// holds more frames than fit in maxFailureStack.
	maxFailureFrames = 512

	// elidedFrames ends a truncated stack trace, in the Go runtime's words.
	elidedFrames = "...additional frames elided...\n"

	// panicFunction runs the deferred calls of a panic, so a stack trace
	// taken in one of them holds it between the frames of the deferred call
	// and the frames that panicked.
	panicFunction = "runtime.gopanic"

	// maxTypeName is the size limit, in bytes, of a logged type name before
	// elidedType.
	maxTypeName = 512

	// maxTypeDepth is the number of levels of nested types that a logged type
	// name spells out.
	maxTypeDepth = 16

	// elidedType ends a type name that was cut.
	elidedType = "..."
)

// failureLog writes contained failures and self-disable to a logger, as
// WithLogger describes.
type failureLog struct {
	logger *slog.Logger
}

// failureReporter returns a reporter that logs to logger, or nil when logger
// is nil, so the guard reports nothing.
func failureReporter(logger *slog.Logger) guard.Reporter {
	if logger == nil {
		return nil
	}

	return failureLog{logger: logger}
}

// Failure logs failure at Warn level. After a panic, the guard calls it from
// the deferred call that recovered the panic, so the stack trace still
// includes the panicking frames.
func (l failureLog) Failure(failure guard.Failure) {
	ctx := context.Background()
	if !l.logger.Enabled(ctx, slog.LevelWarn) {
		return
	}

	kind, value := "panic", failure.Recovered
	if value == nil {
		kind, value = "error", failure.Err
	}

	attrs := make([]slog.Attr, 0, 7)
	attrs = append(
		attrs,
		slog.String("stage", failure.Stage.String()),
		slog.String("kind", kind),
		slog.String("type", typeName(reflect.TypeOf(value))),
	)
	if message, ok := runtimeErrorMessage(value); ok {
		attrs = append(attrs, slog.String("error", message))
	}
	attrs = append(attrs, slog.Uint64("failures", failure.Failures))
	if failure.Skipped > 0 {
		attrs = append(attrs, slog.Uint64("skipped", failure.Skipped))
	}
	if failure.Recovered != nil {
		attrs = append(attrs, slog.String("stack", failureStack()))
	}

	l.logger.LogAttrs(ctx, slog.LevelWarn, failureMessage, attrs...)
}

// Disabled logs self-disable at Error level.
func (l failureLog) Disabled(failures uint64) {
	l.logger.LogAttrs(
		context.Background(),
		slog.LevelError,
		disabledMessage,
		slog.Uint64("failures", failures),
	)
}

// typeName returns the name of t in Go syntax, without the parts of a type
// that package reflect can build at run time, which can hold application
// data: the fields of a struct, such as their names and tags, the signature
// of a function, the element of a channel, and the length of an array. So an
// unnamed struct, function, or channel type is named by its kind, and an
// array type is written [...]T. Only source code declares named types and
// interface types, so their names are written as Go prints them.
//
// Package reflect can also nest types without limit, so the name is cut
// after maxTypeDepth levels or at maxTypeName bytes, and then ends with
// elidedType. The bounds keep the work and the result small.
func typeName(t reflect.Type) string {
	var w typeNameWriter
	w.writeType(t, 0)
	if w.elided {
		w.name.WriteString(elidedType)
	}

	return w.name.String()
}

// typeNameWriter builds a type name of up to maxTypeName bytes, which
// typeName ends with elidedType when it was cut.
type typeNameWriter struct {
	name   strings.Builder
	elided bool
}

// writeType appends the name of t, which is nested depth levels deep. A call
// returns at once, writes a byte, or cuts the name, so the work stays within
// the bounds.
func (w *typeNameWriter) writeType(t reflect.Type, depth int) {
	if w.elided {
		return
	}
	if depth == maxTypeDepth {
		w.elided = true
		return
	}
	if t.Name() != "" || t.Kind() == reflect.Interface {
		w.write(t.String())
		return
	}

	switch t.Kind() {
	case reflect.Array:
		w.write("[...]")
		w.writeType(t.Elem(), depth+1)
	case reflect.Map:
		w.write("map[")
		w.writeType(t.Key(), depth+1)
		w.write("]")
		w.writeType(t.Elem(), depth+1)
	case reflect.Pointer:
		w.write("*")
		w.writeType(t.Elem(), depth+1)
	case reflect.Slice:
		w.write("[]")
		w.writeType(t.Elem(), depth+1)
	default:
		w.write(t.Kind().String())
	}
}

// write appends s, or the part of s that fits, after which the name is cut.
func (w *typeNameWriter) write(s string) {
	if w.elided {
		return
	}

	room := maxTypeName - w.name.Len()
	if len(s) > room {
		// The cut can split a rune, whose remains ToValidUTF8 drops.
		w.name.WriteString(strings.ToValidUTF8(s[:room], ""))
		w.elided = true
		return
	}
	w.name.WriteString(s)
}

// runtimeErrorMessage returns the message of a Go runtime error when it is one
// of the fixed messages that runtimeMessage lists, such as for a nil pointer
// dereference. The messages of other panic values and errors can hold
// application data, and so can those of the other runtime errors, which hold
// values or type names: an index out of range error holds the index and the
// length, and a failed type assertion names types that package reflect can
// build. Only the runtime's own Error methods are called.
func runtimeErrorMessage(value any) (string, bool) {
	err, ok := value.(runtime.Error)
	if !ok {
		return "", false
	}

	errType := reflect.TypeOf(err)
	if errType.Kind() == reflect.Pointer {
		errType = errType.Elem()
	}
	if errType.PkgPath() != "runtime" {
		return "", false
	}

	message, ok := errorMessage(err)
	if !ok || !runtimeMessage(message) {
		return "", false
	}

	return message, true
}

// runtimeMessage reports whether message is the fixed message of a runtime
// error from a failed language operation, as the Go releases that Curo
// supports word it. Go 1.27 adds the "runtime error: " prefix to the message
// of a panic with a nil value.
func runtimeMessage(message string) bool {
	switch message {
	case "runtime error: invalid memory address or nil pointer dereference",
		"runtime error: integer divide by zero",
		"runtime error: integer overflow",
		"runtime error: floating point error",
		"runtime error: negative shift amount",
		"runtime error: makeslice: len out of range",
		"runtime error: makeslice: cap out of range",
		"runtime error: growslice: len out of range",
		"runtime error: unsafe.Slice: len out of range",
		"runtime error: unsafe.Slice: ptr is nil and len is not zero",
		"runtime error: unsafe.String: len out of range",
		"runtime error: unsafe.String: ptr is nil and len is not zero",
		"runtime error: range function continued iteration after function for loop body returned false",
		"runtime error: range function continued iteration after loop body panic",
		"runtime error: range function continued iteration after whole loop exit",
		"runtime error: range function recovered a loop body panic and did not resume panicking",
		"assignment to entry in nil map",
		"close of closed channel",
		"close of nil channel",
		"send on closed channel",
		"makechan: size out of range",
		"runtime: allocation size out of range",
		"panic called with nil argument",
		"runtime error: panic called with nil argument":
		return true
	default:
		return false
	}
}

// errorMessage returns err's message, or false if Error panics, as it does for
// a nil or zero *runtime.TypeAssertionError.
func errorMessage(err error) (message string, ok bool) {
	defer func() {
		if recover() != nil {
			message, ok = "", false
		}
	}()

	return err.Error(), true
}

// failureStack returns the calling goroutine's stack trace from the panic
// that it recovered, without the frames that report the failure. Each frame
// is a function and a source line, in the form of a Go panic trace without
// argument values, which can hold application data. The trace is truncated to
// whole frames within maxFailureStack bytes.
func failureStack() string {
	pcs := make([]uintptr, maxFailureFrames)
	count := runtime.Callers(2, pcs)
	truncated := count == len(pcs)

	var trace strings.Builder
	panicked := false
	frames := runtime.CallersFrames(pcs[:count])
	for more := true; more; {
		var frame runtime.Frame
		frame, more = frames.Next()
		if !panicked && frame.Function == panicFunction {
			trace.Reset()
			panicked = true
		}

		line := frame.Function + "(...)\n\t" + frame.File + ":" + strconv.Itoa(frame.Line) + "\n"
		if trace.Len()+len(line) > maxFailureStack-len(elidedFrames) {
			truncated = true
			break
		}
		trace.WriteString(line)
	}
	if truncated {
		trace.WriteString(elidedFrames)
	}

	return trace.String()
}
