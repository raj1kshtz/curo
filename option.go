package curo

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/raj1kshtz/curo/internal/timeout"
)

// Option configures a Transport during construction.
//
// Options are created by this package's With functions.
type Option interface {
	apply(*config) error
}

type config struct {
	logger   *slog.Logger
	timeouts timeout.Bounds
	mode     Mode
}

type optionFunc func(*config) error

func (f optionFunc) apply(cfg *config) error {
	return f(cfg)
}

// WithMode sets the initial operating mode. The default is Observe.
func WithMode(mode Mode) Option {
	return optionFunc(func(cfg *config) error {
		if !mode.valid() {
			return invalidModeError(mode)
		}

		cfg.mode = mode
		return nil
	})
}

// WithTimeoutBounds sets the floor and ceiling of adaptive timeouts. The
// default floor is 2 seconds and the default ceiling is 30 seconds.
//
// In Enforce mode, a read request to a target with enough latency evidence
// must receive response headers within the target's adaptive timeout: three
// times its slowest retained latency, clamped to these bounds. Reading the
// response body is not bounded. Set the ceiling above the longest time to
// response headers that a read may legitimately take, because no read may
// wait longer. Also set it below the deadlines that callers put on those
// reads: a read is timed only when its deadline leaves more time than its
// timeout, and only a timeout at the ceiling counts as a dependency failure.
// A read that its caller's deadline ends is never a dependency failure, even
// when the dependency hangs.
//
// Passing zero for both disables adaptive timeouts. Otherwise minimum must be
// positive and must not exceed maximum.
func WithTimeoutBounds(minimum, maximum time.Duration) Option {
	return optionFunc(func(cfg *config) error {
		bounds := timeout.Bounds{Minimum: minimum, Maximum: maximum}
		if bounds != (timeout.Bounds{}) && !bounds.Enabled() {
			return fmt.Errorf(
				"invalid timeout bounds %v to %v: want 0 < minimum <= maximum, or both zero",
				minimum,
				maximum,
			)
		}

		cfg.timeouts = bounds
		return nil
	})
}

// WithLogger logs Curo's internal failures to logger. By default, Curo logs
// nothing.
//
// Curo contains a panic or an error in its own work, including application
// code that the work calls, such as a body's Close method or an error's Unwrap
// method. It offers each failure to logger as a Warn record with these
// attributes:
//
//   - stage: the work that failed: preflight, postflight, retry, timeout,
//     body, or report
//   - kind: panic, or error when the work returned an error
//   - type: the Go type of the panic value or error. Types that package
//     reflect builds at run time can hold application data, so an unnamed
//     struct, function, or channel type is written as struct, func, or chan,
//     and an array type as [...]T, without its length. A name is cut after
//     16 levels of nested types or at 512 bytes, and then ends with "...".
//   - error: the message of a Go runtime error whose message is fixed text,
//     such as a nil pointer dereference. Other messages can hold application
//     data, as an index out of range error holds the index and a failed type
//     assertion names types, so they are omitted.
//   - failures: the failure's number, from the count that [Stats] reports as
//     InternalFailures
//   - skipped: the number of failures since the previous failure record that
//     were not offered to logger, present only when some were
//   - stack: for a panic, the functions and source lines of the goroutine
//     that panicked, from the panic outward, without argument values, and
//     truncated to whole frames within 8 KiB
//
// When three internal failures within a minute disable the Transport, it
// offers that once as an Error record with a failures attribute.
//
// A Transport offers one record at a time, and at most three failure records
// a minute. A failure that happens while a record is offered is skipped,
// including one that the handler causes by calling the Transport, so the
// handler is never called reentrantly. The per-minute limit skips no failure
// before self-disable, only failures in the work that continues afterwards:
// settling the timeouts of requests in progress, closing bodies, and building
// reports.
//
// Records are offered synchronously, outside Curo's locks, with
// [context.Background]. A failure record is offered on the goroutine where the
// failure happened, and the self-disable record follows a failure record on
// the same goroutine. The logger's level and handler decide which records are
// written. A panic from the handler is recovered and ignored, and does not
// count as an internal failure.
//
// A nil logger is rejected.
func WithLogger(logger *slog.Logger) Option {
	return optionFunc(func(cfg *config) error {
		if logger == nil {
			return errors.New("nil logger")
		}

		cfg.logger = logger
		return nil
	})
}

func applyOptions(options []Option) (config, error) {
	cfg := config{mode: Observe, timeouts: timeout.DefaultBounds}

	for index, option := range options {
		if option == nil {
			return config{}, fmt.Errorf("curo: option %d is nil", index+1)
		}
		if err := option.apply(&cfg); err != nil {
			return config{}, fmt.Errorf("curo: apply option %d: %w", index+1, err)
		}
	}

	return cfg, nil
}

func invalidModeError(mode Mode) error {
	return fmt.Errorf("%w: %d", ErrInvalidMode, mode)
}
