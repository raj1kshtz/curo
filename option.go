package curo

import (
	"fmt"
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
