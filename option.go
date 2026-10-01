package curo

import "fmt"

// Option configures a Transport during construction.
//
// Options are created by this package's With functions.
type Option interface {
	apply(*config) error
}

type config struct {
	mode Mode
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

func applyOptions(options []Option) (config, error) {
	cfg := config{mode: Observe}

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
