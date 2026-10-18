package curogrpc

import (
	"errors"
	"fmt"
	"slices"
)

const (
	// maxReadMethods bounds the methods that an interceptor lists.
	maxReadMethods = 1024

	// maxMethodBytes bounds the length of a listed method name.
	maxMethodBytes = 1024
)

// Option configures an interceptor during construction.
//
// Options are created by this package's With functions.
type Option interface {
	apply(*config) error
}

type config struct {
	reads map[string]struct{}
}

type optionFunc func(*config) error

func (f optionFunc) apply(cfg *config) error {
	return f(cfg)
}

// WithReadMethods lists methods whose calls are read-only, in addition to the
// methods whose proto declares that they have no side effects. A name is a
// full method name, as grpc-go passes it to interceptors, such as
// "/inventory.v1.Inventory/GetItem". An interceptor lists at most 1024
// distinct names of at most 1024 bytes each.
//
// List only methods that change nothing at the server. In Enforce mode, an
// adaptive timeout may end a read-only call, which grpc-go reports to the
// server as a cancellation.
func WithReadMethods(methods ...string) Option {
	methods = slices.Clone(methods)

	return optionFunc(func(cfg *config) error {
		for _, method := range methods {
			if len(method) > maxMethodBytes {
				return fmt.Errorf(
					"read method of %d bytes exceeds %d bytes",
					len(method),
					maxMethodBytes,
				)
			}
			if _, _, ok := splitMethod(method); !ok {
				return fmt.Errorf(
					"read method %q is not of the form /service/method",
					method,
				)
			}

			if cfg.reads == nil {
				cfg.reads = make(map[string]struct{})
			}
			cfg.reads[method] = struct{}{}
			if len(cfg.reads) > maxReadMethods {
				return errors.New("more than 1024 read methods")
			}
		}

		return nil
	})
}

func applyOptions(options []Option) (config, error) {
	var cfg config

	for index, option := range options {
		if option == nil {
			return config{}, fmt.Errorf("curogrpc: option %d is nil", index+1)
		}
		if err := option.apply(&cfg); err != nil {
			return config{}, fmt.Errorf(
				"curogrpc: apply option %d: %w",
				index+1,
				err,
			)
		}
	}

	return cfg, nil
}
