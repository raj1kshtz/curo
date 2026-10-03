package curo_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/raj1kshtz/curo"
)

func TestWithTimeoutBoundsValidatesBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		minimum time.Duration
		maximum time.Duration
		valid   bool
	}{
		{valid: true},
		{minimum: time.Second, maximum: time.Second, valid: true},
		{minimum: time.Nanosecond, maximum: math.MaxInt64, valid: true},
		{minimum: 0, maximum: time.Second},
		{minimum: time.Second, maximum: 0},
		{minimum: -time.Second, maximum: time.Second},
		{minimum: -time.Second, maximum: -time.Second},
		{minimum: 2 * time.Second, maximum: time.Second},
	}

	for _, test := range tests {
		transport, err := curo.New(
			roundTripperFunc(nil),
			curo.WithTimeoutBounds(test.minimum, test.maximum),
		)
		if test.valid {
			if err != nil || transport == nil {
				t.Errorf(
					"New(WithTimeoutBounds(%v, %v)) = %v, %v, want a Transport",
					test.minimum,
					test.maximum,
					transport,
					err,
				)
			}
			continue
		}
		if transport != nil ||
			err == nil ||
			!strings.Contains(err.Error(), "curo: apply option 1: invalid timeout bounds") {
			t.Errorf(
				"New(WithTimeoutBounds(%v, %v)) = %v, %v, want an invalid bounds error",
				test.minimum,
				test.maximum,
				transport,
				err,
			)
		}
	}
}

func TestErrTimeoutIsADeadlineTimeout(t *testing.T) {
	t.Parallel()

	if got := curo.ErrTimeout.Error(); got != "curo: adaptive timeout awaiting response headers" {
		t.Errorf("Error() = %q", got)
	}
	if !errors.Is(curo.ErrTimeout, context.DeadlineExceeded) {
		t.Error("ErrTimeout does not match context.DeadlineExceeded")
	}
	if errors.Is(curo.ErrTimeout, context.Canceled) {
		t.Error("ErrTimeout matches context.Canceled")
	}
	if !errors.Is(fmt.Errorf("get: %w", curo.ErrTimeout), curo.ErrTimeout) {
		t.Error("wrapped ErrTimeout does not match ErrTimeout")
	}

	var netErr net.Error
	if !errors.As(curo.ErrTimeout, &netErr) || !netErr.Timeout() {
		t.Error("ErrTimeout is not a net.Error timeout")
	}
	var temporary interface{ Temporary() bool }
	if !errors.As(curo.ErrTimeout, &temporary) || !temporary.Temporary() {
		t.Error("ErrTimeout is not temporary")
	}
}
