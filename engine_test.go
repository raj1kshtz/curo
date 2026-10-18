package curo_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/raj1kshtz/curo"
)

var errEngineTestAttempt = errors.New("attempt failed")

func inventoryCall() curo.Call {
	return curo.Call{
		Scheme:     "grpc",
		Host:       "inventory.internal",
		Port:       443,
		ReadOnly:   true,
		ReplaySafe: true,
	}
}

func TestNewEngineDefaultsToObserve(t *testing.T) {
	t.Parallel()

	engine, err := curo.NewEngine()
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if got := engine.Mode(); got != curo.Observe {
		t.Errorf("Mode() = %v, want Observe", got)
	}
}

func TestNewEngineRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	engine, err := curo.NewEngine(curo.WithMode(curo.Mode(99)))
	if !errors.Is(err, curo.ErrInvalidMode) {
		t.Fatalf("NewEngine() error = %v, want ErrInvalidMode", err)
	}
	if engine != nil {
		t.Fatalf("NewEngine() engine = %v, want nil", engine)
	}
}

func TestEngineSetMode(t *testing.T) {
	t.Parallel()

	engine, err := curo.NewEngine(curo.WithMode(curo.Enforce))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	for _, mode := range []curo.Mode{curo.Off, curo.Observe, curo.Enforce} {
		if setErr := engine.SetMode(mode); setErr != nil {
			t.Fatalf("SetMode(%v) error = %v", mode, setErr)
		}
		if got := engine.Mode(); got != mode {
			t.Errorf("Mode() = %v, want %v", got, mode)
		}
	}

	if setErr := engine.SetMode(curo.Mode(99)); !errors.Is(setErr, curo.ErrInvalidMode) {
		t.Errorf("SetMode(invalid) error = %v, want ErrInvalidMode", setErr)
	}
	if got := engine.Mode(); got != curo.Enforce {
		t.Errorf("Mode() after invalid SetMode = %v, want Enforce", got)
	}
}

func TestEngineCloseIsIdempotentAndKeepsRunningAttempts(t *testing.T) {
	t.Parallel()

	engine, err := curo.NewEngine(curo.WithMode(curo.Enforce))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	ctx := context.Background()
	if doErr := engine.Do(ctx, inventoryCall(), func(context.Context) error {
		return nil
	}); doErr != nil {
		t.Fatalf("Do() error = %v", doErr)
	}

	for range 2 {
		if closeErr := engine.Close(); closeErr != nil {
			t.Fatalf("Close() error = %v", closeErr)
		}
	}
	for _, mode := range []curo.Mode{curo.Off, curo.Mode(99)} {
		if setErr := engine.SetMode(mode); !errors.Is(setErr, curo.ErrClosed) {
			t.Errorf("SetMode(%d) after Close error = %v, want ErrClosed", mode, setErr)
		}
	}
	if got := engine.Mode(); got != curo.Enforce {
		t.Errorf("Mode() after Close = %v, want Enforce", got)
	}

	var attempts []context.Context
	doErr := engine.Do(ctx, inventoryCall(), func(attempt context.Context) error {
		attempts = append(attempts, attempt)
		return errEngineTestAttempt
	})
	if !errors.Is(doErr, errEngineTestAttempt) {
		t.Errorf("Do() after Close error = %v, want the attempt's error", doErr)
	}
	if len(attempts) != 1 || attempts[0] != ctx {
		t.Errorf("attempts after Close = %d, want one with the caller's context", len(attempts))
	}

	// Close keeps the observations made before it.
	if got := engine.Stats().ObservedRequests; got != 1 {
		t.Errorf("ObservedRequests after Close = %d, want 1", got)
	}
	if got := len(engine.Report().Targets); got != 1 {
		t.Errorf("Report() targets after Close = %d, want 1", got)
	}
}

func TestUninitializedEngineRunsAttemptsDirectly(t *testing.T) {
	t.Parallel()

	engines := map[string]*curo.Engine{"nil": nil, "zero": {}}
	for name, engine := range engines {
		if got := engine.Mode(); got != curo.Off {
			t.Errorf("%s Mode() = %v, want Off", name, got)
		}
		if err := engine.SetMode(curo.Observe); !errors.Is(err, curo.ErrUninitializedEngine) {
			t.Errorf("%s SetMode() error = %v, want ErrUninitializedEngine", name, err)
		}
		if err := engine.Close(); !errors.Is(err, curo.ErrUninitializedEngine) {
			t.Errorf("%s Close() error = %v, want ErrUninitializedEngine", name, err)
		}

		attempts := 0
		err := engine.Do(context.Background(), inventoryCall(), func(context.Context) error {
			attempts++
			return errEngineTestAttempt
		})
		if !errors.Is(err, errEngineTestAttempt) || attempts != 1 {
			t.Errorf("%s Do() = %v after %d attempts, want the attempt's error after 1", name, err, attempts)
		}
		if report := engine.Report(); len(report.Targets) != 0 || len(report.Changes) != 0 {
			t.Errorf("%s Report() = %+v, want an empty report", name, report)
		}
		if stats := engine.Stats(); stats != (curo.Stats{}) {
			t.Errorf("%s Stats() = %+v, want zero Stats", name, stats)
		}
	}
}

// TestOutcomeValuesHaveStableNames pins the name and number of each Outcome,
// which a Classify function returns.
func TestOutcomeValuesHaveStableNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want   string
		number uint64
		value  curo.Outcome
	}{
		{value: curo.OutcomeUnspecified, number: 0, want: "Unspecified"},
		{value: curo.OutcomeSuccess, number: 1, want: "Success"},
		{value: curo.OutcomeClientFailure, number: 2, want: "ClientFailure"},
		{value: curo.OutcomeRateLimited, number: 3, want: "RateLimited"},
		{value: curo.OutcomeDependencyFailure, number: 4, want: "DependencyFailure"},
		{value: curo.OutcomeTransportFailure, number: 5, want: "TransportFailure"},
		{value: curo.Outcome(9), number: 9, want: "Outcome(9)"},
	}

	for _, test := range tests {
		if got, want := fmt.Sprintf("%d", test.value), fmt.Sprint(test.number); got != want {
			t.Errorf("Outcome %s = %s, want %s", test.want, got, want)
		}
		if got := test.value.String(); got != test.want {
			t.Errorf("Outcome(%d).String() = %q, want %q", test.number, got, test.want)
		}
	}
}
