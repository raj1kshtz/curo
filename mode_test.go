package curo_test

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/raj1kshtz/curo"
)

func TestNewDefaultsToObserve(t *testing.T) {
	t.Parallel()

	transport, err := curo.New(roundTripperFunc(nil))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if got := transport.Mode(); got != curo.Observe {
		t.Errorf("Mode() = %v, want Observe", got)
	}
}

func TestWithModeSetsInitialMode(t *testing.T) {
	t.Parallel()

	for _, mode := range []curo.Mode{curo.Off, curo.Observe, curo.Enforce} {
		t.Run(fmt.Sprintf("mode_%d", mode), func(t *testing.T) {
			t.Parallel()

			transport, err := curo.New(roundTripperFunc(nil), curo.WithMode(mode))
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if got := transport.Mode(); got != mode {
				t.Errorf("Mode() = %v, want %v", got, mode)
			}
		})
	}
}

func TestWithModeRejectsInvalidMode(t *testing.T) {
	t.Parallel()

	transport, err := curo.New(roundTripperFunc(nil), curo.WithMode(curo.Mode(99)))
	if !errors.Is(err, curo.ErrInvalidMode) {
		t.Fatalf("New() error = %v, want ErrInvalidMode", err)
	}
	if transport != nil {
		t.Fatalf("New() transport = %v, want nil", transport)
	}
}

func TestSetMode(t *testing.T) {
	t.Parallel()

	transport, err := curo.New(roundTripperFunc(nil))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	for _, mode := range []curo.Mode{curo.Off, curo.Enforce, curo.Observe} {
		if err := transport.SetMode(mode); err != nil {
			t.Fatalf("SetMode(%d) error = %v", mode, err)
		}
		if got := transport.Mode(); got != mode {
			t.Errorf("Mode() = %v, want %v", got, mode)
		}
	}
}

func TestSetModeRejectsInvalidModeWithoutChangingState(t *testing.T) {
	t.Parallel()

	transport, err := curo.New(roundTripperFunc(nil), curo.WithMode(curo.Enforce))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	err = transport.SetMode(curo.Mode(99))
	if !errors.Is(err, curo.ErrInvalidMode) {
		t.Fatalf("SetMode() error = %v, want ErrInvalidMode", err)
	}
	if got := transport.Mode(); got != curo.Enforce {
		t.Errorf("Mode() = %v, want Enforce", got)
	}
}

func TestModeIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	transport, err := curo.New(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       http.NoBody,
		}, nil
	}))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	const (
		workers    = 16
		iterations = 100
	)

	var wg sync.WaitGroup
	errorsFound := make(chan error, workers)

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for iteration := 0; iteration < iterations; iteration++ {
				mode := curo.Mode((worker + iteration) % 3)
				if err := transport.SetMode(mode); err != nil {
					errorsFound <- err
					return
				}
				if got := transport.Mode(); got < curo.Off || got > curo.Enforce {
					errorsFound <- fmt.Errorf("Mode() = %d", got)
					return
				}
				response, roundTripErr := transport.RoundTrip(nil)
				if roundTripErr != nil {
					errorsFound <- roundTripErr
					return
				}
				if closeErr := response.Body.Close(); closeErr != nil {
					errorsFound <- closeErr
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errorsFound)

	for err := range errorsFound {
		t.Errorf("concurrent operation error = %v", err)
	}
}
