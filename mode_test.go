package curo_test

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
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
		t.Run(mode.String(), func(t *testing.T) {
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

// TestModeHasStableNames pins each mode's name and number, which ADR-0010
// makes compatibility promises.
func TestModeHasStableNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want   string
		mode   curo.Mode
		number uint32
	}{
		{mode: curo.Off, number: 0, want: "Off"},
		{mode: curo.Observe, number: 1, want: "Observe"},
		{mode: curo.Enforce, number: 2, want: "Enforce"},
	}

	for _, test := range tests {
		if uint32(test.mode) != test.number {
			t.Errorf("%s = %d, want %d", test.want, test.mode, test.number)
		}
		if got := test.mode.String(); got != test.want {
			t.Errorf("Mode(%d).String() = %q, want %q", test.mode, got, test.want)
		}
		text, err := test.mode.MarshalText()
		if err != nil || string(text) != test.want {
			t.Errorf("Mode(%d).MarshalText() = %q, %v, want %q, nil",
				test.mode, text, err, test.want)
		}
	}
}

func TestInvalidModeHasNoName(t *testing.T) {
	t.Parallel()

	invalid := curo.Mode(99)
	if got := invalid.String(); got != "Mode(99)" {
		t.Errorf("String() = %q, want %q", got, "Mode(99)")
	}
	text, err := invalid.MarshalText()
	if !errors.Is(err, curo.ErrInvalidMode) {
		t.Errorf("MarshalText() error = %v, want ErrInvalidMode", err)
	}
	if text != nil {
		t.Errorf("MarshalText() text = %q, want nil", text)
	}
}

func TestModeUnmarshalTextIgnoresCase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		text string
		want curo.Mode
	}{
		{text: "off", want: curo.Off},
		{text: "Off", want: curo.Off},
		{text: "observe", want: curo.Observe},
		{text: "OBSERVE", want: curo.Observe},
		{text: "Enforce", want: curo.Enforce},
		{text: "eNfOrCe", want: curo.Enforce},
	}

	for _, test := range tests {
		mode := curo.Mode(99)
		if err := mode.UnmarshalText([]byte(test.text)); err != nil {
			t.Errorf("UnmarshalText(%q) error = %v", test.text, err)
		}
		if mode != test.want {
			t.Errorf("UnmarshalText(%q) mode = %v, want %v", test.text, mode, test.want)
		}
	}
}

func TestModeUnmarshalTextRejectsOtherText(t *testing.T) {
	t.Parallel()

	for _, text := range []string{"", " enforce", "enforced", "on", "2", "Mode(2)"} {
		mode := curo.Enforce
		err := mode.UnmarshalText([]byte(text))
		if !errors.Is(err, curo.ErrInvalidMode) {
			t.Errorf("UnmarshalText(%q) error = %v, want ErrInvalidMode", text, err)
		}
		if mode != curo.Enforce {
			t.Errorf("UnmarshalText(%q) changed the mode to %v", text, mode)
		}
	}
}

func TestNilModeUnmarshalTextReturnsError(t *testing.T) {
	t.Parallel()

	var mode *curo.Mode
	if err := mode.UnmarshalText([]byte("Off")); !errors.Is(err, curo.ErrInvalidMode) {
		t.Errorf("nil UnmarshalText() error = %v, want ErrInvalidMode", err)
	}
}

func TestModeWorksWithFlags(t *testing.T) {
	t.Parallel()

	flags := flag.NewFlagSet("service", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var mode curo.Mode
	flags.TextVar(&mode, "curo-mode", curo.Observe, "Curo operating mode")
	if mode != curo.Observe {
		t.Errorf("default mode = %v, want Observe", mode)
	}

	if err := flags.Parse([]string{"-curo-mode=enforce"}); err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if mode != curo.Enforce {
		t.Errorf("parsed mode = %v, want Enforce", mode)
	}
	if err := flags.Parse([]string{"-curo-mode=on"}); err == nil {
		t.Error("Parse(-curo-mode=on) error = nil, want an error")
	}
	if mode != curo.Enforce {
		t.Errorf("mode after a rejected value = %v, want Enforce", mode)
	}
}

func TestModeEncodesAsNameInJSON(t *testing.T) {
	t.Parallel()

	type config struct {
		Mode curo.Mode
	}

	data, err := json.Marshal(config{Mode: curo.Enforce})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if got, want := string(data), `{"Mode":"Enforce"}`; got != want {
		t.Errorf("Marshal() = %s, want %s", got, want)
	}

	var decoded config
	err = json.Unmarshal([]byte(`{"Mode":"observe"}`), &decoded)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if decoded.Mode != curo.Observe {
		t.Errorf("decoded Mode = %v, want Observe", decoded.Mode)
	}

	_, err = json.Marshal(config{Mode: curo.Mode(99)})
	if !errors.Is(err, curo.ErrInvalidMode) {
		t.Errorf("Marshal(Mode(99)) error = %v, want ErrInvalidMode", err)
	}
	err = json.Unmarshal([]byte(`{"Mode":"on"}`), &decoded)
	if !errors.Is(err, curo.ErrInvalidMode) {
		t.Errorf("Unmarshal(on) error = %v, want ErrInvalidMode", err)
	}
	if decoded.Mode != curo.Observe {
		t.Errorf("Mode after a rejected value = %v, want Observe", decoded.Mode)
	}
}
