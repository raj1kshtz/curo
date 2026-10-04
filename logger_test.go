package curo_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/raj1kshtz/curo"
)

func TestWithLoggerRejectsNilLogger(t *testing.T) {
	t.Parallel()

	transport, err := curo.New(roundTripperFunc(nil), curo.WithLogger(nil))
	if transport != nil || err == nil || err.Error() != "curo: apply option 1: nil logger" {
		t.Errorf("New(WithLogger(nil)) = %v, %v, want a nil logger error", transport, err)
	}
}

func TestWithLoggerLogsContainedFailure(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	wantError := &panickingMatcherError{}
	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, wantError
	})
	transport, err := curo.New(
		base,
		curo.WithLogger(slog.New(slog.NewJSONHandler(&output, nil))),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	response, gotError := transport.RoundTrip(nil)
	if response != nil {
		_ = response.Body.Close()
	}
	//nolint:errorlint // Exact identity proves containment preserved the base error.
	if gotError != wantError {
		t.Errorf("RoundTrip() error = %v, want the base transport's error", gotError)
	}

	var record map[string]any
	if decodeErr := json.Unmarshal(output.Bytes(), &record); decodeErr != nil {
		t.Fatalf("log output is not one JSON record: %v\n%s", decodeErr, output.String())
	}
	want := map[string]any{
		"level":    "WARN",
		"msg":      "curo contained an internal failure",
		"stage":    "postflight",
		"kind":     "panic",
		"type":     "string",
		"failures": float64(1),
	}
	for key, value := range want {
		if record[key] != value {
			t.Errorf("%s = %v, want %v", key, record[key], value)
		}
	}
	for _, key := range []string{"error", "skipped"} {
		if value, ok := record[key]; ok {
			t.Errorf("%s = %v, want no such attribute", key, value)
		}
	}
	if stack, _ := record["stack"].(string); !strings.Contains(stack, "(*panickingMatcherError).Is(") {
		t.Errorf("stack lacks the panicking frame:\n%s", stack)
	}
	if strings.Contains(output.String(), "matcher failure") {
		t.Errorf("log output contains the panic message:\n%s", output.String())
	}
}
