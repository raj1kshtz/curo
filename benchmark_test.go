package curo_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/raj1kshtz/curo"
)

func BenchmarkTransportRoundTrip(b *testing.B) {
	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		"https://example.com/items",
		nil,
	)
	if err != nil {
		b.Fatalf("NewRequestWithContext() error = %v", err)
	}

	baseResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       http.NoBody,
	}
	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return baseResponse, nil
	})

	for _, mode := range []curo.Mode{curo.Off, curo.Observe, curo.Enforce} {
		b.Run(modeName(mode), func(b *testing.B) {
			transport, newErr := curo.New(base, curo.WithMode(mode))
			if newErr != nil {
				b.Fatalf("New() error = %v", newErr)
			}

			response, roundTripErr := transport.RoundTrip(request)
			if roundTripErr != nil {
				b.Fatalf("warm RoundTrip() error = %v", roundTripErr)
			}
			if closeErr := response.Body.Close(); closeErr != nil {
				b.Fatalf("warm response Body.Close() error = %v", closeErr)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				response, roundTripErr := transport.RoundTrip(request)
				if roundTripErr != nil {
					b.Fatalf("RoundTrip() error = %v", roundTripErr)
				}
				if closeErr := response.Body.Close(); closeErr != nil {
					b.Fatalf("response Body.Close() error = %v", closeErr)
				}
			}
		})
	}
}

func modeName(mode curo.Mode) string {
	switch mode {
	case curo.Off:
		return "Off"
	case curo.Observe:
		return "Observe"
	case curo.Enforce:
		return "Enforce"
	default:
		return "Other"
	}
}
