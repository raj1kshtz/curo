package curogrpc

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/raj1kshtz/curo"
)

func TestClassify(t *testing.T) {
	t.Parallel()

	tests := map[codes.Code]curo.Outcome{
		codes.OK:                 curo.OutcomeSuccess,
		codes.Canceled:           curo.OutcomeTransportFailure,
		codes.Unknown:            curo.OutcomeDependencyFailure,
		codes.InvalidArgument:    curo.OutcomeClientFailure,
		codes.DeadlineExceeded:   curo.OutcomeTransportFailure,
		codes.NotFound:           curo.OutcomeClientFailure,
		codes.AlreadyExists:      curo.OutcomeClientFailure,
		codes.PermissionDenied:   curo.OutcomeClientFailure,
		codes.ResourceExhausted:  curo.OutcomeRateLimited,
		codes.FailedPrecondition: curo.OutcomeClientFailure,
		codes.Aborted:            curo.OutcomeClientFailure,
		codes.OutOfRange:         curo.OutcomeClientFailure,
		codes.Unimplemented:      curo.OutcomeClientFailure,
		codes.Internal:           curo.OutcomeDependencyFailure,
		codes.Unavailable:        curo.OutcomeTransportFailure,
		codes.DataLoss:           curo.OutcomeDependencyFailure,
		codes.Unauthenticated:    curo.OutcomeClientFailure,
		codes.Code(99):           curo.OutcomeTransportFailure,
	}
	for code, want := range tests {
		if got := classify(status.Error(code, "test")); got != (curo.Classification{Outcome: want}) {
			t.Errorf("classify(%s) = %+v, want %s and not retryable", code, got, want)
		}
	}
}

func TestConvertGivesCuroErrorsAStatus(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		err  error
		code codes.Code
	}{
		"breaker open": {err: curo.ErrBreakerOpen, code: codes.Unavailable},
		"timeout":      {err: curo.ErrTimeout, code: codes.DeadlineExceeded},
	}
	for name, test := range tests {
		err := convert(test.err)
		if !errors.Is(err, test.err) {
			t.Errorf("%s: convert() = %v, want an error that matches %v", name, err, test.err)
		}
		got, ok := status.FromError(err)
		if !ok || got.Code() != test.code || got.Message() != test.err.Error() {
			t.Errorf("%s: status = %v, want %s with message %q", name, got, test.code, test.err)
		}
		want := "rpc error: code = " + test.code.String() + " desc = " + test.err.Error()
		if err.Error() != want {
			t.Errorf("%s: Error() = %q, want %q", name, err.Error(), want)
		}
	}

	other := status.Error(codes.Unavailable, "connection refused")
	for _, err := range []error{nil, other} {
		//nolint:errorlint // convert must return any other error itself.
		if got := convert(err); got != err {
			t.Errorf("convert(%v) = %v, want it unchanged", err, got)
		}
	}
}
