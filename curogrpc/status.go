package curogrpc

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/raj1kshtz/curo"
)

var (
	breakerOpenStatus = status.New(codes.Unavailable, curo.ErrBreakerOpen.Error())
	timeoutStatus     = status.New(codes.DeadlineExceeded, curo.ErrTimeout.Error())
)

// classify classifies a failed call by its status code. Curo never retries a
// gRPC call, so no failure is retryable.
func classify(err error) curo.Classification {
	var outcome curo.Outcome
	switch status.Code(err) {
	case codes.OK:
		outcome = curo.OutcomeSuccess
	case codes.InvalidArgument,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.FailedPrecondition,
		codes.Aborted,
		codes.OutOfRange,
		codes.Unimplemented,
		codes.Unauthenticated:
		outcome = curo.OutcomeClientFailure
	case codes.ResourceExhausted:
		outcome = curo.OutcomeRateLimited
	case codes.Unknown, codes.Internal, codes.DataLoss:
		outcome = curo.OutcomeDependencyFailure
	default:
		outcome = curo.OutcomeTransportFailure
	}

	return curo.Classification{Outcome: outcome}
}

// convert gives Curo's errors a status, so that the caller and the
// interceptors outside this one see a rejected call as Unavailable and a call
// that an adaptive timeout ended as DeadlineExceeded.
func convert(err error) error {
	switch {
	case errors.Is(err, curo.ErrBreakerOpen):
		return &statusError{err: err, status: breakerOpenStatus}
	case errors.Is(err, curo.ErrTimeout):
		return &statusError{err: err, status: timeoutStatus}
	default:
		return err
	}
}

// statusError is one of Curo's errors with a status.
type statusError struct {
	err    error
	status *status.Status
}

func (e *statusError) Error() string {
	return "rpc error: code = " + e.status.Code().String() +
		" desc = " + e.err.Error()
}

// GRPCStatus returns the error's status, which package status reads.
func (e *statusError) GRPCStatus() *status.Status {
	return e.status
}

func (e *statusError) Unwrap() error {
	return e.err
}
