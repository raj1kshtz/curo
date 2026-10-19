// Package curogrpc applies Curo to unary gRPC client calls.
//
// This package belongs to the github.com/raj1kshtz/curo module and shares its
// version and release tag. It has no separate module requirement.
//
// [NewUnaryClientInterceptor] returns an interceptor that runs each unary call
// through a [curo.Engine], which learns how each server behaves from the
// outcomes of its calls. In [curo.Enforce] mode, the engine fails calls fast
// while a server is down, and ends a read-only call that takes much longer
// than the server's recent calls:
//
//	engine, err := curo.NewEngine()
//	if err != nil {
//		return err
//	}
//	defer engine.Close()
//
//	interceptor, err := curogrpc.NewUnaryClientInterceptor(engine)
//	if err != nil {
//		return err
//	}
//	conn, err := grpc.NewClient(
//		"dns:///inventory.internal:443",
//		grpc.WithTransportCredentials(credentials.NewTLS(nil)),
//		grpc.WithChainUnaryInterceptor(logging, interceptor),
//	)
//
// Install the interceptor as the last unary interceptor, so that it measures
// only the call. A call that the engine rejects returns before it reaches the
// interceptors after this one, and grpc-go's stats handlers do not see it.
// Streams are not intercepted.
//
// # Targets
//
// The interceptor names each call's target from the channel's canonical
// target, as [grpc.ClientConn.CanonicalTarget] returns it. A dns target names
// its host and its port, 443 by default. A passthrough target names its
// address, which must include a port. Calls on any other channel, such as
// one with a unix, xds, or custom resolver, count toward the engine's
// overflow aggregate. [curo.Engine.Report] shows each target with the scheme
// grpc.
//
// # Read-only methods
//
// Only a read-only call can have an adaptive timeout. A method is read-only
// when [WithReadMethods] lists it, or when its proto declares that it has no
// side effects:
//
//	rpc GetItem(GetItemRequest) returns (Item) {
//		option idempotency_level = NO_SIDE_EFFECTS;
//	}
//
// The interceptor finds that declaration in [protoregistry.GlobalFiles],
// where generated code registers it.
//
// # Retries
//
// The interceptor never marks a call replay-safe, so Curo never retries a
// gRPC call. The retries of a grpc-go retry policy run inside the call that
// the interceptor measures, and its retry throttling still applies.
//
// # Status codes
//
// The interceptor classifies a failed call by its status code:
//
//   - InvalidArgument, NotFound, AlreadyExists, PermissionDenied,
//     FailedPrecondition, Aborted, OutOfRange, Unimplemented, and
//     Unauthenticated are client failures, which leave the server healthy.
//   - ResourceExhausted means that the server limits the rate of calls.
//   - Unknown, Internal, and DataLoss are failures of the server.
//   - DeadlineExceeded, Unavailable, Canceled, and any other code are
//     failures to reach the server, and give no latency sample.
//
// A call that failed after its context was canceled, or after its context's
// deadline expired, is the caller's, whatever its code. Statuses that grpc-go
// creates itself count as the server's, such as Unavailable when it cannot
// connect, or ResourceExhausted when a message is too large.
//
// # Errors
//
// A call that the engine rejects while the server is down fails with an
// Unavailable status, and a call that an adaptive timeout ended fails with a
// DeadlineExceeded status. [errors.Is] still matches them with
// [curo.ErrBreakerOpen] and [curo.ErrTimeout]. Every other error passes
// through unchanged.
package curogrpc
