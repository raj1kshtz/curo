// Package curo adds adaptive resilience to outbound HTTP requests and other
// calls to dependencies.
//
// A [Transport] is an [http.RoundTripper] that wraps a base transport, such
// as [http.DefaultTransport]. It observes the requests sent through it, learns
// how each dependency behaves, and in [Enforce] mode applies three controls
// that need no tuning:
//
//   - a retry of a replay-safe read that failed transiently, within budgets
//     that allow about one retry per ten requests;
//   - a dependency breaker that fails requests fast with [ErrBreakerOpen]
//     once a dependency is diagnosed as down; and
//   - an adaptive timeout that ends a read with [ErrTimeout] when its
//     response headers take too long.
//
// Wrap the base transport once, share the client, and close the Transport
// when the application shuts down:
//
//	transport, err := curo.New(http.DefaultTransport)
//	if err != nil {
//		return err
//	}
//	client := &http.Client{Transport: transport, Timeout: time.Minute}
//
// # Other calls
//
// An [Engine] applies the same controls to calls that do not go through
// net/http, such as gRPC calls and database queries. Each call to
// [Engine.Do] names its target, says whether it is read-only and
// replay-safe, classifies its errors, and passes the attempt to make:
//
//	err = engine.Do(ctx, curo.Call{
//		Classify:   classify,
//		Scheme:     "grpc",
//		Host:       "inventory.internal",
//		Port:       443,
//		ReadOnly:   true,
//		ReplaySafe: true,
//	}, func(ctx context.Context) error {
//		item, err = inventory.GetItem(ctx, request)
//		return err
//	})
//
// The Engine retries only a replay-safe call whose failure Classify marks
// retryable, and its adaptive timeout bounds the whole attempt of a read-only
// call.
//
// # Modes
//
// A new Transport or Engine starts in [Observe]: it sends every request and
// runs every call unchanged, and reports what it would do. [Enforce] applies
// the controls, and [Off] passes requests straight to the base transport and
// runs calls directly. [Transport.SetMode] and [Engine.SetMode] change the
// mode at runtime, so a rollback needs no deploy. Apart from the timeout
// bounds, set with [WithTimeoutBounds], every threshold is fixed.
//
// # Errors
//
// [http.Client] wraps the errors that Curo returns in a [*url.Error], so
// match them with [errors.Is]. [ErrTimeout] also matches
// [context.DeadlineExceeded], so check it first.
//
// # Visibility
//
// [Transport.Stats] and [Engine.Stats] return aggregate counters, and
// [Transport.Report] and [Engine.Report] return each dependency's latest
// decision with its evidence and reasons. Both encode to JSON.
//
// # Protocols
//
// Curo wraps the base transport above the wire protocol, and the base
// transport keeps its own connections, TLS settings, and protocol
// negotiation. HTTP/1.1, HTTPS, and HTTP/2 therefore need no Curo
// configuration. The [operations guide] shows how to turn on HTTP/2 health
// checks in the base transport.
//
// # Safety
//
// Curo runs inside the application it protects. A failure inside Curo never
// fails a request or call, and repeated failures make a Transport pass
// requests straight through and an Engine run attempts directly. Stats counts
// these failures, and [WithLogger] logs where each one happened. Curo never
// retries a request or call that is not replay-safe, never extends a
// caller's deadline, and keeps its state bounded however many hosts the
// application calls.
//
// The [operations guide] explains how to run Curo in a service, and the
// [behavior reference] describes each mechanism in detail.
//
// [operations guide]: https://github.com/raj1kshtz/curo/blob/main/docs/guides/operations.md
// [behavior reference]: https://github.com/raj1kshtz/curo/blob/main/docs/reference/behavior.md
package curo
