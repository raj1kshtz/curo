// Package curo adds adaptive resilience to outbound HTTP calls.
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
// # Modes
//
// A new Transport starts in [Observe]: it sends every request unchanged and
// reports what it would do. [Enforce] applies the controls, and [Off] passes
// requests straight to the base transport. [Transport.SetMode] changes the
// mode at runtime, so a rollback needs no deploy. Only the mode and the
// timeout bounds, set with [WithTimeoutBounds], are configurable.
//
// # Errors
//
// [http.Client] wraps the errors that Curo returns in a [*url.Error], so
// match them with [errors.Is]. [ErrTimeout] also matches
// [context.DeadlineExceeded], so check it first.
//
// # Visibility
//
// [Transport.Stats] returns aggregate counters, and [Transport.Report]
// returns each dependency's latest decision with its evidence and reasons.
// Both encode to JSON.
//
// # Safety
//
// Curo runs inside the application it protects. A failure inside Curo never
// fails a request, and repeated failures make the Transport pass requests
// straight through. Curo never retries a request that is not replay-safe,
// never extends a caller's deadline, and keeps its state bounded however
// many hosts the application calls.
//
// The [operations guide] explains how to run Curo in a service, and the
// [behavior reference] describes each mechanism in detail.
//
// [operations guide]: https://github.com/raj1kshtz/curo/blob/main/docs/guides/operations.md
// [behavior reference]: https://github.com/raj1kshtz/curo/blob/main/docs/reference/behavior.md
package curo
