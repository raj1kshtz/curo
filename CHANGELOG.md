# Changelog

All notable changes to the root module, `github.com/raj1kshtz/curo`, are
documented in this file. The `curogrpc` module has its own
[changelog](curogrpc/CHANGELOG.md).

The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While Curo is `v0.x`, a minor release may change the public API. Every
breaking change is listed under **Changed** or **Removed** with migration
notes, and [ADR-0010](docs/adr/0010-versioning-and-compatibility.md) defines
the compatibility policy.

## [Unreleased]

## [0.2.0] - 2026-10-18

Curo now applies to calls that do not go through `net/http`, through an
`Engine`.

### Added

- `Engine`, which applies Curo to calls that do not go through `net/http`,
  such as gRPC calls, database queries, and the requests of client libraries.
  `Engine.Do` runs a call's attempt and observes it. In `Enforce`, it can
  fail the call fast while its target's breaker is open, end a read-only
  attempt with an adaptive timeout, or retry a replay-safe call once within
  the retry budgets. `NewEngine` takes the same options as `New`, and an
  `Engine` has the same `Mode`, `SetMode`, `Close`, `Report`, and `Stats`
  methods as a `Transport`.
- `Call`, which describes a call's target, whether it is read-only or
  replay-safe, and a `Classify` function that maps an attempt's error to a
  `Classification`: an `Outcome` and whether a retry might succeed. An error
  that `Classify` does not classify counts as a transport failure that is
  never retried.
- `ErrUninitializedEngine`, which `SetMode` and `Close` return for a nil or
  zero `Engine`.
- ADR-0012, which records the `Engine` and places protocol adapters that need
  third-party dependencies, starting with a gRPC interceptor, in nested
  modules of their own.
- An `Engine.Do` example, `Engine` benchmarks, and an allocation gate that
  keeps every `Engine` path free of allocations, except a timed attempt.
- The documentation explains how Curo works over HTTPS, HTTP/2, and
  unencrypted HTTP/2, how to turn on HTTP/2 health checks in the base
  transport, and why TLS certificate and DNS errors count as dependency
  failures. Tests run Curo's retries, breakers, and timeouts over each
  protocol, and the scope now names the outbound traffic that Curo does not
  cover because it does not go through `net/http`.

### Changed

- Error messages no longer mention HTTP, because `Engine` calls return the
  same errors. `ErrClosed` reads `curo: closed` instead of
  `curo: transport is closed`, `ErrTimeout` reads
  `curo: adaptive timeout expired` instead of
  `curo: adaptive timeout awaiting response headers`, and the cause of a
  context that an adaptive timeout ended reads, for example,
  `curo: adaptive timeout of 2s expired`. The errors match `errors.Is` as
  before. Migration: compare errors with `errors.Is`, not their text.
- Target state takes 10,016 bytes instead of 10,000 on 64-bit platforms, and
  the change journal 49,168 bytes instead of 45,072, because targets keep any
  URI scheme.

### Fixed

- A breaker probe that returned no result, because a contained internal
  failure or self-disable kept Curo from recording it or because the base
  transport panicked, now settles as inconclusive, so the next probe follows
  after the base cooldown. Before, it held the breaker in its probe until the
  30-second lease expired, and then counted as a failed probe, which doubled
  the cooldown.

## [0.1.0] - 2026-10-04

The first release.

### Added

- `Transport`, an `http.RoundTripper` that wraps a base transport, with `New`,
  `WithMode`, `Close`, and `CloseIdleConnections`.
- Operating modes `Observe`, `Enforce`, and `Off`. A new transport starts in
  `Observe`, `SetMode` changes the mode at runtime, and modes encode as their
  names, so flags, JSON, and admin controls can set them.
- Bounded observation keyed by scheme, host, port, and method class. Paths,
  queries, user information, headers, and bodies are never retained, and at
  most 128 targets are tracked, with an overflow aggregate beyond that.
- Readiness and diagnosis from a recent window and a historical baseline,
  with fixed reason codes and decisions that expire within ten seconds,
  under policy version 3.
- Budgeted retries in `Enforce`: a replay-safe read that failed with a
  transport error, or with a 502, 503, or 504 response without
  `Retry-After`, is retried at most once, within target and instance budgets.
- Dependency breakers in `Enforce`: requests to a target diagnosed as down
  fail fast with `ErrBreakerOpen`, and after a cooldown that starts at 5
  seconds and doubles up to 60 seconds, a probe request tests recovery.
- Adaptive timeouts in `Enforce`: a read that waits for response headers
  longer than its target's learned limit ends with `ErrTimeout`.
  `WithTimeoutBounds` sets the floor and ceiling, 2 and 30 seconds by
  default, or disables adaptive timeouts.
- `Transport.Stats`, with aggregate counters for observation, retries,
  breakers, timeouts, and internal failures.
- `Transport.Report`, with each target's latest decision, its evidence and
  reasons, and the latest 256 changes. Reports encode to JSON with names.
- Failure containment: a failure inside Curo never fails a request, and
  repeated failures make the transport pass requests straight through.
- `WithLogger`, which logs contained internal failures through `log/slog`,
  with the stage that failed and, for a panic, a stack trace without argument
  values. A transport offers its logger one failure at a time and at most
  three a minute, and self-disable once.
- Documentation: an operations guide, a behavior reference, the engine
  design, and Architecture Decision Records 0001 through 0011.
- Runnable examples for wiring, timeout bounds, logging, runtime mode
  changes, `expvar` export, error handling, and reports.
- Quality gates in CI: a deterministic simulation scorecard, allocation
  gates, an API gate, full statement coverage, benchmarks, and tests on Go
  1.23, 1.26, and 1.27.

[Unreleased]: https://github.com/raj1kshtz/curo/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/raj1kshtz/curo/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/raj1kshtz/curo/releases/tag/v0.1.0
