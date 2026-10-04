# Changelog

All notable changes to this project are documented in this file.

The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While Curo is `v0.x`, a minor release may change the public API. Every
breaking change is listed under **Changed** or **Removed** with migration
notes, and [ADR-0010](docs/adr/0010-versioning-and-compatibility.md) defines
the compatibility policy.

## [Unreleased]

## [0.1.0] - 2026-10-03

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

[Unreleased]: https://github.com/raj1kshtz/curo/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/raj1kshtz/curo/releases/tag/v0.1.0
