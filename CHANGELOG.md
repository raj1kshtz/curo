# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While `curo` is `v0.x`, the public API may change between minor versions. Breaking
changes will always be listed here under **Changed** or **Removed**.

## [Unreleased]

### Added

- Project foundation: Apache-2.0 licence, contribution guide, code of conduct, security
  policy, governance model and CI pipeline.
- ADR lifecycle and authoring guidance, plus Architecture Decision Records
  `0001` through `0009` covering the embedded Go library, host application
  inviolability, adaptive policy, dependencies, operating modes, retry
  budgets, package boundaries, dependency breakers, and adaptive timeouts.
- Engine high-level design with reviewed context, component, runtime,
  containment, state-ownership, and mitigation-control diagrams.
- Initial public transport API with explicit base ownership, validated
  operating modes, concurrency-safe mode changes, idempotent lifecycle, and
  transparent request delegation.
- Internal request guard with stage-aware failure containment, a bounded
  rolling failure signal, atomic self-disable, and direct pass-through after
  repeated internal failures.
- Bounded request observation with normalized target identities, fixed rolling
  evidence buckets, deterministic capacity handling, lazy idle replacement,
  and a non-actionable overflow aggregate.
- Privacy-safe `Transport.Stats` snapshots for observed requests, tracked
  targets, overflow use, contained internal failures, and self-disable state.
- Bounded readiness and deterministic internal diagnosis using disjoint recent
  and historical evidence, closed classes, fixed reason codes, and expiring
  immutable results.
- Policy version 3 control candidates: a `Ready` `Transient` diagnosis selects
  a retry candidate, a `Ready` `DependencyDown` diagnosis selects a
  breaker-open candidate, and enough retained latency evidence selects a
  timeout candidate. Candidates are applied only in `Enforce`.
- `Transport.Report` with detached per-target decisions covering normalized
  identity, readiness, diagnosis, ordered reason codes, recent and historical
  evidence summaries, a latency summary, policy version, candidates, the
  adaptive timeout, and evaluation and expiry times, plus the latest 256
  candidate changes with contiguous sequence numbers.
- Guarded report construction: a Curo failure returns an empty report and
  counts toward `InternalFailures` and self-disable.
- Budgeted retries in `Enforce`: a replay-safe `GET`, `HEAD`, `OPTIONS`, or
  `TRACE` request whose initial attempt failed with a transport error, or with
  a 502, 503, or 504 response without `Retry-After`, is retried at most once
  when its target's current decision selects the retry candidate. Lock-free
  target and instance budgets funded by recorded initial attempts bound retry
  volume, a cancellable 25 to 100 millisecond backoff precedes each retry, and
  a mode change, `Close`, or self-disable withdraws a retry that has not
  started.
- `Stats` fields `RetryAttempts`, `RetrySuccesses`, and `RetryBudgetDenials`.
- Adaptive dependency breakers in `Enforce`: a dependency failure opens a
  target's breaker when its current decision selects the breaker-open
  candidate. While the breaker is open, requests to the target fail fast with
  `ErrBreakerOpen` without being sent. After a cooldown that starts at 5
  seconds and doubles up to 60 seconds, one request at a time is sent as a
  probe, and any response other than 429 or 5xx closes the breaker.
- `ErrBreakerOpen`, plus `Stats` fields `BreakerOpens`, `BreakerProbes`, and
  `BreakerRejections`.
- Adaptive timeouts in `Enforce`: once a target holds at least 100 latency
  samples from about the last 30 minutes, a read request other than a breaker
  probe must receive response headers within three times the upper bound of
  the bucket that holds the target's slowest sample, clamped to the timeout
  bounds. The request is sent as a shallow copy with a derived context, the
  caller's deadline is never extended, an attempt that the timeout ended is
  never retried, and a mode change, `Close`, or self-disable withdraws a
  timeout that has not fired. An attempt that the timeout ended is recorded
  as a latency sample at the timeout, and is a dependency failure only when
  the timeout was the ceiling.
- `WithTimeoutBounds`, with a default floor of 2 seconds and ceiling of 30
  seconds. Zero for both disables adaptive timeouts.
- `ErrTimeout`, which reports true from `Timeout` and matches
  `context.DeadlineExceeded`, plus `Stats` fields `Timeouts` and
  `ShadowTimeouts`. In `Observe`, `ShadowTimeouts` counts attempts that the
  adaptive timeout would have ended.
- `CandidateTimeout`, plus `Decision` fields `Latency` and `Timeout`.
- Regression gates: a deterministic virtual-time simulation of nine
  dependency failure scenarios in `Enforce` and `Observe`, checked against
  invariants and a reviewed golden scorecard; exact allocation gates for the
  request paths; benchmarks for retry budgets, registry lookups, and
  concurrent observation; and CI checks for full statement coverage and a
  single run of every benchmark.

### Changed

- Policy version 3 replaces version 2. It selects `CandidateTimeout` from
  retained latency evidence, independently of the diagnosis and readiness.
- Policy version 2 replaces version 1. A `Saturation` diagnosis no longer
  selects the breaker-open candidate and is reported without a candidate.
- A change is also recorded when a target's adaptive timeout changes.

### Fixed

- A request whose deprecated `Cancel` channel is closed, as `http.Client` does
  when its `Timeout` expires, is recorded as a caller cancellation instead of
  a dependency failure.
- Adaptive timeouts no longer skip latency samples. Samples from the part of
  a minute just before the recent window began were briefly in neither the
  recent window nor the historical baseline, so a target's timeout could drop
  for about a minute and then rise again.

[Unreleased]: https://github.com/raj1kshtz/curo/commits/main
