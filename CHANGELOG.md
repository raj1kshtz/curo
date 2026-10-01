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
  `0001` through `0007` covering the embedded Go library, host application
  inviolability, adaptive policy, dependencies, operating modes, retry
  budgets, and package boundaries.
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
- Policy version 1 control candidates: a `Ready` `Transient` diagnosis selects
  a retry candidate, and a `Ready` `DependencyDown` or `Saturation` diagnosis
  selects a breaker-open candidate. Candidates are reported but never applied.
- `Transport.Report` with detached per-target decisions covering normalized
  identity, readiness, diagnosis, ordered reason codes, recent and historical
  evidence summaries, policy version, candidates, and evaluation and expiry
  times, plus the latest 256 candidate changes with contiguous sequence
  numbers.
- Guarded report construction: a Curo failure returns an empty report and
  counts toward `InternalFailures` and self-disable.

[Unreleased]: https://github.com/raj1kshtz/curo/commits/main
