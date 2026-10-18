# Changelog

All notable changes to `github.com/raj1kshtz/curo/curogrpc` are documented
in this file. The root module has its own
[changelog](https://github.com/raj1kshtz/curo/blob/main/CHANGELOG.md).

The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this module
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html), with
tags of the form `curogrpc/vX.Y.Z`. While curogrpc is `v0.x`, a minor release
may change its API, with migration notes under **Changed** or **Removed**.

## [Unreleased]

### Changed

- Require `google.golang.org/protobuf` v1.36.12 or later, up from v1.36.11.

## [0.1.0] - 2026-10-18

The first release.

### Added

- `NewUnaryClientInterceptor`, which returns a gRPC unary client interceptor
  that runs each call through a `curo.Engine`. A nil engine passes every call
  through, and a failure inside the interceptor before a call makes that
  call bypass Curo.
- Targets named from the channel's canonical target: the host and port of a
  `dns` target, with port 443 by default, or of a `passthrough` target with a
  port. Calls on any other channel count toward the engine's overflow
  aggregate.
- Read-only methods: those that `WithReadMethods` lists, and those whose
  proto declares `idempotency_level = NO_SIDE_EFFECTS`. `WithReadMethods`
  accepts at most 1024 full method names of at most 1024 bytes each.
- Classification of failed calls by status code. Curo never retries a gRPC
  call, so retries stay with grpc-go's retry policy and its throttling.
- `Unavailable` and `DeadlineExceeded` statuses for calls that the engine
  rejects while the server is down or that an adaptive timeout ended, which
  still match `curo.ErrBreakerOpen` and `curo.ErrTimeout` with `errors.Is`.

[Unreleased]: https://github.com/raj1kshtz/curo/compare/curogrpc/v0.1.0...HEAD
[0.1.0]: https://github.com/raj1kshtz/curo/releases/tag/curogrpc/v0.1.0
