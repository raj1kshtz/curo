# Changelog

This file preserves the release history of the former standalone
`github.com/raj1kshtz/curo/curogrpc` module. Starting with v1.0.0,
`curogrpc` is a package in the root module. All new changes,
including the protobuf v1.36.12 requirement, are recorded in the
[root changelog](../CHANGELOG.md).

The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Historical adapter
tags and releases remain unchanged. There is no separate
`curogrpc/v1.0.0` tag or adapter release.

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

[0.1.0]: https://github.com/raj1kshtz/curo/releases/tag/curogrpc/v0.1.0
