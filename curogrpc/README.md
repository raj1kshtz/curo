# curogrpc

[![Go Reference][reference-badge]][reference]

curogrpc applies [Curo][curo] to unary gRPC client calls. Its interceptor
runs each call through a `curo.Engine`, which learns how each server behaves
from the outcomes of its calls. In `Enforce` mode, the engine fails calls
fast while a server is down, and ends a read-only call that takes much longer
than the server's recent calls.

curogrpc is a separate module, so only applications that import it depend on
grpc-go. The root module, `github.com/raj1kshtz/curo`, stays free of
third-party dependencies.

## Install

```sh
go get github.com/raj1kshtz/curo/curogrpc@latest
```

curogrpc requires Go 1.26 or later, `github.com/raj1kshtz/curo` v0.2.0 or
later, and `google.golang.org/grpc` v1.84.0 or later.

## Usage

Create one engine when the process starts, and install its interceptor as
the last unary interceptor of each channel, so that it measures only the
call:

```go
engine, err := curo.NewEngine()
if err != nil {
    return err
}
defer engine.Close()

interceptor, err := curogrpc.NewUnaryClientInterceptor(
    engine,
    curogrpc.WithReadMethods("/inventory.v1.Inventory/GetItem"),
)
if err != nil {
    return err
}

conn, err := grpc.NewClient(
    "dns:///inventory.internal:443",
    grpc.WithTransportCredentials(credentials.NewTLS(nil)),
    grpc.WithChainUnaryInterceptor(logging, interceptor),
)
```

A new engine starts in `Observe`, which changes no call. Read
`engine.Report()` to see what Curo learned about each server, then switch to
`Enforce` with `engine.SetMode(curo.Enforce)`. The
[operations guide][rollout] describes the rollout.

## How curogrpc describes a call

| Part | How curogrpc sets it |
| --- | --- |
| Target | The host and port of the channel's canonical target. A `dns` target names its host and its port, 443 by default. A `passthrough` target names its address, which must include a port. Calls on any other channel, such as one with a `unix`, `xds`, or custom resolver, count toward the engine's overflow aggregate, where Curo does not act. Reports show each target with the scheme `grpc`. |
| Read-only | A method is read-only when `WithReadMethods` lists it, or when its proto declares `option idempotency_level = NO_SIDE_EFFECTS;`. Only a read-only call can have an adaptive timeout. |
| Replay-safe | Never. Curo never retries a gRPC call, so the retries of a grpc-go retry policy run inside the call that curogrpc measures, and its retry throttling still applies. |
| Outcome | The status code of a failed call, as the next table shows. |

List only methods that change nothing at the server. In `Enforce`, an
adaptive timeout may end a read-only call, and grpc-go reports that to the
server as a cancellation.

## Status codes

| Status code | Outcome |
| --- | --- |
| `OK` | Success |
| `InvalidArgument`, `NotFound`, `AlreadyExists`, `PermissionDenied`, `FailedPrecondition`, `Aborted`, `OutOfRange`, `Unimplemented`, `Unauthenticated` | Client failure, which leaves the server healthy |
| `ResourceExhausted` | Rate limited |
| `Unknown`, `Internal`, `DataLoss` | Failure of the server |
| `DeadlineExceeded`, `Unavailable`, `Canceled`, and any other code | Failure to reach the server, which gives no latency sample |

A call that failed after its context was canceled, or after its context's
deadline expired, is the caller's, whatever its code. Statuses that grpc-go
creates itself count as the server's, such as `Unavailable` when it cannot
connect, or `ResourceExhausted` when a message is too large.

## Errors

A call that the engine rejects while the server is down fails with an
`Unavailable` status, and a call that an adaptive timeout ended fails with a
`DeadlineExceeded` status, so callers handle them as other gRPC failures.
`errors.Is` still matches them with Curo's errors:

```go
reply, err := client.GetItem(ctx, request)
if errors.Is(err, curo.ErrBreakerOpen) {
    // The server is down, and the call never left the process.
}
```

Every other error passes through unchanged.

## Limits

- Streams are not intercepted.
- A call that the engine rejects returns before it reaches the interceptors
  installed after this one, and grpc-go's stats handlers do not see it.
- A nil engine, an engine in `Off` mode, a closed engine, and a self-disabled
  engine pass every call through. A failure inside curogrpc before a call
  makes that call bypass Curo.
- curogrpc acts on what the method list and the protos declare, so a method
  wrongly declared read-only may be ended by an adaptive timeout after the
  server applied it.

[ADR-0012][adr-0012] records the design and the alternatives that were
rejected.

## Compatibility

curogrpc has its own `v0.x` versions, tagged `curogrpc/vX.Y.Z`, under
Curo's [compatibility policy][adr-0010]. `TestAPI` compares its exported API
with `testdata/api.golden`. CI tests Go 1.26, the minimum in its `go.mod`,
and the two most recent Go releases. The [changelog](CHANGELOG.md) lists the
changes in each release.

## License

[Apache License 2.0][license], as for the rest of the repository.

[reference-badge]: https://pkg.go.dev/badge/github.com/raj1kshtz/curo/curogrpc.svg
[reference]: https://pkg.go.dev/github.com/raj1kshtz/curo/curogrpc
[curo]: https://github.com/raj1kshtz/curo
[rollout]: https://github.com/raj1kshtz/curo/blob/main/docs/guides/operations.md#roll-out-enforce
[adr-0012]: https://github.com/raj1kshtz/curo/blob/main/docs/adr/0012-engine-and-adapter-modules.md
[adr-0010]: https://github.com/raj1kshtz/curo/blob/main/docs/adr/0010-versioning-and-compatibility.md
[license]: https://github.com/raj1kshtz/curo/blob/main/LICENSE
