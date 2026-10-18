# ADR-0012: Extend Curo Beyond net/http Through an Engine and Adapter Modules

- **Status:** Accepted
- **Date:** 2026-10-18
- **Scope:** The `Engine` API and protocol adapter modules, starting with
  `curogrpc`
- **Decision owners:** Curo maintainers

## Context

[ADR-0001](0001-embedded-library-over-sidecar-proxy.md) made outbound calls
through `net/http` the first integration surface, and `v0.1.0` acts on
nothing else. Services also depend on gRPC services, databases, caches, and
message brokers whose clients do not use `net/http`. Those dependencies fail
in the same ways: errors, slow answers, and outages. Curo cannot see the
calls, so it can neither diagnose those dependencies nor act on them.

The parts of Curo that diagnose and act do not depend on HTTP. Observation,
diagnosis, policy, retry budgets, dependency breakers, and adaptive timeouts
work from a target, an outcome, and a latency. Only the `Transport` knows
about requests, responses, status codes, and bodies.

Several constraints shape the design:

- The root module has no third-party dependencies
  ([ADR-0004](0004-zero-dependency-core.md)). An integration that needs a
  third-party API, such as `google.golang.org/grpc`, must live in a separate
  module that the root never imports.
- A failure inside Curo must never fail a call, and Curo must never replay a
  call that may have taken effect
  ([ADR-0002](0002-host-application-inviolability.md)).
- Only the application knows what an error means. A gRPC `NotFound` or a
  database constraint violation may be a normal answer, the caller's fault,
  or the dependency's failure. A wrong guess either hides an outage or opens
  a breaker on a healthy dependency.
- Only the application, or its client library, knows whether a call changes
  anything and whether it may run twice.
- Client libraries already retry. grpc-go retries under a service config with
  retry throttling, and database drivers reconnect. Retries at two layers
  multiply the load on a struggling dependency.
- Exported identifiers are compatibility promises
  ([ADR-0010](0010-versioning-and-compatibility.md)), so the new API must be
  small and must not name any protocol.
- A request does not start a Curo goroutine (invariant 7 of the
  [engine design](../design/engine.md)).

## Decision

### A protocol-neutral Engine in the root module

The root package adds `Engine`, which applies Curo's observation, diagnosis,
and controls to any call that the application makes through a function:

```go
engine, err := curo.NewEngine(curo.WithMode(curo.Observe))
err = engine.Do(ctx, call, func(ctx context.Context) error {
	return client.Lookup(ctx, request)
})
```

- A `Call` describes one call. `Scheme`, `Host`, and `Port` name its target.
  `ReadOnly` says that the call changes nothing, so an adaptive timeout may
  end it. `ReplaySafe` says that the attempt may run a second time.
  `Classify` maps a non-nil error to a `Classification`: an `Outcome` and
  whether a retry might succeed.
- `Outcome` is an enumeration: `OutcomeSuccess`, `OutcomeClientFailure`,
  `OutcomeRateLimited`, `OutcomeDependencyFailure`, and
  `OutcomeTransportFailure`, with `OutcomeUnspecified` as the zero value. Its
  values map onto the evidence that HTTP status classes already produce, so
  diagnosis and policy need no new rules.
- An `Engine` takes the same options as a `Transport` and has the same
  `Mode`, `SetMode`, `Close`, `Stats`, and `Report` methods, with the same
  types. Both are built on one shared core, so they apply the same rules,
  bounds, and policy version. Each `Engine` owns its targets, evidence, and
  budgets, as each `Transport` does.

### Safety rules for Engine calls

- An error is a transport failure that is not retryable unless `Classify`
  says otherwise. This covers a nil `Classify`, `OutcomeUnspecified`, and any
  value that is not an `Outcome` constant. The default never hides a failure
  and never retries.
- `Retryable` applies only to dependency and transport failures. `Enforce`
  retries at most once, only a replay-safe call, and only within the retry
  budgets, as it does for HTTP.
- An adaptive timeout applies only to a read-only call and bounds its whole
  attempt. `Do` waits for the attempt to return, so Curo starts no goroutine.
  The attempt runs with a context that Curo cancels when the timeout expires
  or the attempt returns, so the attempt must finish using that context
  before it returns.
- A failed call whose caller canceled its context, or whose caller's deadline
  expired, before the attempt returned belongs to the caller whatever
  `Classify` reports, so a caller's cancellations never count against the
  dependency.
- `Classify` runs inside Curo's recovery boundary. A panic in it is contained
  and counted as an internal failure, and `Do` returns the attempt's error. A
  panic in the attempt propagates unchanged, as a panic in a base transport
  does.
- A breaker probe that returns no result Curo can record, because Curo failed
  to record it or because the attempt panicked, settles as inconclusive. A
  contained failure or a panicking attempt therefore cannot hold a breaker in
  its probe. The same rule now applies to a `Transport` and its base
  transport.
- A nil or zero `Engine`, `Off`, a closed `Engine`, and a self-disabled
  `Engine` run the attempt directly, without Curo.
- Call targets share the target limit and the overflow aggregate of HTTP
  targets. A scheme is a URI scheme of at most 32 bytes. A call without a
  port counts toward the overflow aggregate, except an `http` or `https` call,
  which takes the scheme's default port.

### Protocol adapters as nested modules

- An adapter that needs a third-party API lives in its own module in this
  repository, such as `github.com/raj1kshtz/curo/curogrpc`, with its own
  `go.mod`, version tags of the form `curogrpc/vX.Y.Z`, changelog, CI jobs,
  and vulnerability checks, as ADR-0004 requires.
- An adapter uses only the public `Engine` API. It adds what the protocol
  knows: how to name a target, which calls are read-only, how errors map to
  outcomes, and how Curo's errors reach the protocol's callers.
- ADR-0010 governs each adapter's exported API in its own `v0.x` series. An
  adapter states the minimum Go version and root module version it needs,
  and may require a newer Go version than the root module.

### The gRPC adapter

The first adapter, `curogrpc`, provides a unary client interceptor built on
an `Engine`:

- It names the target from the channel's canonical target. Targets that it
  cannot name precisely count toward the overflow aggregate.
- A method is read-only only when the application lists it, or when its
  proto descriptor declares that it has no side effects. The list is bounded.
- It never marks a call replay-safe, so retries stay with grpc-go's retry
  policy and its throttling, which see every attempt.
- It maps status codes to outcomes. `ErrBreakerOpen` reaches the caller as an
  `Unavailable` status and `ErrTimeout` as a `DeadlineExceeded` status, and
  `errors.Is` still matches Curo's errors.
- A nil `Engine` passes calls through, and a failure inside the adapter
  before the call falls back to a direct call. Streams are not intercepted.

### Compatibility

`Engine`, `NewEngine`, `Call`, `Classification`, `Outcome`, and
`ErrUninitializedEngine` join the root API under ADR-0010, and the names and
numbers of `Outcome` follow its rules for enumerations. `ErrClosed`,
`ErrTimeout`, and the cause of an expired adaptive timeout get messages that
do not mention HTTP. Messages are not covered by ADR-0010, and `errors.Is`
matches the errors as before.

## Decision Drivers

- Services need the same protection for every dependency, not only HTTP ones.
- The root module stays free of dependencies, and its API stays small.
- One core with one set of rules is easier to verify than one per protocol.
- Curo must not misjudge a dependency because it guessed what an error means.
- Curo must not multiply the retries of a client library.
- Every guarantee of ADR-0002 holds for calls as it does for requests.

## Options Considered

1. **A protocol-neutral `Engine` in the root, with adapters in nested
   modules - selected.**
   - Strengths: any client library can use Curo through a function, and an
     adapter makes a common protocol a one-line change. The root stays free
     of dependencies, and every protocol shares one core, one set of tests,
     and one policy.
   - Weaknesses: the application or adapter must classify errors and declare
     read-only and replay-safe calls, and a wrong declaration produces wrong
     decisions.
2. **Protocol integrations in the root package - rejected.**
   - Strengths: one module to install and version.
   - Weaknesses: a gRPC interceptor in the root would make every user depend
     on grpc-go, which ADR-0004 forbids.
3. **Count every error as a dependency failure, with no classifier -
   rejected.**
   - Strengths: no setup.
   - Weaknesses: normal answers, such as a lookup that finds nothing, and the
     caller's own mistakes would open breakers on healthy dependencies.
4. **Run each attempt on a goroutine, so a timeout can return before an
   attempt that ignores its context - rejected.**
   - Strengths: an adaptive timeout would bound attempts that do not honor
     their context.
   - Weaknesses: it starts a goroutine per call, which the engine design
     forbids, and it leaks the attempt, which keeps running and holding its
     resources after `Do` returns.
5. **An error interface, such as a method that reports an outcome -
   rejected.**
   - Strengths: errors that the application owns could classify themselves.
   - Weaknesses: errors from third-party clients cannot implement it, so a
     classifier function is needed anyway, and the interface would couple
     application types to Curo.
6. **Let `curogrpc` retry idempotent methods - deferred.**
   - Strengths: Curo's budgets would bound retries for channels without a
     retry policy.
   - Weaknesses: with a grpc-go retry policy, every Curo retry would run the
     whole policy again, multiplying attempts.
7. **A sidecar or proxy for other protocols - rejected.**
   - Strengths: no code change in the application.
   - Weaknesses: ADR-0001 rejected a separate process for `v0.x`, and a proxy
     cannot know what an application's errors mean.

## Consequences

### Positive

- gRPC services, databases, and any other dependency get Curo's diagnosis,
  reports, and controls with the same modes and safety rules as HTTP.
- The root module stays free of dependencies, and an adapter's dependencies
  reach only the applications that import it.
- `Transport` and `Engine` share one core, so a fix or an improvement applies
  to both.
- A `Transport` probe whose result Curo failed to record, or whose base
  transport panicked, no longer holds its breaker until the probe lease
  expires.

### Negative

- A wrong `Classify`, or a call wrongly declared read-only or replay-safe,
  leads Curo to wrong decisions. The defaults are conservative, but a
  `Classify` that is too generous can still hide failures.
- An adaptive timeout ends only an attempt that honors its context, and an
  attempt must finish using its context before it returns, so results that
  are read later, such as rows or a stream, must be read inside the attempt.
- Each adapter module adds versions, CI jobs, dependency updates, and
  release work.
- `curogrpc` covers only unary calls, and channels whose targets it cannot
  name share the overflow aggregate, where Curo does not act.

## Revisit When

- Users need Curo on streaming calls or on server-side handlers.
- A protocol needs call attributes that `Call` cannot express, such as a
  route or a tenant.
- Applications need to report a failure that their client returned without
  an error.
- Applications need adapters for other protocols, such as database drivers.
- grpc-go changes how its retries interact with interceptors.
