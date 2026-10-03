# curo

> From Latin *curo*: "I care for, I heal."

`curo` is an experimental Go library for adaptive resilience around outbound
HTTP calls. The project is exploring whether bounded runtime observations can
support useful retry, breaker, and timeout decisions without requiring every
application to tune static thresholds.

[![CI][ci-badge]][ci-workflow]
[![License][license-badge]](LICENSE)

> [!IMPORTANT]
> Curo is pre-alpha and has no release. The current API provides transparent
> transport behavior, bounded request observation, aggregate runtime
> statistics, and a pull-based decision report. Curo evaluates readiness,
> deterministic diagnoses, and control candidates. `Enforce` applies the retry
> candidate, as at most one budgeted retry of a replay-safe request, the
> breaker candidate, by failing requests fast while a dependency is down, and
> the timeout candidate, by ending a read that waits too long for response
> headers.

## Current status

| Area | Status |
| ---- | ------ |
| Architecture decisions | Accepted and documented in [`docs/adr`](docs/adr/) |
| Engine design | Documented in [`docs/design/engine.md`](docs/design/engine.md) |
| Public Go API | Transport, lifecycle, operating modes, timeout bounds, aggregate statistics, decision reports, and fail-fast and timeout errors |
| Runtime implementation | Guarded observation, diagnosis, candidate evaluation, budgeted retries, dependency breakers, and adaptive timeouts around direct delegation |
| Adaptive behavior | Readiness, diagnosis, and candidates reported; retries, dependency breakers, and adaptive timeouts applied in `Enforce` |
| Performance data | Benchmarks for every measured path, with exact allocation gates in CI |
| Regression gates | Deterministic failure simulation of nine scenarios with a reviewed scorecard, and full statement coverage in CI |
| Operational guidance | [Operations guide](docs/guides/operations.md) for wiring, deadlines, timeout bounds, rollout, monitoring, and incidents |

Public documentation is updated as features become real, rather than
documenting planned APIs as if they already exist.

## Current API

This section describes each mechanism. To run Curo in a service, start with
the [operations guide](docs/guides/operations.md).

```go
func newHTTPClient() (*http.Client, *curo.Transport, error) {
    transport, err := curo.New(
        http.DefaultTransport,
        curo.WithMode(curo.Observe),
    )
    if err != nil {
        return nil, nil, err
    }

    client := &http.Client{Transport: transport}
    return client, transport, nil
}
```

One `curo.Transport` binds one host-owned `http.RoundTripper`. Curo does not
close the base transport. A nil base is rejected, mode access is safe under
concurrency, and `Close` is idempotent. The application closes the returned
`curo.Transport` during shutdown.

`Off` and `Observe` delegate the original request exactly once and return the
base response or error unchanged, and so does `Enforce` for every request that
it does not retry, fail fast, or send with an adaptive timeout. `Observe` and
`Enforce` record completed initial attempts and evaluate bounded diagnoses and
control candidates; `Off`, closed, and self-disabled transports use direct
pass-through without collecting new evidence.

Observation keys include only a normalized HTTP or HTTPS scheme, bounded
hostname, effective port, and closed method class. Paths, queries, URL user
information, headers, bodies, and raw error text are excluded. One transport
admits at most 128 regular targets and uses one non-actionable overflow
aggregate for additional or invalid identities.

Each regular target combines a two-minute recent window with a bounded
non-overlapping historical baseline. Deterministic rules produce cold,
warming, ready, or stale readiness and a closed diagnosis class. Caller-owned
cancellation does not warm readiness, the overflow aggregate is never
diagnosed, and diagnosis results expire after ten seconds.

`Stats` provides privacy-safe aggregate visibility:

```go
stats := transport.Stats()
fmt.Printf(
    "observed=%d targets=%d overflow=%d internal_failures=%d disabled=%t\n",
    stats.ObservedRequests,
    stats.TrackedTargets,
    stats.OverflowRequests,
    stats.InternalFailures,
    stats.SelfDisabled,
)
fmt.Printf(
    "retries=%d retry_successes=%d budget_denials=%d\n",
    stats.RetryAttempts,
    stats.RetrySuccesses,
    stats.RetryBudgetDenials,
)
fmt.Printf(
    "breaker_opens=%d breaker_probes=%d breaker_rejections=%d\n",
    stats.BreakerOpens,
    stats.BreakerProbes,
    stats.BreakerRejections,
)
fmt.Printf(
    "timeouts=%d shadow_timeouts=%d\n",
    stats.Timeouts,
    stats.ShadowTimeouts,
)
```

`Stats` intentionally does not expose target identities or diagnoses.
Per-target decisions and recent candidate changes are available through
`Report`:

```go
report := transport.Report()
for _, decision := range report.Targets {
    fmt.Printf(
        "%s %s readiness=%s diagnosis=%s candidates=%s timeout=%s reasons=%v\n",
        decision.Target.Host,
        decision.Target.Method,
        decision.Readiness,
        decision.Diagnosis,
        decision.Candidates,
        decision.Timeout,
        decision.Reasons,
    )
}
for _, change := range report.Changes {
    fmt.Printf(
        "change=%d host=%s candidates=%s\n",
        change.Sequence,
        change.Decision.Target.Host,
        change.Decision.Candidates,
    )
}
```

Completed `Observe` and `Enforce` requests, except caller-owned cancellations
and deadlines, refresh a target's decision once the previous decision expires,
at most ten seconds after its evaluation. A latency sample slow enough to
raise the current adaptive timeout refreshes the decision at once, and a read
request that starts after a decision with a timeout expired refreshes it
before the request is sent, so the timeout does not lapse while its evidence
is retained. `Report` copies published state without evaluating evidence, so
an idle target keeps its last decision; compare `ExpiresAt` with the current
time before relying on it. A tracked target without an evaluation appears
with zero times and `Cold` readiness, and the overflow aggregate is never
reported.

Policy version 3 selects `CandidateRetry` for a `Transient` diagnosis and
`CandidateBreakerOpen` for `DependencyDown`. `Saturation` is reported without
a candidate, because a dependency that is rate limiting is still answering.
These candidates require `Ready` evidence; a matching diagnosis without it
selects nothing and adds `ReasonReadinessRequired`. `CandidateTimeout`
depends only on latency evidence, as described below. `Changes` keeps the
latest 256 candidate changes with contiguous sequence numbers, so a gap
between reads means older changes were overwritten. `Enforce` applies all
three candidates as described below.

Report targets use the same normalized identity as observation. Hostnames can
be influenced by untrusted input when an application calls user-supplied URLs,
so do not use `Target.Host` as an unbounded metric label. `Report` is safe for
concurrent use and remains readable after `Close`, in `Off` mode, and after
self-disable. If Curo fails while building a report, it returns an empty
report and counts the failure in `InternalFailures`.

In `Enforce` mode, Curo retries a request at most once, and only when all of
these hold:

- The method is `GET`, `HEAD`, `OPTIONS`, `TRACE`, or empty, and the body is
  absent or reproducible through `GetBody`.
- The initial attempt failed with a transport error, or with a 502, 503, or
  504 response without a `Retry-After` header. Caller cancellations and
  deadlines, attempts that the adaptive timeout ended, 429, and other
  statuses are never retried.
- The target's current decision includes `CandidateRetry` and has not
  expired, both after the initial attempt and again just before the retry.
- The target's dependency breaker is closed at both of those points, and the
  request is not a breaker probe.
- The caller has not canceled the request through its context or the
  deprecated `Cancel` channel, and the context deadline leaves room for the
  backoff and another attempt as long as the first.
- The target and instance retry budgets each hold a token.
- The mode has not changed since the request started, even temporarily, the
  transport has not been closed, and Curo has not self-disabled.

Each recorded initial attempt to a tracked target adds a tenth of a token to
its target budget, capped at 10 tokens, and to the instance budget, capped at
50 tokens. Budgets start empty, retries never refill them, and `Observe` funds
them without spending, so credit earned while observing is available after a
switch to `Enforce`. These values are fixed, and unsafe methods cannot opt in
yet. `RetryBudgetDenials` counts retries that met every other condition but
found a budget empty.

Before a retry, Curo waits a random 25 to 100 milliseconds, ending early if
the caller's context is done or the request's `Cancel` channel closes. An
`http.Client` `Timeout` applies through both, so it bounds the initial
attempt, the backoff, and the retry together. The retry sends a clone of the
original request, with a fresh body from `GetBody` when there is one. Once the
retry starts, Curo closes the discarded first response body without reading
it, and `RoundTrip` returns the retry's response or error. Otherwise the first
result is returned untouched. Retries are not recorded as evidence. A decision
refreshes only after the previous one expires, so a failure that arrives
while a `Healthy` decision is still current is not retried.

In `Enforce` mode, each tracked target also has a dependency breaker. It opens
when a request fails with a transport error or a 5xx response while the
target's current decision includes `CandidateBreakerOpen`, provided the mode
has not changed since the request started, the transport is still open, and
Curo has not self-disabled. Caller cancellations and deadlines, 429, and other
4xx responses never open it. While the breaker is open, requests to that
target fail fast: `RoundTrip` closes the request body and returns
`curo.ErrBreakerOpen` without sending the request. `http.Client` wraps the
error in `*url.Error`, so check it with `errors.Is`:

```go
response, err := client.Do(request)
if errors.Is(err, curo.ErrBreakerOpen) {
    // Curo did not send the request. Use a fallback or report the outage.
    return nil, err
}
if err != nil {
    return nil, err
}
defer response.Body.Close()
```

After 5 seconds, the next request is sent as a probe while other requests keep
failing fast. A probe that receives any response other than 429 or 5xx closes
the breaker. A probe that fails, or holds its 30-second lease without a
result, doubles the cooldown, up to 60 seconds, and so does opening again
within 2 minutes of a close. A probe is never retried, and a request its
caller already canceled is never chosen as one. After a close, the target
needs fresh evidence before its breaker can open again. These values are
fixed. `BreakerOpens`, `BreakerProbes`, and `BreakerRejections` in `Stats`
count openings, probes, and fail-fast rejections.

The breaker has known limits:

- A target covers every path on a host with the same method class, so
  failures on one path can make requests to other paths fail fast.
- A breaker opens only after a `Ready` `DependencyDown` diagnosis, about a
  minute into a complete outage of a steadily used target. Low-volume targets
  may never open one.
- Breaker state belongs to one process, so replicas open and close their
  breakers independently.
- A probe is a real application request and may fail.
- While the breaker is open, a request its caller already canceled receives
  `ErrBreakerOpen` instead of its context error.
- `Off`, `Observe`, closed, and self-disabled transports never fail a request
  fast, and a Curo failure never causes a rejection. Breaker state persists
  across mode changes, so requests fail fast again if `Enforce` is restored
  before a probe closes an open breaker.

In `Enforce` mode, Curo also bounds how long a read request (`GET`, `HEAD`,
`OPTIONS`, `TRACE`, or an empty method) waits for response headers. Every
initial attempt that returns a response is a sample of its time to response
headers, and a target retains samples from about the last 30 minutes. Once it
holds at least 100, the target's adaptive timeout is three times the upper
bound of the latency bucket that holds its slowest sample, clamped to the
timeout bounds. The buckets are fixed, so with the default bounds of 2 and 30
seconds the timeout is 2, 3, 7.5, 15, or 30 seconds. It does not depend on
the diagnosis or on readiness, and `Decision.Timeout` reports it.

```go
transport, err := curo.New(
    http.DefaultTransport,
    curo.WithMode(curo.Enforce),
    curo.WithTimeoutBounds(time.Second, 10*time.Second),
)
```

Set the ceiling above the longest time to response headers that a read may
legitimately take, because no read waits longer. `WithTimeoutBounds(0, 0)`
disables adaptive timeouts. A request whose context deadline leaves no more
time than its timeout keeps only that deadline, and Curo never extends a
caller's deadline. A read that its caller's deadline ends is never a
dependency failure, so keep the ceiling below the deadlines that callers set,
as the [operations guide](docs/guides/operations.md#choose-timeout-bounds)
explains.

A timed request is sent as a shallow copy with a derived context, which Curo
cancels when the timeout expires. `RoundTrip` then returns `curo.ErrTimeout`
and closes any response that arrives too late. Check it with
`errors.Is(err, curo.ErrTimeout)`. Like the timeout error of an
`http.Client`, `ErrTimeout` reports true from `Timeout`, and
`errors.Is(err, context.DeadlineExceeded)` holds. The request may have reached
the dependency. An attempt that returns in time keeps its response, with the
body wrapped so that closing it, or reading it to the end, releases the
derived context. Reading the body is not bounded. The wrapper does not keep
the body's concrete type, but a body that implements `io.Writer`, such as the
connection of a `101 Switching Protocols` response, stays writable and
releases the context only when closed.

An attempt that the timeout ended is never retried. A retry of an attempt
that failed in time gets the same timeout, when the caller's deadline still
allows it. A breaker probe, a write, and any other method are never timed.
An attempt that the timeout ended is recorded as a sample at the timeout,
which is a lower bound of its true latency, so a dependency that slows down
gets a longer timeout. It counts as a dependency failure only when its
timeout was the ceiling. Below the ceiling the dependency may only be slower
than before, so the attempt is only a latency sample and does not dilute the
failure rates that diagnosis uses.

`Observe` never ends a request. Instead, `ShadowTimeouts` in `Stats` counts
initial attempts that took longer than the timeout `Enforce` would have
applied, and `Timeouts` counts attempts that `Enforce` ended. A mode change,
`Close`, or self-disable withdraws a timeout that has not fired, and `Off`,
closed, and self-disabled transports never end a request with `ErrTimeout`.
Only the bounds are configurable; the multiplier and the 100-sample minimum
are fixed.

The adaptive timeout has known limits:

- A target covers every path on a host with the same method class, so a
  rarely used slow path on a fast host is cut until the samples of its cuts
  raise the timeout, one step at a time.
- After a single slow response, the timeout stays high for up to about 30
  minutes, until that sample ages out. A target with fewer than 100 samples
  in that time never gets a timeout.
- The timeout is cooperative. A base transport that ignores context
  cancellation keeps the request waiting, and `ErrTimeout` is returned only
  when that transport returns.
- Breaker probes are never timed, so a probe to a hung dependency waits for
  the caller's deadline or the base transport.
- A request in flight loses its timeout when the mode changes, even if
  `Enforce` is restored.
- Timeout state belongs to one process, so replicas learn their timeouts
  independently.

The transport now separates optional Curo-owned preflight and postflight work
from the unguarded base transport call. A preflight failure falls back to the
original request, while a postflight or retry-preparation failure preserves
the captured transport result. Repeated internal failures permanently select
direct pass-through for that instance. Aggregate failure counts and
self-disable state remain readable through `Stats`; detailed failure events,
logging sinks, and reset are deferred.

## Intended scope

The initial release is planned as an embedded Go library that explicitly wraps
an application's `http.RoundTripper`. It is intentionally limited to outbound
`net/http` traffic and process-local state.

The design includes:

- `Off`, `Observe`, and `Enforce` operating modes.
- Bounded observations and low-cardinality target identity.
- Explainable diagnosis based on deterministic rules.
- Aggregate retry budgets rather than per-request retry counts.
- Adaptive breaker and timeout controls with hard safety bounds.
- Fail-safe behavior for failures owned by Curo.

The initial scope excludes sidecars, a control plane, non-Go SDKs, ingress
middleware, gRPC interception, and machine-learning policy.

## Safety model

Curo runs inside the application it is intended to protect. The primary design
constraint is therefore host application inviolability:

- Curo-owned failures must not propagate into the host.
- The original request must not be mutated.
- An attempt with an unknown side effect must never be replayed blindly.
- State, concurrency, cardinality, and mitigation work must remain bounded.
- `Observe` is the default; behavior changes require explicit `Enforce`
  authority.
- The wrapped transport remains host-owned and keeps its existing panic
  semantics.

The guarded request-stage boundary, bounded observer and historical baseline,
deterministic diagnoser, candidate policy, guarded decision report, aggregate
internal-fault visibility, self-disable signal, budgeted replay-safe retries,
adaptive dependency breakers, and adaptive timeouts are implemented.
Applied-action auditing remains a design requirement. The full contract is
recorded in [ADR-0002](docs/adr/0002-host-application-inviolability.md).

## Architecture

![Curo system context](docs/design/diagrams/system-context.svg)

The [engine high-level design](docs/design/engine.md) describes component
boundaries, runtime paths, failure containment, state ownership, mitigation
controls, observability, and the test strategy expected from the implementation.

Architectural decisions are recorded separately so that rejected alternatives
and long-term constraints remain reviewable. See the
[ADR index](docs/adr/README.md).

The [documentation index](docs/README.md) links architecture, design,
operations, and project governance material.

## Implementation sequence

The next milestone is to review API compatibility and prepare the first
release.

## Contributing

Design feedback is welcome, especially around failure semantics, replay safety,
and bounded concurrency. Please read
[CONTRIBUTING.md](.github/CONTRIBUTING.md) before proposing an implementation
or changing an accepted architectural decision.

## License

[Apache License 2.0](LICENSE).

[ci-badge]: https://github.com/raj1kshtz/curo/actions/workflows/ci.yml/badge.svg
[ci-workflow]: https://github.com/raj1kshtz/curo/actions/workflows/ci.yml
[license-badge]: https://img.shields.io/badge/license-Apache--2.0-blue.svg
