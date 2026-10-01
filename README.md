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
> deterministic diagnoses, and control candidates. `Enforce` applies only the
> retry candidate, as at most one budgeted retry of a replay-safe request;
> breaker and timeout controls are not implemented.

## Current status

| Area | Status |
| ---- | ------ |
| Architecture decisions | Accepted and documented in [`docs/adr`](docs/adr/) |
| Engine design | Documented in [`docs/design/engine.md`](docs/design/engine.md) |
| Public Go API | Transport, lifecycle, operating modes, aggregate statistics, and decision reports |
| Runtime implementation | Guarded observation, diagnosis, candidate evaluation, and budgeted retries around direct delegation |
| Adaptive behavior | Readiness, diagnosis, and candidates reported; retries applied in `Enforce`; breaker and timeouts pending |
| Performance data | Allocation benchmarks added; regression gates pending |

Public documentation is updated as features become real, rather than
documenting planned APIs as if they already exist.

## Current API

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
base response or error unchanged, and so does `Enforce` for every request it
does not retry. `Observe` and `Enforce` record completed initial attempts and
evaluate bounded diagnoses and control candidates; `Off`, closed, and
self-disabled transports use direct pass-through without collecting new
evidence.

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
```

`Stats` intentionally does not expose target identities or diagnoses.
Per-target decisions and recent candidate changes are available through
`Report`:

```go
report := transport.Report()
for _, decision := range report.Targets {
    fmt.Printf(
        "%s %s readiness=%s diagnosis=%s candidates=%s reasons=%v\n",
        decision.Target.Host,
        decision.Target.Method,
        decision.Readiness,
        decision.Diagnosis,
        decision.Candidates,
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
and timeouts, refresh a target's decision once the previous decision expires,
at most ten seconds after its evaluation. `Report` copies published state
without evaluating evidence, so an idle target keeps its last decision;
compare `ExpiresAt` with the current time before relying on it. A tracked
target without an evaluation appears with zero times and `Cold` readiness, and
the overflow aggregate is never reported.

Policy version 1 selects `CandidateRetry` for a `Transient` diagnosis and
`CandidateBreakerOpen` for `DependencyDown` or `Saturation`. Candidates
require `Ready` evidence; a matching diagnosis without it selects nothing and
adds `ReasonReadinessRequired`. `Changes` keeps the latest 256 candidate
changes with contiguous sequence numbers, so a gap between reads means older
changes were overwritten. `Enforce` applies `CandidateRetry` as described
below. `CandidateBreakerOpen` is reported for review only.

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
  deadlines, 429, and other statuses are never retried.
- The target's current decision includes `CandidateRetry` and has not
  expired, both after the initial attempt and again just before the retry.
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
internal-fault visibility, self-disable signal, and budgeted replay-safe
retries are implemented. Breaker and timeout mitigation and applied-action
auditing remain design requirements. The full contract is recorded in
[ADR-0002](docs/adr/0002-host-application-inviolability.md).

## Architecture

![Curo system context](docs/design/diagrams/system-context.svg)

The [engine high-level design](docs/design/engine.md) describes component
boundaries, runtime paths, failure containment, state ownership, mitigation
controls, observability, and the test strategy expected from the implementation.

Architectural decisions are recorded separately so that rejected alternatives
and long-term constraints remain reviewable. See the
[ADR index](docs/adr/README.md).

The [documentation index](docs/README.md) links architecture, design, and
project governance material.

## Implementation sequence

The next milestones are:

1. Add adaptive breaker and timeout controls with safety tests and benchmarks.
2. Publish operational guidance after behavior is measured.

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
