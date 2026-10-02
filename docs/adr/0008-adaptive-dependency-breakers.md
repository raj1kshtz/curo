# ADR-0008: Fail Fast Through Adaptive Dependency Breakers

- **Status:** Accepted
- **Date:** 2026-10-02
- **Scope:** Outbound dependency breaker mitigation
- **Decision owners:** Curo maintainers

## Context

When a dependency is down, every request sent to it still waits for its own
failure: a connection error, a transport timeout, or a 5xx response. Callers
spend their deadlines, goroutines, and connections on attempts that are
expected to fail, and the dependency keeps receiving traffic while it tries to
recover. Retry budgets under [ADR-0006](0006-retry-budgets.md) bound the
additional attempts Curo creates, but they do not reduce original traffic.

Curo already diagnoses this condition. Policy version 1 reports a breaker-open
candidate for a `Ready` `DependencyDown` or `Saturation` diagnosis, but does
not apply it. Applying it changes the `http.RoundTripper` contract a caller
observes: a request can fail without reaching the network. That is a new
exported compatibility promise and a new form of autonomous authority, so it
requires a decision record.

Common breakers open after a configured number of failures or a configured
failure rate. [ADR-0003](0003-adaptive-policies-over-static-configuration.md)
rejects static thresholds as the primary model and requires hard bounds,
cooldowns, and hysteresis. [ADR-0005](0005-operating-modes.md) allows only
`Enforce` to open a breaker. [ADR-0002](0002-host-application-inviolability.md)
requires Curo's own failures to degrade to transparent pass-through, so a
Curo failure must never be what makes a request fail fast.

A rate-limited dependency differs from a down one. It is still answering, and
it asks callers to slow down rather than stop. Failing every request fast
would turn partial throttling into a complete outage for the caller.

## Decision

In `Enforce`, Curo will apply a **process-local dependency breaker per regular
target**, driven by the adaptive diagnosis rather than by a configured
threshold.

Each breaker is a deterministic state machine with three states:

- **Closed:** requests pass.
- **Open:** requests fail fast until the cooldown ends.
- **Probing:** one request holds a probe lease, and other requests fail fast.

A closed breaker opens only when all of the following hold:

- The request started in `Enforce`, was admitted by the closed breaker, and
  its authority is still current when its result is processed.
- The request's own attempt failed for a reason attributed to the dependency.
- The target's published plan is unexpired, uses the current policy version,
  and selects the breaker-open candidate.

Policy version 2 replaces version 1. Only a `Ready` `DependencyDown` diagnosis
selects the breaker-open candidate. `Saturation` remains a reported diagnosis
without a candidate, and any throttling control requires its own decision.

A request rejected by an open breaker returns a nil response and the exported
sentinel error `ErrBreakerOpen`. The base transport is not called, and Curo
closes the request body as the `http.RoundTripper` contract requires. A
rejected request is not recorded as evidence. Curo does not synthesize a
response.

Retries under [ADR-0006](0006-retry-budgets.md) require a closed breaker. A
retry that has not started when the breaker opens is withdrawn.

Recovery uses one probe lease at a time:

- The first cooldown is 5 seconds. Each failed probe, expired lease, or
  reopening within 2 minutes of a close doubles it, up to 60 seconds. An
  opening more than 2 minutes after the latest close starts again at 5
  seconds.
- After the cooldown, the next request becomes the probe unless its caller has
  already canceled it. A probe is sent to the base transport unchanged and is
  never retried.
- A probe lease lasts 30 seconds. A lease that expires without a result counts
  as a failed probe.
- A probe that receives any response other than 429 or 5xx closes the breaker,
  because the dependency answered. A 429, a 5xx, or a transport error
  attributed to the dependency reopens it with a longer cooldown. Caller
  cancellation or a caller deadline reopens it for another 5 seconds without
  escalation.
- When a probe closes the breaker, evidence recorded before the close leaves
  the recent window, so opening again requires fresh evidence. The historical
  baseline is not changed.

Authority follows [ADR-0005](0005-operating-modes.md):

- `Off` and `Observe` never open a breaker, reject a request, or send a probe.
- An open breaker keeps its state across mode changes. If `Enforce` is
  restored while it is still open, requests fail fast again until the next
  probe. A probe already sent still settles the breaker after a mode change.
- A mode change, `Close`, or self-disable after a request started prevents
  that request from opening a breaker.
- If a Curo stage fails before the attempt, the request is sent unchanged.
  Self-disabled and closed transports never fail a request fast.

The breaker's bounds are fixed internal constants. Visibility is limited to the
`ErrBreakerOpen` sentinel and aggregate `Stats` counters for opens, probes,
and rejections. Per-target breaker state, Observe-mode simulation, and
configuration options are deferred.

## Decision Drivers

- Stop spending caller resources on attempts expected to fail.
- Reduce load on a dependency that is failing broadly.
- Recover automatically without operator action.
- Derive opening from bounded adaptive evidence, not a tuned threshold.
- Bound the impact of a wrong decision in time and in traffic.
- Keep Curo failures fail-open under ADR-0002.
- Give callers an unambiguous way to detect a fail-fast result.

## Options Considered

1. **Diagnosis-driven breaker with one probe lease - selected.**
   - Strengths: reuses the readiness and diagnosis gates, needs no tuning, and
     has a bounded cooldown and one probe at a time.
   - Weaknesses: opens only after a diagnosis is ready, and probing is slower
     than with several concurrent probes.
2. **Static failure-count or failure-rate breaker - rejected.**
   - Strengths: familiar and quick to react.
   - Weaknesses: requires per-dependency tuning that ADR-0003 rejects.
3. **Synthetic 503 response instead of an error - rejected.**
   - Strengths: callers that only inspect status codes need no change.
   - Weaknesses: indistinguishable from a real server answer, and Curo does
     not create success-shaped or server-shaped fallback values.
4. **Open on `Saturation` as well - rejected for now.**
   - Strengths: also sheds load from a throttling dependency.
   - Weaknesses: rejects requests the dependency would still serve.
5. **Close when the diagnosis no longer selects the candidate - rejected.**
   - Strengths: no separate recovery state.
   - Weaknesses: an open breaker blocks the traffic that would produce new
     evidence, and aging failure evidence would release a flood of requests to
     a dependency that may still be down.
6. **Shadow breaker in `Observe` - deferred.**
   - Strengths: shows which requests would fail fast before enforcement.
   - Weaknesses: adds per-request simulation state. Reports already show the
     breaker-open candidate.

## Consequences

### Positive

- During a diagnosed outage, most `Enforce` requests to the target return
  immediately instead of waiting for a failure.
- While a breaker is open, each process sends its target only probes, one
  lease at a time.
- Recovery needs no operator action. How long a recovered dependency stays
  blocked is bounded by the 60 second maximum cooldown and the 30 second
  probe lease.
- `errors.Is(err, curo.ErrBreakerOpen)` works through `http.Client`, which
  wraps the error in `*url.Error`.

### Negative

- A target is a scheme, host, port, and method class, so failures on one path
  can open the breaker for every path on that host with the same method class.
- A breaker opens only after a `Ready` `DependencyDown` diagnosis. For a target
  with steady healthy traffic, failures must make up half of the two-minute
  recent window, so a complete outage is detected after about a minute.
  Low-volume targets may never open a breaker.
- Breaker state is process-local, so replicas open and close independently.
- A probe is a real application request, and it may fail.
- One lease is not one physical request: after a lease expires, a new probe
  can start while the earlier one is still in flight.
- A request whose caller already canceled it receives `ErrBreakerOpen`
  instead of its context error while the breaker is open.
- Callers must handle a new error that does not come from the base transport.
- Replacing an idle target discards its breaker state.

## Revisit When

This decision should be reconsidered if production evidence shows false opens
or unacceptable detection latency, if users need per-route breakers or
configurable bounds, or if rate limiting requires a throttling control. A
breaker coordinated across processes requires a new ADR because it would add
the shared state that [ADR-0001](0001-embedded-library-over-sidecar-proxy.md)
excludes.
