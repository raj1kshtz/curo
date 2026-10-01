# Curo Engine High-Level Design

- **Status:** Design target for `v0.1.0`
- **Updated:** 2026-09-30
- **Scope:** Embedded Go engine for outbound `net/http` calls
- **Audience:** Maintainers, contributors, reviewers, and adopters

> [!NOTE]
> This document describes the intended `v0.1.0` architecture. Sections state
> which parts are implemented today; everything else remains a design target.

## 1. Executive Summary

Curo is designed as an embedded Go resilience engine that wraps an
application's outbound `http.RoundTripper`. It observes bounded transport
evidence, produces an explainable diagnosis, and may apply a bounded mitigation
when the instance is explicitly placed in `Enforce` mode.

The engine is local to one host process. It does not depend on a sidecar,
control plane, external data store, or third-party runtime module. Each Curo
instance owns its configuration, mode, bounded target registry, adaptive
state, and lifecycle worker.

Host application safety has precedence over mitigation. Curo-owned failures
are contained at internal boundaries. A failure before a transport attempt
falls back to the untouched request exactly once. A failure after a result is
captured returns that result. Curo never blindly replays an attempt whose
side-effect state is unknown.

## 2. Governing Decisions

This design is governed by the following accepted ADRs:

- [ADR-0001](../adr/0001-embedded-library-over-sidecar-proxy.md):
  embedded Go library for `v0.x`.
- [ADR-0002](../adr/0002-host-application-inviolability.md):
  host application inviolability.
- [ADR-0003](../adr/0003-adaptive-policies-over-static-configuration.md):
  deterministic adaptive policy.
- [ADR-0004](../adr/0004-zero-dependency-core.md):
  standard-library-only root module.
- [ADR-0005](../adr/0005-operating-modes.md):
  `Off`, `Observe`, and `Enforce`.
- [ADR-0006](../adr/0006-retry-budgets.md):
  aggregate retry budgets.
- [ADR-0007](../adr/0007-root-api-and-internal-packages.md):
  root public API and internal implementation.

If this document conflicts with an accepted ADR, the ADR controls and this
document must be corrected.

## 3. Goals

The `v0.1.0` engine must:

1. Integrate explicitly with outbound `net/http` clients.
2. Preserve the wrapped transport's behavior when Curo is off or degraded.
3. Keep every Curo-owned resource bounded.
4. Produce deterministic and auditable diagnoses and actions.
5. Prevent retries from creating unbounded traffic amplification.
6. Support staged adoption through `Observe` before `Enforce`.
7. Contain recoverable failures originating in Curo-owned code.
8. Avoid locks across network I/O or host-provided callbacks.
9. Remain testable with injected time and deterministic decision inputs.
10. Keep the root module free of third-party dependencies.

## 4. Non-Goals

The first engine does not provide:

- A sidecar proxy, daemon, or centralized control plane.
- SDKs for languages other than Go.
- Fleet-wide state or cross-process retry coordination.
- Ingress middleware or gRPC interception.
- Request or response body inspection.
- Response caching or business-level fallback values.
- Machine-learning policy.
- Persistent adaptive state across process restarts.
- Arbitrary workflow execution.
- Public replay and mitigation-control types beyond the transport,
  statistics, and decision report API.

## 5. Architectural Invariants

The implementation must preserve these invariants:

1. **Original request integrity.**
   The caller's request is never mutated.
2. **No unknown replay.**
   Curo never retries when an earlier attempt may have produced an unknown
   side effect and replay safety is not established.
3. **Bounded ownership.**
   Registries, windows, queues, attempts, goroutines, and labels have hard
   upper bounds.
4. **Explicit authority.**
   Only `Enforce` may change request behavior.
5. **Direct escape path.**
   `Off` and self-disabled operation reach the base transport without entering
   the adaptive pipeline.
6. **No lock across I/O.**
   All decision data needed for an attempt is copied into an immutable
   snapshot before calling the base transport.
7. **No request-scoped goroutine.**
   A request does not create a Curo goroutine.
8. **Deterministic policy.**
   Equal ordered evidence, configuration, time, and random input produce the
   same decision.
9. **No sensitive observation.**
   Bodies, credentials, cookies, query values, and raw headers are excluded
   from adaptive state and telemetry.
10. **Visible autonomous behavior.**
    Every proposed or applied mitigation has a reason and policy version.

## 6. System Context

![Curo system context](diagrams/system-context.svg)

[Diagram source](diagrams/system-context.mmd)

Curo runs inside the host Go process. The application owns the HTTP client,
request, context, and base transport. Curo owns only its wrapper and internal
state.

The base transport remains the only component that performs network I/O.
Curo does not resolve an alternative destination, rewrite the request URL, or
introduce a service dependency.

Configuration enters through the public API. Sanitized operational evidence
leaves through the observability boundary. Optional OpenTelemetry or
Prometheus integrations belong in separate modules and are not part of the
core runtime.

## 7. Logical Architecture

![Curo logical components](diagrams/logical-components.svg)

[Diagram source](diagrams/logical-components.mmd)

The engine has two cooperating paths:

- The **request control path** executes bounded checks and calls the base
  transport.
- The **adaptive analysis path** consumes captured attempt evidence and
  publishes immutable action plans.

The paths exchange values, not shared mutable workflows. A request may use a
plan produced by earlier evidence. A captured result may also produce a retry
plan for the same request, subject to replay and budget checks.

### 7.1 Public Boundary

The public boundary owns:

- Instance construction and validation.
- Base transport wrapping.
- Mode reads and updates.
- Lifecycle close and explicit reset.
- Read-only diagnostic and audit access.

It must not expose internal state machines, mutable registries, or policy
implementation types.

The current public surface is deliberately limited to `Transport`, `Mode`,
`Option`, `Stats`, `Report` and its detached decision types, `New`,
`WithMode`, mode access, aggregate statistics, the pull-based decision report,
and lifecycle close. Report values use closed enums and copied fields, so
internal registries, evaluation state, and policy types stay private. Retries
add no public type or option: their bounds are fixed internal constants, and
their aggregate counts appear in `Stats`. Replay opt-in for unsafe methods,
mitigation configuration, and applied-action auditing remain deferred.

### 7.2 Transport Adapter

The transport adapter implements the `http.RoundTripper` contract and divides
execution into:

1. Guarded Curo preflight.
2. An unguarded call to the host-owned base transport.
3. Guarded Curo postflight.

Preflight receives an immutable value snapshot of the request metadata it
needs. The caller's original request remains reserved for the base transport,
so a failed preflight cannot mutate fallback input or consume its body.

The base call is not placed inside a Curo recovery boundary. If the wrapped
transport panics, that panic preserves the application's baseline semantics
and is not reported as a Curo-owned failure.

When `Enforce` retries a request, guarded retry stages run after postflight
and are followed by one more unguarded base call. That retry call has no
postflight: its result is returned as captured.

### 7.3 Guard

The guard owns:

- Recovery boundaries around Curo-owned work.
- Internal failure classification and bounded counting.
- Stage-aware fallback results.
- Atomic self-disable state.
- Nested containment around host-provided observation hooks.

Recovery is not normal control flow. A recovered panic always creates an
internal failure record and may contribute to self-disable.

The implementation provides this boundary through an internal guard and an
active bounded observation, diagnosis, and candidate stage. Failures in that
stage contribute to the self-disable signal. `Transport.Report` builds its
snapshot inside the same guard, including after self-disable, and its failures
contribute as well. The base transport remains outside the guard. The retry
path adds guarded preparation and commit stages. It closes a discarded
response body through a containment boundary that runs even after
self-disable, because the body must still be released.

### 7.4 Mode Gate

The mode gate atomically snapshots authority at the start of a request. The
snapshot is immutable for that request.

Self-disable takes precedence over the configured mode:

```text
self-disabled > Off > Observe > Enforce
```

The ordering means a self-disabled instance cannot be forced back into its
failing adaptive path merely by setting `Enforce`.

The implemented gate loads one atomic authority word that holds the mode and a
generation. Every effective mode change advances the generation; setting the
current mode again changes nothing. The snapshot is an upper bound on the
request's authority: a request that started in `Observe` never retries, even
if `Enforce` is selected before its attempt completes.

A retry can be pending only for a request that started in `Enforce`, so any
later mode change first leaves `Enforce`. ADR-0005 requires that moving from
`Enforce` to `Observe` stop new interventions immediately, `Off` grants less
authority than `Observe`, and self-disable takes priority over every mode.
Curo therefore treats a retry that has not started as a new intervention. A
mode change, `Close`, or self-disable after the request started withdraws it,
even if `Enforce` is restored before the retry would start, because a request
never regains authority it lost. Authority is checked before and after the
backoff. A revocation does not interrupt the wait, so one that happens during
it takes effect when the wait ends, at most 100 milliseconds later.

### 7.5 Observer

The observer converts a completed attempt into bounded evidence. It records
initial, retry, and breaker-probe attempts separately.

It does not read or wrap a response body. Attempt latency ends when the base
transport returns a response or error.

The current implementation records completed initial attempts only. It uses a
fixed target registry and fixed rolling buckets, classifies errors without
retaining their text, maintains a disjoint bounded historical baseline, and
exposes aggregate counts through `Transport.Stats`. Each recorded initial
attempt on a regular target also funds that target's retry budget and the
instance retry budget. Retry attempts are counted only in aggregate `Stats`:
they never enter evidence or fund a budget, so they cannot inflate demand or
influence diagnosis. Per-target retry, probe, and in-flight evidence remain
deferred.

### 7.6 Diagnoser

The diagnoser applies deterministic rules to an immutable evidence snapshot.
It produces:

- A primary diagnosis class.
- Readiness state.
- Reason codes.
- Evidence timestamps.
- Expiry.

It performs no network I/O and does not mutate transport state.

The implemented diagnoser is a pure internal package. The observer creates an
immutable bounded snapshot while holding the short target lock, evaluates at
most once per ten seconds during completion traffic, and stores the result as a
cache for later internal reads. An expired internal read recomputes from
retained evidence, so a future policy does not depend on continued
completions. `Transport.Report` copies the published result without
recomputing it. The overflow aggregate skips diagnosis entirely.

Each result also summarizes the recent and historical windows it used:
dependency-relevant attempts, dependency failures, rate limits, client
failures, and the span between the first and last relevant attempts.

### 7.7 Policy Evaluator

The policy evaluator converts diagnosis and safety state into an immutable
action plan. The plan may contain:

- No action.
- A retry candidate.
- A breaker transition.
- An adaptive timeout candidate.
- An audit-only proposal.

A plan is not authority. The mitigation coordinator still verifies mode,
freshness, request eligibility, context, and capacity.

The implemented evaluator is a pure internal package with policy version 1.
It runs whenever a diagnosis is published, under the same target lock, and
maps classes to a closed candidate set:

| Diagnosis | Candidate |
| --------- | --------- |
| `Transient` | Retry |
| `DependencyDown` | Breaker open |
| `Saturation` | Breaker open |
| Any other class | None |

Candidates require `Ready` evidence. A mapped class without it selects no
candidate and records a `ReadinessRequired` reason. A plan expires with the
diagnosis it was derived from. Plans are published through `Transport.Report`.
In `Enforce`, a current plan with the retry candidate lets the retry control
proceed with its own checks. The breaker-open candidate is reported but not
applied.

### 7.8 Mitigation Coordinator

The coordinator owns the final safety gates for retries, breaker behavior, and
timeouts. It does not own long-term evidence.

Before network I/O, it copies all required state into an immutable request
snapshot and releases internal locks.

The implemented coordinator covers retries only and lives in the root
transport. Postflight checks request eligibility and reserves retry budget. A
final guarded commit check after the backoff verifies authority, caller
cancellation and deadline, and the published plan again. No lock is held
during the backoff or either base transport call.

### 7.9 Base Transport

The base transport is host-owned. Curo:

- Calls it without holding an internal lock.
- Does not close or replace it.
- Does not change its global configuration.
- Does not recover its panics.
- Preserves its response and error when no mitigation is applied.

## 8. Runtime Paths by Mode

![Curo operating modes](diagrams/operating-modes.svg)

[Diagram source](diagrams/operating-modes.mmd)

### 8.1 Off

For each new request:

1. Load the self-disable and mode state.
2. If either selects direct pass-through, call the base transport.
3. Return the base response and error.

The Off path performs no target lookup, observation, diagnosis, or audit event.
It is the smallest Curo request path.

### 8.2 Observe

For each new request:

1. Snapshot `Observe`.
2. Call the base transport exactly once with the original request.
3. Capture the response or error.
4. Record bounded evidence.
5. Produce a diagnosis.
6. Evaluate a proposed action.
7. Emit an audit record marked `applied=false`.
8. Return the original captured result unchanged.

Observe does not add a timeout, delay, retry, fail-fast response, or breaker
probe. It funds retry budgets from recorded initial attempts but never
reserves from them, so credit earned while observing is available after a
switch to `Enforce`.

The current implementation performs steps 1 through 6 and step 8. Step 7 is
pull-based: a candidate change is retained in a bounded journal that
`Transport.Report` exposes, without an applied flag because Observe applies no
candidate. The captured base response and error are unchanged.

### 8.3 Enforce

For each new request:

1. Run guarded preflight and classify the target.
2. Load a fresh immutable action snapshot.
3. Apply breaker and timeout gates.
4. Call the base transport without a Curo recovery wrapper.
5. Capture the result before running more Curo logic.
6. Run guarded postflight and update bounded evidence.
7. Evaluate retry eligibility and acquire budget atomically.
8. Perform cancellable backoff when a retry is authorized.
9. Call the base transport again within the hard attempt ceiling.
10. Emit proposed and applied action records.
11. Return the deterministically selected captured result.

The exact result-selection rule across multiple completed attempts is part of
the public error and response contract.

The current implementation has no breaker or timeout gates, so steps 2 and 3
do nothing yet. Steps 6 through 9 implement the retry path:

1. Postflight records the initial attempt, refreshes an expired decision, and
   checks retry eligibility. Its last step reserves one token from both retry
   budgets.
2. Curo waits a cancellable 25 to 100 millisecond backoff, clones the request,
   and obtains a fresh body from `GetBody` when the request has one.
3. A final guarded commit check confirms that the request's authority is still
   current, the caller has not canceled the request, the caller's deadline
   still leaves time for an attempt as long as the first, and the target's
   published plan still permits a retry.
4. Curo commits the reservation, closes the discarded first response body,
   and calls the base transport once more with the clone.

Step 10 is not implemented: decisions are reported without applied-action
records, and `Stats` counts retries in aggregate. For step 11, a retry that
starts supplies the result, whether a response, an error, or a panic from the
base transport. A request without a started retry returns its first captured
result untouched. A request makes at most two base transport calls.

## 9. Failure Containment

![Curo failure containment](diagrams/failure-containment.svg)

[Diagram source](diagrams/failure-containment.mmd)

### 9.1 Guarded Partitions

Curo does not place one broad `recover` around the entire `RoundTrip` method.
Instead, it guards only Curo-owned partitions:

```text
mode and self-disable check
    -> guarded preflight
    -> unguarded base RoundTrip
    -> capture response and error
    -> guarded postflight and retry reservation
    -> guarded backoff, clone, and body replay
    -> guarded commit check
    -> contained close of the discarded response body
    -> unguarded base retry attempt
    -> return the retry result
```

This structure preserves host-owned transport panics and removes Curo-owned
fallible work from the interval between starting an attempt and capturing its
result.

If a retry stage before the commit fails or declines, Curo returns the reserved
token to both budgets and the first captured result to the caller. A failure
in a guarded retry stage counts like any other stage failure. After the
commit, closing the discarded body cannot stop the retry: a `Close` error is
ignored, and a `Close` panic is counted as an internal failure.

### 9.2 Failure Before an Attempt

If guarded preflight fails:

- Record the internal failure.
- Evaluate self-disable.
- Discard any partially built clone or action plan.
- Call the base transport with the untouched original request exactly once.

### 9.3 Failure After Result Capture

If observation, diagnosis, policy, retry preparation, or audit preparation
fails after a response or error has been captured:

- Record the internal failure.
- Evaluate self-disable.
- Return the captured response and error.
- Do not create another attempt.

### 9.4 Ambiguous Attempt State

The implementation must be structured so Curo-owned code cannot panic between
attempt start and result capture. If a future integration cannot preserve that
invariant:

- It must not blindly replay.
- It must return an explicit failure when no captured result exists.
- It requires an ADR before implementation.

### 9.5 Background Failure

Every Curo-owned goroutine installs its recovery boundary first. If the
maintenance worker panics:

- Record the fault through a nested-safe reporting path.
- Atomically self-disable adaptive behavior.
- Stop that worker.
- Do not enter an automatic restart loop.

The registry remains memory-bounded after worker loss. Request access performs
limited lazy expiry so worker loss cannot create unbounded growth.

### 9.6 Initial Self-Disable Threshold

The initial guard uses a fixed three-entry rolling failure window. A guarded
operation contributes one failure when it returns an internal error or raises
a recovered panic. The third failure within one minute, including the exact
window boundary, atomically and permanently selects self-disabled
pass-through.

The window is updated only on failure and has fixed memory use. Successful
operations do not erase recent failures; elapsed time removes them when the
next failure is recorded. Once self-disabled, the guard skips request stages
that have not started. Failures from stages already in flight can still
increase the aggregate failure count.

The threshold is an internal safety setting rather than a public option.
The active observation stage runs only in `Observe` and `Enforce`. Its
contained failures are visible through aggregate `Stats`. Report construction
is not a request stage: it still runs after self-disable, and its contained
failures count toward the same window and aggregate count.

## 10. State and Concurrency Ownership

![Curo state ownership](diagrams/state-ownership.svg)

[Diagram source](diagrams/state-ownership.mmd)

### 10.1 Instance Ownership

One Curo instance owns:

- Immutable validated configuration.
- Atomic mode and self-disable state.
- A bounded sharded target registry.
- Per-target observations and mitigation state.
- At most one maintenance worker.
- Cancellation and close state.

There is no package-global registry or policy state.

The current observer starts no worker. It performs bounded lazy idle
replacement during target admission.

### 10.2 Request Concurrency

Application goroutines call the wrapped transport concurrently. Curo does not
spawn a goroutine for a request.

The concurrency model is:

- Atomic loads for mode, self-disable, and immutable policy pointers.
- A short registry-shard lock for target lookup or admission.
- A short per-target lock for evidence and state transitions.
- A short change-journal lock for recording or copying candidate changes.
- Immutable copies for policy evaluation and network attempts.
- Lock-free atomic words for retry budgets and reservations.
- One lock order: registry shard, then target, then change journal. A shard
  lock covers a target lock only while retiring a replaced idle target.
- No lock held during transport I/O, backoff, logging, or a host callback.

Removing a target from the registry does not invalidate a pointer already held
by an in-flight request. Normal Go reachability keeps that state alive until
the request releases it.

### 10.3 Maintenance Worker

The instance owns at most one maintenance worker. It performs:

- Idle target expiry.
- Bounded cleanup.
- Internal failure-window rotation.

It does not perform network I/O or invoke a host-provided callback.

The initial observer does not start this worker. Its fixed registry remains
bounded without background cleanup, and a full-shard miss may replace one
expired target lazily.

When a worker is introduced, `Close` cancels and joins it idempotently. It does
not close the base transport. New requests after close use direct pass-through.

## 11. Target Identity and Registry Admission

Raw URL paths and queries are not safe state keys because identifiers can
create unbounded cardinality.

The implemented target identity contains only bounded dimensions:

- HTTP or HTTPS scheme.
- Canonical IP literal with any zone identifier preserved exactly, or a
  lowercase DNS hostname with one trailing root dot removed.
- Effective port.
- A bounded HTTP method class.

It excludes:

- User information.
- Raw path.
- Raw query.
- Fragment.
- Header values.
- Resolved IP addresses.

An optional route classifier may add a low-cardinality route label. Its output
must have a length limit and remains subject to registry capacity. A classifier
panic is contained at the callback boundary.

The initial method classes are read, write, connect, and other. An empty method
uses the read class because `net/http` treats it as `GET`. Unknown schemes,
invalid ports, empty hostnames, and hostnames longer than 253 bytes use the
overflow aggregate.

Registry admission follows these rules:

1. Existing targets are reused.
2. Expired idle targets are eligible for removal.
3. New targets are admitted only while capacity remains.
4. When full, new identities use a bounded overflow aggregate.
5. High-cardinality input cannot churn established hot state continuously.

The initial registry admits at most 128 regular targets across eight lookup
shards and owns one permanent overflow target. The global admission counter,
not a per-shard quota, enforces capacity. The overflow target is
non-actionable: its mixed evidence can never authorize diagnosis or mitigation.

Hostnames are copied into detached bounded storage only when a target is
admitted. Transient lookup values cannot retain a caller's complete URL.

A target idle for 15 minutes may be replaced when a miss reaches a full
registry and its shard contains an expired entry. Replacement always allocates
fresh target state. An in-flight request may finish against its old reachable
state but cannot write into the replacement. The old target is retired before
removal, so a late completion can still update aggregate counts but cannot
publish a decision or record a candidate change. Public capacity and expiry
options remain deferred.

## 12. Observation Model

Each target owns 12 fixed 10-second buckets, giving a two-minute rolling
window. A completed initial attempt increments exactly one outcome:

- Success for a 2xx or 3xx response without a transport error.
- HTTP failure for another response without a transport error.
- Transport failure.
- Caller cancellation.
- Timeout, classified as caller-owned or transport-owned.

HTTP response class is a parallel bounded dimension whenever a response
exists. Therefore attempt count equals the sum of the five outcome counts.
Retry, probe, and in-flight high-water evidence remain deferred.

Each attempt also contributes exactly one diagnosis signal:

- Neutral.
- Caller-owned cancellation or timeout.
- Explicit HTTP 429 rate limit.
- Non-429 HTTP 4xx client failure.
- Transport, transport-timeout, or HTTP 5xx dependency failure.

An error takes precedence over a response status, so a response returned
alongside a transport error cannot be mistaken for rate limiting. Caller-owned
signals remain recorded but do not contribute to readiness or dependency
failure rates.

The latency histogram supports bounded approximate quantiles such as p95 and
p99 without retaining individual samples. It has fixed bounds from one
millisecond through 30 seconds plus an overflow bucket. Only attempts that
return a response without a transport error contribute to latency evidence, so
caller cancellation and transport failure do not distort a future timeout
baseline.

Completion time selects the rolling bucket. Each bucket stores its generation.
A completion older than a newer generation already occupying the same slot is
dropped rather than erasing current evidence.

Snapshot reads include only generations inside the requested window. Old ring
slots are ignored even if a target resumes after a long idle interval.

Each actionable target also owns 30 fixed one-minute historical buckets.
Historical snapshots omit every bucket that overlaps the two-minute recent
window. Depending on the current minute boundary, this leaves 27 to 28 minutes
of disjoint comparison history. One recent admission decision controls updates
to both windows, completion timestamps move only forward, and an older
completion cannot publish an older diagnosis.

Initial demand, retry traffic, and probe traffic remain separate. Retry
attempts never replenish a retry budget or inflate original demand.

Caller cancellation is recorded for audit but does not count as dependency
failure unless transport evidence independently identifies a dependency
failure.

The observer classifies errors into stable categories. It does not persist raw
error text. Error-chain inspection is cycle-aware, recognizes bounded
per-node cancellation and timeout semantics, and stops after 32 nodes.
Malformed or over-budget chains fall back to transport-failure evidence rather
than delaying the captured result indefinitely.

The two-minute window can make only sufficiently active targets ready.
Lower-volume targets may become ready from the bounded historical window.

## 13. Readiness and Diagnosis

### 13.1 Readiness

Readiness is separate from diagnosis:

- `Cold`: no dependency-relevant observation has been retained.
- `Warming`: relevant recent evidence exists but neither readiness path is
  satisfied.
- `Ready`: either 20 relevant recent attempts span at least 30 seconds, or 20
  relevant historical attempts span at least ten minutes.
- `Stale`: prior relevant evidence exists but the two-minute recent window has
  no relevant attempts.

Relevant attempts exclude caller-owned cancellation and caller-owned timeout.
This prevents caller behavior from warming dependency state. The
non-actionable overflow aggregate never becomes ready.

Insufficient readiness means no autonomous intervention.

### 13.2 Diagnosis Classes

The initial closed set is:

| Class | Meaning |
| ----- | ------- |
| `Healthy` | Evidence remains within the ready baseline |
| `Transient` | Isolated retryable failure without sustained decline |
| `DependencyDown` | Broad transport, timeout, or server failure |
| `Saturation` | Rate limiting or overload evidence |
| `ClientError` | Request-side failure that mitigation cannot repair |
| `Degrading` | Sustained latency or error movement from baseline |

`None` is the sentinel when evidence supports no class. The public API exports
it as `DiagnosisNone`. Internal policy must not invent unbounded diagnosis
labels.

### 13.3 Deterministic Rule Order

The diagnoser applies conservative ordered rules to aggregate target evidence:

1. `ClientError` requires at least three client failures and at least half of
   relevant recent attempts.
2. `Saturation` requires at least three rate limits, ten relevant recent
   attempts, and a rate-limit share of at least 20 percent.
3. `DependencyDown` requires at least five dependency failures, ten relevant
   recent attempts, and a dependency-failure share of at least 50 percent.
4. `Degrading` requires a ready historical baseline, at least 20 relevant
   recent samples and 50 historical comparison samples, then either a
   20-percentage-point dependency-failure increase or a two-bucket p95 latency
   increase.
5. Remaining isolated dependency failures are `Transient`.
6. Ready evidence with at least five relevant recent attempts is `Healthy`;
   otherwise the class remains `None`.

Caller-owned signals are excluded from classification, and client or
rate-limit classes require aggregate evidence rather than one preceding
request. An isolated dependency failure may produce `Transient`. Response
status is considered only when the transport returned no error.

A diagnosis carries at most four fixed reason codes. Its immutable result is
cached for at most ten seconds. A result derived from recent evidence also
expires at the earliest retained generation boundary. It is not a
machine-learning probability.

## 14. Policy Evaluation

Policy evaluation is a pure transformation of:

- Mode and self-disable snapshot.
- Request eligibility.
- Evidence snapshot.
- Diagnosis and readiness.
- Breaker, timeout, and budget snapshot.
- Current time.
- Policy version.

It returns an immutable action plan. The evaluator performs no I/O and does
not acquire budget.

Safety precedence is:

1. Host request and context constraints.
2. Self-disable and operating mode.
3. Readiness and freshness.
4. Replay eligibility.
5. Breaker state.
6. Timeout bounds.
7. Retry budget.

A stale policy version or expired plan is rejected before use.

The implemented version 1 evaluator uses only the published diagnosis:
class, readiness, and expiry. Its readiness gate is the first implemented
safety precedence rule. The retry control checks mode, request eligibility,
cancellation, deadline, and budget itself rather than passing them through
the evaluator. Breaker and timeout inputs are added with the controls that
consume them.

The retry control is the first plan consumer. It uses a plan only when the
target is live and the plan has the current policy version, contains the
retry candidate, and has not expired. It checks again after the backoff and
never evaluates a diagnosis itself. Plans refresh lazily during completion
traffic, so a failure that arrives while an earlier `Healthy` plan is still
current is not retried.

## 15. Mitigation Controls

![Curo mitigation controls](diagrams/mitigation-control.svg)

[Diagram source](diagrams/mitigation-control.mmd)

### 15.1 Retry Budget

Retry capacity is derived from original request demand. A target budget and an
instance budget must both reserve capacity before an attempt starts.

Reservation is all-or-nothing. A partial reservation is rolled back before
returning. No request attempt starts until both levels confirm capacity.

A retry also requires:

- A retryable diagnosis.
- A method permitted by the replay policy.
- An absent or reproducible body.
- An active caller context and an open `Cancel` channel, when the request
  has one.
- Sufficient remaining deadline.
- A breaker state that permits the attempt.
- Capacity under the hard per-request attempt ceiling.

The default replay policy is conservative. Safe read methods are eligible
first. Unsafe methods require explicit application intent; the presence of an
idempotency header alone is not assumed to prove safe replay.

Backoff is cancellable, uses bounded jitter, and does not hold a lock.

When a response is discarded for a retry, its body is closed.

The implemented budgets are lock-free token buckets. Each regular target owns
one, and each transport owns one instance budget:

| Parameter | Value |
| --------- | ----- |
| Funding | 0.1 token to the target and instance budgets per recorded initial attempt on a regular target |
| Target capacity | 10 tokens |
| Instance capacity | 50 tokens |
| Starting balance | Empty |
| Cost | One token from each budget per retry |
| Per-request ceiling | One retry, so at most two base transport calls |
| Backoff | Uniform random delay from 25 to 100 milliseconds |

Budgets are funded in `Observe` and `Enforce`. Overflow attempts and retry
attempts never fund them, and tokens do not decay. Over any interval, retries
against one target therefore number at most its 10-token capacity plus one
tenth of the initial attempts recorded for it during that interval. The
instance budget applies the same bound across all targets with its 50-token
capacity. A replaced idle target starts with an empty budget.

A reservation moves a token from available credit into a pending count.
Available credit plus pending reservations never exceeds capacity, so a
deposit cannot refill capacity that a pending reservation may still spend.
The target token is reserved first and returned if the instance budget is
empty. A reservation is held only from postflight to the commit check: the
commit spends it just before the retry starts, and every other exit returns
it.

The implemented eligibility rules are:

- **Replay safety:** the method is `GET`, `HEAD`, `OPTIONS`, `TRACE`, or
  empty, and the body is absent, `http.NoBody`, or reproducible through
  `GetBody`. Unsafe methods have no opt-in yet.
- **Outcome:** a transport error attributed to the dependency, or a 502, 503,
  or 504 response without a `Retry-After` field. A `Retry-After` field under
  any key casing, even an empty or malformed one, means the dependency asked
  callers to wait. Caller-owned cancellation and deadlines, 429, other 4xx,
  and other 5xx responses never qualify.
- **Plan:** the initial attempt was recorded on a regular target whose
  published plan is current, contains the retry candidate, and has not
  expired.
- **Cancellation and deadline:** the caller's context is active, the
  request's deprecated `Cancel` channel is nil or open, and any deadline
  leaves more than the backoff plus the initial attempt's latency.
- **Authority:** the request started in `Enforce`.

No breaker exists yet, so no breaker state forbids a retry.

An `http.Client` with a `Timeout` sets the deprecated `Cancel` channel on
requests sent through a transport it does not recognize, which includes Curo,
and also bounds the context deadline. Postflight, the backoff, and the commit
check therefore treat a closed channel like a done context, and the retry
clone keeps the channel so the base transport observes it too. Observing the
channel needs no goroutine.

Reservation is the last postflight check, so a denied reservation means every
other condition held, and it increments `RetryBudgetDenials`. After the
backoff, the commit check repeats the authority, cancellation, deadline, and
plan checks. The remaining deadline must then exceed only the initial
attempt's latency.

The retry request is a clone of the original with the same context and
`Cancel` channel. `GetBody` is called once, after the backoff and before the
commit check. A `GetBody` error or nil body prevents the retry without
counting as a Curo failure, because the body belongs to the application. A
`GetBody` panic is contained and counted. A fresh body obtained for a retry
that is then withdrawn is closed.

When the retry starts, the discarded first response body is closed without
being read. This can give up connection reuse in the base transport, but
spends no time or bytes on a drain. The backoff uses a timer, ends early when
the caller's context is done or the `Cancel` channel closes, and holds no
lock. Each transport owns its jitter function, which draws from the Go
runtime's random source; tests inject a fixed delay.

`RetryAttempts` counts base transport calls that Curo starts for retries.
Retries inside the base transport itself, such as `http.Transport` replaying a
request after a reused connection fails, are not counted. A retry allocates
its request clone and any replayed body.

### 15.2 Dependency Breaker

The dependency breaker has three conceptual states:

- `Closed`: normal attempts are allowed.
- `Open`: Enforce mode fails eligible calls fast for a bounded interval.
- `Probe`: a limited lease permits recovery evidence.

After cooldown, a bounded number of concurrent probe leases are available.
Probe success may close the breaker after its evidence requirement is met.
Probe failure starts a capped cooldown.

Observe mode advances a shadow breaker and reports what would happen. Switching
to Enforce may reuse it only when evidence is fresh and ready.

The dependency breaker is separate from Curo's internal self-disable state.

### 15.3 Adaptive Timeout

An adaptive timeout candidate uses:

- Ready latency evidence.
- A selected bounded quantile.
- A safety margin.
- A configured floor.
- A configured ceiling.
- The caller's remaining deadline.

Curo never extends an earlier caller deadline. A Curo-created timeout clones
the request with a derived context and tags the timeout source internally so
diagnosis can distinguish it from caller cancellation.

Observe records the candidate but does not create a new deadline.

### 15.4 Diagnosis-to-Control Matrix

| Diagnosis | Retry | Breaker | Timeout |
| --------- | ----- | ------- | ------- |
| `Healthy` | No | Close candidate | Baseline only |
| `Transient` | Budgeted candidate | Usually no | Candidate |
| `DependencyDown` | Probe only | Open candidate | No extension |
| `Saturation` | Normally no | Open candidate | No extension |
| `ClientError` | No | No | No |
| `Degrading` | Limited candidate | Watch or open | Candidate |

Every cell describes eligibility, not a guaranteed action.

Policy version 1 implements only the retry candidate for `Transient` and the
breaker-open candidate for `DependencyDown` and `Saturation`, each gated on
`Ready` evidence. Close, probe, watch, limited-retry, and timeout candidates
are not implemented. `Enforce` applies the retry candidate through the retry
budget rules above. The breaker-open candidate is reported only.

## 16. Observability and Privacy

### 16.1 Audit Record

Each proposed or applied action includes bounded fields:

- Timestamp.
- Mode.
- Bounded target identifier or explicit alias.
- Readiness and diagnosis.
- Reason codes.
- Action kind.
- Proposed or applied state.
- Policy version.
- Evidence and action expiry.
- Budget decision where relevant.

`Transport.Report` implements a pull-based subset. Each decision carries its
evaluation and expiry times, normalized target identity, readiness, diagnosis,
ordered reason codes, recent and historical evidence summaries, policy
version, and candidates. Mode, applied state, and per-request budget decisions
are not recorded yet; retries are visible only through aggregate `Stats`
counters. The diagnosis and plan share one expiry.

### 16.2 Excluded Data

Curo never places these values in adaptive state, logs, metrics, or audit
records:

- Request or response bodies.
- Authorization or cookie values.
- Raw headers.
- Raw query values.
- URL user information.
- Unbounded raw error strings.

### 16.3 Metric Cardinality

Metric labels come from closed enums and bounded target aliases. A raw path,
raw hostname supplied by untrusted input, or error string is not a metric
label.

`Transport.Report` exposes normalized hostnames for review. They can come from
untrusted input, so hosts should not turn them into metric labels unless the
destination set is trusted and bounded.

### 16.4 Host-Provided Sinks

Curo cannot preempt an arbitrary Go callback that blocks forever. Any
host-provided sink:

- Is invoked outside internal locks.
- Has panic containment.
- Is not called from the maintenance worker.
- Is not given mutable internal state.

The implemented delivery contract is pull-based. `Transport.Report` copies
bounded state on the caller's goroutine and invokes no host callback. Callback
sinks and structured logging remain deferred and must follow the rules above.
The core must not create a goroutine per event.

Future internal failure logging must be best effort and rate-limited. A logging
failure cannot replace the request result.

Error `Unwrap`, `Is`, and `Timeout` methods are also host-provided callbacks.
They run outside internal locks and inside postflight containment. As with any
arbitrary callback, Curo cannot preempt an implementation that blocks forever.

### 16.5 Aggregate Statistics

`Transport.Stats` exposes only:

- Completed observed request count.
- Current regular target count.
- Overflow observation count.
- Retry attempts that Curo started.
- Retry successes, whose base call returned a 2xx or 3xx response without an
  error.
- Retry budget denials.
- Contained internal failure count.
- Sticky self-disable state.

The snapshot contains no target identifiers or per-target evidence. Its fields
are sampled independently under concurrency. Overflow never exceeds observed
requests, retry successes never exceed retry attempts, retry attempts plus
budget denials never exceed observed requests, and self-disabled state implies
at least three contained failures.
Per-target decisions are available through `Transport.Report`; callbacks and
logging sinks remain deferred.

### 16.6 Decision Report

`Transport.Report` returns a detached snapshot with two bounded parts:

- `Targets` holds the latest published decision for each tracked regular
  target, at most 128, ordered by scheme, host, port, and method class. A
  target without an evaluation appears with zero times and `Cold` readiness.
  The overflow aggregate is never reported.
- `Changes` holds the latest 256 candidate changes in ascending sequence
  order. Sequence numbers start at 1 and are contiguous within one transport,
  so a gap between reads means older changes were overwritten.

A change is recorded when an evaluation selects a different candidate set, or
the same non-empty set for a different diagnosis class. Renewing an unchanged
decision is not a change. The journal is not a target lifecycle log: eviction
and staleness are not recorded. Candidates that lapse while a target is idle
are cleared, and the change recorded, at its next dependency-relevant
completion. A change can name a target that has since been replaced.

Report never evaluates evidence or reads the clock. An idle target keeps its
last decision, so consumers compare `ExpiresAt` with the current time. The
journal is copied before target decisions, so a reported decision is never
older than a retained change from the same tracked target. Every lock taken by
a report is short and released by a deferred unlock, so a contained failure
cannot leave internal state locked.

Report is safe for concurrent use and remains readable after close, in `Off`,
and after self-disable. A Curo failure while building a report returns an
empty report and counts toward `InternalFailures` and self-disable. Report
allocates its detached copies, so it suits periodic review rather than
per-request use.

## 17. Lifecycle

### 17.1 Construction

Construction:

1. Applies safe defaults.
2. Applies options.
3. Validates bounds and combinations.
4. Creates immutable configuration.
5. Creates the bounded registry and control state.
6. Starts at most one maintenance worker.
7. Defaults mode to `Observe`.

The public constructor is:

```go
func New(base http.RoundTripper, options ...Option) (*Transport, error)
```

`Transport` binds one host-owned base transport. A nil base returns
`ErrNilBaseTransport`. Invalid options return an error and no partially
constructed instance. The caller retains the original base transport and can
continue using it after any construction error.

### 17.2 Mode Change

Mode changes are atomic. They affect requests that have not yet taken their
mode snapshot. An effective change also withdraws every retry that has not
started, as described for the mode gate.

`Off` stops new observation but does not synchronously erase state. State ages
normally and is invalidated by freshness checks.

### 17.3 Self-Disable Reset

Self-disable does not reset automatically. Explicit reset:

- Clears the internal failure window.
- Revalidates lifecycle state.
- Does not silently grant `Enforce`.
- Does not make stale adaptive evidence ready.

The current public API does not expose reset. Reconstructing the `Transport`
creates a fresh guard until reset semantics and detailed failure reporting are
implemented together.

### 17.4 Close

Close is idempotent:

- Cancel the maintenance worker.
- Wait for Curo-owned work to stop.
- Release references held only for maintenance.
- Do not close the base transport.
- Do not wait for arbitrary host callback work.

In-flight requests use their captured snapshots and results. A retry that has
not started is withdrawn, and its request returns its first result. After
close, new requests use direct pass-through, mode changes return `ErrClosed`,
and the configured mode, aggregate statistics, and decision report remain
readable.
Close retains the bounded observer state so an in-flight postflight can finish
safely.

## 18. Failure Model

| Failure | Engine response |
| ------- | --------------- |
| Preflight Curo panic | Report, count, call original transport once |
| Postflight Curo panic | Report, count, return captured result |
| Decision report Curo panic | Count, return an empty report |
| User observer panic | Contain and report outside locks |
| Maintenance panic | Self-disable and stop worker |
| Registry full | Use bounded overflow state |
| Evidence stale | No autonomous intervention |
| Budget exhausted | Count a budget denial, return the first result |
| Retry preparation or commit Curo panic | Count, return the first result |
| `GetBody` error or nil body | Return the first result without counting |
| `GetBody` panic | Count, return the first result |
| Mode change, close, or self-disable before a retry starts | Withdraw the retry, return the first result |
| Discarded body close error | Ignore, continue the retry |
| Discarded body close panic | Count, continue the retry |
| Caller context done | Stop waits and attempts |
| Base transport panic | Preserve host transport semantics |
| Runtime fatal or OOM | Outside recoverable guarantee |

Failures do not create success-shaped fallback values.

## 19. Performance and Resource Targets

The first implementation must meet these structural targets:

| Area | Target |
| ---- | ------ |
| Off path | No Curo allocation, lock, or goroutine |
| Request execution | No request-scoped goroutine |
| Instance workers | At most one core maintenance worker |
| Registry memory | Linear only in configured fixed capacity |
| Per-target memory | Fixed after target admission |
| Canonical target hit and observation update | No heap allocation after warm admission |
| I/O boundary | No Curo lock held |
| Retry attempts | Hard bounded per request and by budgets |
| Metric labels | Closed or capacity-bounded |

Latency and allocation benchmarks compare Curo modes with the same base
transport. The initial harness establishes a local baseline; numerical
regression gates wait for a repeatable CI benchmark environment.

The observer, diagnoser, policy, and retry budget store 9,464 bytes of bounded
target state on 64-bit platforms. The 128 regular targets plus overflow
aggregate therefore use about 1.16 MiB, and the 256-entry change journal adds
38,928 bytes, excluding bounded map and key overhead. A report allocates
detached copies of at most 128 decisions and 256 changes.
Benchmarks cover Off, warm Observe, and warm Enforce paths with an already
canonical target and no retry; all three perform zero Curo heap allocations
per request with the benchmark base transport. Inputs that require case or IP
normalization may use bounded transient allocation.

## 20. Security Considerations

Untrusted inputs include URLs, methods, status codes, timing, error behavior,
and optional route labels.

The engine mitigates:

- Cardinality attacks through normalized keys and fixed capacity.
- Retry amplification through two-level budgets.
- Secret leakage through explicit data exclusions.
- Request duplication through replay eligibility and stage-aware containment.
- Race failures through synchronized ownership and mandatory race tests.
- Callback panics through guarded host boundaries.

Curo does not prevent SSRF already present in application request
construction. It must not change a request destination or make a new class of
destination reachable.

## 21. Test Strategy

### 21.1 Deterministic Unit Tests

Use injected clocks and random sources. Tests must not sleep to wait for state
transitions.

### 21.2 State-Machine Tests

Model:

- Operating modes.
- Self-disable.
- Dependency breaker.
- Registry admission and expiry.
- Retry budget reservation and rollback.

Assert illegal transitions cannot occur.

### 21.3 Fault Injection

Inject a panic or error at every Curo-owned boundary and assert:

- The panic does not escape.
- The safe result rule for that stage is followed.
- The original request remains unchanged.
- No duplicate attempt is created.
- Internal failures contribute to self-disable.

### 21.4 Race and Concurrency Tests

Exercise mode changes, close, registry admission, observation updates,
readiness, diagnosis, budget reservation, and breaker probes under
`go test -race`.

### 21.5 Fuzzing

Fuzz:

- URL normalization.
- Method classification.
- Route labels.
- Error and status classification.
- Request replay eligibility.
- State-machine event sequences.

### 21.6 Simulation

Run deterministic traffic traces for:

- Healthy stable traffic.
- Latency degradation.
- Intermittent transport failure.
- Full dependency outage.
- Rate limiting.
- Recovery and breaker probing.
- High-cardinality target input.
- Simultaneous target failures.

Measure false intervention, retry amplification, recovery delay, and state
bounds.

### 21.7 Benchmarks

Benchmark:

- Off pass-through.
- Observe on an admitted target.
- Enforce without an action.
- Retry budget acquisition.
- Registry hit, miss, overflow, and expiry.
- Concurrent observation updates.

## 22. Rollout Model

The recommended adoption sequence is:

1. Install with the default `Observe` mode.
2. Review diagnoses and proposed actions.
3. Confirm target cardinality and resource bounds.
4. Enable `Enforce` on a limited application population.
5. Compare error rate, latency, retries, and Curo internal faults.
6. Expand gradually.
7. Switch to `Off` immediately if application semantics are unexpected.

Each process starts with cold local evidence after restart. There is no hidden
state dependency on another process.

## 23. Public API Contract

The initial contract is:

- `New(base, options...)` returns a `*Transport` or an error.
- One `Transport` binds one host-owned `http.RoundTripper`.
- `Transport` implements `http.RoundTripper` and `io.Closer`.
- `WithMode` selects the initial mode; `Observe` is the default.
- `Mode` and `SetMode` are safe for concurrent use.
- `Stats` returns privacy-safe aggregate counters and self-disable state. It is
  safe for concurrent use and remains readable after close.
- On an open transport, invalid modes return `ErrInvalidMode` without changing
  the active mode.
- A nil base is rejected. Uninitialized operations that require the base return
  `ErrNilBaseTransport`, while `Mode` reports `Off`.
- `Close` is idempotent and never closes the base transport.
- After close, `RoundTrip` delegates directly and `SetMode` returns
  `ErrClosed`.
- Optional Curo-owned request stages run only in guarded preflight and
  postflight partitions. The wrapped transport call is never inside that
  recovery boundary.
- A preflight stage failure delegates the untouched original request exactly
  once. A postflight stage failure returns the already captured response and
  error without another attempt.
- Three internal failures within one minute, including failures while
  building a report, permanently bypass later stages for that instance.
  Aggregate failure count and self-disable state are visible through `Stats`.
- `Observe` and `Enforce` record bounded completed-attempt evidence and
  evaluate readiness, diagnosis, and control candidates. `Off`, closed, and
  self-disabled paths do not collect new evidence.
- `Report` returns a detached snapshot of at most 128 target decisions and the
  latest 256 candidate changes. It never evaluates evidence, is safe for
  concurrent use, and remains readable after close, in `Off`, and after
  self-disable. A Curo failure while building it returns an empty `Report`.
- Decisions expose normalized target identity, readiness, diagnosis, ordered
  reason codes, recent and historical evidence summaries, policy version,
  candidates, and evaluation and expiry times. Report enums are closed, and
  their `String` methods return stable names. Only `Enforce` applies a
  candidate, and only `CandidateRetry`.
- Target state is bounded to 128 regular identities and one non-actionable
  overflow aggregate. Paths, queries, headers, bodies, URL user information,
  and raw error text are not retained.
- In `Off` and `Observe`, and in `Enforce` when no retry starts, `RoundTrip`
  passes the original request to the base exactly once and preserves its
  response, error, and panic behavior.
- In `Enforce`, a replay-safe request whose initial attempt failed with a
  transport error, or with a 502, 503, or 504 response without `Retry-After`,
  may be retried once as a clone after a cancellable 25 to 100 millisecond
  backoff. The target's current decision must select `CandidateRetry`, the
  caller must not have canceled the request through its context or `Cancel`
  channel, the caller's deadline must leave time, and both retry budgets must
  hold a token.
  A started retry's response, error, or panic is the result, and the
  discarded first response body is closed.
- A mode change, close, or self-disable withdraws a retry that has not
  started, and the request returns its first result untouched.
- `Stats` counts retry attempts, retry successes, and retry budget denials.

The following surfaces remain deferred until their implementations exist:

- Public route classification, latency evidence, and detailed per-target
  evidence access.
- Replay opt-in for unsafe methods.
- Callback or logging delivery of decisions and candidate changes.
- Mode, applied-action, and budget fields in decision records.
- Detailed internal-failure events and logging sinks.
- Fail-fast and internal-failure error types.
- Self-disable reset.
- Capacity, expiry, window, budget, and timeout options.

Each deferred surface requires API review alongside the code that gives it
meaning.
