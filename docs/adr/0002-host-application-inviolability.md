# ADR-0002: Guarantee Host Application Inviolability

- **Status:** Accepted
- **Date:** 2026-09-30
- **Scope:** All Curo library code
- **Decision owners:** Curo maintainers

## Context

Curo executes inside the process it is intended to protect and participates in
outbound request handling. A defect in retries, state tracking, background
work, logging, or callbacks could therefore turn a dependency incident into a
host-application outage.

A manual kill switch is necessary but insufficient. It only helps after an
operator detects a problem, and it cannot help if Curo has already crashed or
wedged the process.

Go's `recover` also has strict limits:

- It only recovers a panic in the same goroutine.
- It cannot recover runtime fatal errors such as concurrent map misuse.
- It cannot recover an out-of-memory kill, stack exhaustion, `os.Exit`, or a
  process signal.
- It does not solve deadlocks, unbounded memory growth, or goroutine leaks.

Calling the requirement a literal "zero-panic guarantee" would therefore
promise more than Go can provide. The useful contract is that no recoverable
failure originating inside Curo is allowed to escape into or terminate the
host application.

## Decision

Curo adopts **host application inviolability** as its primary engineering
constraint:

> Curo-owned code must not terminate, panic through, hang, or cause unbounded
> degradation of the host process. A recoverable internal failure must be
> reported and must degrade Curo to the safest available transparent
> pass-through behavior without operator action.

This is a **zero-propagated-panic contract for Curo-owned failures**, not a
claim that Curo can make the Go runtime, operating system, wrapped transport,
or arbitrary user code infallible.

The following requirements are normative.

### 1. Contain Curo-owned panics

Every request-path entry into fallible Curo logic must have a recovery
boundary. Every goroutine started by Curo must install its recovery boundary
as its first deferred operation.

Panics from user-provided hooks invoked by Curo must also be contained at the
hook boundary. Panics raised by the application's wrapped base transport are
outside Curo's guarantee; masking them would alter the application's original
semantics and hide defects Curo does not own.

Recovery is an emergency boundary, not normal control flow.

### 2. Preserve the original request and avoid duplicate effects

Curo must not mutate the caller's original `*http.Request`. Any metadata or
header changes must be applied to a clone.

Request bodies may only be inspected or replayed when replay safety is known,
for example through `Request.GetBody` or an explicitly bounded copy.

Transparent fallback is stage-aware:

- Before any network attempt starts, fallback may delegate the untouched
  original request to the base transport exactly once.
- After an attempt may have started, recovery must not blindly replay the
  request. That could duplicate a non-idempotent operation.
- If a response or error from the base transport has already been captured,
  recovery returns that result and disables the failing Curo path.
- Curo-owned fallible processing must be structured outside the interval
  between starting an attempt and capturing its result. If that invariant
  cannot be maintained, returning an explicit error is safer than creating an
  unknown duplicate side effect.

Fail-safe does not mean "retry after every panic."

### 3. Disable Curo automatically after repeated internal failures

Curo will track its own bounded internal-failure signal. Repeated failures
within a defined window atomically disarm adaptive behavior and reduce future
requests to the smallest direct pass-through path.

The disarmed path must not depend on the component that caused the failure.
Re-enabling requires an explicit reset or construction of a new instance; the
library must not oscillate automatically between healthy and failing states.

Exact thresholds and state transitions belong in the engine design rather
than this ADR.

### 4. Report failures without endangering fallback

An internal failure must be reported through the configured structured logger
and observable counters. Reporting is best effort and must have its own
containment boundary because a user-supplied logging handler can also fail.

Sensitive request bodies, credentials, cookies, and authorization headers must
never be included in failure reports.

### 5. Prohibit process-terminating behavior

Non-test library code must not call:

- `panic`.
- `os.Exit`.
- `log.Fatal*` or `log.Panic*`.
- Equivalent helpers that terminate or unwind the host process.

CI must enforce these restrictions where static analysis can detect them.

### 6. Bound time, memory, concurrency, and cardinality

- No lock may be held across network I/O or a user callback.
- Internal waits must be cancellable and bounded by context or an explicit
  deadline.
- State keyed by routes or dependencies must have a fixed capacity and an
  eviction policy.
- Queues, retry work, samples, and background goroutines must all have defined
  upper bounds.
- Concurrent state must use synchronization that is race-tested; an
  unsynchronized map shared between goroutines is prohibited.
- Shutdown must be idempotent and must not leak goroutines.

### 7. Prove the contract continuously

The test strategy must include:

- Fault injection at each internal component boundary.
- Tests proving the original request is preserved on fallback.
- Tests proving fallback cannot duplicate an unsafe request.
- Panic tests for every Curo-owned goroutine and user callback boundary.
- Race-detector runs on every supported Go version and operating system.
- Fuzzing of attacker-controlled request metadata and transport results.
- Resource-bound and shutdown tests.

Any bug that allows a Curo-owned panic to escape, creates unbounded resource
growth, or wedges request handling is release-blocking.

## Options Considered

1. **Let panics propagate and rely on application recovery — rejected.**
   A library must not require every caller to defend against its defects.
2. **Recover only in `RoundTrip` — rejected.**
   This does not protect background goroutines or user hook boundaries.
3. **Unconditionally replay after recovery — rejected.**
   This can duplicate non-idempotent operations.
4. **Catch every panic, including the wrapped transport — rejected.**
   This masks host-owned failures and changes baseline semantics.
5. **Use layered containment, stage-aware fallback, and self-disable —
   selected.**

## Consequences

### Positive

- A recoverable Curo defect degrades resilience behavior instead of crashing
  the host.
- The direct pass-through path remains available independently of the adaptive
  engine.
- Failures remain visible rather than silently disappearing.
- Safety expectations are concrete enough to test and review.

### Negative

- Recovery boundaries, stage tracking, and fault injection add implementation
  complexity.
- The hot path pays a small cost for containment and state checks.
- Some failures cannot safely produce a successful fallback response after a
  network attempt starts.
- Containment can delay discovery of internal defects unless telemetry and
  release gates are treated seriously.
- This contract cannot protect against unrecoverable Go runtime or operating
  system failures.

## Revisit When

The constraint itself is not expected to be relaxed. A new ADR is required if
an implementation cannot satisfy one of these invariants or if a new
integration boundary changes what Curo owns.
