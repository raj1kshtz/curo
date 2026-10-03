# ADR-0009: Bound Slow Reads with Adaptive Timeouts

- **Status:** Accepted
- **Date:** 2026-10-03
- **Scope:** Outbound adaptive timeout mitigation
- **Decision owners:** Curo maintainers

## Context

A dependency that stops answering holds every request sent to it until the
caller's deadline or a timeout of the base transport ends the wait. Many
callers have neither: `http.DefaultClient` has no `Timeout`, and
`http.DefaultTransport` sets no `ResponseHeaderTimeout`. Goroutines and
connections then accumulate while the dependency hangs. The hung attempts do
not end, so they do not reach diagnosis either, and the dependency breaker
under [ADR-0008](0008-adaptive-dependency-breakers.md) cannot see the outage.

A static timeout requires tuning for each dependency, which
[ADR-0003](0003-adaptive-policies-over-static-configuration.md) rejects as
the primary model. ADR-0003 does allow operators to set hard bounds such as
minimum and maximum timeout values. [ADR-0005](0005-operating-modes.md)
allows only `Enforce` to change timeouts.

Curo already records each initial attempt's time to response headers in
fixed latency buckets. Applying a timeout derived from them changes the
`http.RoundTripper` contract a caller observes: a request can fail with an
error Curo created, the base transport receives a copy of the request with a
derived context, and the response body is wrapped. That is a new exported
compatibility promise and a new form of autonomous authority, so it requires
a decision record.

A target is a scheme, host, port, and method class, not a path, so one
latency distribution mixes fast and slow endpoints. A timeout also hides the
evidence that would correct it: an attempt Curo ends never shows how long the
dependency would have taken.

## Decision

In `Enforce`, Curo will bound how long a **read request** waits for response
headers with a **process-local adaptive timeout per regular read target**.

The timeout bounds the interval Curo already measures as latency: from
handing the attempt to the base transport until the base transport returns a
response or an error. That includes connection setup and writing the request.
Reading the response body is never bounded.

Only read-class requests (`GET`, `HEAD`, `OPTIONS`, `TRACE`, and the empty
method) to regular targets are timed. Writes are never timed, because a write
cut short may still take effect and the caller could not tell. `CONNECT`,
other methods, the overflow aggregate, and dependency breaker probes are
never timed. A probe is still sent unchanged, as ADR-0008 requires.

The timeout is selected from retained evidence:

- Each target retains its latency samples in the recent window and the
  historical baseline, which together cover about the last 30 minutes.
- A target selects a timeout only when it retains at least 100 samples.
- The timeout is three times the upper bound of the slowest non-empty latency
  bucket, clamped to the operator's floor and ceiling.
- The selection depends only on latency. It does not depend on the diagnosis
  class or on readiness.

Policy version 3 replaces version 2. It keeps the diagnosis candidates
unchanged and adds a timeout candidate and the selected timeout to each plan.
The timeout expires with the plan. A request whose plan with a timeout has
expired re-evaluates the target, so the timeout does not lapse while its
evidence is retained.

`WithTimeoutBounds(minimum, maximum)` sets the floor and ceiling. The default
is 2 seconds and 30 seconds, which allows only 2, 3, 7.5, 15, and 30 second
timeouts. Passing zero for both disables adaptive timeouts. Otherwise the
floor must be positive and must not exceed the ceiling. No other timeout
parameter is configurable.

A timeout applies to an attempt only when:

- The request started in `Enforce` and was admitted by a closed breaker.
- Its caller has not canceled it through its context or deprecated `Cancel`
  channel.
- The caller's deadline, if any, is later than the timeout would be. Curo
  never extends or replaces a caller's deadline.

A timed attempt reaches the base transport as a shallow copy of the request
whose context Curo derives from the caller's. If the timeout elapses before
the base transport returns, Curo cancels that context with its own cause. The
timeout is cooperative: the attempt ends when the base transport honors the
cancellation. Exactly one of the timer and the attempt's return settles the
attempt:

- If the timer settled it, `RoundTrip` returns a nil response and the
  exported sentinel `ErrTimeout`, and closes any response that arrived late.
- If a caller cancellation reached the context first, the base transport's
  result is returned unchanged. A caller cancellation is never reported as a
  timeout.
- Otherwise the result is returned. A response body is wrapped so that
  closing it, or reading it to the end, releases the derived context. A body
  that implements `io.Writer`, such as a 101 Switching Protocols connection,
  keeps it and is released only on close.

`ErrTimeout` reports true from `Timeout` and `Temporary`, and
`errors.Is(ErrTimeout, context.DeadlineExceeded)` is true, like the timeout
error of an `http.Client`. An `http.Client` wraps it in `*url.Error`.

An attempt the timeout ended is recorded as a timed-out attempt with a latency
sample at the timeout, which is a lower bound of its true latency. The next
evaluation therefore selects a longer timeout, and a sample that would raise
the timeout evaluates the target at once. With the default bounds, a hung
dependency is cut after 2, then 7.5, then 30 seconds. Only a cut at the
ceiling is a dependency failure, which diagnosis and the breaker count. Below
the ceiling, a cut shows only that the timeout was too short, so it is only a
latency sample. Diagnosis does not count it among relevant attempts, so
cutting slow reads cannot dilute the failure rate of a dependency that is
also failing. Until its cuts reach the ceiling, a dependency that only slows
down shows up in latency evidence alone.

An attempt the timeout ended is never retried, because a retry would double
the load on a dependency that is already slow. A retry under
[ADR-0006](0006-retry-budgets.md) of another failure uses the same timeout,
checked again against the caller's deadline before the retry starts.

Authority follows ADR-0005:

- `Off` and `Observe` never end an attempt.
- `Observe` computes the same timeout at the start of each request and counts
  initial attempts that took longer in `Stats.ShadowTimeouts`.
- A mode change, `Close`, or self-disable after a request started withdraws
  its timeout if the timer has not fired yet, as it withdraws a retry that has
  not started.

Failure containment follows [ADR-0002](0002-host-application-inviolability.md):

- The timer callback runs on the runtime's timer goroutine inside a recovery
  boundary. It only cancels the context Curo derived, through cancellation the
  base transport already supports.
- If starting the timeout fails, the original request is sent untimed.
- If settling a returned attempt fails, its result is returned.
- If the base transport panics, the timer is stopped, the derived context is
  released, and the panic is not recovered.

Visibility is limited to the `ErrTimeout` sentinel, the `CandidateTimeout`
candidate, the `Timeout` and `Latency` fields of each reported decision, and
the `Timeouts` and `ShadowTimeouts` counters in `Stats`.

## Decision Drivers

- Release caller resources held by a dependency that stopped answering, even
  when the caller set no deadline.
- Make hung dependencies visible to diagnosis and the dependency breaker.
- Derive the limit from observed latency, with operator bounds only.
- Avoid cutting a request the dependency would answer in normal operation.
- Never extend a caller's deadline or report a caller's cancellation as a
  timeout.
- Never cut a write whose side effect the caller could not determine.
- Keep Curo failures fail-open under ADR-0002.
- Keep requests without a timeout free of Curo allocations.

## Options Considered

1. **Slowest retained bucket times three, reads only - selected.**
   - Strengths: covers every endpoint exercised on a target in the last 30
     minutes, allows at least three times the slowest observed response
     unless the ceiling caps it, and is deterministic and auditable.
   - Weaknesses: one slow response holds the timeout high for about 30
     minutes, and fast endpoints that share a target with slow ones get a
     loose limit.
2. **High quantile plus a margin - rejected.**
   - Strengths: tighter limits that ignore rare outliers.
   - Weaknesses: a target spans every path on a host, so a rarely used slow
     endpoint below the quantile would be cut on every call.
3. **Static configured timeout - rejected.**
   - Strengths: familiar, and available today as `http.Client.Timeout` or
     `ResponseHeaderTimeout`.
   - Weaknesses: requires the per-dependency tuning that ADR-0003 rejects.
4. **Bound writes as well - rejected.**
   - Strengths: protects more callers from hung dependencies.
   - Weaknesses: a write cut short may still take effect, and the caller
     could not tell.
5. **Bound the response body as well - rejected.**
   - Strengths: also covers stalled body streams.
   - Weaknesses: body duration depends on response size and on how fast the
     caller reads, which Curo cannot judge. Streaming responses would be cut.
6. **Retry an attempt the timeout ended - rejected.**
   - Strengths: may hide one slow response.
   - Weaknesses: doubles traffic to a dependency that is already slow.
7. **Count every cut as a dependency failure - rejected.**
   - Strengths: opens the breaker sooner for a hung dependency.
   - Weaknesses: a timeout that is too short would make a slow but working
     dependency look down.
8. **Per-route timeouts and hedged requests - deferred.**
   - Strengths: tighter limits for mixed targets, lower tail latency.
   - Weaknesses: per-route state needs a bounded route classifier, and
     hedging adds traffic that needs its own budget and decision.

## Consequences

### Positive

- In `Enforce`, a read to a dependency that stops answering ends after at
  most the ceiling, 30 seconds by default, even without a caller deadline.
- Cuts at the ceiling are dependency failures, so a hung dependency can be
  diagnosed as `DependencyDown` and its breaker can open.
- Operators bound or disable the timeout with one option and tune nothing per
  dependency.
- `errors.Is(err, curo.ErrTimeout)` identifies a Curo timeout through
  `http.Client`, and generic timeout checks also work.
- `Observe` previews the effect through `Stats.ShadowTimeouts` before
  `Enforce` is enabled.

### Negative

- A rarely used slow path on a fast host is cut until the censored samples
  raise the timeout, once per escalation step.
- A read that legitimately waits longer than the ceiling for response headers
  is always cut. Such reads need a higher ceiling, disabled timeouts, or a
  separate client.
- After an incident, or after a single slow response, the timeout stays high
  for up to about 30 minutes, until the slow samples age out.
- A target with fewer than 100 samples in about 30 minutes never gets a
  timeout.
- The timeout is cooperative. A base transport that ignores context
  cancellation keeps the request waiting, and Curo returns `ErrTimeout` only
  when it returns.
- A timed request allocates its request copy, derived context, timer, and
  attempt state. The derived context stays registered with a cancellable
  parent until the response body is closed or read to the end.
- Callers that type-assert a timed response body lose its concrete type.
- A request in flight loses its timeout when the mode changes, even if
  `Enforce` is restored.
- Breaker probes are never timed, so a probe to a hung dependency waits for
  the caller's deadline or the base transport.
- Timeout state is process-local, so replicas learn their timeouts
  independently.

## Revisit When

This decision should be reconsidered if production evidence shows false cuts
or timeouts held high by rare outliers, if users need per-route timeouts,
body-phase bounds, or timeouts for writes with an explicit idempotency opt-in,
or if hedged requests are proposed. A timeout coordinated across processes
requires a new ADR because it would add the shared state that
[ADR-0001](0001-embedded-library-over-sidecar-proxy.md) excludes.
