# ADR-0006: Govern Retries with Aggregate Budgets

- **Status:** Accepted
- **Date:** 2026-09-30
- **Scope:** Outbound retry mitigation
- **Decision owners:** Curo maintainers

## Context

Fixed retry counts are configured per request but consume capacity across an
entire dependency. If every failed request is allowed three retries, a
dependency receiving 1,000 initial requests may suddenly receive 3,000
additional attempts while already unhealthy.

That positive feedback can turn a partial slowdown into a retry storm. It also
makes the operational effect of a retry setting depend on request volume,
which a per-call count does not represent.

Retries can also duplicate side effects. A transport error does not prove that
the remote service failed to process the request, so replay eligibility is a
separate requirement from available retry capacity.

## Decision

Curo will govern retries with **aggregate retry budgets**, not user-configured
per-request retry counts.

A retry budget limits retry attempts to a bounded share of original request
traffic over a rolling interval. Initial attempts replenish demand-based
capacity; retry attempts consume it. Retries do not count as new demand and
cannot replenish the budget.

Budgets will be enforced at two levels:

- A per-target budget prevents one normalized route or dependency from
  retrying without bound.
- A per-instance ceiling limits total retry amplification when several targets
  fail together.

A retry is eligible only when all of the following are true:

- The diagnosis identifies a condition for which another attempt may help.
- The request method and application intent permit replay.
- The body is absent or safely reproducible.
- The request context is active and sufficient time remains.
- The relevant target and instance budgets both have capacity.
- No breaker or safety policy forbids the attempt.

Budget capacity is necessary but does not guarantee a retry. Every operation
also has a hard internal attempt ceiling to prevent a single request from
looping while budget remains. This ceiling is a safety invariant, not the
primary user tuning mechanism.

Capacity must be acquired atomically before an attempt starts. Cancelled or
rejected acquisition must not create an attempt. Backoff waits must be
cancellable and must not hold a lock.

Initial-attempt and retry outcomes must be recorded separately. Retry traffic
must not distort the baseline used to estimate original demand or dependency
health.

The exact budget ratio, interval, refill algorithm, backoff, jitter, and hard
attempt ceiling belong in the engine design and may evolve while preserving
these invariants.

## Decision Drivers

- Bound aggregate load amplification during dependency failure.
- Make retry authority proportional to real demand.
- Coordinate concurrent callers safely.
- Keep unsafe replay separate from capacity decisions.
- Preserve explainable reasons when a retry is allowed or denied.
- Avoid making every user tune a retry count.

## Options Considered

1. **Rolling aggregate retry budgets - selected.**
   - Strengths: bounds fleet pressure at the process level and adapts to
     traffic volume.
   - Weaknesses: concurrent accounting, fairness, and low-volume behavior are
     more complex.
2. **Fixed retry count per request - rejected as the primary policy.**
   - Strengths: familiar and easy to implement.
   - Weaknesses: aggregate amplification grows directly with failing traffic.
3. **Never retry - rejected.**
   - Strengths: cannot amplify dependency load or duplicate a request.
   - Weaknesses: gives up safe recovery from transient failures.
4. **Leave retries entirely to the application - deferred as an escape hatch.**
   - Strengths: application code knows business semantics.
   - Weaknesses: cannot provide autonomous mitigation or shared load bounds.

## Consequences

### Positive

- Retry amplification has an explicit upper bound.
- A large failure burst cannot grant every caller the same retry count.
- Retry decisions can report both eligibility and available capacity.
- Original demand remains distinguishable from mitigation traffic.

### Negative

- Low-volume targets may have little or no retry capacity.
- Budget fairness across routes and callers requires careful design.
- Concurrent token accounting adds hot-path overhead.
- A process-local budget cannot coordinate retries across replicas.
- Safe replay still requires conservative method and body classification.

## Revisit When

The accounting scope should be reconsidered if process-local budgets permit
unacceptable fleet-wide amplification. A distributed budget requires a new
ADR because it would introduce coordination and availability dependencies
excluded from the current embedded architecture.
