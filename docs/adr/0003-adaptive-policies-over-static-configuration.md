# ADR-0003: Prefer Adaptive Policies Over Static Thresholds

- **Status:** Accepted
- **Date:** 2026-09-30
- **Scope:** Detection and mitigation policy
- **Decision owners:** Curo maintainers

## Context

Retries, timeouts, and circuit breakers are widely available in Go libraries.
Most require users to configure fixed values such as:

- Retry a request three times.
- Open a breaker after five failures.
- Treat a 30-second window with a 50% failure rate as unhealthy.
- Apply the same two-second timeout to every route.

Those values encode assumptions about traffic volume, latency, dependency
behavior, and failure modes. A threshold suitable for a high-volume internal
service may be meaningless for a low-volume third-party API. Fixed settings
also drift as traffic and dependencies change.

If Curo only exposes another set of static resilience options, it does not
justify a new project. Its differentiator is deciding when and how to mitigate
from bounded observations of actual runtime behavior.

Fully learned or opaque machine-learning policies would create a different
problem: decisions would be difficult to reproduce, explain, and constrain in
the request path of a safety-sensitive library.

## Decision

Curo will use **deterministic adaptive policies with explicit safety bounds**.

The engine will learn a local baseline for each normalized route or dependency
from bounded rolling observations. The initial signal set may include:

- Request and transport error classes.
- Timeout rate.
- Latency distribution and moving averages.
- In-flight request count.
- HTTP response class.
- Sample count and observation age.

Curo will not inspect request or response bodies to learn policy.

Adaptive policy means that operational thresholds are derived from observed
behavior rather than requiring a user to tune them. It does **not** mean that
the engine is unconstrained. Operators and maintainers will define hard safety
bounds such as:

- Maximum retry-budget share.
- Minimum sample count before an intervention.
- Minimum and maximum timeout values.
- Route-cardinality limits.
- Breaker open-duration bounds.
- Cooldowns and hysteresis.
- Methods and request types eligible for replay.

The MVP will use explainable online statistics and state machines, not machine
learning. Given the same ordered observations, clock, and configuration, the
engine must produce the same decision.

Each proposed or enforced action must include an auditable explanation:

- The normalized target.
- The diagnosis.
- The relevant observations.
- The policy and safety bound applied.
- The action, confidence or readiness state, and expiry.

The engine must collect a minimum amount of evidence before enforcement. A
separate ADR will define operating modes and the default adoption mode.

## Decision Drivers

- Eliminate continuous per-route threshold tuning.
- Adapt to dependencies with different traffic and latency profiles.
- Prevent per-call retry counts from amplifying an outage.
- Keep every automated action explainable and reproducible.
- Preserve deterministic tests with an injectable clock.
- Differentiate Curo from existing static resilience libraries.

## Options Considered

1. **Static thresholds only — rejected as the primary model.**
   - Strengths: simple, predictable, and familiar.
   - Weaknesses: requires tuning, drifts over time, and provides weak product
     differentiation.
2. **Deterministic adaptive policy — selected.**
   - Strengths: learns local behavior while remaining explainable, testable,
     and bounded.
   - Weaknesses: needs warm-up data and more state.
3. **Machine-learning policy — rejected for the MVP.**
   - Strengths: may model complex patterns.
   - Weaknesses: opaque, difficult to reproduce and constrain, and
     operationally heavy.
4. **Central fleet-wide policy — deferred.**
   - Strengths: better cross-instance evidence.
   - Weaknesses: requires the control plane excluded by
     [ADR-0001](0001-embedded-library-over-sidecar-proxy.md).

## Safety Invariants

Adaptive behavior must satisfy all of the following:

1. **Insufficient evidence means no autonomous intervention.**
2. **Retry consumption is budgeted across traffic**, not configured as an
   independent count for every call.
3. **Unsafe or non-replayable requests are not retried automatically.**
4. **Client-caused failures are not treated as dependency instability.**
5. **Hysteresis and cooldowns prevent rapid policy oscillation.**
6. **Every learned value has a hard floor, ceiling, and expiry.**
7. **All per-route state is bounded and evictable.**
8. **An internal engine failure follows
   [ADR-0002](0002-host-application-inviolability.md) and reduces to
   pass-through.**
9. **Actions are observable before and after enforcement.**

The concrete algorithms, windows, and defaults belong in the engine design and
can evolve without changing this decision, provided these invariants remain
true.

## Consequences

### Positive

- Policies can follow real dependency behavior as it changes.
- Users configure risk boundaries rather than guessing every threshold.
- Retry budgets limit aggregate amplification during an incident.
- Deterministic decisions can be simulated, audited, and regression-tested.
- Curo has a clear purpose beyond executing standard resilience primitives.

### Negative

- A new process or route has a cold-start period with insufficient evidence.
- Low-volume routes may never accumulate enough evidence for confident action.
- Process-local baselines can disagree across replicas.
- Time-dependent behavior is more difficult to test than fixed configuration.
- A poor adaptive algorithm can make plausible but incorrect interventions.
- Bounded route eviction can discard useful history.

These risks require an observation-only adoption path, an injectable clock,
recorded decision reasons, simulation tests, and conservative safety limits.

## Revisit When

This decision should be reconsidered if production evidence shows that:

- Deterministic local signals cannot distinguish important failure classes.
- Adaptation creates an unacceptable false-intervention rate despite safety
  bounds.
- Fleet-wide evidence is required for reliable decisions.
- Users consistently need static policies for cases the adaptive model cannot
  represent.

Static overrides may be added as explicit safety or compatibility controls,
but they must not silently become the primary policy model without a new ADR.
