# ADR-0005: Use Off, Observe, and Enforce Operating Modes

- **Status:** Accepted
- **Date:** 2026-09-30
- **Scope:** Runtime authority and adoption workflow
- **Decision owners:** Curo maintainers

## Context

An adaptive resilience library asks users to trust decisions derived from live
traffic. Enabling retries, timeouts, or circuit breaking immediately after
installation would make adoption risky and would leave users unable to compare
Curo's proposed behavior with their production expectations.

A single enabled boolean cannot distinguish between collecting evidence and
changing request behavior. It also provides no safe path for evaluating new
policy versions before granting them authority.

The library needs a runtime kill switch, but the kill switch must not be the
only defense against an internal failure. Automatic self-disable remains an
independent requirement under
[ADR-0002](0002-host-application-inviolability.md).

## Decision

Every Curo instance will have exactly three user-selectable operating modes:

1. **Off**
   - Do not collect request observations.
   - Do not diagnose, propose, or enforce actions.
   - Delegate new requests directly to the wrapped base transport.
2. **Observe**
   - Collect bounded observations and produce diagnoses.
   - Evaluate policy and emit auditable actions Curo would have taken.
   - Do not add attempts, change timeouts, open a breaker, delay a request, or
     alter the response returned by the base transport.
3. **Enforce**
   - Perform the same observation and diagnosis as Observe.
   - Apply eligible actions within all accepted safety bounds.

`Observe` is the default for a newly constructed instance. Granting enforcement
authority must always be explicit.

Mode is scoped to an instance, not to a process-wide global. Reading and
changing it must be safe under concurrency and must not require restarting the
host application.

Each request snapshots its mode when request processing begins. A mode change
affects new requests and does not change authority halfway through an in-flight
request.

Mode transitions follow these rules:

- `Off` does not reset learned state, but observations stop and retained state
  continues to age and expire.
- Moving from `Observe` to `Enforce` may reuse observations only when they are
  fresh, compatible with the active policy version, and satisfy minimum
  evidence requirements.
- Moving from `Enforce` to `Observe` stops new interventions immediately while
  preserving bounded observations.
- Moving to `Off` selects the smallest direct pass-through path for new
  requests.

Internal self-disable has higher priority than the selected mode. A
self-disabled instance must use its fail-safe pass-through path even when its
configured mode is `Observe` or `Enforce`.

## Decision Drivers

- Make production evaluation possible without changing request semantics.
- Require explicit consent before autonomous mitigation.
- Provide a runtime kill switch without global mutable state.
- Make proposed actions auditable before enforcement.
- Preserve a direct path when Curo is disabled.

## Options Considered

1. **Off, Observe, and Enforce - selected.**
   - Strengths: separates visibility from authority and supports staged
     adoption.
   - Weaknesses: adds state transitions and mode-specific testing.
2. **Enabled or disabled boolean - rejected.**
   - Strengths: minimal API and implementation.
   - Weaknesses: cannot evaluate decisions safely before granting authority.
3. **Always observe and enforce automatically - rejected.**
   - Strengths: simplest user experience.
   - Weaknesses: unacceptable adoption risk and no explicit consent boundary.
4. **Configuration-time mode only - rejected.**
   - Strengths: immutable runtime configuration.
   - Weaknesses: incident response requires an application restart or
     redeployment.

## Consequences

### Positive

- Users can validate diagnoses against production traffic before enforcement.
- A mode change can stop new interventions without replacing the HTTP client.
- Audit events have a clear distinction between proposed and applied actions.
- Default installation does not alter transport behavior.

### Negative

- Observe mode still consumes bounded CPU and memory.
- Mode transitions and stale observations introduce additional test cases.
- Results observed in one mode may not perfectly predict an enforced action's
  downstream effects.
- Retaining state while Off requires expiry logic even when observations stop.

## Revisit When

A replacement ADR is required before adding another authority level or making
`Enforce` the default. Per-policy modes may be considered only if production
evidence shows that one instance-level mode is too coarse and the additional
states remain understandable to operators.
