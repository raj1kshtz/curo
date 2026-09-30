# ADR-0001: Use an Embedded Go Library for the Initial Product

- **Status:** Accepted
- **Date:** 2026-09-30
- **Scope:** `v0.x`
- **Decision owners:** Curo maintainers

## Context

Curo started as a language-agnostic self-healing API system. The first
architectural question was where the detect, diagnose, and mitigate loop should
run:

1. In a sidecar proxy beside every application instance.
2. In a centralized agent reached through language-specific SDKs.
3. Inside the application as an embedded library.
4. Across a hybrid control plane with multiple enforcement adapters.

A proxy can enforce retries and circuit breaking without application changes,
but it cannot reliably see application semantics such as typed errors,
request intent, or whether a fallback is safe. It also introduces another
process, network hop, deployment model, and operational failure mode.

A centralized agent provides fleet-wide correlation, but still needs an
in-process or inline component to mitigate a request. Starting there would make
the network protocol, agent lifecycle, and multiple SDKs prerequisites for
validating the core adaptive engine.

The project is currently maintained by a small team and has not yet validated
its detection or mitigation model. The first release therefore needs the
smallest architecture that can demonstrate the product's differentiator while
remaining testable and safe.

## Decision

Curo `v0.x` will be delivered as an **embedded Go library**.

The first supported integration surface will be outbound HTTP calls made
through `net/http`. Applications will explicitly wrap an
`http.RoundTripper`; Curo will not mutate `http.DefaultTransport`, register
global hooks, or rely on `init()` side effects.

Detection state and mitigation decisions will be local to each process. The
`v0.1.0` scope excludes:

- A sidecar proxy.
- A centralized control plane or agent.
- SDKs for languages other than Go.
- Fleet-wide shared state.
- Ingress middleware and gRPC interception.

Internal boundaries between detection, diagnosis, policy, and mitigation will
remain explicit so a future remote decision source or alternative enforcement
adapter does not require replacing the entire engine. Those future components
are not being designed or promised by this decision.

Adding a sidecar, control plane, or another language requires a new ADR based
on demonstrated user demand.

## Decision Drivers

- Reach a useful and testable MVP without operating distributed infrastructure.
- Preserve rich in-process context for diagnosis.
- Avoid an additional network hop on every request.
- Keep installation explicit and idiomatic for Go users.
- Concentrate safety work in one implementation before creating other SDKs.
- Allow the core adaptive model to be validated before defining a wire
  protocol.

## Options Considered

1. **Embedded Go library - selected.**
   - Strengths: rich context, no network hop, simple deployment, and the
     fastest path to validation.
   - Weaknesses: Go-only, process-local state, and library defects share the
     host's blast radius.
2. **Sidecar proxy - rejected for `v0.x`.**
   - Strengths: language-neutral, independently upgradeable, and inline.
   - Weaknesses: Kubernetes-oriented operations, limited semantic context,
     another process, and another network hop.
3. **Central agent with thin SDKs - rejected for `v0.x`.**
   - Strengths: fleet-wide evidence and centralized policy.
   - Weaknesses: agent availability becomes a concern and the design still
     requires SDKs and a protocol.
4. **Hybrid architecture from the start - deferred.**
   - Strengths: broadest long-term reach.
   - Weaknesses: largest initial scope and several unvalidated integration
     surfaces.

## Consequences

### Positive

- A user can adopt Curo as a normal Go dependency.
- Request context, typed errors, and transport results are available directly.
- The hot path does not depend on a Curo network service.
- Performance, race, fuzz, and fault-injection tests can run in one process.
- The team can validate adaptive behavior before standardizing remote APIs.

### Negative

- Applications written in other languages cannot use the initial release.
- Every process learns its own baseline; there is no fleet-wide diagnosis.
- Curo upgrades are coupled to application build and deployment cycles.
- Code executing inside the host has a high potential blast radius.
- The first release only observes traffic using supported Go integration
  points.

The host-process risk is addressed by
[ADR-0002](0002-host-application-inviolability.md), but it cannot be eliminated
merely by choosing an embedded architecture.

## Revisit When

This decision should be reconsidered when at least one of the following is
true:

- Multiple production users need correlation across application instances.
- Demand for a non-Go runtime is sustained and cannot be met through standard
  telemetry alone.
- Users already operate a proxy platform and require Curo to enforce decisions
  there.
- Process-local learning is shown to produce materially worse decisions than
  fleet-level evidence.

Reconsideration must include migration and compatibility plans for existing Go
library users.
