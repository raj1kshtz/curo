# Architecture Decision Records

Architecture Decision Records (ADRs) capture decisions that materially affect
Curo's structure, public contract, safety model, or long-term maintenance.

An ADR explains **why** a decision was made and which alternatives were
rejected. Design documents explain **how** the accepted decisions work
together. Code and API documentation define the exact implementation.

## Decision Index

1. **ADR-0001 - Accepted**
   [Use an Embedded Go Library for the Initial Product](0001-embedded-library-over-sidecar-proxy.md)
2. **ADR-0002 - Accepted**
   [Guarantee Host Application Inviolability](0002-host-application-inviolability.md)
3. **ADR-0003 - Accepted**
   [Prefer Adaptive Policies Over Static Thresholds](0003-adaptive-policies-over-static-configuration.md)
4. **ADR-0004 - Accepted**
   [Keep the Core Module Free of Third-Party Dependencies](0004-zero-dependency-core.md)
5. **ADR-0005 - Accepted**
   [Use Off, Observe, and Enforce Operating Modes](0005-operating-modes.md)
6. **ADR-0006 - Accepted**
   [Govern Retries with Aggregate Budgets](0006-retry-budgets.md)
7. **ADR-0007 - Accepted**
   [Expose the Public API at the Module Root](0007-root-api-and-internal-packages.md)

## Lifecycle

ADRs use the following statuses:

- **Proposed:** open for discussion and not yet authoritative.
- **Accepted:** approved and governing implementation.
- **Superseded:** replaced by a newer ADR that links back to the original.

Merging an ADR marked `Accepted` records project consensus under the process
defined in [GOVERNANCE.md](../../GOVERNANCE.md). A proposal may remain marked
`Proposed` across multiple pull requests while evidence is gathered.

Accepted ADRs are immutable records. Do not rewrite their context, rejected
options, decision, or consequences when the project changes direction.
Instead:

1. Add a new ADR with the next number.
2. Explain why the old decision no longer fits.
3. Set the old ADR's status to `Superseded by ADR-NNNN`.
4. Link both records to each other.

Typographical corrections and repaired links are allowed when they do not
change meaning.

## When an ADR Is Required

Write an ADR before merging a change that:

- Adds or changes an exported compatibility promise.
- Changes the host-safety or failure model.
- Introduces a runtime dependency or external service.
- Changes component, module, package, or deployment boundaries.
- Adds a persistent protocol or data format.
- Changes how autonomous mitigation authority is granted.
- Reverses or materially weakens an accepted decision.

An ADR is usually unnecessary for bug fixes, tests, internal refactoring,
documentation clarification, or implementation choices already permitted by
an accepted ADR.

When uncertain, open an issue and ask before writing the implementation.

## Authoring Process

1. Open an issue describing the problem and constraints.
2. Reserve the next unused four-digit number.
3. Copy [`0000-template.md`](0000-template.md).
4. Name the file `NNNN-short-kebab-case-title.md`.
5. Record context, the decision, alternatives, and both positive and negative
   consequences.
6. Submit the ADR before or with the first implementing pull request.
7. Update this index and `CHANGELOG.md` in the same pull request.

Numbers are never reused or renumbered, including after a proposal is
abandoned. One ADR should make one coherent decision.

## Review Standard

Reviewers should be able to answer:

- Is the problem real and scoped?
- Which constraints and decision drivers matter?
- Were credible alternatives considered fairly?
- Does the decision state enforceable boundaries?
- Are costs, risks, and migration consequences explicit?
- Is it clear when the decision should be revisited?
- Does the decision conflict with an accepted ADR?

Safety-sensitive changes must explicitly evaluate
[ADR-0002](0002-host-application-inviolability.md).
