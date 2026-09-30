# ADR-0004: Keep the Core Module Free of Third-Party Dependencies

- **Status:** Accepted
- **Date:** 2026-09-30
- **Scope:** Root Go module
- **Decision owners:** Curo maintainers

## Context

Curo is embedded in a host application's request path. Every dependency added
to the root module becomes part of the host's module graph and introduces
potential version conflicts, vulnerabilities, licensing obligations, upgrade
work, and transitive behavior outside Curo's direct control.

Resilience libraries must be easier to trust than ordinary application code.
Users should not have to reconcile an observability SDK, logging framework,
configuration library, or utility package merely to wrap an
`http.RoundTripper`.

Optional integrations such as OpenTelemetry or Prometheus necessarily depend
on external APIs. Putting them in the root module, even behind build tags,
would still add their modules to Curo's dependency metadata.

## Decision

The root module, `github.com/raj1kshtz/curo`, will use only the Go standard
library at runtime and in its package implementation.

The root `go.mod` must not contain a third-party module requirement without a
new ADR that supersedes this decision. This restriction also applies to
dependencies used only by root-module tests because they still affect module
maintenance and reproducibility.

Development tools may be invoked at pinned versions from CI without becoming
requirements of the root module.

An optional integration that requires a third-party API must live in a
separate Go module with its own:

- `go.mod` and `go.sum`.
- Dependency and vulnerability checks.
- Compatibility statement.
- Release notes and version tags.

Integration modules may depend on the root Curo module. The root module must
never import an integration module.

Build tags do not qualify as module isolation. Copying third-party source into
the repository to preserve the zero-dependency claim is prohibited.

## Decision Drivers

- Minimize conflicts in host applications.
- Keep the request path auditable and under project control.
- Reduce supply-chain and vulnerability exposure.
- Preserve predictable builds across supported Go versions.
- Make the base library usable without adopting an observability ecosystem.
- Keep dependency-specific release pressure outside the core lifecycle.

## Options Considered

1. **Standard-library-only root plus separate integration modules - selected.**
   - Strengths: smallest consumer graph, clear ownership, and independent
     integration upgrades.
   - Weaknesses: multiple module files, release coordination, and more CI.
2. **Allow selected dependencies in the root module - rejected.**
   - Strengths: less code to maintain and faster access to mature utilities.
   - Weaknesses: conflicts and transitive risk are imposed on every user.
3. **Hide optional dependencies behind build tags - rejected.**
   - Strengths: one module and conditional compilation.
   - Weaknesses: dependencies remain in `go.mod`, and build combinations add
     support complexity.
4. **Copy small third-party implementations into Curo - rejected.**
   - Strengths: no module requirement.
   - Weaknesses: obscured provenance, licensing risk, and forked maintenance.

## Consequences

### Positive

- Importing Curo does not add a third-party runtime dependency.
- Security and compatibility review of the hot path remains tractable.
- Optional ecosystems cannot force upgrades on core users.
- Consumers can choose integrations independently.

### Negative

- Curo must implement and maintain its own bounded data structures,
  statistics, and concurrency primitives.
- Some mature external utilities cannot be reused directly.
- Separate modules require additional release and CI automation.
- Cross-module examples and compatibility testing become more involved.

## Revisit When

A replacement ADR may be proposed only when a standard-library implementation
cannot meet a demonstrated requirement and the proposal includes:

- Why a separate integration module cannot solve the problem.
- The complete transitive dependency and license impact.
- Security and maintenance ownership.
- Measured performance and binary-size impact.
- A removal or migration strategy.
