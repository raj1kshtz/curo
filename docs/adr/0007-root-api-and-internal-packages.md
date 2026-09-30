# ADR-0007: Expose the Public API at the Module Root

- **Status:** Accepted
- **Date:** 2026-09-30
- **Scope:** Go repository and package structure
- **Decision owners:** Curo maintainers

## Context

Go repositories often use `/cmd`, `/internal`, and sometimes `/pkg`. Those
directories communicate different ownership boundaries:

- `/cmd` contains executable entry points.
- `/internal` contains packages the Go compiler prevents external modules from
  importing.
- `/pkg` is sometimes used for reusable packages in repositories whose primary
  product is a binary.

Curo's primary product is a library. Placing its API under `/pkg/curo` would
add an unnecessary import-path segment and could imply that internal engine
packages are supported for external use.

Every exported package becomes a compatibility and documentation commitment.
The repository needs a small obvious API while allowing the implementation to
change rapidly during `v0.x`.

## Decision

The importable Curo API will live at the module root:

```go
import "github.com/raj1kshtz/curo"
```

The root package name is `curo`. Files may be separated by concern, but they
form one public package and one compatibility surface.

Implementation packages will live under `/internal`. Expected areas include
guarding, clocks, detection, diagnosis, policy, retries, and breakers. These
names are illustrative implementation details, not promised package
boundaries.

The repository will not add `/pkg`.

Additional public subpackages require an API review and must represent a
coherent user-facing capability rather than exposing implementation for
convenience. Integrations governed by
[ADR-0004](0004-zero-dependency-core.md) use separate Go modules.

`/cmd` will be added only when the project ships a maintained executable. Test
helpers, generators, or local development scripts do not justify presenting a
new product binary.

Repository-level directories have the following roles:

- `/docs` contains architecture, operation, and contribution documentation.
- `/examples` contains complete consumer examples compiled in CI.
- `/internal` contains non-public implementation packages.
- `/.github` contains repository automation and community configuration.

Exported identifiers at the root are treated as compatibility promises even
before `v1.0`, although semantic versioning permits breaking changes between
minor `v0.x` releases. External-package tests using `package curo_test` should
exercise the public contract independently of internal tests.

## Decision Drivers

- Give users the shortest idiomatic import path.
- Make the supported API boundary visually obvious.
- Let the compiler prevent imports of implementation packages.
- Minimize accidental semantic-versioning commitments.
- Allow internal packages to be reorganized without downstream breakage.
- Match the repository's identity as a library rather than a binary.

## Options Considered

1. **Root public package plus `/internal` implementation — selected.**
   - Strengths: idiomatic import, compiler-enforced boundaries, and minimal
     public surface.
   - Weaknesses: the root package needs discipline to avoid becoming crowded.
2. **Public packages under `/pkg` — rejected.**
   - Strengths: visually separates reusable code in a binary-oriented
     monorepo.
   - Weaknesses: unnecessary for a library and encourages extra public
     packages.
3. **Expose engine subsystems as public subpackages — rejected for `v0.x`.**
   - Strengths: advanced users can compose low-level pieces.
   - Weaknesses: freezes internal abstractions before the engine is validated.
4. **Put most implementation in the root package — rejected.**
   - Strengths: fewer packages and no internal import graph.
   - Weaknesses: obscures boundaries and increases accidental exported API.

## Consequences

### Positive

- Installation and imports remain simple.
- The Go compiler protects internal implementation details.
- Most engine refactoring does not require a public breaking change.
- API documentation centers on one package.

### Negative

- Root-level files can become numerous as the API grows.
- Internal package boundaries require deliberate dependency direction to avoid
  import cycles.
- Users cannot import useful-looking internal primitives directly.
- Moving an accidentally exported root symbol later remains a breaking change.

## Revisit When

A new public package or `/cmd` tree requires evidence of a distinct supported
capability, named ownership, documentation, compatibility expectations, and a
new ADR or an explicit update that supersedes this decision.
