# Governance

Curo is currently maintained by a single project lead. This document describes
the process used today; it will be revised if the maintainer group grows.

## Principles

1. Technical decisions and their rationale are discussed in public.
2. Changes should be small enough to review and maintain.
3. Architectural commitments are recorded in
   [ADRs](../adr/README.md).
4. Safety and compatibility take priority over implementation convenience.

## Roles

**Contributors** open issues, review proposals, improve documentation, and
submit changes.

**Maintainers** review and merge changes, manage releases, and are accountable
for the project's safety and compatibility commitments. Current maintainers are
listed in [maintainers.md](maintainers.md).

## Decision making

The project lead is responsible for final decisions while Curo has one
maintainer. Routine fixes and documentation changes are decided through pull
request review.

Changes to the public API, safety model, dependency policy, package boundaries,
or runtime authority require an ADR. A proposal should describe the problem,
alternatives, tradeoffs, and compatibility impact before implementation is
merged.

If the project gains additional maintainers, this document will define an
appropriate consensus or voting process based on the community that actually
exists at that time.

## Releases

Releases follow [Semantic Versioning](https://semver.org/). During `v0.x`, the
public API may change between minor versions. Breaking changes must be
documented in [CHANGELOG.md](../../CHANGELOG.md).

A release requires a green `main` branch and release notes that describe
user-visible changes.

## Changes to governance

Governance changes are reviewed like architectural changes and should reflect
the project's current contributor and maintainer structure.
