# ADR-0013: Unify HTTP and gRPC in One v1 Module

- **Status:** Accepted
- **Date:** 2026-10-19
- **Scope:** Module boundaries, dependencies, and v1 compatibility
- **Decision owners:** Curo maintainers
- **Supersedes:** [ADR-0004](0004-zero-dependency-core.md)
- **Amends:** [ADR-0007](0007-root-api-and-internal-packages.md) and
  [ADR-0012](0012-engine-and-adapter-modules.md), for adapter module isolation
  and versioning only

## Context

HTTP and generic calls ship in the root module at v0.2.0, while unary gRPC
support ships in a nested module at curogrpc/v0.1.0. Separate modules preserve
a dependency-free root module, but require separate versions, tags,
changelogs, dependency checks, and release coordination.

The v1 distribution should offer HTTP and unary gRPC together under one
version. A single GitHub release page would not achieve this by itself: Go
still needs a directory-prefixed tag for each nested module. Both packages
must therefore belong to the root module.

This deliberately replaces ADR-0004's dependency-isolation goal with a
single distribution and compatibility boundary. A separate integration
module remains technically workable, but cannot satisfy the single-tag
requirement. This is a packaging decision, not a failure of the standard
library or a change to the resilience algorithms.

## Decision

### One module and release

- Keep `github.com/raj1kshtz/curo` as the only Go module.
- Keep `curo` at the root and `curogrpc` as a public subpackage. Preserve
  both import paths and their exported APIs.
- Remove `curogrpc/go.mod` and `curogrpc/go.sum`. Move the adapter's external
  requirements and checksums into the root module, without a dependency on
  a published version of Curo itself.
- Release v1 with only `v1.0.0`, one GitHub release, and the root changelog.
  Later versions follow the same scheme. Do not create `curogrpc/v1.0.0`.
- Keep all published v0 tags and release pages unchanged. Retain the
  adapter's v0 changelog as an archive, not a second active changelog.
- This ADR prepares v1; it does not publish or date that release. The
  release runbook still requires a clean, merged commit with green CI.

### Dependency and toolchain policy

The module requires Go 1.26, the minimum needed by its existing x/net
requirement. It carries the same runtime dependency versions as the former
adapter; this change does not upgrade grpc-go or protobuf.

| Runtime module | Version | License |
| --- | --- | --- |
| `google.golang.org/grpc` | v1.84.0 | Apache-2.0 |
| `google.golang.org/protobuf` | v1.36.12 | BSD-3-Clause |
| `golang.org/x/net` | v0.60.0 | BSD-3-Clause |
| `golang.org/x/sys` | v0.48.0 | BSD-3-Clause |
| `golang.org/x/text` | v0.42.0 | BSD-3-Clause |
| `google.golang.org/genproto/googleapis/rpc` | v0.0.0-20260706201446-f0a921348800 | Apache-2.0 |

These dependencies now participate in every consumer's module graph.
HTTP-only consumers are not isolated from dependency-version selection or
the Go minimum. However, the root package continues to import only the
standard library, so an HTTP-only binary does not compile or link gRPC.
There is no runtime implementation change or new hot-path work.

Maintainers own dependency updates and vulnerability checks for the whole
module. Dependabot updates the root directory in one grouped Go dependency
PR. Future third-party integrations require their own API and dependency
review; this decision does not authorize arbitrary additions.

### Compatibility and safety

[ADR-0010](0010-versioning-and-compatibility.md) applies to both public
packages under one version. From v1.0.0, breaking either package's exported
API or documented compatibility promise requires a new major module path,
such as `github.com/raj1kshtz/curo/v2`. Both package API goldens remain
independent review gates within one module.

The embedded-library choice of
[ADR-0001](0001-embedded-library-over-sidecar-proxy.md), the root/internal
layout of ADR-0007, and the Engine and interceptor runtime rules of ADR-0012
continue into v1. This ADR changes only distribution, dependencies, and the
shared compatibility boundary.

[ADR-0002](0002-host-application-inviolability.md) remains unchanged.
The root package does not import the adapter, and no new runtime
initialization or request-path behavior is introduced. gRPC retries remain
owned by grpc-go; Curo still intercepts only unary client calls.

CI runs builds, tests, race checks, both API gates, complete statement
coverage, lint, and vulnerability checks from the root with `./...`.
It tests the minimum and the two newest Go releases, deduplicating overlaps.
Separate adapter jobs and temporary workspaces are no longer necessary.

### Migration

After v1.0.0 is published, existing standalone-adapter consumers run:

```sh
go mod edit -droprequire=github.com/raj1kshtz/curo/curogrpc
go get github.com/raj1kshtz/curo@v1.0.0
go mod tidy
```

Imports and interceptor setup do not change. A transitive dependency that
still requires the old nested module must also be upgraded; otherwise both
modules can provide the same import and Go reports an ambiguous import.
Use `go mod graph` to identify such requirements.

The old adapter remains available at its immutable v0 tag. Consumers unable
to raise their Go version may retain the existing root v0 release, subject
to the published security-support policy.

## Options Considered

1. **One root module with a gRPC subpackage - selected.**
   Provides one Go version, tag, compatibility boundary, and release. It
   accepts external module requirements and a higher minimum Go version.
2. **One GitHub release with two module tags - rejected.**
   Reduces release-page duplication but retains separate Go versions and
   tags, so it does not provide a single distribution.
3. **Keep separate modules and releases - rejected.**
   Preserves dependency isolation but retains the coordination overhead.
4. **Move the gRPC API into the root package - rejected.**
   Unnecessarily changes imports and makes HTTP consumers compile the gRPC
   integration. A public subpackage preserves API separation.

## Consequences

- One release contains both supported public packages, and one CI pipeline
  checks their integration against the same source.
- Existing source imports and exported declarations remain unchanged.
- HTTP-only consumers accept a Go 1.26 minimum and gRPC dependencies in
  their module graph, but not in their compiled package dependency graph.
- Old nested-module requirements must be removed during the transition.
- A breaking change in either package requires a shared major release.
- Dependency and security maintenance can require a release even when only
  one protocol is affected.

## Revisit When

- A new integration materially expands dependency or toolchain costs.
- A dependency introduces runtime behavior inconsistent with ADR-0002.
- A future major version requires different public package boundaries.
