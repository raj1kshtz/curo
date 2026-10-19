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

Releases follow [Semantic Versioning](https://semver.org/). From v1.0.0,
HTTP, `Engine`, and the `curogrpc` package share one module version,
annotated tag, changelog, and GitHub release. Breaking
changes in either public package require a new major module version and
must be documented in [CHANGELOG.md](../../CHANGELOG.md).
[ADR-0010](../adr/0010-versioning-and-compatibility.md) defines what the
compatibility promise covers, how releases are made and corrected, and which
Go versions are supported.
[ADR-0013](../adr/0013-unified-v1.md) records the v1 module transition.

A maintainer makes a release in these steps:

1. Merge a pull request that moves the `Unreleased` changelog entries into a
   dated section for the new version and updates the comparison links. For a
   new minor or major version, it also updates the supported versions in
   [SECURITY.md](../../.github/SECURITY.md).
2. Check out the release commit, the head of `main`, in a clean worktree,
   and confirm that CI passed for it:

   ```sh
   git switch main
   git pull --ff-only
   commit=$(git rev-parse HEAD)
   gh run list --commit "$commit"
   ```

3. Check the module and its API changes against the previous root release.
   For v1.0.0, also compare the adapter's API golden and documented behavior
   with its standalone v0.1.0 release; it was not part of the previous root
   module, so the root comparison alone cannot detect an adapter regression:

   ```sh
   go run golang.org/x/exp/cmd/gorelease@latest -base=vPREV -version=vX.Y.Z
   go test -run '^TestAPI$' ./...
   ```

4. Tag that commit with an annotated tag and push the tag:

   ```sh
   git tag -a vX.Y.Z -m vX.Y.Z "$commit"
   git push origin vX.Y.Z
   ```

5. Save the version's changelog section to a file outside the repository,
   such as `../notes.md`, with each paragraph
   and list item on one line, because GitHub keeps single line breaks in
   release notes. Then publish the release:

   ```sh
   gh release create vX.Y.Z --verify-tag --title vX.Y.Z --notes-file ../notes.md
   ```

6. Fetch the version through the module proxy so that it appears on
   pkg.go.dev:

   ```sh
   GOPROXY=https://proxy.golang.org go list -m github.com/raj1kshtz/curo@vX.Y.Z
   ```

   If the proxy reports the version as unknown, something may have asked
   for it before the tag was pushed. As the
   [mirror's FAQ](https://proxy.golang.org/) explains, fresh data can then
   take up to 30 minutes, so repeat the command later. pkg.go.dev can list
   a version later than the proxy does, and the Request button on the
   version's page fetches it at once.

No separate adapter tag or release is created for v1 or later. The root
module's archive includes `curogrpc`, and the same proxy fetch publishes
both packages. `curogrpc/CHANGELOG.md` is retained only as the historical
record of the standalone v0 adapter.

A repository ruleset blocks moving or deleting `v*` and `curogrpc/v*` tags.
Repository admins can bypass it, but ADR-0010 forbids moving or deleting a
published tag in any case. A broken release is corrected by a new version
whose `go.mod` retracts it.

## Changes to governance

Governance changes are reviewed like architectural changes and should reflect
the project's current contributor and maintainer structure.
