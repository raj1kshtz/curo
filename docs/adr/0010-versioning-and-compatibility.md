# ADR-0010: Version Releases with an Explicit Compatibility Promise

- **Status:** Accepted
- **Date:** 2026-10-03
- **Scope:** Releases, the public API, and supported Go versions
- **Decision owners:** Curo maintainers

## Context

Curo is about to publish its first release, `v0.1.0`. Once the Go module proxy
has fetched a version, it caches it so that builds keep working even if the
tag is deleted, and the checksum database records its hash, so the code of a
published version can never change. A user who adopts Curo needs to know what
an upgrade can change.

Semantic Versioning answers only part of that. It treats `v0.x` as unstable,
so any minor release may break anything, and it does not say which parts of a
library form its API. For Curo, the API is more than Go signatures. Users also
depend on behavior, such as when a request is retried; on error identities
matched with `errors.Is`; on the names that `Mode` and the report types print
in logs, configuration, and JSON; and on the Go versions they build with.

Some observable details are deliberately not stable. The adaptive policy's
thresholds are tuned from evidence under
[ADR-0003](0003-adaptive-policies-over-static-configuration.md), and the
simulation scorecard changes with them.

[ADR-0007](0007-root-api-and-internal-packages.md) already treats exported root
identifiers as compatibility promises before `v1.0`. That statement needs a
concrete policy: what is covered, how changes are made and announced, how
releases are cut and corrected, and which Go versions are supported. It also
needs a mechanism, because an accidental change to an exported declaration is
easy to miss in review.

## Decision

Curo releases follow [Semantic Versioning 2.0.0](https://semver.org/) as Go
modules apply it, with the compatibility promise below.

### What the promise covers

- Every exported identifier of the root package `github.com/raj1kshtz/curo`,
  and its documented behavior.
- Error identities for `errors.Is`, including that `ErrTimeout` also matches
  `context.DeadlineExceeded`.
- The names that `Mode` and the report enumerations print, and their text and
  JSON encodings.
- The JSON shape that `encoding/json` produces for `Stats`, `Report`, and their
  nested types: field names and value types.

When behavior contradicts its documentation, the fix follows the
documentation, even in a patch release.

### What the promise does not cover

- Packages under `/internal`, which other modules cannot import.
- Error message text. Match errors with `errors.Is`.
- Adaptive policy tuning: thresholds, windows, budgets, and other numbers that
  are not options. They may change in a minor release with a changelog entry.
  `Decision.PolicyVersion` increases whenever the rules that select candidates
  change.
- Simulation scorecard numbers, performance, and allocation counts. A
  regression is a bug, but the numbers are not promises.

### How the API changes

- In `v0.x`, a minor release may make a breaking change when it is needed, and
  must describe it in the changelog with migration notes. A patch release
  never changes the API: it fixes bugs, documentation, or security issues.
- From `v1.0.0`, a breaking change requires a new major version and module
  path, as Go modules require.
- Enumerations are append-only. A new value gets a new number and name. An
  existing number or name never changes meaning and is never reused. Tests
  pin every number and name.
- Consumers of names and JSON must tolerate names and fields they do not
  know, and should decode names as strings. The report enumerations
  implement `encoding.TextMarshaler` but not `encoding.TextUnmarshaler`, so
  the JSON of a `Report` is output only, and an older program never has to
  decode a newer name into a Curo type. `Mode` decodes text because it is
  input.
- Exported struct types may gain fields in a minor release. Construct them
  with keyed fields.
- A removal or an incompatible change starts with a `Deprecated:` notice in
  the doc comment for at least one minor release when practical.
- `TestAPI` compares the exported API with `testdata/api.golden`. Any change to
  an exported declaration fails the test until the file is regenerated, so the
  change appears in review. The pull request states whether the change is
  compatible. A new or changed compatibility promise also needs an ADR, as the
  [ADR index](README.md) requires.

### How releases are made

- A release is an annotated `vX.Y.Z` tag on a `main` commit whose CI passed,
  with a dated `CHANGELOG.md` section and a GitHub release that carries the
  same notes.
- Before tagging, `gorelease` checks the module and compares its API with the
  previous release.
- After tagging, a request through the module proxy fetches the version, so
  that it appears on pkg.go.dev:

  ```sh
  GOPROXY=https://proxy.golang.org go list -m github.com/raj1kshtz/curo@vX.Y.Z
  ```

- Tags are never moved or deleted. A broken release is corrected by a new
  version whose `go.mod` retracts it with a `retract` directive. The `go`
  command reads retractions only from the latest version, so the correcting
  version must become `@latest`, and every later release keeps the
  directives.
- A larger change may first ship as a pre-release, such as `v0.2.0-rc.1`.
- Security fixes are released for the latest minor version, as
  [SECURITY.md](../../.github/SECURITY.md) states.

### Supported Go versions

- The `go` directive in `go.mod` is the minimum supported Go version. CI tests
  it together with the two most recent Go releases.
- Raising the minimum is a minor-release change with a changelog entry, never
  part of a patch release.

## Decision Drivers

- Users must be able to tell what an upgrade can change without reading the
  code.
- A published version cannot change once the Go module proxy has fetched it.
- The adaptive policy must stay tunable from evidence.
- Logs, dashboards, and configuration depend on names and encodings, not only
  on Go signatures.
- The project has one maintainer, so the process must be cheap and mostly
  mechanical.
- Standard Go module tooling, such as `go get`, `retract`, and `gorelease`,
  should work as designed.

## Options Considered

1. **Semantic Versioning with an explicit `v0` policy and an API gate -
   selected.**
   - Strengths: users know what a release can change, API changes cannot pass
     review unnoticed, and tuning can improve in minor releases.
   - Weaknesses: the policy needs upkeep, and the gate shows changes without
     judging whether they are compatible.
2. **Release `v1.0.0` immediately - rejected.**
   - Strengths: the strongest promise and a clear signal of stability.
   - Weaknesses: the API has no external users yet, and fixing a mistake would
     need a new major version and module path.
3. **Semantic Versioning `v0.x` without a written policy - rejected.**
   - Strengths: no process to maintain.
   - Weaknesses: users cannot tell what a minor release may break, and names,
     encodings, and error identities would have no stated protection.
4. **Calendar versioning - rejected.**
   - Strengths: dates show how recent a release is.
   - Weaknesses: dates say nothing about compatibility. Go requires semantic
     version tags, so using the year as the major version would need a new
     module path every year.

## Consequences

### Positive

- Users can build logs, dashboards, configuration, and error handling on
  names, encodings, and `errors.Is` identities.
- Tuning can improve between minor releases without breaking a promise.
- Every change to an exported declaration is visible in review.
- A broken release has a documented recovery path.

### Negative

- Enumeration names and numbers are permanent. A poor name can only be
  deprecated.
- Every exported change requires regenerating `testdata/api.golden`.
- Behavior that depends on tuning can shift between minor releases, so users
  who need fixed behavior must pin a version.
- Testing three Go versions costs CI time, and the minimum Go version can be
  older than the versions the Go project still supports.

## Revisit When

- The API has been stable across several minor releases and has external
  users. A new ADR should then decide on `v1.0.0`.
- Another maintainer joins and release duties need to be shared.
- A change requires breaking a promise that this ADR makes.
