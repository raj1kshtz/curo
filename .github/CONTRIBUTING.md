# Contributing to curo

Thanks for considering a contribution. `curo` runs *inside* other people's
applications, so we hold a higher-than-usual bar for safety. This document
explains what that means in practice.

The project is currently in its design phase. Runtime contributions should
follow an accepted design and include the tests needed to establish the
claimed behavior.

## Before you write code

**Open an issue first** for anything beyond a typo or an obvious bug fix.
Design discussion happens in public on the issue tracker so that the reasoning
is preserved for everyone. A substantial pull request without prior discussion
may be redirected to an issue before review continues.

If your change alters an architectural decision, it needs an
[ADR](../docs/adr/README.md) - not just code.

## The one rule that is not negotiable

> `curo` must never terminate, hang, or degrade the host process.

This is [ADR-0002](../docs/adr/0002-host-application-inviolability.md). A patch
that is correct, fast and elegant will still be rejected if it can take down a
host application.

Concretely, in all non-test library code:

- **Never** call `panic`, `os.Exit`, or `log.Fatal*`. CI enforces this via
  `forbidigo`.
- **Every goroutine** Curo owns must install a deferred recovery function
  before running fallible work. Calling `recover` on a parent goroutine cannot
  contain a panic from a child goroutine.
- **Never** hold a lock across I/O. Every internal wait must be `context`-bounded.
  A hang is treated as exactly as severe as a panic, because it is worse in production.
- **Never** use a bare `map` reachable from more than one goroutine. A concurrent
  map read/write is a `fatal error`, not a panic - `recover()` cannot save you.
- **All state must be bounded.** Ring buffers, capped cardinality. Never
  `map[string]*state` keyed by a raw URL - that is a memory leak with extra steps.
- **No `init()` side effects**, no mutation of `http.DefaultTransport`, and no
  global registries.

## Development

```sh
go build ./...
go vet ./...
go test -race ./...
golangci-lint run
govulncheck ./...
```

Add focused tests with each implementation change. Time-dependent code must
accept a controllable time source, and tests must not depend on wall-clock
sleeps for state transitions.

## Documentation

- Keep architecture diagram sources and rendered SVGs synchronized. Rendering
  instructions live in
  [`docs/design/diagrams/README.md`](../docs/design/diagrams/README.md).
- Update related ADRs and the changelog in the same pull request.

## Commits

We use [Conventional Commits](https://www.conventionalcommits.org/):

```text
feat(breaker): derive open threshold from observed baseline
fix(guard): recover panics raised inside the sampling goroutine
docs(adr): accept ADR-0006 retry budgets
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`, and
`chore`. Breaking changes get a `!` (`feat(api)!: ...`) and a
`BREAKING CHANGE:` footer.

## Sign your work (DCO)

`curo` uses the
[Developer Certificate of Origin](https://developercertificate.org/). Every
commit must carry a `Signed-off-by` line matching the commit author:

```sh
git commit -s -m "fix(guard): ..."
```

By signing off you certify that you wrote the patch or otherwise have the right
to submit it under the Apache-2.0 licence. There is no CLA.

## Pull requests

- Keep PRs focused. One logical change.
- Add tests. Bug fixes need a regression test that fails before your change.
- Update docs and ADRs in the same PR as the behaviour change.
- Keep the public API surface as small as you can. Anything exported is a
  compatibility promise; prefer `internal/`.
- CI must be green. Maintainers will not merge a red build.

## Reporting security issues

Do **not** open a public issue. See [SECURITY.md](SECURITY.md).

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By
participating you agree to uphold it.
