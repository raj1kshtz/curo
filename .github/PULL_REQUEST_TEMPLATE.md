<!--
Thanks for contributing to curo!
Please make sure an issue exists for anything beyond a trivial fix — see CONTRIBUTING.md.
-->

## What does this change?

<!-- A short description of the change and the problem it solves. -->

## Why?

Fixes #<!-- issue number -->

## How was it verified?

<!-- Commands you ran, tests you added, benchmarks before/after if the hot path changed. -->

- [ ] `go test -race ./...` passes
- [ ] New/changed behaviour is covered by tests
- [ ] Benchmarks show no hot-path regression (if applicable)

## Safety checklist (ADR-0002)

<!-- curo runs inside other people's applications. Please confirm each item. -->

- [ ] No `panic`, `os.Exit` or `log.Fatal*` added to library code
- [ ] Every new goroutine has `defer recover()` as its first statement
- [ ] No lock is held across I/O; every wait is `context`-bounded
- [ ] No unbounded growth (maps, slices, goroutines); cardinality stays capped
- [ ] Failure of this code degrades to pass-through rather than affecting the host
- [ ] No new third-party dependency in the core module (or an ADR justifies it)

## Housekeeping

- [ ] Commits follow [Conventional Commits](https://www.conventionalcommits.org/)
- [ ] Commits are signed off (`git commit -s`) — DCO
- [ ] Docs / ADRs updated in this PR
- [ ] `CHANGELOG.md` updated under `[Unreleased]`
