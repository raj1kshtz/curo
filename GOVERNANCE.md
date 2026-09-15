# Governance

`curo` is an open-source project. This document describes how decisions get made. It is
deliberately lightweight for the project's current size and will grow as the community does.

## Principles

1. **Community over code.** A technically excellent patch that nobody can maintain is a
   liability. Reviewability and clear reasoning beat cleverness.
2. **If it did not happen in public, it did not happen.** Design discussion, decisions and
   rationale live on the issue tracker and in [ADRs](docs/adr/) — never in private DMs.
3. **Decisions are recorded, not remembered.** Every architectural decision becomes an ADR,
   including the options we rejected and why.
4. **Merit, not tenure.** Influence is earned through sustained, high-quality contribution.

## Current structure

The project is in its founding phase and is currently maintained by a single maintainer
(see [MAINTAINERS.md](MAINTAINERS.md)). This is a stage, not an end state — the structure
below describes where we are heading.

## Roles

**Contributor** — anyone who opens an issue, reviews a PR, improves docs, or submits code.
No formal process; just participate.

**Maintainer** — has write access; reviews and merges PRs; shepherds releases.
Nominated by an existing maintainer after a track record of sustained, high-quality
contribution and good judgement about the project's safety constraints. Confirmed by lazy
consensus among existing maintainers.

**Maintainers may step down at any time** and are moved to emeritus. Maintainers inactive
for 6 months may be moved to emeritus by consensus; returning is expected to be easy.

## How decisions are made

We use **lazy consensus**. A proposal is considered accepted if no maintainer objects
within a reasonable review window (typically 72 hours for substantive changes).

- **Trivial changes** (typos, docs, dependency bumps): one maintainer approval.
- **Ordinary changes**: one maintainer approval, 72-hour window for others to object.
- **Architectural changes**: require an ADR, an issue for discussion, and explicit approval
  from a majority of maintainers. Changes affecting
  [ADR-0002 (host inviolability)](docs/adr/0002-host-application-inviolability.md)
  require unanimous maintainer approval.
- **Breaking public API changes**: ADR plus majority approval, and a documented migration path.

Objections must be technical and must come with reasoning. "I do not like it" is not an
objection; "this can deadlock when X" is.

Anyone may call for a vote if consensus cannot be reached. Votes follow the Apache
convention: `+1` (agree), `0` (abstain), `-1` (object — **must** include reasoning and,
where possible, an alternative). Maintainer votes are binding; all others are advisory
and genuinely welcomed.

## Releases

Releases follow [Semantic Versioning](https://semver.org/). While the project is `v0.x`,
the public API may change between minor versions; breaking changes are always documented
in [CHANGELOG.md](CHANGELOG.md).

A release requires a green CI run on `main` and approval from at least one maintainer who
did not author the release commit (once there is more than one maintainer).

## Changing this document

Amendments to this document follow the "architectural changes" process above.
