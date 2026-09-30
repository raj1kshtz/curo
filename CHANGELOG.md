# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While `curo` is `v0.x`, the public API may change between minor versions. Breaking
changes will always be listed here under **Changed** or **Removed**.

## [Unreleased]

### Added

- Project foundation: Apache-2.0 licence, contribution guide, code of conduct, security
  policy, governance model and CI pipeline.
- ADR lifecycle and authoring guidance, plus Architecture Decision Records
  `0001`–`0007` covering the embedded Go library, host application
  inviolability, adaptive policy, dependencies, operating modes, retry
  budgets, and package boundaries.
- Engine high-level design with reviewed context, component, runtime,
  containment, state-ownership, and mitigation-control diagrams.

### Changed

- Standardized Markdown on ASCII punctuation and added CI enforcement against
  Unicode em dashes (`U+2014`).

[Unreleased]: https://github.com/raj1kshtz/curo/commits/main
