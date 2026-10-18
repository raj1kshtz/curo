# Security Policy

## Supported versions

Security fixes are released for the latest minor version of each module, as
patch releases.

| Module | Version | Supported |
| --- | --- | --- |
| `github.com/raj1kshtz/curo` | 0.2.x | Yes |
| `github.com/raj1kshtz/curo` | 0.1.x and commits before `v0.1.0` | No |
| `github.com/raj1kshtz/curo/curogrpc` | 0.1.x | Yes |

Upgrade to the latest patch release to receive fixes. A vulnerability in a
module that `curogrpc` requires, such as grpc-go, is fixed upstream, and a
`curogrpc` release then requires the fixed version.
[ADR-0010](../docs/adr/0010-versioning-and-compatibility.md) describes the
release and compatibility policy.

## Reporting a vulnerability

Do not report security vulnerabilities through public issues.

Use
[GitHub Security Advisories](https://github.com/raj1kshtz/curo/security/advisories/new)
to contact the maintainer privately. Include:

- The affected version or commit.
- A description of the issue and its impact.
- Steps to reproduce, ideally as a minimal Go program.
- Any suggested mitigation.

Reports will be assessed privately and coordinated disclosure will be arranged
when a fix is available.

## Security scope

Curo is intended to execute inside a host application and participate in its
outbound request path. Security-sensitive failures therefore include:

- **Host compromise.** Input that makes Curo propagate a panic, deadlock, or
  terminate the process.
- **Unbounded resource growth.** Attacker-controlled input that causes
  unbounded memory, state, or goroutine growth.
- **Credential or payload leakage.** Bodies, authorization values, cookies, or
  tokens appearing in logs, metrics, errors, or audit events.
- **Request mutation or duplication.** A replayed request differs from the
  caller's request, or an unsafe request, or an `Engine` call that is not
  replay-safe, is attempted more than once.
- **Retry amplification.** Retry behavior exceeds its aggregate safety bounds.

The governing safety requirements are recorded in
[ADR-0002](../docs/adr/0002-host-application-inviolability.md).

## Out of scope

- Vulnerabilities in the host application's own code or its dependencies.
- Denial of service that requires deliberate operator misconfiguration.
- Reports for behavior that is described only in design documents and has not
  been implemented.
