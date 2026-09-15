# Security Policy

## Supported versions

`curo` is pre-`v1.0`. Until `v1.0.0`, only the latest released minor version receives
security fixes.

| Version   | Supported |
| --------- | --------- |
| `v0.1.x`  | ✅ (once released) |
| `< v0.1`  | ❌ pre-alpha, unreleased |

## Reporting a vulnerability

**Please do not report security vulnerabilities through public issues.**

Report privately through
[GitHub Security Advisories](https://github.com/raj1kshtz/curo/security/advisories/new).

Please include:

- The affected version or commit.
- A description of the issue and its impact.
- Steps to reproduce, ideally a minimal Go program.
- Any suggested mitigation.

**Response targets:** acknowledgement within 3 working days; an initial assessment within
10 working days; coordinated disclosure once a fix is available. We will credit you in the
advisory unless you prefer otherwise.

## What counts as a vulnerability in curo

`curo` executes inside a host application and sits in the request path, so our threat
model is broader than "can an attacker read memory".

We treat the following as security issues, not just bugs:

- **Host compromise.** Any input that makes `curo` panic out to the host, deadlock, or
  exit the process. This violates
  [ADR-0002](docs/adr/0002-host-application-inviolability.md).
- **Unbounded resource growth.** Attacker-controlled input (URLs, headers, response
  bodies, status codes) that causes unbounded memory or goroutine growth — for example by
  exploding route cardinality.
- **Credential or payload leakage.** Request/response bodies, `Authorization` headers,
  cookies or tokens appearing in logs, metrics, error values or action hooks.
- **Request smuggling or mutation.** Any case where a retried or replayed request differs
  from what the caller constructed, or where a response is attributed to the wrong request.
- **Retry amplification.** Any path where `curo` can be induced to bypass its retry budget
  and amplify traffic against a third party.

## Out of scope

- Vulnerabilities in the host application's own code or in the services it calls.
- Denial of service that requires the operator to deliberately misconfigure `curo`.
- Findings against unreleased `main` that are already fixed on `main`.
