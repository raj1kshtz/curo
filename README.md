# curo

> **cūrō** _(Latin)_ — “I care for, I heal.”

**Self-healing HTTP resilience for Go.** `curo` watches your outbound calls, works out
_why_ they are failing, and applies the right mitigation on its own — no thresholds to
tune, no dashboards to watch, no human in the loop.

[![Go Reference](https://pkg.go.dev/badge/github.com/raj1kshtz/curo.svg)](https://pkg.go.dev/github.com/raj1kshtz/curo)
[![CI](https://github.com/raj1kshtz/curo/actions/workflows/ci.yml/badge.svg)](https://github.com/raj1kshtz/curo/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/raj1kshtz/curo)](https://goreportcard.com/report/github.com/raj1kshtz/curo)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> [!WARNING]
> **Status: pre-alpha — design phase.** The API is being shaped in the open and will
> change without notice until `v0.1.0`. Do not use in production yet.
> Follow [`docs/adr/`](docs/adr/) to see how decisions are being made.

---

## Why not just a circuit breaker?

Circuit breakers, retries and timeouts are solved problems. **Tuning them is not.**
Every static threshold you hard-code is a guess about a production incident that has not
happened yet — and it is wrong the moment traffic shape changes.

|                     | Static resilience libraries      | `curo`                                        |
| ------------------- | -------------------------------- | --------------------------------------------- |
| Thresholds          | You configure and re-tune them   | Learned continuously from observed behaviour   |
| Failure response    | Identical for every failure      | Classified by cause, then matched to an action |
| Retries             | Per-call counts — amplify brownouts into retry storms | A **budget** capped as a share of traffic |
| Timeouts            | One fixed constant               | Derived from the route's own observed p99      |
| Adoption risk       | A library bug becomes your outage | Fail-safe pass-through + self-disabling guard  |

`curo` is not trying to replace [`gobreaker`](https://github.com/sony/gobreaker) or
[`failsafe-go`](https://github.com/failsafe-go/failsafe-go) as a policy _executor_.
It replaces the **human** who decides what the policy should be.

---

## Install

```sh
go get github.com/raj1kshtz/curo
```

The core has **zero third-party dependencies**. Integrations (OpenTelemetry, Prometheus)
ship as separate Go modules so they never enter your dependency graph unless you ask.

---

## Quick start

```go
package main

import (
	"log/slog"
	"net/http"

	"github.com/raj1kshtz/curo"
)

func main() {
	// h is ALWAYS non-nil and ALWAYS safe to use — even when err != nil.
	h, err := curo.New(
		curo.WithMode(curo.Observe), // the default; promote to Enforce once you trust it
		curo.WithLogger(slog.Default()),
	)
	if err != nil {
		// Advisory only. curo has already degraded itself to transparent pass-through.
		// You never have to handle this to remain safe.
		slog.Warn("curo degraded", "err", err)
	}
	defer h.Close()

	client := &http.Client{
		Transport: h.Wrap(http.DefaultTransport),
	}

	resp, err := client.Get("https://api.example.com/v1/things")
	_ = resp
	_ = err
}
```

That is the entire integration. One wrap on the transport you already have.

---

## Operating modes

Adoption is staged on purpose. You get value on day one without granting `curo` any
authority over your traffic.

| Mode           | Observes | Acts | Use it when                                                  |
| -------------- | :------: | :--: | ------------------------------------------------------------ |
| `curo.Off`     |    ✗     |  ✗   | Kill switch. Behaves exactly like the transport you wrapped.  |
| `curo.Observe` |    ✓     |  ✗   | **Default.** Full detection and diagnosis, zero intervention. |
| `curo.Enforce` |    ✓     |  ✓   | You have reviewed the diagnoses and trust them.               |

In `Observe`, `curo` reports every action it _would_ have taken. Run it in production
on day one, read the findings for a week, then flip to `Enforce`.

Modes are switchable at runtime — no restart, no redeploy:

```go
h.SetMode(curo.Off)
```

---

## The healer cannot break your application

A library that fixes outages is worthless if it can cause them. `curo` is built around a
single non-negotiable constraint, recorded in
[ADR-0002](docs/adr/0002-host-application-inviolability.md):

> **`curo` must never terminate, hang, or degrade the host process.** Any internal
> failure degrades `curo` to a transparent pass-through — without operator action.

How that is enforced:

- **Panic containment** at every entry point, including _inside every goroutine_ `curo`
  spawns (a parent `recover()` cannot catch those — the process would die).
- **Self-disabling guard.** `curo` runs a circuit breaker over _its own_ internal error
  rate. Enough internal faults and it permanently disarms itself. The healer heals itself.
- **Pristine fallback.** The original `*http.Request` is never mutated; `curo` works on a
  clone. If anything goes wrong, your untouched request is replayed on the base transport.
- **No `panic`, `os.Exit` or `log.Fatal`** in library code — enforced by
  [`forbidigo`](.golangci.yml) in CI, not by discipline.
- **Bounded everything.** Ring buffers and capped route cardinality. No unbounded map
  keyed by URL, no goroutine leaks — verified with `goleak`.
- **No `init()` side effects.** `curo` never touches `http.DefaultTransport` or any
  global you did not hand it.

This is a claim we test rather than assert: a fault-injection harness randomly panics
inside every internal component and asserts that the host survives **and the request
still succeeds**.

---

## How it works

```
                  ┌──────────────────── guard (panic containment, self-disable) ────────────────────┐
                  │                                                                                 │
  http.Request ──▶│  detect ──▶ diagnose ──▶ policy ──▶ mitigate ──▶ base http.RoundTripper         │──▶ http.Response
                  │  (rolling   (classify   (choose    (retry /                                     │
                  │   windows)   cause)      action)    breaker /                                   │
                  │                                     timeout)                                    │
                  └─────────────────────────────────────────────────────────────────────────────────┘
                                       any failure in here ──▶ transparent pass-through
```

1. **Detect** — per-route rolling windows track error rate, latency EWMA, p99, timeout
   rate and in-flight count. Fixed memory per route, capped route cardinality.
2. **Diagnose** — the signal is classified into a small closed set of causes:
   `Healthy`, `Transient`, `DependencyDown`, `Saturation`, `ClientError`, `Degrading`.
3. **Mitigate** — the diagnosis selects the mitigation. A `ClientError` is never retried.
   `Saturation` sheds load rather than adding to it. `DependencyDown` opens the breaker
   and fails fast.

Every autonomous decision is auditable through the `OnAction` hook and exported metrics.
`curo` is never allowed to be a black box.

Full detail: [`docs/design/engine.md`](docs/design/engine.md).

---

## Roadmap

**v0.1.0 (MVP)** — egress `http.RoundTripper`, guard layer, three modes, adaptive retry
budgets, adaptive breaker, adaptive timeouts, `OnAction` auditing.

**Deliberately out of scope for v0.1** — control plane, ingress middleware, gRPC,
fallback caching, distributed/shared state, anything ML.
See [ADR-0001](docs/adr/0001-embedded-library-over-sidecar-proxy.md) for why.

---

## Contributing

Contributions are very welcome — especially adversarial ones. If you can make `curo`
panic, hang, or leak inside a host application, that is the most valuable issue you can file.

Start with [`CONTRIBUTING.md`](CONTRIBUTING.md) and the
[Architecture Decision Records](docs/adr/). Discussion happens in the open on the issue
tracker; please open an issue before a large PR.

---

## License

[Apache License 2.0](LICENSE) — chosen for its explicit patent grant.
