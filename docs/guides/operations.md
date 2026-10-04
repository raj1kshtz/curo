# Operating Curo

This guide explains how to run Curo in a service. It covers wiring Curo,
bounding requests, choosing timeout bounds, rolling out `Enforce`,
monitoring, handling Curo's errors, and what to expect when a dependency
fails. The [behavior reference](../reference/behavior.md) describes each
mechanism in detail, and the [engine design](../design/engine.md) explains
the reasons behind it.

Apart from the timeout bounds, every threshold is fixed. This guide therefore
explains what the thresholds mean in operation rather than how to tune them.

Measured numbers come from the deterministic failure simulation, whose
scorecard is [`testdata/simulation.golden`](../../testdata/simulation.golden).
Most scenarios send 5 reads per second to one dependency, some add 1 write
per second, and callers give up after one minute. Other traffic gives
different numbers.

## Contents

1. [Wire one transport](#wire-one-transport)
2. [Bound every request](#bound-every-request)
3. [Choose timeout bounds](#choose-timeout-bounds)
4. [Roll out Enforce](#roll-out-enforce)
5. [Monitor](#monitor)
6. [Handle Curo errors](#handle-curo-errors)
7. [What to expect during incidents](#what-to-expect-during-incidents)
8. [Restarts and low traffic](#restarts-and-low-traffic)
9. [Capacity](#capacity)
10. [Troubleshooting](#troubleshooting)
11. [Known limitations](#known-limitations)

## Wire one transport

Create one `curo.Transport` per base transport when the process starts, and
close it during shutdown:

```go
func run(logger *slog.Logger) error {
    transport, err := curo.New(
        http.DefaultTransport,
        curo.WithTimeoutBounds(2*time.Second, 9*time.Second),
        curo.WithLogger(logger),
    )
    if err != nil {
        return err
    }
    defer transport.Close()

    client := &http.Client{
        Transport: transport,
        Timeout:   10 * time.Second,
    }

    return serve(client, transport)
}
```

`curo.New` starts in `Observe` mode, which never changes a request. The
timeout ceiling stays below the client's timeout, as
[Choose timeout bounds](#choose-timeout-bounds) explains. `WithLogger` gives
Curo the service's logger, which receives only Curo's own failures, as
[Monitor](#monitor) describes.

- Share the transport across goroutines and clients for the life of the
  process. Evidence, retry budgets, breakers, and timeouts belong to one
  transport, so a transport created per request never warms up and never
  acts.
- `Close` does not close the base transport, and requests made after `Close`
  pass straight through to it. `http.Client.CloseIdleConnections` reaches the
  base transport through Curo.
- The mode applies to the whole transport. To roll out one dependency at a
  time, give its client a transport of its own.
- Give clients that call user-supplied URLs, such as webhooks, link previews,
  or crawlers, their own transport, or none. A transport tracks at most 128
  targets, one per scheme, host, port, and method class, in the order it
  first sees them. Once all 128 are tracked, a new target can replace only
  one that has been idle for 15 minutes and shares its internal shard, one
  of eight. Requests to any other target, and requests whose scheme is not
  HTTP or HTTPS or whose host or port is invalid, share an overflow
  aggregate that gets no retries, breakers, or timeouts. In the cardinality
  simulation, 12,000 one-off hosts sent 11,746 requests to overflow. The
  main dependency kept its protection because it was tracked before the
  registry filled and stayed busy.

## Bound every request

Curo never extends a caller's deadline, and even in `Enforce` mode it never
times some requests: writes, breaker probes, reads to a target in overflow
or with fewer than 100 latency samples, and every request when adaptive
timeouts are disabled. Bound them yourself:

- Set `http.Client.Timeout` or a context deadline on every request. It
  bounds the caller's whole wait, including Curo's retry backoff and retry.
- Consider a `ResponseHeaderTimeout` on the base `http.Transport`. It bounds
  the wait for response headers after a request is written, including for
  probes and writes, and unlike a caller's deadline, Curo counts it as a
  dependency failure, so a hang can open a breaker. It counts slow
  legitimate responses as failures too, so set it above the slowest
  legitimate response to any request the transport sends, and below the
  callers' deadlines. It does not bound sending the request or reading the
  response body; the caller's deadline does:

```go
base := http.DefaultTransport.(*http.Transport).Clone()
base.ResponseHeaderTimeout = 9 * time.Second

transport, err := curo.New(base)
```

Without a bound, a probe to a hung dependency waits for as long as the
dependency holds the connection open. In the hang simulation, each of four
probes held its caller until the caller gave up after one minute.

## Choose timeout bounds

The adaptive timeout applies only in `Enforce` mode, only to reads, and only
after a target holds 100 latency samples from about the last 30 minutes. It
is three times the upper bound of the latency bucket that holds the slowest
retained sample, clamped to the bounds. With the default bounds of 2 and 30
seconds:

| Slowest retained sample | Adaptive timeout |
| --- | --- |
| 500ms or less | 2s, the floor |
| Over 500ms, up to 1s | 3s |
| Over 1s, up to 2.5s | 7.5s |
| Over 2.5s, up to 5s | 15s |
| Over 5s | 30s, the ceiling |

Choose the bounds with three rules:

1. Set the ceiling above the slowest response that a read may legitimately
   take, because Curo cuts reads that wait longer.
2. Set the ceiling below the shortest deadline that callers put on those
   reads, with a margin. Curo times a read only when its deadline leaves more
   time than the timeout, and only a cut at the ceiling counts as a
   dependency failure. When a caller's deadline ends a hung read first, Curo
   counts it as the caller giving up rather than as a dependency failure, so
   the hang does not open the breaker. A lower ceiling also lets Curo count
   a hang as failures sooner.
3. Raise the floor for a dependency whose legitimately slow reads may be
   missing from the last 30 minutes of samples, such as cold caches or rare
   expensive queries. A slowdown beyond the current timeout cuts reads: the
   first cut raises the timeout for later reads, but reads already in flight
   keep the timeout they started with. When answers in the latency-step
   simulation jumped to 8 or 9 seconds, the first cut at 7.5 seconds raised
   the timeout to 30 seconds, and 37 more reads already in flight were cut
   at 7.5 seconds. All 38 would have succeeded.

For a client with a 10-second timeout:

```go
transport, err := curo.New(
    http.DefaultTransport,
    curo.WithTimeoutBounds(2*time.Second, 9*time.Second),
)
```

`WithTimeoutBounds(0, 0)` disables adaptive timeouts, and retries and
breakers still apply. In `Observe` mode, `ShadowTimeouts` counts reads that
the timeout would have ended, so bounds can be tested before `Enforce` uses
them.

## Roll out Enforce

`Observe`, the default, records evidence and evaluates decisions but never
changes a request. It also funds the retry budgets, so credit is ready when
the mode switches.

1. Deploy in `Observe`.
2. Let targets warm up. A target becomes `Ready` after 20 requests, not
   counting those that their callers canceled or let time out, spanning at
   least 30 seconds within two minutes. That takes about 30 seconds at
   steady traffic. A target gets an adaptive timeout after 100 samples.
   Retries and breakers act only on `Ready` targets.
3. Review a representative period that includes peak traffic:
   - `InternalFailures` stays at zero and `SelfDisabled` stays false.
   - `OverflowRequests` stays flat and `TrackedTargets` stays well below 128.
   - `ShadowTimeouts` stays near zero outside incidents. Each one is a read
     that `Enforce` would have ended; if those reads are legitimate, raise
     the bounds.
   - `Report` lists the targets you expect, mostly `Healthy`, with timeouts
     you accept.
4. Enable `Enforce` on a canary, such as one instance or a small share of
   instances, with `curo.WithMode(curo.Enforce)` at startup or `SetMode` at
   runtime. Every process decides alone, so the canary affects only its own
   requests.
5. Compare the canary's error rate and latency with the other instances,
   along with its retries, rejections, and timeouts.
6. Expand gradually.

Connect `SetMode` to a runtime control, such as an admin endpoint or a
configuration watcher, so that a rollback needs no deploy:

- `SetMode(curo.Observe)` stops every action and keeps learning. Use it when
  an action misbehaves.
- `SetMode(curo.Off)` also stops collecting evidence. Use it when Curo itself
  is suspect.

`Mode.UnmarshalText` accepts `off`, `observe`, or `enforce` in any letter case
and rejects any other text with `ErrInvalidMode`, so the control can take the
mode as text. The same method lets `flag.TextVar` or a configuration decoder
set the startup mode.

A mode change applies to requests that start after it, and it withdraws
retries that have not started and timeouts that have not fired. Breaker state
survives it, so restoring `Enforce` while a breaker is open fails requests
fast again until a probe closes the breaker.

## Monitor

`Stats` returns cumulative counters for one transport, without target names.
Export it periodically and alert on rates rather than totals.
[`ExampleTransport_Stats`](../../example_test.go) shows an export through
`expvar`.

| Field | Meaning and action |
| --- | --- |
| `InternalFailures` | Curo contained a failure of its own, and requests were unaffected. Report any increase as a bug, with the records that `WithLogger` wrote. |
| `SelfDisabled` | Three internal failures within one minute made the transport pass every request straight through for the rest of its life. Requests still work but are unprotected. Report the bug and restart. |
| `OverflowRequests` | Requests that went to the overflow aggregate, which gets no actions: requests to targets beyond the tracked 128, or with an invalid scheme, host, or port. If it grows, move high-cardinality traffic to its own transport. |
| `TrackedTargets` | Tracked targets. Close to 128 means overflow is near. |
| `RetryAttempts` | Retries sent. Each first attempt that Curo observes to a tracked target earns a tenth of a retry, and the budgets start empty and hold at most 10 retries per target and 50 per transport. A sustained rate near one retry per ten requests means sustained failures. |
| `RetrySuccesses` | Retries that received a 2xx or 3xx response. A low share of `RetryAttempts` means retries are not recovering requests, as during an outage. |
| `RetryBudgetDenials` | Retries skipped because a budget was empty. Growth means failures exceed what the budget allows. |
| `BreakerOpens` | Breakers opened. `Report.Changes` shows which targets gained `CandidateBreakerOpen`. |
| `BreakerProbes` | Requests sent to check whether a dependency recovered. |
| `BreakerRejections` | Requests failed fast with `ErrBreakerOpen` without being sent. |
| `Timeouts` | Reads that `Enforce` ended with `ErrTimeout`. |
| `ShadowTimeouts` | Reads that `Observe` let finish but `Enforce` would have ended. |

With `WithLogger`, internal failures are also offered to the logger at `Warn`
level with the stage that failed and, for a panic, a stack trace without
argument values. Self-disable is offered once, at `Error` level. Curo logs
nothing else. The logger's level and handler decide which records are
written, so alert on `SelfDisabled`, which does not depend on the logger, and
count failures with `InternalFailures`, not with records: a transport offers
its logger one record at a time and at most three failure records a minute.
The [behavior reference](../reference/behavior.md#failure-containment) lists
every attribute and limit.

`Report` gives per-target detail, for a debug endpoint or a periodic log. It
encodes to JSON with names, such as `"Diagnosis":"Transient"`, so
`encoding/json` can serve it as it is. `Report.Targets` holds the latest
decision for each tracked target, and `Report.Changes` holds the latest 256
candidate changes with contiguous sequence numbers. Requests produce the
decisions, and each decision expires at most ten seconds after it was made. A
request after that replaces it, and some events, such as the timeout rising
or a breaker closing, replace it sooner. `Report` only copies the decisions,
so polling much more often than every ten seconds gains little. Remember the
last sequence logged, so that a gap shows changes overwritten between reads:

```go
func logChanges(transport *curo.Transport, last uint64) uint64 {
    for _, change := range transport.Report().Changes {
        if change.Sequence <= last {
            continue
        }
        if change.Sequence > last+1 {
            log.Printf("curo: %d changes missed", change.Sequence-last-1)
        }
        decision := change.Decision
        log.Printf(
            "curo: host=%s method=%s diagnosis=%s candidates=%s",
            decision.Target.Host,
            decision.Target.Method,
            decision.Diagnosis,
            decision.Candidates,
        )
        last = change.Sequence
    }

    return last
}
```

`Report` never evaluates evidence, so an idle target keeps its last decision;
compare `ExpiresAt` with the current time before relying on one. Hosts can
come from untrusted input, so do not use `Target.Host` as a metric label
unless the hosts are known in advance.

## Handle Curo errors

`http.Client` wraps transport errors in `*url.Error`, so check Curo's errors
with `errors.Is`. [`Example_errorHandling`](../../example_test.go) shows a
complete check.

- `ErrBreakerOpen` means Curo did not send the request, because the
  dependency is diagnosed down. Fail fast with a fallback, cached data, or an
  error for your caller. Retrying at once fails the same way until a probe
  closes the breaker. While a breaker is open, a request that its caller
  already canceled also gets `ErrBreakerOpen` rather than its context error.
- `ErrTimeout` means Curo ended a read that waited too long for response
  headers. The dependency may have received the request, and Curo never
  retries it. It reports true from `Timeout` and matches
  `context.DeadlineExceeded`, so existing timeout handling keeps working.
  Check `ErrTimeout` first to tell it apart from the caller's own deadline.
- Curo never retries a 429 response or opens a breaker for one, so honor
  `Retry-After` in your own code.
- Other errors come from the base transport.

## What to expect during incidents

| Simulated incident | What Curo did |
| --- | --- |
| Every attempt gets 503 for 3 minutes | Retried reads during the first minute, 39 times without success, within the budget. Opened the read and write breakers 60 seconds in, then probed after 5, 10, 20, 40, and 60 seconds. During the fault the dependency received 0.38 attempts per read instead of 1. The read breaker closed 16 seconds after recovery and the write breaker 20 seconds after, rejecting 80 reads and 20 writes that would have succeeded. |
| The dependency stops answering for 5 minutes | Cut reads at 7.5 seconds, then at the 30-second ceiling, where cuts count as failures. Opened the breaker 80 seconds in. Over the run, callers waited 1.6 seconds on average instead of 12, with at most 150 requests in flight instead of 300. Four probes each held a caller for one minute. The breaker closed 35 seconds after recovery, rejecting 175 reads that would have succeeded. |
| 5% of attempts get a connection reset for 10 minutes | Retried failed reads once, recovering 157 of 164 failures for 2.7% more attempts. Writes were not retried. |
| 30% of attempts get 429 for 5 minutes | Diagnosed `Saturation`, and neither retried nor opened a breaker. |
| Answers take 8 to 9 seconds for 5 minutes | Cut 38 reads at 7.5 seconds: the first cut raised the timeout to 30 seconds, and 37 reads already in flight kept 7.5 seconds. No breaker opened. |
| Connection resets for 5 minutes, then again for 2 minutes starting 45 seconds after recovery | Reopened the breaker 26 seconds into the relapse. Because it reopened within 2 minutes of closing, it waited 60 seconds before probing, and closed 26 seconds after the relapse ended. |
| 10 of 20 dependencies get 503 for the same 3 minutes | Opened 10 independent breakers and left the 10 healthy dependencies untouched. |

The breaker trades a short period of rejections after recovery for failing
fast during an outage. Expect these costs:

- Detection takes about a minute in a complete outage at steady traffic,
  because failures must make up half of the two-minute window. A hang takes
  longer, because only cuts at the ceiling count as failures.
- Recovery waits for the next successful probe, and a probe needs a request
  to carry it. The cooldown starts at 5 seconds and doubles up to 60, and a
  probe that hangs first holds its 30-second lease, so after a long outage,
  requests can fail fast for a minute or more after the dependency
  recovers.
- Retries during the first minute of an outage are wasted, although budgets
  limit them.

## Restarts and low traffic

Curo keeps all state in memory, per transport. After a restart:

- Every target starts `Cold`. Retries and breakers wait until it is `Ready`,
  about 30 seconds at steady traffic. In the simulation, a target got its
  adaptive timeout after 20 seconds and reached `Ready` after 30.
- Retry budgets start empty. Each observed first attempt adds a tenth of a
  token, so the tenth request to a target is the first that can be retried.
- Replicas learn and act independently.

Low-volume dependencies get fewer actions:

- A target becomes `Ready` from recent traffic after 20 requests spanning 30
  seconds within two minutes. It also becomes `Ready` from its history after
  20 requests spanning 10 minutes, as long as at least one more came in the
  last two minutes. Requests that their callers canceled or let time out do
  not count, nor do reads that Curo cut below the ceiling.
- A breaker opens only with at least 10 requests and 5 failures within two
  minutes, so a dependency called fewer than 10 times in two minutes never
  gets one.
- An adaptive timeout needs 100 samples within about 30 minutes.

## Capacity

- Retained state is bounded regardless of traffic. On 64-bit platforms,
  target state takes about 1.23 MiB per transport with all 128 targets in
  use, and the change journal takes 45,072 bytes, plus bounded map overhead.
  Requests in flight add transient allocations.
- Curo runs no background workers. Each timed attempt uses one timer, whose
  callback runs on its own goroutine only if the timeout fires.
- Benchmarks and allocation gates in CI measure the per-request cost, as the
  [engine design](../design/engine.md#19-performance-and-resource-targets)
  describes.

## Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| A request waited until its caller gave up while a dependency was down | Curo did not time it. Curo never times breaker probes, writes, reads to a target in overflow or with fewer than 100 samples, or reads whose deadline leaves no more time than the timeout. [Bound every request](#bound-every-request). |
| A hung dependency never opens a breaker | Caller deadlines end the reads before Curo's ceiling does, so they do not count as failures. [Set the ceiling below them](#choose-timeout-bounds). A breaker also needs `Enforce` and a `Ready` target outside overflow. |
| Requests fail with `ErrBreakerOpen` after the dependency recovered | The breaker closes only when a probe succeeds. After a long outage, that can take a minute or more. |
| A breaker took about a minute to open | Expected: failures must make up half of the two-minute window. |
| `Report` shows `Degrading` right after a breaker closes | Evidence is thin right after the close, so the comparison with history can briefly flag it. `Degrading` selects no retry or breaker, though an adaptive timeout may stay active. |
| A dependency gets no retries or breaker | The transport is not in `Enforce`, was closed, or self-disabled; the target is not `Ready` because of low traffic or a restart; or the target is in overflow. Check the decision's `Readiness` and `Reasons`. |
| A failed request was not retried | Curo retries only replay-safe reads after a transport error, or a 502, 503, or 504 without `Retry-After`, when the target's decision holds `CandidateRetry`, the deadline leaves room, and both budgets hold a token. The [behavior reference](../reference/behavior.md#retries) lists every condition. |
| Curo stopped acting | `SelfDisabled` is true, the transport was closed, or the mode changed. |
| `OverflowRequests` keeps growing | The transport sees more than 128 targets, or requests with an invalid scheme, host, or port. Move user-supplied URLs to their own transport. |
| Reads fail with `ErrTimeout` after a slowdown | The first cut raises the timeout for later reads, up to the ceiling, but reads already in flight keep the timeout they started with. Raise the floor if slow periods are expected, or the ceiling if legitimate reads take longer than it. |
| The timeout stays high long after one slow response | A slow sample keeps it raised for up to about 30 minutes, until the sample ages out. |

## Known limitations

- Thresholds are fixed, apart from the timeout bounds.
- State belongs to one process, so replicas decide independently and a
  restart starts cold.
- A target covers every path on a host with the same method class, so a
  failing or slow path affects the others.
- Curo never times breaker probes, and a probe is a real application
  request, which may be a write.
- The mode applies to a whole transport.
- Logged failures carry no request context, so they cannot be joined to a
  request's trace.

The behavior reference lists the detailed limits of
[dependency breakers](../reference/behavior.md#dependency-breakers) and
[adaptive timeouts](../reference/behavior.md#adaptive-timeouts).
