# ADR-0011: Log Internal Failures Through log/slog

- **Status:** Accepted
- **Date:** 2026-10-03
- **Scope:** Internal-failure reporting and the `WithLogger` option
- **Decision owners:** Curo maintainers

## Context

Section 4 of [ADR-0002](0002-host-application-inviolability.md) requires
that internal failures be reported through the configured structured logger
and observable counters, that reporting have its own containment boundary,
and that failure reports never include request bodies, credentials, cookies,
or authorization headers. Curo has no way to configure a logger, so it
implements only the counters: `InternalFailures` and `SelfDisabled` in
`Stats`. They show that Curo contained a failure, but not where or why. An
operator cannot tell a bug in Curo from a bug in the application, or report
enough to fix either.

In practice, a contained failure is a recovered panic. Curo's own stages
return no errors, but its recovery boundary also runs application code: a
request's `GetBody` function, its body's `Close` method, and the `Unwrap`,
`Is`, and `Timeout` methods of errors from the base transport. A panic there
is a bug in the application, and only the failed stage and the stack trace
show it.

Several constraints shape the design:

- The core module has no third-party dependencies
  ([ADR-0004](0004-zero-dependency-core.md)) and supports Go 1.23, so the
  logger must come from the standard library.
- Section 16.4 of the [engine design](../design/engine.md) requires a sink
  that the host provides to run outside internal locks and inside panic
  containment, and it forbids a goroutine per event.
- Section 16.2 of the engine design keeps unbounded raw error strings out of
  logs. Panic values, error messages, and the argument values in a Go stack
  trace come from application code and can hold any data. So can the
  message of a runtime error that reports an index, and the name of a type
  that package `reflect` builds at run time, such as a struct type whose
  field tags, or an array type whose length, come from a request. `%T` and
  the messages of some runtime errors print such names.
- A handler can call back into Curo, for example to add a report to a
  record, and it can hold a lock while it does.
- After self-disable, Curo still settles the timeouts of requests in
  progress, closes request bodies, and builds reports, and their failures
  are still counted, so a disabled transport can keep failing without bound.

## Decision

Curo logs internal failures to a `*slog.Logger` that the application passes
with a new option, `WithLogger`.

### What is logged

- Without `WithLogger`, Curo logs nothing. A nil logger is rejected.
- Each contained failure is offered to the logger at `Warn` level with the
  message `curo contained an internal failure` and these attributes, in
  order:
  - `stage`: the work that failed: `preflight`, `postflight`, `retry`,
    `timeout`, `body`, or `report`.
  - `kind`: `panic`, or `error` when the work returned an error.
  - `type`: the Go type of the panic value or error. Only source code
    declares named types and interface types, so their names are written as
    Go prints them. Other types are written by shape, because package
    `reflect` can build them: `*T`, `[]T`, `map[K]V`, `[...]T` for an array,
    without its length, and `struct`, `func`, or `chan`. A type built at run
    time can also nest without limit, so a name is cut after 16 levels of
    nested types or at 512 bytes, and then ends with `...`.
  - `error`: the message of a Go runtime error whose message is fixed text,
    such as a nil pointer dereference, a division by zero, or a closed
    channel. Curo calls `Error` only on types from package `runtime`, and
    logs the message only if it is on a list of the runtime's fixed
    messages, taken from the supported Go releases. Other runtime errors
    hold values or type names: a bounds error holds the index and the
    length, and a failed type assertion or an unhashable map key names
    types. Their messages are not logged, and neither is the message of an
    `Error` method that panics.
  - `failures`: the failure's number. The guard numbers failures under its
    lock, in the order that they count toward self-disable.
  - `skipped`: the number of failures since the previous failure record
    that were not offered to the logger, only when there were any.
  - `stack`: for a panic, the frames of the goroutine that panicked, from
    the panic outward. Each frame is a function and a source line, in the
    form of a Go panic trace without argument values. The trace is taken
    while the panicking frames are still on the stack and is truncated to
    whole frames within 8 KiB, ending with the runtime's
    `...additional frames elided...` line.
- Self-disable is offered once, as an `Error` record,
  `curo disabled itself after repeated internal failures`, whose `failures`
  attribute is the number of the failure that disabled the transport.
- Panic values, the messages of other errors, argument values, the fields
  of unnamed struct types, the lengths of array types, request bodies,
  headers, and target identities are never logged.

### How it is delivered

- Each guarded operation names its stage. After a failure, the guard
  releases its lock and passes the stage and the recovered value or returned
  error to a reporter.
- One goroutine at a time reports for a transport. A failure that happens
  while it does, on any goroutine, is counted but not offered, and the next
  failure record counts it in `skipped`. So a handler is never called
  reentrantly, even when it calls the transport while it holds a lock.
- The reporter runs synchronously, with `context.Background()`. A failure
  record is offered on the goroutine where the failure happened. The
  self-disable record is offered after a failure record, on the same
  goroutine: the record of the failure that disabled the transport, or the
  record that was being offered when it did. Curo starts no goroutine and
  keeps no buffer for logging. The logger's level and handler decide which
  records are written.
- A panic from the handler is recovered and ignored. It does not count as an
  internal failure, so a broken handler cannot disable Curo or make it log
  without end. A handler that ends its goroutine with `runtime.Goexit`, as
  `testing.T.FailNow` does, still ends that goroutine's turn to report.
- A transport offers at most three failure records per rolling minute. The
  limit follows the self-disable threshold and window, so it skips no
  failure up to and including the one that disables the transport. After
  self-disable, it can skip failures, and the next failure record counts
  them.

### Compatibility

The attribute keys, the stage and kind names, and the levels are documented
behavior that [ADR-0010](0010-versioning-and-compatibility.md) covers. Stage
and kind names follow its rules for enumeration names: new names can be
added, and an existing name never changes meaning, so log consumers must
tolerate names they do not know. The messages, the stack format, the Go type
names and how they are shortened, the runtime messages that are logged, and
the limit are not covered. The limit follows the self-disable threshold,
which is tuning.

## Decision Drivers

- ADR-0002 requires failure reports through a structured logger.
- An operator must be able to tell a bug in Curo from a bug in the
  application, and report it with enough detail to fix it.
- Reporting must not endanger the request, its fallback, or the host's logs.
- Logs must not leak application data.
- The core module stays free of dependencies, and the public API stays small.

## Options Considered

1. **`WithLogger(*slog.Logger)` with synchronous records, one at a time and
   rate-limited - selected.**
   - Strengths: `log/slog` is the standard library's structured logger, and
     common logging libraries provide `slog` handlers. The handler chooses
     the format and filters by level, and the option adds no exported type.
   - Weaknesses: the handler runs on the goroutine where the failure
     happened, so a slow handler delays that work, and failures on other
     goroutines while it runs are not logged.
2. **A callback option with an exported failure type - rejected.**
   - Strengths: the application decides what to do with each failure.
   - Weaknesses: every exported field becomes a compatibility promise, and
     every user must write the formatting that `slog` already provides.
3. **Log to `slog.Default()` unless told otherwise - rejected.**
   - Strengths: failures are visible without setup.
   - Weaknesses: a library should not write to the host's global logger
     without consent. `WithLogger(slog.Default())` does so explicitly.
4. **Deliver records through a buffered channel and a goroutine - rejected.**
   - Strengths: a slow handler cannot delay a request.
   - Weaknesses: the goroutine needs a lifecycle and a policy for a full
     buffer. At three records a minute, synchronous delivery costs little.
5. **Log panic values and error messages - rejected.**
   - Strengths: more detail for debugging.
   - Weaknesses: they come from application code and can hold credentials or
     other private data, which ADR-0002 and section 16.2 of the engine design
     keep out of logs. The type, the stack trace, and the messages of runtime
     errors with fixed text locate the failure without them.
6. **Log the stack trace that `runtime.Stack` writes - rejected.**
   - Strengths: the familiar format of a crash, with no code to maintain.
   - Weaknesses: it prints the argument words of each frame, raw integers,
     lengths, and pointers that can hold application data, and it starts
     with the frames that report the failure. Function names and source
     lines locate the failure without the argument words.
7. **Log the type that `%T` prints, and runtime error messages by type -
   rejected.**
   - Strengths: complete type names, and the types in a failed type
     assertion.
   - Weaknesses: package `reflect` builds struct and array types at run
     time, and their names hold field names, tags, and lengths that can
     come from application data, such as the keys of a JSON document. Named
     types, the shapes of other types, and the stack trace locate the
     failure without them.
8. **Log every failure, including those during a record - rejected.**
   - Strengths: no failure before self-disable goes unlogged.
   - Weaknesses: a handler that calls the transport would be called
     reentrantly, and a handler that held a lock while it did so would
     deadlock its goroutine.
9. **Keep counters only, and supersede the logging requirement of ADR-0002 -
   rejected.**
   - Strengths: no new API and no new risk.
   - Weaknesses: counters cannot say what failed, so failures cannot be
     diagnosed or fixed.
10. **Also log decisions, breaker changes, and request context - deferred.**
    - Strengths: one log would explain both failures and mitigation.
    - Weaknesses: each needs its own API review, and decisions are already
      available through `Transport.Report`.

## Consequences

### Positive

- A contained failure can be diagnosed from its stage and stack trace, and a
  failure in application code points to the application.
- ADR-0002's reporting requirement is met without a dependency.
- A handler can call the transport, even while it holds a lock.
- Applications that do not set a logger see no change.

### Negative

- Stack traces put the host binary's function names and source paths in its
  logs.
- A type that package `reflect` could build is named by its shape, such as
  `struct`, and a failed type assertion has no message. The stack trace
  shows where it happened.
- A slow handler delays the work whose failure it logs, at most three times a
  minute per transport, plus the self-disable record. While it runs,
  failures on other goroutines are counted and skipped.
- Beyond the limit, failures after self-disable are only counted.
- Transports that share a logger do not coordinate, so a handler that calls
  another transport with the same logger can still be called reentrantly.
- Records carry no request context, so they cannot be joined to traces.

## Revisit When

- Users need failure records joined to request traces.
- Users need logs of decisions, breaker changes, or other dependency events.
- Users need records of failures that happen while a record is written.
- A self-disable reset is added, since the limit and the self-disable record
  assume that self-disable is permanent.
- A slow or failing handler causes an incident despite the limit.
