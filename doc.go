// Package curo provides an explicit HTTP transport wrapper for adaptive
// resilience.
//
// The current implementation provides transparent delegation, operating-mode
// control, lifecycle semantics, bounded request observation, aggregate runtime
// statistics, deterministic diagnosis, control-candidate evaluation, a
// pull-based decision report, and guarded request stages. In Enforce mode it
// applies both control candidates. A replay-safe request whose initial attempt
// failed with a transport error or a 502, 503, or 504 response may be retried
// once within aggregate retry budgets. When a target is diagnosed as down, a
// dependency failure opens its dependency breaker: requests to the target then
// fail fast with ErrBreakerOpen, and after a cooldown one request at a time
// probes whether the dependency recovered.
package curo
