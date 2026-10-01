// Package curo provides an explicit HTTP transport wrapper for adaptive
// resilience.
//
// The current implementation provides transparent delegation, operating-mode
// control, lifecycle semantics, bounded request observation, aggregate runtime
// statistics, deterministic internal diagnosis, and guarded request stages.
// Policy evaluation, public diagnosis delivery, and mitigation are not
// implemented yet.
package curo
