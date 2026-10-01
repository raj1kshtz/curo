// Package curo provides an explicit HTTP transport wrapper for adaptive
// resilience.
//
// The current implementation provides transparent delegation, operating-mode
// control, lifecycle semantics, bounded request observation, aggregate runtime
// statistics, deterministic diagnosis, control-candidate evaluation, a
// pull-based decision report, and guarded request stages. Mitigation is not
// implemented yet, so candidates are never applied.
package curo
