// Package curo provides an explicit HTTP transport wrapper for adaptive
// resilience.
//
// The current implementation provides transparent delegation, operating-mode
// control, lifecycle semantics, and guarded boundaries for internal request
// stages. Adaptive observation and mitigation are not implemented yet.
package curo
