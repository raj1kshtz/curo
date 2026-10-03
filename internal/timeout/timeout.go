// Package timeout contains Curo's adaptive timeout rule and the per-attempt
// mechanism that enforces it.
//
// The rule is a pure function of latency evidence and operator bounds. An
// Attempt bounds how long one attempt may wait for response headers. Neither
// reads the observer's clock nor locks.
package timeout

import "time"

const (
	// MinimumSamples is how many latency samples the retained evidence must
	// hold before a timeout is selected.
	MinimumSamples = 100

	// Multiplier scales the slowest retained latency into a timeout.
	Multiplier = 3
)

// Bounds are the operator's floor and ceiling for adaptive timeouts. The zero
// value disables them.
type Bounds struct {
	Minimum time.Duration
	Maximum time.Duration
}

// DefaultBounds are the bounds a Transport uses unless configured otherwise.
var DefaultBounds = Bounds{
	Minimum: 2 * time.Second,
	Maximum: 30 * time.Second,
}

// Enabled reports whether bounds permit adaptive timeouts. Bounds are enabled
// only when 0 < Minimum <= Maximum.
func (bounds Bounds) Enabled() bool {
	return bounds.Minimum > 0 && bounds.Minimum <= bounds.Maximum
}

// Select returns the timeout for a target whose retained evidence holds
// samples latency samples, the slowest of which falls in a latency bucket
// with upper bound slowest. An overflow bucket is passed as the largest
// Duration. Select returns zero when bounds are disabled, samples are fewer
// than MinimumSamples, or slowest is not positive.
//
// The timeout is Multiplier times slowest, clamped to bounds.
func Select(samples uint64, slowest time.Duration, bounds Bounds) time.Duration {
	if !bounds.Enabled() || samples < MinimumSamples || slowest <= 0 {
		return 0
	}
	if slowest > bounds.Maximum/Multiplier {
		return bounds.Maximum
	}

	return max(slowest*Multiplier, bounds.Minimum)
}
