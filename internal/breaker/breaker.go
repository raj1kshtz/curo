// Package breaker contains Curo's per-target dependency breaker.
//
// A Breaker is a deterministic state machine. It never starts an attempt,
// reads a clock, or locks. Callers serialize access and supply observer ticks,
// which are nanoseconds since an observer origin.
package breaker

import (
	"math"
	"time"
)

const (
	// BaseCooldown is how long a breaker stays open after it first opens. It is
	// also the delay before the next probe after an inconclusive one.
	BaseCooldown = 5 * time.Second

	// MaxCooldown caps the cooldown, which doubles after every failed probe and
	// every reopening during probation.
	MaxCooldown = time.Minute

	// ProbeTimeout is how long a probe holds its lease. A lease that expires
	// without a result counts as a failed probe.
	ProbeTimeout = 30 * time.Second

	// Probation is how long after a close that opening again continues the
	// previous cooldown escalation instead of starting over.
	Probation = 2 * time.Minute
)

// maxLevel is the first escalation level whose cooldown reaches MaxCooldown.
const maxLevel = 4

// State is a breaker state.
type State uint8

const (
	// Closed lets every request through.
	Closed State = iota

	// Open rejects requests until its cooldown ends. Then the first eligible
	// request becomes a probe.
	Open

	// Probing rejects requests while one probe holds the lease.
	Probing
)

// Admission is how a breaker treats one request.
type Admission uint8

const (
	// Pass lets a request through a closed breaker.
	Pass Admission = iota

	// Probe lets one request through an open breaker to test whether the
	// dependency recovered. The request holds the lease returned with it.
	Probe

	// Reject fails a request fast without sending it.
	Reject
)

// Outcome is what a probe's result shows about the dependency.
type Outcome uint8

const (
	// Inconclusive means the caller canceled the probe, or the caller's
	// deadline expired first. It shows nothing about the dependency.
	Inconclusive Outcome = iota

	// Healthy means the dependency answered without a dependency failure or a
	// rate limit.
	Healthy

	// Failed means the probe failed for a reason attributed to the dependency,
	// or the dependency rate limited it.
	Failed
)

// Breaker is one dependency breaker. The zero value is closed.
type Breaker struct {
	last     int64  // latest tick seen; transitions never move backwards
	closedAt int64  // tick of the latest close
	until    int64  // Open: end of the cooldown; Probing: end of the lease
	lease    uint64 // latest probe lease; zero before the first probe
	episode  uint64 // first lease of the current open episode
	state    State
	level    uint8
	closed   bool // whether the breaker has closed after opening
}

// State returns the current state without advancing time. A probe lease that
// expired is reported as Probing until the next Admit or Settle.
func (breaker *Breaker) State() State {
	return breaker.state
}

// Admit decides how the breaker treats a request at tick.
//
// Only an eligible request may become a probe. A request its caller already
// canceled is not eligible, so it is rejected instead of being spent as a
// probe. A probe lease that expired before tick counts as a failed probe as of
// its deadline.
func (breaker *Breaker) Admit(tick int64, eligible bool) (Admission, uint64) {
	tick = breaker.advance(tick)
	breaker.expire(tick)

	if breaker.state == Closed {
		return Pass, 0
	}
	if breaker.state == Probing || tick < breaker.until || !eligible {
		return Reject, 0
	}

	breaker.lease++
	breaker.state = Probing
	breaker.until = deadline(tick, ProbeTimeout)
	return Probe, breaker.lease
}

// Trip opens a closed breaker at tick and reports whether it opened.
//
// Opening again within Probation of the latest close escalates the cooldown.
// Otherwise the cooldown starts at BaseCooldown.
func (breaker *Breaker) Trip(tick int64) bool {
	tick = breaker.advance(tick)
	if breaker.state != Closed {
		return false
	}

	if breaker.closed && tick-breaker.closedAt < int64(Probation) {
		breaker.level = min(breaker.level+1, maxLevel)
	} else {
		breaker.level = 0
	}
	breaker.episode = breaker.lease + 1
	breaker.state = Open
	breaker.until = deadline(tick, cooldown(breaker.level))
	return true
}

// Settle applies the outcome of the probe that held lease at tick and reports
// whether the breaker closed.
//
// Settlements of a closed breaker and leases from an earlier open episode are
// ignored. A probe whose lease expired was already counted as failed, so only
// a Healthy outcome still matters: it closes the breaker, because the
// dependency answered.
func (breaker *Breaker) Settle(lease uint64, outcome Outcome, tick int64) bool {
	tick = breaker.advance(tick)
	breaker.expire(tick)

	if breaker.state == Closed ||
		lease < breaker.episode ||
		lease > breaker.lease {
		return false
	}

	current := breaker.state == Probing && lease == breaker.lease
	switch {
	case outcome == Healthy:
		breaker.state = Closed
		breaker.closedAt = tick
		breaker.closed = true
		return true
	case current && outcome == Failed:
		breaker.reopen(tick)
	case current:
		breaker.state = Open
		breaker.until = deadline(tick, BaseCooldown)
	}

	return false
}

// advance returns tick, or the latest tick seen if tick is earlier, so
// transitions never move backwards. Negative ticks therefore clamp to zero.
func (breaker *Breaker) advance(tick int64) int64 {
	if tick < breaker.last {
		return breaker.last
	}

	breaker.last = tick
	return tick
}

// expire counts an expired probe lease as a failed probe as of its deadline.
func (breaker *Breaker) expire(tick int64) {
	if breaker.state == Probing && tick >= breaker.until {
		breaker.reopen(breaker.until)
	}
}

func (breaker *Breaker) reopen(at int64) {
	breaker.level = min(breaker.level+1, maxLevel)
	breaker.state = Open
	breaker.until = deadline(at, cooldown(breaker.level))
}

func cooldown(level uint8) time.Duration {
	return min(BaseCooldown<<level, MaxCooldown)
}

func deadline(tick int64, delay time.Duration) int64 {
	if tick > math.MaxInt64-int64(delay) {
		return math.MaxInt64
	}

	return tick + int64(delay)
}
