package retry

import "sync/atomic"

const (
	// TargetCapacity is the most retry tokens one target budget can hold.
	TargetCapacity = 10

	// InstanceCapacity is the most retry tokens one instance budget can hold.
	InstanceCapacity = 50

	// DepositsPerToken is the number of recorded initial attempts that fund one
	// retry token.
	DepositsPerToken = 10

	milliTokensPerToken = 1_000
	depositMilliTokens  = milliTokensPerToken / DepositsPerToken

	targetCapacityMilliTokens   = TargetCapacity * milliTokensPerToken
	instanceCapacityMilliTokens = InstanceCapacity * milliTokensPerToken

	// A budget state packs available milli-tokens into its low bits and the
	// pending reservation count into its high bits.
	pendingShift  = 32
	pendingUnit   = int64(1) << pendingShift
	availableMask = pendingUnit - 1
)

// Budget is a lock-free retry token bucket funded by recorded initial
// attempts. The zero value is empty and ready to use. A Budget must not be
// copied after first use.
//
// Available credit plus pending reservations never exceeds capacity, so a
// deposit cannot refill capacity that a pending reservation may still spend.
type Budget struct {
	state atomic.Int64
}

// Lease holds one retry token reserved from a target budget and an instance
// budget. The zero Lease holds nothing. The first Commit or Cancel releases
// the reservation, and later calls on that Lease do nothing. Copies of a held
// Lease share its reservation, so exactly one copy may release it.
type Lease struct {
	target   *Budget
	instance *Budget
}

// Deposit credits one recorded initial attempt to a target budget and its
// instance budget.
func Deposit(target, instance *Budget) {
	target.deposit(targetCapacityMilliTokens)
	instance.deposit(instanceCapacityMilliTokens)
}

// Reserve takes one retry token from both budgets, or from neither. The
// target budget is reserved first and released again if the instance budget
// is empty.
func Reserve(target, instance *Budget) (Lease, bool) {
	if !target.reserve() {
		return Lease{}, false
	}
	if !instance.reserve() {
		target.cancel()
		return Lease{}, false
	}

	return Lease{target: target, instance: instance}, true
}

// Held reports whether lease still holds a reservation.
func (lease *Lease) Held() bool {
	return lease.target != nil
}

// Commit spends the reserved token because its retry attempt is starting.
func (lease *Lease) Commit() {
	if lease.target == nil {
		return
	}

	lease.target.commit()
	lease.instance.commit()
	*lease = Lease{}
}

// Cancel returns the reserved token because its retry attempt will not start.
func (lease *Lease) Cancel() {
	if lease.target == nil {
		return
	}

	lease.target.cancel()
	lease.instance.cancel()
	*lease = Lease{}
}

func (budget *Budget) deposit(capacity int64) {
	for {
		state := budget.state.Load()
		credit := available(state) + pending(state)*milliTokensPerToken
		if credit >= capacity {
			return
		}

		next := state + min(depositMilliTokens, capacity-credit)
		if budget.state.CompareAndSwap(state, next) {
			return
		}
	}
}

func (budget *Budget) reserve() bool {
	for {
		state := budget.state.Load()
		if available(state) < milliTokensPerToken {
			return false
		}

		next := state - milliTokensPerToken + pendingUnit
		if budget.state.CompareAndSwap(state, next) {
			return true
		}
	}
}

func (budget *Budget) commit() {
	budget.state.Add(-pendingUnit)
}

func (budget *Budget) cancel() {
	budget.state.Add(milliTokensPerToken - pendingUnit)
}

func available(state int64) int64 {
	return state & availableMask
}

func pending(state int64) int64 {
	return state >> pendingShift
}
