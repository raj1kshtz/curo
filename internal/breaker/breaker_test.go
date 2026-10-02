package breaker

import (
	"math"
	"testing"
	"time"
)

const second = int64(time.Second)

func TestZeroBreakerIsClosed(t *testing.T) {
	t.Parallel()

	var breaker Breaker
	assertState(t, &breaker, Closed)
	if lease := mustAdmit(t, &breaker, 0, false, Pass); lease != 0 {
		t.Fatalf("closed Admit() lease = %d, want 0", lease)
	}
	if breaker.Settle(1, Healthy, 0) {
		t.Fatal("Settle() on a closed breaker closed it")
	}
	assertState(t, &breaker, Closed)
}

func TestTripOpensForBaseCooldownThenAdmitsOneProbe(t *testing.T) {
	t.Parallel()

	var breaker Breaker
	start := 10 * second
	if !breaker.Trip(start) {
		t.Fatal("Trip() on a closed breaker = false, want true")
	}
	if breaker.Trip(start) {
		t.Fatal("Trip() on an open breaker = true, want false")
	}
	assertState(t, &breaker, Open)

	ready := start + int64(BaseCooldown)
	mustAdmit(t, &breaker, ready-1, true, Reject)
	lease := mustAdmit(t, &breaker, ready, true, Probe)
	if lease != 1 {
		t.Fatalf("first probe lease = %d, want 1", lease)
	}
	assertState(t, &breaker, Probing)
	mustAdmit(t, &breaker, ready, true, Reject)
	mustAdmit(t, &breaker, ready+int64(ProbeTimeout)-1, true, Reject)

	if !breaker.Settle(lease, Healthy, ready+int64(ProbeTimeout)-1) {
		t.Fatal("healthy probe did not close the breaker")
	}
	assertState(t, &breaker, Closed)
	mustAdmit(t, &breaker, ready+int64(ProbeTimeout), true, Pass)
}

func TestFailedProbesDoubleCooldownUpToCap(t *testing.T) {
	t.Parallel()

	var breaker Breaker
	var tick int64
	breaker.Trip(tick)
	for _, cooldown := range []time.Duration{
		5 * time.Second,
		10 * time.Second,
		20 * time.Second,
		40 * time.Second,
		time.Minute,
		time.Minute,
	} {
		mustAdmit(t, &breaker, tick+int64(cooldown)-1, true, Reject)
		tick += int64(cooldown)
		lease := mustAdmit(t, &breaker, tick, true, Probe)
		if breaker.Settle(lease, Failed, tick) {
			t.Fatalf("failed probe after %v closed the breaker", cooldown)
		}
		assertState(t, &breaker, Open)
	}
}

func TestExpiredLeaseCountsAsFailedProbe(t *testing.T) {
	t.Parallel()

	var breaker Breaker
	breaker.Trip(0)
	mustAdmit(t, &breaker, 5*second, true, Probe)
	mustAdmit(t, &breaker, 35*second-1, true, Reject)
	assertState(t, &breaker, Probing)

	// The lease expired at 35s. The doubled cooldown runs from its deadline.
	mustAdmit(t, &breaker, 35*second, true, Reject)
	assertState(t, &breaker, Open)
	mustAdmit(t, &breaker, 45*second-1, true, Reject)
	if lease := mustAdmit(t, &breaker, 45*second, true, Probe); lease != 2 {
		t.Fatalf("probe after expiry lease = %d, want 2", lease)
	}

	// Sparse traffic is not penalized for noticing an expiry late.
	var sparse Breaker
	sparse.Trip(0)
	mustAdmit(t, &sparse, 5*second, true, Probe)
	if lease := mustAdmit(t, &sparse, 100*second, true, Probe); lease != 2 {
		t.Fatalf("late probe lease = %d, want 2", lease)
	}
}

func TestSettleCountsExpiryBeforeOutcome(t *testing.T) {
	t.Parallel()

	var failed Breaker
	failed.Trip(0)
	lease := mustAdmit(t, &failed, 5*second, true, Probe)
	if failed.Settle(lease, Failed, 36*second) {
		t.Fatal("late failed probe closed the breaker")
	}
	// The expiry at 35s counts once: a 10s cooldown from the lease deadline.
	mustAdmit(t, &failed, 45*second-1, true, Reject)
	mustAdmit(t, &failed, 45*second, true, Probe)

	var healthy Breaker
	healthy.Trip(0)
	lease = mustAdmit(t, &healthy, 5*second, true, Probe)
	if !healthy.Settle(lease, Healthy, 36*second) {
		t.Fatal("late healthy probe did not close the breaker")
	}
	// The counted expiry raised the level, so reopening during probation
	// waits 20s.
	healthy.Trip(40 * second)
	mustAdmit(t, &healthy, 60*second-1, true, Reject)
	mustAdmit(t, &healthy, 60*second, true, Probe)
}

func TestLateResultsOfExpiredLeases(t *testing.T) {
	t.Parallel()

	var breaker Breaker
	breaker.Trip(0)
	first := mustAdmit(t, &breaker, 5*second, true, Probe)
	mustAdmit(t, &breaker, 35*second, true, Reject)

	if breaker.Settle(first, Failed, 40*second) {
		t.Fatal("expired failed probe closed the breaker")
	}
	if breaker.Settle(first, Inconclusive, 41*second) {
		t.Fatal("expired inconclusive probe closed the breaker")
	}
	mustAdmit(t, &breaker, 45*second-1, true, Reject)
	next := mustAdmit(t, &breaker, 45*second, true, Probe)

	if !breaker.Settle(first, Healthy, 46*second) {
		t.Fatal("expired healthy probe did not close the breaker")
	}
	if breaker.Settle(next, Failed, 47*second) {
		t.Fatal("probe result after close changed the breaker")
	}
	assertState(t, &breaker, Closed)
}

func TestInconclusiveProbeWaitsBaseCooldownWithoutEscalating(t *testing.T) {
	t.Parallel()

	var breaker Breaker
	breaker.Trip(0)
	lease := mustAdmit(t, &breaker, 5*second, true, Probe)
	if breaker.Settle(lease, Inconclusive, 6*second) {
		t.Fatal("inconclusive probe closed the breaker")
	}
	assertState(t, &breaker, Open)
	mustAdmit(t, &breaker, 11*second-1, true, Reject)

	lease = mustAdmit(t, &breaker, 11*second, true, Probe)
	breaker.Settle(lease, Failed, 12*second)
	mustAdmit(t, &breaker, 22*second-1, true, Reject)
	mustAdmit(t, &breaker, 22*second, true, Probe)
}

func TestIneligibleRequestDoesNotTakeProbe(t *testing.T) {
	t.Parallel()

	var breaker Breaker
	breaker.Trip(0)
	mustAdmit(t, &breaker, 5*second, false, Reject)
	assertState(t, &breaker, Open)

	lease := mustAdmit(t, &breaker, 5*second, true, Probe)
	if lease != 1 {
		t.Fatalf("probe lease = %d, want 1", lease)
	}
	breaker.Settle(lease, Healthy, 6*second)
	mustAdmit(t, &breaker, 6*second, false, Pass)
}

func TestProbationEscalatesReopening(t *testing.T) {
	t.Parallel()

	var breaker Breaker
	breaker.Trip(0)
	lease := mustAdmit(t, &breaker, 5*second, true, Probe)
	breaker.Settle(lease, Healthy, 5*second)

	reopened := 5*second + int64(Probation) - 1
	breaker.Trip(reopened)
	mustAdmit(t, &breaker, reopened+10*second-1, true, Reject)
	lease = mustAdmit(t, &breaker, reopened+10*second, true, Probe)
	closedAt := reopened + 10*second
	breaker.Settle(lease, Healthy, closedAt)

	afterProbation := closedAt + int64(Probation)
	breaker.Trip(afterProbation)
	mustAdmit(t, &breaker, afterProbation+5*second-1, true, Reject)
	mustAdmit(t, &breaker, afterProbation+5*second, true, Probe)
}

func TestSettleIgnoresLeasesFromOtherEpisodes(t *testing.T) {
	t.Parallel()

	var breaker Breaker
	breaker.Trip(0)
	first := mustAdmit(t, &breaker, 5*second, true, Probe)
	breaker.Settle(first, Healthy, 6*second)
	breaker.Trip(7 * second)

	if breaker.Settle(first, Healthy, 8*second) {
		t.Fatal("lease from an earlier episode closed the breaker")
	}
	if breaker.Settle(first+1, Healthy, 8*second) {
		t.Fatal("lease that was never issued closed the breaker")
	}
	assertState(t, &breaker, Open)
}

func TestTransitionsNeverMoveBackwards(t *testing.T) {
	t.Parallel()

	var breaker Breaker
	breaker.Trip(100 * second)
	mustAdmit(t, &breaker, 50*second, true, Reject)
	lease := mustAdmit(t, &breaker, 105*second, true, Probe)
	breaker.Settle(lease, Failed, second)
	mustAdmit(t, &breaker, 115*second-1, true, Reject)
	mustAdmit(t, &breaker, 115*second, true, Probe)

	var negative Breaker
	negative.Trip(-int64(time.Hour))
	mustAdmit(t, &negative, 5*second-1, true, Reject)
	mustAdmit(t, &negative, 5*second, true, Probe)
}

func TestDeadlinesSaturate(t *testing.T) {
	t.Parallel()

	if got := deadline(10, time.Second); got != 10+second {
		t.Errorf("deadline(10, 1s) = %d, want %d", got, 10+second)
	}
	if got := deadline(math.MaxInt64-1, time.Second); got != math.MaxInt64 {
		t.Errorf("deadline near max = %d, want MaxInt64", got)
	}

	var breaker Breaker
	breaker.Trip(math.MaxInt64 - 1)
	mustAdmit(t, &breaker, math.MaxInt64-1, true, Reject)
	mustAdmit(t, &breaker, math.MaxInt64, true, Probe)
}

func FuzzBreakerInvariants(f *testing.F) {
	f.Add([]byte{1, 0, 0, 10, 2, 2, 0, 70, 6, 1, 0, 255, 3, 30})
	f.Add([]byte{1, 0, 4, 10, 0, 1, 2, 0, 128, 255, 18, 1, 34, 1})

	f.Fuzz(func(t *testing.T, operations []byte) {
		var (
			breaker Breaker
			tick    int64
		)
		for len(operations) >= 2 {
			operation, step := operations[0], operations[1]
			operations = operations[2:]
			if operation&0x80 == 0 {
				tick += int64(step) * second / 2
			} else {
				tick -= int64(step) * second / 2
			}

			before := breaker
			switch operation & 3 {
			case 0:
				admission, lease := breaker.Admit(tick, operation&4 == 0)
				checkAdmission(t, before, &breaker, admission, lease)
			case 1:
				if opened := breaker.Trip(tick); opened != (before.state == Closed) {
					t.Fatalf("Trip() from state %d = %t", before.state, opened)
				}
			default:
				lease := breaker.lease - uint64(operation>>2&3)
				outcome := Outcome(operation >> 4 % 3)
				if closed := breaker.Settle(lease, outcome, tick); closed &&
					outcome != Healthy {
					t.Fatalf("Settle() closed with outcome %d", outcome)
				}
			}
			checkInvariants(t, before, &breaker)
		}
	})
}

func checkAdmission(
	t *testing.T,
	before Breaker,
	breaker *Breaker,
	admission Admission,
	lease uint64,
) {
	t.Helper()

	switch {
	case before.state == Closed && (admission != Pass || lease != 0):
		t.Fatalf("closed Admit() = %d with lease %d, want Pass", admission, lease)
	case admission == Probe &&
		(lease != before.lease+1 || breaker.state != Probing):
		t.Fatalf(
			"probe lease %d in state %d, want lease %d while Probing",
			lease,
			breaker.state,
			before.lease+1,
		)
	case admission != Probe && lease != 0:
		t.Fatalf("Admit() = %d with lease %d, want no lease", admission, lease)
	}
}

func checkInvariants(t *testing.T, before Breaker, breaker *Breaker) {
	t.Helper()

	if breaker.state > Probing || breaker.level > maxLevel {
		t.Fatalf("state %d level %d outside bounds", breaker.state, breaker.level)
	}
	if breaker.last < before.last || breaker.last < 0 {
		t.Fatalf("logical tick moved from %d to %d", before.last, breaker.last)
	}
	if breaker.lease < before.lease || breaker.lease > before.lease+1 {
		t.Fatalf("lease moved from %d to %d", before.lease, breaker.lease)
	}

	switch breaker.state {
	case Open:
		if breaker.until > deadline(breaker.last, MaxCooldown) {
			t.Fatalf("cooldown ends %d, more than MaxCooldown after %d", breaker.until, breaker.last)
		}
	case Probing:
		if breaker.until > deadline(breaker.last, ProbeTimeout) {
			t.Fatalf("lease ends %d, more than ProbeTimeout after %d", breaker.until, breaker.last)
		}
	}
	if breaker.state != Closed &&
		(breaker.episode == 0 || breaker.episode > breaker.lease+1) {
		t.Fatalf("episode %d outside leases up to %d", breaker.episode, breaker.lease)
	}
}

func mustAdmit(
	t *testing.T,
	breaker *Breaker,
	tick int64,
	eligible bool,
	want Admission,
) uint64 {
	t.Helper()

	got, lease := breaker.Admit(tick, eligible)
	if got != want {
		t.Fatalf(
			"Admit(%v, %t) = %d, want %d",
			time.Duration(tick),
			eligible,
			got,
			want,
		)
	}
	if (got == Probe) != (lease != 0) {
		t.Fatalf("Admit(%v) = %d with lease %d", time.Duration(tick), got, lease)
	}

	return lease
}

func assertState(t *testing.T, breaker *Breaker, want State) {
	t.Helper()

	if got := breaker.State(); got != want {
		t.Fatalf("State() = %d, want %d", got, want)
	}
}
