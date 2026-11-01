package retry

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestBudgetStartsEmpty(t *testing.T) {
	t.Parallel()

	var target, instance Budget
	lease, reserved := Reserve(&target, &instance)
	if reserved || lease.Held() {
		t.Fatalf("Reserve() on empty budgets = held %t, reserved %t, want false, false", lease.Held(), reserved)
	}
}

func TestDepositsFundOneTokenPerTenAttempts(t *testing.T) {
	t.Parallel()

	var target, instance Budget
	for range DepositsPerToken - 1 {
		Deposit(&target, &instance)
	}
	if _, reserved := Reserve(&target, &instance); reserved {
		t.Fatalf("Reserve() after %d deposits = true, want false", DepositsPerToken-1)
	}

	Deposit(&target, &instance)
	lease, reserved := Reserve(&target, &instance)
	if !reserved || !lease.Held() {
		t.Fatalf("Reserve() after %d deposits = held %t, reserved %t, want true, true", DepositsPerToken, lease.Held(), reserved)
	}
	lease.Commit()
	if _, reserved := Reserve(&target, &instance); reserved {
		t.Fatal("second Reserve() = true, want false")
	}
}

func TestReserveIsAllOrNothing(t *testing.T) {
	t.Parallel()

	var target, instance Budget
	for range DepositsPerToken {
		target.deposit(targetCapacityMilliTokens)
	}
	if _, reserved := Reserve(&target, &instance); reserved {
		t.Fatal("Reserve() with an empty instance budget = true, want false")
	}
	assertBudgetState(t, "target after denied Reserve()", &target, milliTokensPerToken, 0)

	for range DepositsPerToken {
		instance.deposit(instanceCapacityMilliTokens)
	}
	lease, reserved := Reserve(&target, &instance)
	if !reserved {
		t.Fatal("Reserve() with funded budgets = false, want true")
	}
	assertBudgetState(t, "target with a pending reservation", &target, 0, 1)
	assertBudgetState(t, "instance with a pending reservation", &instance, 0, 1)

	lease.Commit()
	assertBudgetState(t, "target after Commit()", &target, 0, 0)
	assertBudgetState(t, "instance after Commit()", &instance, 0, 0)
}

func TestBudgetsStopAtCapacity(t *testing.T) {
	t.Parallel()

	var instance Budget
	targets := make([]Budget, 10)
	for index := range targets {
		for range 100 * DepositsPerToken {
			Deposit(&targets[index], &instance)
		}
		assertBudgetState(t, "full target", &targets[index], targetCapacityMilliTokens, 0)
	}
	assertBudgetState(t, "full instance", &instance, instanceCapacityMilliTokens, 0)

	reserved := 0
	for index := range targets {
		perTarget := 0
		for {
			lease, ok := Reserve(&targets[index], &instance)
			if !ok {
				break
			}
			lease.Commit()
			perTarget++
		}
		if perTarget > TargetCapacity {
			t.Fatalf("target reserved %d tokens, want at most %d", perTarget, TargetCapacity)
		}
		reserved += perTarget
	}
	if reserved != InstanceCapacity {
		t.Fatalf("reserved %d tokens, want instance capacity %d", reserved, InstanceCapacity)
	}
}

func TestLeaseReleasesItsReservationOnce(t *testing.T) {
	t.Parallel()

	var target, instance Budget
	for range 2 * DepositsPerToken {
		Deposit(&target, &instance)
	}

	canceled, reserved := Reserve(&target, &instance)
	if !reserved {
		t.Fatal("Reserve() = false, want true")
	}
	canceled.Cancel()
	if canceled.Held() {
		t.Fatal("Held() after Cancel() = true, want false")
	}
	canceled.Cancel()
	canceled.Commit()
	assertBudgetState(t, "target after repeated Cancel()", &target, 2*milliTokensPerToken, 0)
	assertBudgetState(t, "instance after repeated Cancel()", &instance, 2*milliTokensPerToken, 0)

	committed, reserved := Reserve(&target, &instance)
	if !reserved {
		t.Fatal("Reserve() after Cancel() = false, want true")
	}
	committed.Commit()
	if committed.Held() {
		t.Fatal("Held() after Commit() = true, want false")
	}
	committed.Commit()
	committed.Cancel()
	assertBudgetState(t, "target after repeated Commit()", &target, milliTokensPerToken, 0)
	assertBudgetState(t, "instance after repeated Commit()", &instance, milliTokensPerToken, 0)

	var zero Lease
	zero.Commit()
	zero.Cancel()
	if zero.Held() {
		t.Fatal("zero Lease Held() = true, want false")
	}
}

func TestLeaseCopiesReleaseOnce(t *testing.T) {
	t.Parallel()

	actions := []struct {
		name    string
		release func(*Lease)
		refund  int64
	}{
		{name: "commit", release: (*Lease).Commit},
		{name: "cancel", release: (*Lease).Cancel, refund: milliTokensPerToken},
	}
	for _, first := range actions {
		for _, second := range actions {
			t.Run(first.name+"/"+second.name, func(t *testing.T) {
				t.Parallel()

				var target, instance Budget
				for range 2 * DepositsPerToken {
					Deposit(&target, &instance)
				}
				lease, reserved := Reserve(&target, &instance)
				if !reserved {
					t.Fatal("Reserve() = false, want true")
				}
				copied := lease
				first.release(&lease)
				second.release(&copied)
				lease.Commit()
				copied.Cancel()

				if lease.Held() || copied.Held() {
					t.Fatal("released copies still hold a reservation")
				}
				want := milliTokensPerToken + first.refund
				assertBudgetState(t, "target after copied release", &target, want, 0)
				assertBudgetState(t, "instance after copied release", &instance, want, 0)
			})
		}
	}
}

func TestConcurrentLeaseReleaseOnce(t *testing.T) {
	t.Parallel()

	for _, ownership := range []string{"copies", "same lease"} {
		for _, action := range []string{"commit", "cancel", "mixed"} {
			t.Run(ownership+"/"+action, func(t *testing.T) {
				t.Parallel()

				var target, instance Budget
				for range 2 * DepositsPerToken {
					Deposit(&target, &instance)
				}
				lease, reserved := Reserve(&target, &instance)
				if !reserved {
					t.Fatal("Reserve() = false, want true")
				}

				const workers = 64
				copies := make([]Lease, workers)
				start := make(chan struct{})
				var group sync.WaitGroup
				for index := range copies {
					copies[index] = lease
					releasing := &copies[index]
					if ownership == "same lease" {
						releasing = &lease
					}
					group.Add(1)
					go func() {
						defer group.Done()
						<-start
						_ = releasing.Held()
						if action == "cancel" || (action == "mixed" && index%2 == 0) {
							releasing.Cancel()
						} else {
							releasing.Commit()
						}
						_ = releasing.Held()
					}()
				}
				close(start)
				group.Wait()

				want := int64(milliTokensPerToken)
				if action == "cancel" ||
					(action == "mixed" && available(target.state.Load()) == 2*milliTokensPerToken) {
					want *= 2
				}
				assertBudgetState(t, "target after concurrent release", &target, want, 0)
				assertBudgetState(t, "instance after concurrent release", &instance, want, 0)
				for index := range copies {
					if copies[index].Held() {
						t.Fatalf("copy %d still holds a released reservation", index)
					}
					copies[index].Commit()
					copies[index].Cancel()
				}
				lease.Commit()
				lease.Cancel()
				assertBudgetState(t, "target after repeated copied release", &target, want, 0)
				assertBudgetState(t, "instance after repeated copied release", &instance, want, 0)
			})
		}
	}
}

func TestPendingReservationsKeepCapacityCounted(t *testing.T) {
	t.Parallel()

	var target, instance Budget
	for range TargetCapacity * DepositsPerToken {
		Deposit(&target, &instance)
	}

	leases := make([]Lease, 0, TargetCapacity)
	for {
		lease, reserved := Reserve(&target, &instance)
		if !reserved {
			break
		}
		leases = append(leases, lease)
	}
	if len(leases) != TargetCapacity {
		t.Fatalf("pending reservations = %d, want %d", len(leases), TargetCapacity)
	}

	for range TargetCapacity * DepositsPerToken {
		Deposit(&target, &instance)
	}
	assertBudgetState(t, "target deposits with full pending capacity", &target, 0, TargetCapacity)

	for index := range leases {
		leases[index].Cancel()
	}
	assertBudgetState(t, "target after canceling pending capacity", &target, targetCapacityMilliTokens, 0)

	for index := range leases {
		lease, reserved := Reserve(&target, &instance)
		if !reserved {
			t.Fatalf("Reserve() %d after Cancel() = false, want true", index)
		}
		lease.Commit()
	}
	if _, reserved := Reserve(&target, &instance); reserved {
		t.Fatal("Reserve() beyond target capacity = true, want false")
	}

	Deposit(&target, &instance)
	assertBudgetState(t, "target deposit after committed capacity", &target, depositMilliTokens, 0)
}

func TestConcurrentReservationsNeverExceedFunding(t *testing.T) {
	t.Parallel()

	var target, instance Budget
	for range TargetCapacity * DepositsPerToken {
		Deposit(&target, &instance)
	}

	var reserved atomic.Int64
	var group sync.WaitGroup
	for range 64 {
		group.Add(1)
		go func() {
			defer group.Done()
			for range 4 {
				if lease, ok := Reserve(&target, &instance); ok {
					reserved.Add(1)
					lease.Commit()
				}
			}
		}()
	}
	group.Wait()

	if got := reserved.Load(); got != TargetCapacity {
		t.Fatalf("concurrent reservations = %d, want %d", got, TargetCapacity)
	}
}

func TestConcurrentLeasesRespectBound(t *testing.T) {
	t.Parallel()

	const (
		workers           = 16
		depositsPerWorker = 500
	)

	var target, instance Budget
	var committed atomic.Int64
	var group sync.WaitGroup
	for worker := range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for attempt := range depositsPerWorker {
				Deposit(&target, &instance)
				lease, reserved := Reserve(&target, &instance)
				if !reserved {
					continue
				}
				if (worker+attempt)%3 == 0 {
					lease.Cancel()
					continue
				}
				committed.Add(1)
				lease.Commit()
			}
		}()
	}
	group.Wait()

	deposits := int64(workers * depositsPerWorker)
	if limit := TargetCapacity + deposits/DepositsPerToken; committed.Load() > limit {
		t.Fatalf("committed retries = %d, want at most %d", committed.Load(), limit)
	}
	if committed.Load() == 0 {
		t.Fatal("committed retries = 0, want funded retries")
	}
	for _, budget := range []struct {
		budget   *Budget
		name     string
		capacity int64
	}{
		{name: "target", budget: &target, capacity: targetCapacityMilliTokens},
		{name: "instance", budget: &instance, capacity: instanceCapacityMilliTokens},
	} {
		state := budget.budget.state.Load()
		if pending(state) != 0 || available(state) > budget.capacity {
			t.Fatalf(
				"%s budget = %d available, %d pending, want at most %d available and none pending",
				budget.name,
				available(state),
				pending(state),
				budget.capacity,
			)
		}
	}
}

func assertBudgetState(t *testing.T, name string, budget *Budget, wantAvailable, wantPending int64) {
	t.Helper()

	state := budget.state.Load()
	if available(state) != wantAvailable || pending(state) != wantPending {
		t.Fatalf(
			"%s = %d available, %d pending, want %d available, %d pending",
			name,
			available(state),
			pending(state),
			wantAvailable,
			wantPending,
		)
	}
}

// BenchmarkReserve measures acquiring a retry token from full target and
// instance budgets and returning it, so every reservation succeeds.
func BenchmarkReserve(b *testing.B) {
	var target, instance Budget
	for range TargetCapacity * DepositsPerToken {
		Deposit(&target, &instance)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		lease, reserved := Reserve(&target, &instance)
		if !reserved {
			b.Fatal("Reserve() = false, want a token")
		}
		lease.Cancel()
	}
}
