package observe

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRegistryReusesTargetsAndEnforcesGlobalCapacity(t *testing.T) {
	t.Parallel()

	const capacity = 4

	registry := newRegistry(capacity, 8, time.Hour)
	keys := make([]targetKey, 0, capacity+1)
	for index := 0; index < capacity+1; index++ {
		keys = append(keys, testTargetKey(fmt.Sprintf("host-%d.example", index)))
	}

	first, overflow := registry.get(keys[0], 1)
	if overflow {
		t.Fatal("first target used overflow")
	}
	reused, overflow := registry.get(keys[0], 2)
	if overflow {
		t.Fatal("existing target used overflow")
	}
	if reused != first {
		t.Fatal("existing target was not reused")
	}
	if got := reused.lastSeen.Load(); got != 2 {
		t.Errorf("last seen = %d, want 2", got)
	}

	for index := 1; index < capacity; index++ {
		if _, usedOverflow := registry.get(
			keys[index],
			int64(index+2),
		); usedOverflow {
			t.Fatalf("target %d used overflow before global capacity", index)
		}
	}
	if got := registry.trackedTargets(); got != capacity {
		t.Errorf("tracked targets = %d, want %d", got, capacity)
	}

	overflowTarget, overflow := registry.get(keys[capacity], 10)
	if !overflow {
		t.Fatal("target beyond global capacity did not use overflow")
	}
	if overflowTarget != registry.overflow {
		t.Fatal("overflow lookup returned a regular target")
	}
	if overflowTarget.actionable {
		t.Fatal("overflow target is actionable")
	}
	if got := registry.trackedTargets(); got != capacity {
		t.Errorf("tracked targets after overflow = %d, want %d", got, capacity)
	}
}

func TestRegistryReplacesExpiredTargetWithFreshState(t *testing.T) {
	t.Parallel()

	const idleTTL = 10 * time.Second

	registry := newRegistry(1, 1, idleTTL)
	oldKey := testTargetKey("old.example")
	newKey := testTargetKey("new.example")

	oldTarget, overflow := registry.get(oldKey, 0)
	if overflow {
		t.Fatal("old target used overflow")
	}

	if got, usedOverflow := registry.get(newKey, int64(idleTTL)-1); !usedOverflow ||
		got != registry.overflow {
		t.Fatal("unexpired target was replaced")
	}

	newTarget, overflow := registry.get(newKey, int64(idleTTL))
	if overflow {
		t.Fatal("new target used overflow after idle expiry")
	}
	if newTarget == oldTarget {
		t.Fatal("expired target state was reset in place")
	}
	if got := registry.trackedTargets(); got != 1 {
		t.Errorf("tracked targets = %d, want 1", got)
	}

	if !oldTarget.record(0, observation{outcome: outcomeSuccess}) {
		t.Fatal("stale in-flight target rejected its own observation")
	}
	if got := bucketAttempts(newTarget, 0); got != 0 {
		t.Errorf("replacement target attempts = %d, want 0", got)
	}
}

func TestRegistryUsesDeterministicVictimForEqualAge(t *testing.T) {
	t.Parallel()

	const idleTTL = time.Second

	registry := newRegistry(2, 1, idleTTL)
	firstKey := testTargetKey("a.example")
	secondKey := testTargetKey("b.example")
	replacementKey := testTargetKey("c.example")

	_, _ = registry.get(secondKey, 0)
	_, _ = registry.get(firstKey, 0)
	_, overflow := registry.get(replacementKey, int64(idleTTL))
	if overflow {
		t.Fatal("replacement used overflow")
	}

	shard := &registry.shards[0]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, exists := shard.targets[firstKey]; exists {
		t.Fatal("lexicographically first equal-age target was not evicted")
	}
	if _, exists := shard.targets[secondKey]; !exists {
		t.Fatal("lexicographically later target was evicted")
	}
	if _, exists := shard.targets[replacementKey]; !exists {
		t.Fatal("replacement target was not admitted")
	}
}

func TestRegistryDoesNotExpireOnBackwardClockOrDisabledTTL(t *testing.T) {
	t.Parallel()

	key := testTargetKey("example.com")
	targets := map[targetKey]*target{
		key: newTarget(10, true),
	}

	registry := newRegistry(1, 1, time.Second)
	if _, found := registry.expiredVictim(targets, 9); found {
		t.Fatal("backward clock expired target")
	}

	registry.idleTTL = 0
	if _, found := registry.expiredVictim(targets, 20); found {
		t.Fatal("disabled TTL expired target")
	}
}

func TestRegistryConstructionNormalizesInvalidBounds(t *testing.T) {
	t.Parallel()

	registry := newRegistry(-1, 0, -time.Second)
	if registry.capacity != 0 {
		t.Errorf("capacity = %d, want 0", registry.capacity)
	}
	if len(registry.shards) != 1 {
		t.Errorf("shards = %d, want 1", len(registry.shards))
	}
	if registry.idleTTL != 0 {
		t.Errorf("idle TTL = %d, want 0", registry.idleTTL)
	}

	state, overflow := registry.get(testTargetKey("example.com"), -1)
	if !overflow || state != registry.overflow {
		t.Fatal("zero-capacity registry admitted a regular target")
	}
}

func TestRegistryHandlesNilAndInvalidTrackedState(t *testing.T) {
	t.Parallel()

	var registry *registry
	state, overflow := registry.get(testTargetKey("example.com"), 0)
	if state != nil || !overflow {
		t.Fatalf("nil registry get = (%v, %t), want (nil, true)", state, overflow)
	}
	if got := registry.trackedTargets(); got != 0 {
		t.Errorf("nil registry tracked targets = %d, want 0", got)
	}

	registry = newRegistry(1, 1, time.Second)
	registry.tracked.Store(-1)
	if got := registry.trackedTargets(); got != 0 {
		t.Errorf("negative tracked targets = %d, want 0", got)
	}
}

func TestRegistryAdmissionIsSafeUnderConcurrency(t *testing.T) {
	t.Parallel()

	const (
		capacity = 16
		workers  = 128
	)

	registry := newRegistry(capacity, 8, time.Hour)
	start := make(chan struct{})
	var regular atomic.Int32
	var overflow atomic.Int32
	var wait sync.WaitGroup
	wait.Add(workers)

	for worker := 0; worker < workers; worker++ {
		go func(index int) {
			defer wait.Done()
			<-start

			_, usedOverflow := registry.get(
				testTargetKey(fmt.Sprintf("host-%d.example", index)),
				0,
			)
			if usedOverflow {
				overflow.Add(1)
			} else {
				regular.Add(1)
			}
		}(worker)
	}

	close(start)
	wait.Wait()

	if got := regular.Load(); got != capacity {
		t.Errorf("regular admissions = %d, want %d", got, capacity)
	}
	if got := overflow.Load(); got != workers-capacity {
		t.Errorf("overflow admissions = %d, want %d", got, workers-capacity)
	}
	if got := registry.trackedTargets(); got != capacity {
		t.Errorf("tracked targets = %d, want %d", got, capacity)
	}
}

func TestTargetTouchNeverMovesBackward(t *testing.T) {
	t.Parallel()

	state := newTarget(10, true)
	state.touch(9)
	if got := state.lastSeen.Load(); got != 10 {
		t.Errorf("last seen after backward touch = %d, want 10", got)
	}
	state.touch(11)
	if got := state.lastSeen.Load(); got != 11 {
		t.Errorf("last seen after forward touch = %d, want 11", got)
	}
}

func testTargetKey(host string) targetKey {
	return targetKey{
		host:   host,
		port:   443,
		scheme: schemeHTTPS,
		method: methodRead,
	}
}

func bucketAttempts(state *target, index int) uint64 {
	state.mu.Lock()
	defer state.mu.Unlock()

	return state.buckets[index].attempts
}
