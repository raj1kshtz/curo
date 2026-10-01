package observe

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultTargetCapacity = 128
	defaultShardCount     = 8
	defaultIdleTTL        = 15 * time.Minute
)

type registry struct {
	overflow *target
	changes  *journal
	shards   []registryShard

	capacity int64
	idleTTL  int64
	tracked  atomic.Int64
}

type registryShard struct {
	targets map[targetKey]*target
	mu      sync.Mutex
}

func newRegistry(
	capacity int,
	shardCount int,
	idleTTL time.Duration,
	changes *journal,
) *registry {
	if capacity < 0 {
		capacity = 0
	}
	if shardCount < 1 {
		shardCount = 1
	}
	if idleTTL < 0 {
		idleTTL = 0
	}

	registry := &registry{
		shards:   make([]registryShard, shardCount),
		overflow: newTarget(0, false),
		changes:  changes,
		capacity: int64(capacity),
		idleTTL:  int64(idleTTL),
	}

	initialShardSize := capacity / shardCount
	if initialShardSize < 1 {
		initialShardSize = 1
	}
	for index := range registry.shards {
		registry.shards[index].targets = make(map[targetKey]*target, initialShardSize)
	}

	return registry
}

func (registry *registry) get(
	key targetKey,
	tick int64,
) (*target, bool) {
	if registry == nil || len(registry.shards) == 0 {
		return nil, true
	}
	if tick < 0 {
		tick = 0
	}

	shard := &registry.shards[key.hash()%uint64(len(registry.shards))]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if existing := shard.targets[key]; existing != nil {
		existing.touch(tick)
		return existing, false
	}

	if registry.reserve() {
		return registry.admitLocked(shard, key, tick), false
	}

	if victim, found := registry.expiredVictim(shard.targets, tick); found {
		// Lock order is shard, then target. No path takes a shard lock while
		// holding a target lock.
		shard.targets[victim].retire()
		delete(shard.targets, victim)

		return registry.admitLocked(shard, key, tick), false
	}

	return registry.overflow, true
}

func (registry *registry) admitLocked(
	shard *registryShard,
	key targetKey,
	tick int64,
) *target {
	admitted := newTarget(tick, true)
	admitted.key = key.retained()
	admitted.changes = registry.changes
	shard.targets[admitted.key] = admitted

	return admitted
}

func (registry *registry) reserve() bool {
	for {
		current := registry.tracked.Load()
		if current >= registry.capacity {
			return false
		}
		if registry.tracked.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (registry *registry) expiredVictim(
	targets map[targetKey]*target,
	tick int64,
) (targetKey, bool) {
	if registry.idleTTL <= 0 {
		return targetKey{}, false
	}

	var (
		victim     targetKey
		victimTick int64
		found      bool
	)

	for key, candidate := range targets {
		lastSeen := candidate.lastSeen.Load()
		if tick < lastSeen || tick-lastSeen < registry.idleTTL {
			continue
		}
		if !found || lastSeen < victimTick ||
			(lastSeen == victimTick && key.less(victim)) {
			victim = key
			victimTick = lastSeen
			found = true
		}
	}

	return victim, found
}

func (registry *registry) trackedTargets() uint64 {
	if registry == nil {
		return 0
	}

	tracked := registry.tracked.Load()
	if tracked < 0 {
		return 0
	}

	return uint64(tracked)
}

// regularTargets returns tracked regular targets ordered by identity.
func (registry *registry) regularTargets() []*target {
	if registry == nil {
		return nil
	}

	targets := make([]*target, 0, registry.trackedTargets())
	for index := range registry.shards {
		targets = registry.shards[index].appendTargets(targets)
	}
	slices.SortFunc(targets, func(left, right *target) int {
		return left.key.compare(right.key)
	})

	return targets
}

func (shard *registryShard) appendTargets(targets []*target) []*target {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	for _, state := range shard.targets {
		targets = append(targets, state)
	}

	return targets
}
