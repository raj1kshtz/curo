package observe

import (
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
	shards   []registryShard

	capacity int64
	idleTTL  int64
	tracked  atomic.Int64
}

type registryShard struct {
	targets map[targetKey]*target
	mu      sync.Mutex
}

func newRegistry(capacity, shardCount int, idleTTL time.Duration) *registry {
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
		admitted := newTarget(tick, true)
		shard.targets[key.retained()] = admitted
		return admitted, false
	}

	if victim, found := registry.expiredVictim(shard.targets, tick); found {
		delete(shard.targets, victim)

		admitted := newTarget(tick, true)
		shard.targets[key.retained()] = admitted
		return admitted, false
	}

	return registry.overflow, true
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
