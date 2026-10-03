package observe

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/raj1kshtz/curo/internal/timeout"
)

// registryBenchmarkCycle is how many lookups the miss and expiry benchmarks
// make before they replace the registry while the timer is stopped.
const registryBenchmarkCycle = 1_024

func BenchmarkRegistry(b *testing.B) {
	keys := make([]targetKey, defaultTargetCapacity+registryBenchmarkCycle)
	for index := range keys {
		keys[index] = testTargetKey(fmt.Sprintf("host-%04d.example", index))
	}
	full := func() *registry {
		registry := newRegistry(defaultTargetCapacity, defaultShardCount, defaultIdleTTL, nil)
		for _, key := range keys[:defaultTargetCapacity] {
			if _, overflow := registry.get(key, 0); overflow {
				b.Fatal("registry overflowed before reaching capacity")
			}
		}
		return registry
	}

	b.Run("hit", func(b *testing.B) {
		registry := full()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, overflow := registry.get(keys[0], 0); overflow {
				b.Fatal("hit used the overflow aggregate")
			}
		}
	})

	// Every lookup admits a new target into a registry with free capacity.
	b.Run("miss", func(b *testing.B) {
		var registry *registry
		b.ReportAllocs()
		b.ResetTimer()
		for index := range b.N {
			slot := index % defaultTargetCapacity
			if slot == 0 {
				b.StopTimer()
				registry = newRegistry(defaultTargetCapacity, defaultShardCount, defaultIdleTTL, nil)
				b.StartTimer()
			}
			if _, overflow := registry.get(keys[slot], 0); overflow {
				b.Fatal("miss used the overflow aggregate")
			}
		}
	})

	b.Run("overflow", func(b *testing.B) {
		registry := full()
		key := keys[defaultTargetCapacity]
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, overflow := registry.get(key, 0); !overflow {
				b.Fatal("lookup beyond capacity admitted a target")
			}
		}
	})

	// Every lookup replaces the oldest expired target in its shard. Each
	// lookup is one idle TTL later than the previous one, so every retained
	// target has expired, and no key repeats within a registry's lifetime.
	b.Run("expiry", func(b *testing.B) {
		var registry *registry
		b.ReportAllocs()
		b.ResetTimer()
		for index := range b.N {
			step := index % registryBenchmarkCycle
			if step == 0 {
				b.StopTimer()
				registry = full()
				b.StartTimer()
			}
			tick := int64(step+1) * int64(defaultIdleTTL)
			if _, overflow := registry.get(keys[defaultTargetCapacity+step], tick); overflow {
				b.Fatal("lookup did not replace an expired target")
			}
		}
	})
}

// BenchmarkConcurrentObservations measures Begin and Finish from parallel
// goroutines on one target, where every update contends for one lock, and on
// one target per shard. Each goroutine cycles through the targets.
func BenchmarkConcurrentObservations(b *testing.B) {
	for _, hosts := range []int{1, defaultShardCount} {
		b.Run(fmt.Sprintf("targets=%d", hosts), func(b *testing.B) {
			observer := New(timeout.DefaultBounds)
			requests := shardRequests(b, hosts)
			for _, request := range requests {
				observer.Finish(observer.Begin(request), breakerTestSuccess)
			}

			var next atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				index := next.Add(1)
				for pb.Next() {
					request := requests[index%uint64(len(requests))]
					observer.Finish(observer.Begin(request), breakerTestSuccess)
					index++
				}
			})
		})
	}
}

// shardRequests returns count requests whose targets fall in different
// registry shards.
func shardRequests(b *testing.B, count int) []Request {
	b.Helper()

	requests := make([]Request, 0, count)
	used := make(map[uint64]bool, count)
	for index := 0; len(requests) < count; index++ {
		request := testRequest("GET", "https", fmt.Sprintf("host-%d.example", index), "")
		key, ok := normalizeTarget(request)
		if !ok {
			b.Fatalf("request %+v has no target", request)
		}
		shard := key.hash() % uint64(defaultShardCount)
		if !used[shard] {
			used[shard] = true
			requests = append(requests, request)
		}
	}
	return requests
}
