package observe

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/raj1kshtz/curo/internal/diagnose"
)

func TestSnapshotFiltersStaleRingSlots(t *testing.T) {
	t.Parallel()

	state := newTarget(0, true)
	value := observation{
		outcome: outcomeSuccess,
		signal:  diagnosisNeutral,
	}
	for epoch := int64(0); epoch < observationBucketCount; epoch++ {
		tick := epoch * int64(observationBucketWidth)
		if !state.record(tick, value) {
			t.Fatalf("observation at epoch %d was rejected", epoch)
		}
	}

	resumeTick := int64(8 * time.Minute)
	if !state.record(resumeTick, value) {
		t.Fatal("resumed observation was rejected")
	}

	resumed := state.snapshotAt(resumeTick)
	if resumed.Recent.Attempts != 1 {
		t.Errorf(
			"resumed recent attempts = %d, want 1",
			resumed.Recent.Attempts,
		)
	}
	if resumed.Historical.Attempts != observationBucketCount {
		t.Errorf(
			"resumed historical attempts = %d, want %d",
			resumed.Historical.Attempts,
			observationBucketCount,
		)
	}

	expired := state.snapshotAt(int64(40 * time.Minute))
	if expired.Recent.Attempts != 0 || expired.Historical.Attempts != 0 {
		t.Errorf("expired snapshot = %#v, want empty windows", expired)
	}
	if !expired.EverRelevant {
		t.Fatal("expired snapshot forgot that relevant evidence previously existed")
	}
}

func TestHistoricalSnapshotNeverOverlapsRecentWindow(t *testing.T) {
	t.Parallel()

	state := newTarget(0, true)
	value := observation{
		outcome: outcomeSuccess,
		signal:  diagnosisNeutral,
	}
	for _, tick := range []int64{
		0,
		int64(119 * time.Second),
		int64(120 * time.Second),
		int64(179 * time.Second),
		int64(180 * time.Second),
		int64(5 * time.Minute),
	} {
		if !state.record(tick, value) {
			t.Fatalf("observation at %v was rejected", time.Duration(tick))
		}
	}

	snapshot := state.snapshotAt(int64(5 * time.Minute))
	if snapshot.Recent.Attempts != 1 {
		t.Errorf("recent attempts = %d, want 1", snapshot.Recent.Attempts)
	}
	if snapshot.Historical.Attempts != 4 {
		t.Errorf(
			"historical attempts = %d, want 4",
			snapshot.Historical.Attempts,
		)
	}
	if snapshot.Historical.LastRelevantTick != int64(179*time.Second) {
		t.Errorf(
			"historical last tick = %v, want 2m59s",
			time.Duration(snapshot.Historical.LastRelevantTick),
		)
	}
	if snapshot.Recent.FirstRelevantTick <=
		snapshot.Historical.LastRelevantTick {
		t.Fatal("recent and historical evidence windows overlap")
	}
}

func TestHistoricalSnapshotIncludesDisjointBoundaryMinute(t *testing.T) {
	t.Parallel()

	state := newTarget(0, true)
	value := observation{
		outcome: outcomeSuccess,
		signal:  diagnosisNeutral,
	}
	if !state.record(int64(239*time.Second), value) {
		t.Fatal("historical observation was rejected")
	}
	if !state.record(int64(5*time.Minute+50*time.Second), value) {
		t.Fatal("recent observation was rejected")
	}

	snapshot := state.snapshotAt(int64(5*time.Minute + 50*time.Second))
	if snapshot.Historical.Attempts != 1 {
		t.Errorf(
			"historical attempts = %d, want boundary minute included",
			snapshot.Historical.Attempts,
		)
	}
	if snapshot.Recent.Attempts != 1 {
		t.Errorf("recent attempts = %d, want 1", snapshot.Recent.Attempts)
	}
}

func TestDiagnosisExpiresAtRecentGenerationBoundary(t *testing.T) {
	t.Parallel()

	state := newTarget(0, true)
	if !state.record(
		int64(9*time.Second),
		observation{
			outcome: outcomeSuccess,
			signal:  diagnosisNeutral,
		},
	) {
		t.Fatal("observation was rejected")
	}

	beforeRotation := state.diagnosisAt(int64(119 * time.Second))
	if beforeRotation.ExpiresAt != int64(120*time.Second) {
		t.Errorf(
			"expiry = %v, want 2m generation boundary",
			time.Duration(beforeRotation.ExpiresAt),
		)
	}

	afterRotation := state.diagnosisAt(int64(120 * time.Second))
	if afterRotation.Readiness != diagnose.ReadinessStale {
		t.Errorf(
			"readiness after rotation = %v, want Stale",
			afterRotation.Readiness,
		)
	}

	if !state.record(
		int64(121*time.Second),
		observation{
			outcome: outcomeSuccess,
			signal:  diagnosisNeutral,
		},
	) {
		t.Fatal("post-stale observation was rejected")
	}
	rewarmed := state.diagnosisAt(int64(121 * time.Second))
	if rewarmed.Readiness != diagnose.ReadinessWarming {
		t.Errorf("rewarmed readiness = %v, want Warming", rewarmed.Readiness)
	}
}

func TestDiagnosisEvaluationIsAmortizedAndMonotonic(t *testing.T) {
	t.Parallel()

	state := newTarget(0, true)
	initial := state.diagnosisAt(0)
	if initial.Readiness != diagnose.ReadinessCold {
		t.Errorf("initial readiness = %v, want Cold", initial.Readiness)
	}

	value := observation{
		outcome: outcomeSuccess,
		signal:  diagnosisNeutral,
	}
	for index := int64(0); index < 20; index++ {
		if !state.record(index*int64(2*time.Second), value) {
			t.Fatalf("observation %d was rejected", index)
		}
	}

	cached := state.diagnosisAt(int64(38 * time.Second))
	if cached.Readiness != diagnose.ReadinessWarming {
		t.Errorf("cached readiness = %v, want Warming", cached.Readiness)
	}
	if cached.EvaluatedAt != int64(30*time.Second) {
		t.Errorf(
			"cached evaluation tick = %v, want 30s",
			time.Duration(cached.EvaluatedAt),
		)
	}

	refreshed := state.diagnosisAt(int64(40 * time.Second))
	if refreshed.Readiness != diagnose.ReadinessReady {
		t.Errorf("refreshed readiness = %v, want Ready", refreshed.Readiness)
	}
	if refreshed.Class != diagnose.ClassHealthy {
		t.Errorf("refreshed class = %v, want Healthy", refreshed.Class)
	}

	if !state.record(
		int64(35*time.Second),
		observation{
			outcome: outcomeTransportFailure,
			signal:  diagnosisDependencyFailure,
		},
	) {
		t.Fatal("older same-generation observation was rejected")
	}
	afterOlder := state.diagnosisAt(int64(41 * time.Second))
	if afterOlder.EvaluatedAt != refreshed.EvaluatedAt {
		t.Errorf(
			"evaluation regressed from %v to %v",
			time.Duration(refreshed.EvaluatedAt),
			time.Duration(afterOlder.EvaluatedAt),
		)
	}
}

func TestHistoricalEvidenceMakesLowVolumeTargetReady(t *testing.T) {
	t.Parallel()

	state := newTarget(0, true)
	value := observation{
		outcome: outcomeSuccess,
		signal:  diagnosisNeutral,
	}
	for minute := int64(0); minute <= 22; minute++ {
		if !state.record(minute*int64(time.Minute), value) {
			t.Fatalf("minute %d observation was rejected", minute)
		}
	}

	result := state.diagnosisAt(int64(22 * time.Minute))
	if result.Readiness != diagnose.ReadinessReady {
		t.Errorf("readiness = %v, want Ready", result.Readiness)
	}
	if result.Class != diagnose.ClassNone {
		t.Errorf("class = %v, want None with sparse recent evidence", result.Class)
	}

	snapshot := state.snapshotAt(int64(22 * time.Minute))
	if snapshot.Historical.RelevantAttempts != 20 {
		t.Errorf(
			"historical relevant attempts = %d, want 20",
			snapshot.Historical.RelevantAttempts,
		)
	}
}

func TestConcurrentRecordsProduceCoherentSnapshot(t *testing.T) {
	t.Parallel()

	const workers = 128

	state := newTarget(0, true)
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(workers)
	for index := 0; index < workers; index++ {
		go func(worker int) {
			defer wait.Done()
			<-start

			signal := diagnosisNeutral
			outcomeValue := outcomeSuccess
			if worker%2 == 0 {
				signal = diagnosisDependencyFailure
				outcomeValue = outcomeTransportFailure
			}
			if !state.record(
				int64(time.Minute),
				observation{
					outcome: outcomeValue,
					signal:  signal,
				},
			) {
				t.Errorf("worker %d observation was rejected", worker)
			}
		}(index)
	}

	close(start)
	wait.Wait()

	snapshot := state.snapshotAt(int64(time.Minute))
	if snapshot.Recent.Attempts != workers {
		t.Errorf("recent attempts = %d, want %d", snapshot.Recent.Attempts, workers)
	}
	if snapshot.Recent.RelevantAttempts != workers {
		t.Errorf(
			"relevant attempts = %d, want %d",
			snapshot.Recent.RelevantAttempts,
			workers,
		)
	}
	if snapshot.Recent.DependencyFailures != workers/2 {
		t.Errorf(
			"dependency failures = %d, want %d",
			snapshot.Recent.DependencyFailures,
			workers/2,
		)
	}
}

func TestRejectedRecentObservationDoesNotEnterBaseline(t *testing.T) {
	t.Parallel()

	state := newTarget(0, true)
	futureTick := int64(observationBucketWidth) * observationBucketCount
	if !state.record(
		futureTick,
		observation{
			outcome: outcomeSuccess,
			signal:  diagnosisNeutral,
		},
	) {
		t.Fatal("future observation was rejected")
	}
	if state.record(
		0,
		observation{
			outcome: outcomeTransportFailure,
			signal:  diagnosisDependencyFailure,
		},
	) {
		t.Fatal("stale recent observation was accepted")
	}

	var baselineAttempts uint64
	state.mu.Lock()
	for index := range state.baseline {
		for _, count := range state.baseline[index].signals {
			addCounter(&baselineAttempts, count)
		}
	}
	state.mu.Unlock()

	if baselineAttempts != 1 {
		t.Errorf("baseline attempts = %d, want 1", baselineAttempts)
	}
}

func TestBaselineRejectsOlderGenerationAfterRecentAdmission(t *testing.T) {
	t.Parallel()

	state := newTarget(0, true)
	state.mu.Lock()
	state.baseline[0].epoch = baselineBucketCount
	state.mu.Unlock()

	if !state.record(
		0,
		observation{
			outcome: outcomeSuccess,
			signal:  diagnosisNeutral,
		},
	) {
		t.Fatal("recent observation was rejected")
	}

	state.mu.Lock()
	count := state.baseline[0].signals[diagnosisNeutral]
	state.mu.Unlock()
	if count != 0 {
		t.Errorf("older baseline generation count = %d, want 0", count)
	}
}

func TestDiagnosisAccessClampsInvalidAndBackwardTicks(t *testing.T) {
	t.Parallel()

	var nilTarget *target
	if got := nilTarget.diagnosisAt(0); got != (diagnose.Result{}) {
		t.Errorf("nil target diagnosis = %#v, want zero", got)
	}
	if got := nilTarget.snapshotAt(0); got != (diagnose.Snapshot{}) {
		t.Errorf("nil target snapshot = %#v, want zero", got)
	}

	state := newTarget(0, true)
	if !state.record(
		int64(time.Second),
		observation{
			outcome: outcomeSuccess,
			signal:  diagnosisNeutral,
		},
	) {
		t.Fatal("observation was rejected")
	}
	if got := state.diagnosisAt(-1); got.EvaluatedAt != int64(time.Second) {
		t.Errorf(
			"negative-tick diagnosis evaluated at %v, want 1s",
			time.Duration(got.EvaluatedAt),
		)
	}
	if got := state.snapshotAt(-1); got.Now != int64(time.Second) {
		t.Errorf(
			"negative-tick snapshot evaluated at %v, want 1s",
			time.Duration(got.Now),
		)
	}

	state.mu.Lock()
	state.diagnosis.result = diagnose.Result{
		EvaluatedAt: int64(10 * time.Second),
		ExpiresAt:   int64(20 * time.Second),
	}
	state.diagnosis.initialized = true
	state.evaluateDiagnosisLocked(int64(5 * time.Second))
	evaluatedAt := state.diagnosis.result.EvaluatedAt
	state.mu.Unlock()
	if evaluatedAt != int64(10*time.Second) {
		t.Errorf(
			"evaluation tick regressed to %v",
			time.Duration(evaluatedAt),
		)
	}
}

func TestOverflowTargetSkipsDiagnosisState(t *testing.T) {
	t.Parallel()

	state := newTarget(0, false)
	if !state.record(
		0,
		observation{
			outcome: outcomeSuccess,
			signal:  diagnosisNeutral,
		},
	) {
		t.Fatal("overflow observation was rejected")
	}

	state.mu.Lock()
	initialized := state.diagnosis.initialized
	hasRelevant := state.diagnosis.hasRelevant
	baselineEpoch := state.baseline[0].epoch
	state.mu.Unlock()

	if initialized || hasRelevant {
		t.Fatal("overflow target initialized diagnosis state")
	}
	if baselineEpoch != uninitializedEpoch {
		t.Fatal("overflow target recorded historical diagnosis evidence")
	}
	if got := state.diagnosisAt(0); got != (diagnose.Result{}) {
		t.Errorf("overflow diagnosis = %#v, want zero", got)
	}
}

func TestPercentileBucketHandlesBoundsAndSaturation(t *testing.T) {
	t.Parallel()

	var buckets [len(latencyUpperBounds) + 1]uint64
	if got := percentileBucket(buckets, 0, 95); got != 0 {
		t.Errorf("empty percentile bucket = %d, want 0", got)
	}
	if got := percentileBucket(buckets, 1, 0); got != 0 {
		t.Errorf("zero percentile bucket = %d, want 0", got)
	}
	if got := percentileBucket(buckets, 1, 101); got != 0 {
		t.Errorf("invalid percentile bucket = %d, want 0", got)
	}

	buckets[0] = 95
	buckets[1] = 5
	if got := percentileBucket(buckets, 100, 95); got != 0 {
		t.Errorf("exact p95 bucket = %d, want 0", got)
	}
	buckets[0] = 94
	buckets[1] = 6
	if got := percentileBucket(buckets, 100, 95); got != 1 {
		t.Errorf("upper p95 bucket = %d, want 1", got)
	}

	buckets = [len(latencyUpperBounds) + 1]uint64{}
	buckets[len(buckets)-1] = math.MaxUint64
	if got := percentileBucket(
		buckets,
		math.MaxUint64,
		95,
	); got != uint8(len(buckets)-1) {
		t.Errorf("saturated p95 bucket = %d, want overflow", got)
	}

	buckets = [len(latencyUpperBounds) + 1]uint64{}
	if got := percentileBucket(buckets, 1, 95); got != uint8(len(buckets)-1) {
		t.Errorf("inconsistent p95 bucket = %d, want final bucket", got)
	}

	counter := uint64(math.MaxUint64 - 1)
	addCounter(&counter, 2)
	if counter != math.MaxUint64 {
		t.Errorf("saturated counter = %d, want MaxUint64", counter)
	}
}

func TestRecentExpirySaturates(t *testing.T) {
	t.Parallel()

	width := int64(observationBucketWidth)
	bucket := evidenceBucket{
		epoch:           math.MaxInt64/width - observationBucketCount + 1,
		hasRelevantTick: true,
	}
	var accumulator windowAccumulator
	accumulator.addRecentExpiry(&bucket)
	if accumulator.expiresAt != math.MaxInt64 {
		t.Errorf("recent expiry = %d, want MaxInt64", accumulator.expiresAt)
	}
}
