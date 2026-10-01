package observe

import (
	"math"
	"time"

	"github.com/raj1kshtz/curo/internal/diagnose"
	"github.com/raj1kshtz/curo/internal/policy"
)

const (
	baselineBucketCount = 30
	baselineBucketWidth = time.Minute
)

type baselineBucket struct {
	epoch int64

	signals [diagnosisSignalCount]uint64
	latency [len(latencyUpperBounds) + 1]uint64

	firstRelevantTick int64
	lastRelevantTick  int64
	hasRelevantTick   bool
}

type diagnosisState struct {
	result       diagnose.Result
	plan         policy.Plan
	lastRelevant int64
	hasRelevant  bool
	initialized  bool
}

type publication struct {
	result    diagnose.Result
	plan      policy.Plan
	evaluated bool
}

type windowAccumulator struct {
	window    diagnose.Window
	latency   [len(latencyUpperBounds) + 1]uint64
	expiresAt int64

	hasRelevantTick bool
	hasLastTick     bool
	hasExpiry       bool
}

func (target *target) recordDiagnosisLocked(
	tick int64,
	value observation,
) {
	if !target.actionable || target.retired {
		return
	}

	target.recordBaselineLocked(tick, value)
	if value.signal == diagnosisCallerOwned {
		return
	}

	target.diagnosis.hasRelevant = true
	if tick > target.diagnosis.lastRelevant {
		target.diagnosis.lastRelevant = tick
	}

	evaluationTick := target.diagnosis.lastRelevant
	if !target.diagnosis.initialized ||
		target.diagnosis.result.Readiness == diagnose.ReadinessCold ||
		target.diagnosis.result.Readiness == diagnose.ReadinessStale ||
		evaluationTick >= target.diagnosis.result.ExpiresAt {
		target.evaluateDiagnosisLocked(evaluationTick)
	}
}

func (target *target) recordBaselineLocked(
	tick int64,
	value observation,
) {
	epoch := tick / int64(baselineBucketWidth)
	index := int(epoch % baselineBucketCount)
	bucket := &target.baseline[index]
	if bucket.epoch > epoch {
		return
	}
	if bucket.epoch < epoch {
		*bucket = baselineBucket{epoch: epoch}
	}

	increment(&bucket.signals[value.signal])
	if value.hasLatency {
		increment(&bucket.latency[value.latencyBucket])
	}
	if value.signal != diagnosisCallerOwned {
		recordTick(
			tick,
			&bucket.firstRelevantTick,
			&bucket.lastRelevantTick,
			&bucket.hasRelevantTick,
		)
	}
}

func (target *target) diagnosisAt(tick int64) diagnose.Result {
	if target == nil || !target.actionable {
		return diagnose.Result{}
	}
	if tick < 0 {
		tick = 0
	}

	target.mu.Lock()
	defer target.mu.Unlock()

	if target.retired {
		return diagnose.Result{}
	}
	if tick < target.diagnosis.lastRelevant {
		tick = target.diagnosis.lastRelevant
	}
	if !target.diagnosis.initialized ||
		tick >= target.diagnosis.result.ExpiresAt {
		target.evaluateDiagnosisLocked(tick)
	}

	return target.diagnosis.result
}

// published returns the latest evaluation without evaluating. It reports
// false for retired targets.
func (target *target) published() (publication, bool) {
	target.mu.Lock()
	defer target.mu.Unlock()

	if target.retired {
		return publication{}, false
	}

	return publication{
		result:    target.diagnosis.result,
		plan:      target.diagnosis.plan,
		evaluated: target.diagnosis.initialized,
	}, true
}

func (target *target) snapshotAt(tick int64) diagnose.Snapshot {
	if target == nil || !target.actionable {
		return diagnose.Snapshot{}
	}
	if tick < 0 {
		tick = 0
	}

	target.mu.Lock()
	defer target.mu.Unlock()

	if tick < target.diagnosis.lastRelevant {
		tick = target.diagnosis.lastRelevant
	}

	return target.snapshotLocked(tick)
}

func (target *target) evaluateDiagnosisLocked(tick int64) {
	candidate := diagnose.Evaluate(target.snapshotLocked(tick))
	if target.diagnosis.initialized &&
		candidate.EvaluatedAt < target.diagnosis.result.EvaluatedAt {
		return
	}

	plan := policy.Evaluate(candidate)
	if candidatesChanged(target.diagnosis, candidate, plan) {
		target.changes.record(target.key, candidate, plan)
	}

	target.diagnosis.result = candidate
	target.diagnosis.plan = plan
	target.diagnosis.initialized = true
}

// candidatesChanged reports a different candidate set, or the same non-empty
// set justified by a different diagnosis. Renewals are not changes.
func candidatesChanged(
	previous diagnosisState,
	result diagnose.Result,
	plan policy.Plan,
) bool {
	if plan.Candidates != previous.plan.Candidates {
		return true
	}

	return plan.Candidates != 0 && result.Class != previous.result.Class
}

func (target *target) snapshotLocked(tick int64) diagnose.Snapshot {
	var recent windowAccumulator
	currentEpoch := tick / int64(observationBucketWidth)
	firstEpoch := currentEpoch - observationBucketCount + 1
	for index := range target.buckets {
		bucket := &target.buckets[index]
		if bucket.epoch < firstEpoch || bucket.epoch > currentEpoch {
			continue
		}

		recent.addEvidenceBucket(bucket)
	}
	recent.finish()

	var historical windowAccumulator
	cutoff := firstEpoch * int64(observationBucketWidth)
	if cutoff >= int64(baselineBucketWidth) {
		currentBaselineEpoch := tick / int64(baselineBucketWidth)
		firstBaselineEpoch := currentBaselineEpoch - baselineBucketCount + 1
		lastCompleteEpoch := cutoff/int64(baselineBucketWidth) - 1
		for index := range target.baseline {
			bucket := &target.baseline[index]
			if bucket.epoch < firstBaselineEpoch ||
				bucket.epoch > lastCompleteEpoch {
				continue
			}

			historical.addBaselineBucket(bucket)
		}
	}
	historical.finish()

	return diagnose.Snapshot{
		Recent:          recent.window,
		Historical:      historical.window,
		Now:             tick,
		RecentExpiresAt: recent.expiresAt,
		EverRelevant:    target.diagnosis.hasRelevant,
	}
}

func (accumulator *windowAccumulator) addEvidenceBucket(
	bucket *evidenceBucket,
) {
	addCounter(&accumulator.window.Attempts, bucket.attempts)
	accumulator.addSignals(bucket.signals)
	accumulator.addLatency(bucket.latency)
	accumulator.addLastTick(bucket.lastTick, bucket.hasTick)
	accumulator.addRelevantTicks(
		bucket.firstRelevantTick,
		bucket.lastRelevantTick,
		bucket.hasRelevantTick,
	)
	accumulator.addRecentExpiry(bucket)
}

func (accumulator *windowAccumulator) addBaselineBucket(
	bucket *baselineBucket,
) {
	var attempts uint64
	for _, count := range bucket.signals {
		addCounter(&attempts, count)
	}

	addCounter(&accumulator.window.Attempts, attempts)
	accumulator.addSignals(bucket.signals)
	accumulator.addLatency(bucket.latency)
	accumulator.addRelevantTicks(
		bucket.firstRelevantTick,
		bucket.lastRelevantTick,
		bucket.hasRelevantTick,
	)
}

func (accumulator *windowAccumulator) addSignals(
	signals [diagnosisSignalCount]uint64,
) {
	for signal, count := range signals {
		if diagnosisSignal(signal) != diagnosisCallerOwned {
			addCounter(&accumulator.window.RelevantAttempts, count)
		}
	}

	addCounter(
		&accumulator.window.RateLimited,
		signals[diagnosisRateLimited],
	)
	addCounter(
		&accumulator.window.ClientFailures,
		signals[diagnosisClientFailure],
	)
	addCounter(
		&accumulator.window.DependencyFailures,
		signals[diagnosisDependencyFailure],
	)
}

func (accumulator *windowAccumulator) addLatency(
	latency [len(latencyUpperBounds) + 1]uint64,
) {
	for index, count := range latency {
		addCounter(&accumulator.latency[index], count)
		addCounter(&accumulator.window.LatencySamples, count)
	}
}

func (accumulator *windowAccumulator) addLastTick(
	tick int64,
	recorded bool,
) {
	if !recorded {
		return
	}
	if !accumulator.hasLastTick || tick > accumulator.window.LastTick {
		accumulator.window.LastTick = tick
		accumulator.hasLastTick = true
	}
}

func (accumulator *windowAccumulator) addRelevantTicks(
	first int64,
	last int64,
	recorded bool,
) {
	if !recorded {
		return
	}
	if !accumulator.hasRelevantTick {
		accumulator.window.FirstRelevantTick = first
		accumulator.window.LastRelevantTick = last
		accumulator.hasRelevantTick = true
		return
	}
	if first < accumulator.window.FirstRelevantTick {
		accumulator.window.FirstRelevantTick = first
	}
	if last > accumulator.window.LastRelevantTick {
		accumulator.window.LastRelevantTick = last
	}
}

func (accumulator *windowAccumulator) addRecentExpiry(
	bucket *evidenceBucket,
) {
	if !bucket.hasRelevantTick {
		return
	}

	width := int64(observationBucketWidth)
	var expiresAt int64
	if bucket.epoch > math.MaxInt64/width-observationBucketCount {
		expiresAt = math.MaxInt64
	} else {
		expiresAt = (bucket.epoch + observationBucketCount) * width
	}
	if !accumulator.hasExpiry || expiresAt < accumulator.expiresAt {
		accumulator.expiresAt = expiresAt
		accumulator.hasExpiry = true
	}
}

func (accumulator *windowAccumulator) finish() {
	accumulator.window.P95LatencyBucket = percentileBucket(
		accumulator.latency,
		accumulator.window.LatencySamples,
		95,
	)
}

func percentileBucket(
	buckets [len(latencyUpperBounds) + 1]uint64,
	total uint64,
	percentile uint64,
) uint8 {
	if total == 0 || percentile == 0 || percentile > 100 {
		return 0
	}

	rank := total / 100 * percentile
	remainder := total % 100
	rankRemainder := remainder * percentile
	rank += rankRemainder / 100
	if rankRemainder%100 != 0 {
		rank++
	}

	var cumulative uint64
	for index, count := range buckets {
		addCounter(&cumulative, count)
		if cumulative >= rank {
			return uint8(index)
		}
	}

	return uint8(len(buckets) - 1)
}

func addCounter(target *uint64, value uint64) {
	if math.MaxUint64-*target < value {
		*target = math.MaxUint64
		return
	}

	*target += value
}
