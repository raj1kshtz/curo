package observe

import (
	"math"
	"time"

	"github.com/raj1kshtz/curo/internal/diagnose"
	"github.com/raj1kshtz/curo/internal/policy"
	"github.com/raj1kshtz/curo/internal/timeout"
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

	// provisional reports that the published evaluation ran when a request
	// started, to renew the target's timeout. The next relevant result then
	// evaluates again, so the diagnosis takes in that result as soon as it
	// would have without the earlier evaluation.
	provisional bool
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

// recordDiagnosisLocked records value in the baseline and reports whether the
// target must evaluate it. A relevant result is due when the published
// evaluation is missing, provisional, not ready, or expired. Any latency
// sample is due when it raises the published timeout.
func (target *target) recordDiagnosisLocked(
	tick int64,
	value observation,
) bool {
	if !target.actionable || target.retired {
		return false
	}

	target.recordBaselineLocked(tick, value)
	switch value.signal {
	case diagnosisCallerOwned:
		return false
	case diagnosisLatencyOnly:
		return target.raisesTimeoutLocked(value)
	}

	target.diagnosis.hasRelevant = true
	if tick > target.diagnosis.lastRelevant {
		target.diagnosis.lastRelevant = tick
	}

	return !target.diagnosis.initialized ||
		target.diagnosis.provisional ||
		target.diagnosis.result.Readiness == diagnose.ReadinessCold ||
		target.diagnosis.result.Readiness == diagnose.ReadinessStale ||
		target.diagnosis.lastRelevant >= target.diagnosis.result.ExpiresAt ||
		target.raisesTimeoutLocked(value)
}

// evaluateRecordedLocked evaluates the target for a value that
// recordDiagnosisLocked reported due. A relevant result evaluates at the
// latest relevant tick. A latency-only sample evaluates only to raise the
// timeout. It evaluates late enough to include the sample and to replace the
// published evaluation, which stays provisional, so the next relevant result
// still evaluates the target as it would have without this evaluation.
func (target *target) evaluateRecordedLocked(tick int64, value observation) {
	if value.signal.relevant() {
		target.evaluateDiagnosisLocked(target.diagnosis.lastRelevant)
		return
	}

	target.evaluateDiagnosisLocked(max(
		tick,
		target.diagnosis.lastRelevant,
		target.diagnosis.result.EvaluatedAt,
	))
	target.diagnosis.provisional = true
}

// raisesTimeoutLocked reports whether value is a latency sample slow enough
// to raise the published timeout. Evaluating at once applies the higher
// timeout to the next request rather than after the plan expires.
func (target *target) raisesTimeoutLocked(value observation) bool {
	limit := target.diagnosis.plan.Timeout
	return value.hasLatency &&
		limit > 0 &&
		timeout.Select(
			timeout.MinimumSamples,
			bucketUpperBound(value.latencyBucket),
			target.timeouts,
		) > limit
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
	if value.signal.relevant() {
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

	plan := policy.Evaluate(candidate, target.timeouts)
	if candidatesChanged(target.diagnosis, candidate, plan) {
		target.changes.record(target.key, candidate, plan)
	}

	target.diagnosis.result = candidate
	target.diagnosis.plan = plan
	target.diagnosis.initialized = true
	target.diagnosis.provisional = false
	target.publishTimeoutLocked()
}

// publishTimeoutLocked mirrors the published plan's timeout for lock-free
// reads by Timeout.
func (target *target) publishTimeoutLocked() {
	plan := target.diagnosis.plan
	if plan.Candidates&policy.CandidateTimeout == 0 {
		if target.timeout.Load() != nil {
			target.timeout.Store(nil)
		}
		return
	}

	target.timeout.Store(&publishedTimeout{
		limit:     plan.Timeout,
		expiresAt: plan.ExpiresAt,
	})
}

// timeoutAt returns the timeout that the target's plan selects at tick,
// evaluating again first when the plan expired. That evaluation is
// provisional, because the request it runs for has no result yet.
func (target *target) timeoutAt(tick int64) time.Duration {
	target.mu.Lock()
	defer target.mu.Unlock()

	if target.retired {
		return 0
	}
	if tick >= target.diagnosis.plan.ExpiresAt {
		target.evaluateDiagnosisLocked(max(tick, target.diagnosis.lastRelevant))
		target.diagnosis.provisional = true
	}
	if !target.selectsLocked(policy.CandidateTimeout, tick) {
		return 0
	}

	return target.diagnosis.plan.Timeout
}

// candidatesChanged reports a different candidate set or timeout, or the same
// set of diagnosis candidates justified by a different diagnosis. Renewals
// are not changes. CandidateTimeout alone does not depend on the diagnosis,
// so a new diagnosis without other candidates is not a change either.
func candidatesChanged(
	previous diagnosisState,
	result diagnose.Result,
	plan policy.Plan,
) bool {
	if plan.Candidates != previous.plan.Candidates ||
		plan.Timeout != previous.plan.Timeout {
		return true
	}

	return plan.Candidates&^policy.CandidateTimeout != 0 &&
		result.Class != previous.result.Class
}

func (target *target) snapshotLocked(tick int64) diagnose.Snapshot {
	var (
		recent   windowAccumulator
		latency  latencySummary
		boundary [len(latencyUpperBounds) + 1]uint64
	)
	currentEpoch := tick / int64(observationBucketWidth)
	firstEpoch := currentEpoch - observationBucketCount + 1
	cutoff := firstEpoch * int64(observationBucketWidth)
	boundaryEpoch := cutoff / int64(baselineBucketWidth)
	// Closing the dependency breaker fences everything recorded before the
	// close out of the recent window. The historical cutoff is unchanged, and
	// the latency summary keeps every retained sample.
	recentFirst := max(firstEpoch, target.recentFloor)
	for index := range target.buckets {
		bucket := &target.buckets[index]
		if bucket.epoch < max(firstEpoch, 0) || bucket.epoch > currentEpoch {
			continue
		}

		latency.add(&bucket.latency)
		if bucket.epoch*int64(observationBucketWidth)/
			int64(baselineBucketWidth) == boundaryEpoch {
			for slot, count := range bucket.latency {
				addCounter(&boundary[slot], count)
			}
		}
		if bucket.epoch >= recentFirst {
			recent.addEvidenceBucket(bucket)
		}
	}
	recent.finish()

	var historical windowAccumulator
	if cutoff >= int64(baselineBucketWidth) {
		currentBaselineEpoch := tick / int64(baselineBucketWidth)
		firstBaselineEpoch := currentBaselineEpoch - baselineBucketCount + 1
		lastCompleteEpoch := boundaryEpoch - 1
		for index := range target.baseline {
			bucket := &target.baseline[index]
			if bucket.epoch < firstBaselineEpoch ||
				bucket.epoch > lastCompleteEpoch {
				continue
			}

			historical.addBaselineBucket(bucket)
			latency.add(&bucket.latency)
		}
	}
	historical.finish()

	// The historical window ends before the baseline minute that holds the
	// recent window's start, so the windows never overlap, and that minute's
	// samples from before the start belong to neither. The latency summary
	// takes them from the minute's baseline bucket, less the samples its
	// evidence buckets in the recent window already added.
	if cutoff > 0 {
		bucket := &target.baseline[boundaryEpoch%baselineBucketCount]
		if bucket.epoch == boundaryEpoch {
			latency.addExcess(&bucket.latency, &boundary)
		}
	}

	return diagnose.Snapshot{
		Recent:          recent.window,
		Historical:      historical.window,
		Now:             tick,
		RecentExpiresAt: recent.expiresAt,
		Latency:         latency.summary(),
		EverRelevant:    target.diagnosis.hasRelevant,
	}
}

// latencySummary accumulates retained latency buckets for the adaptive
// timeout.
type latencySummary struct {
	samples uint64

	// slowest is one more than the index of the slowest non-empty bucket, or
	// zero without samples.
	slowest uint8
}

func (summary *latencySummary) add(
	latency *[len(latencyUpperBounds) + 1]uint64,
) {
	for index, count := range latency {
		if count == 0 {
			continue
		}

		addCounter(&summary.samples, count)
		summary.slowest = max(summary.slowest, uint8(index)+1)
	}
}

// addExcess adds the samples in total beyond those in counted.
func (summary *latencySummary) addExcess(
	total *[len(latencyUpperBounds) + 1]uint64,
	counted *[len(latencyUpperBounds) + 1]uint64,
) {
	var excess [len(latencyUpperBounds) + 1]uint64
	for index, count := range total {
		if count > counted[index] {
			excess[index] = count - counted[index]
		}
	}
	summary.add(&excess)
}

func (summary latencySummary) summary() diagnose.Latency {
	if summary.slowest == 0 {
		return diagnose.Latency{}
	}

	return diagnose.Latency{
		Samples: summary.samples,
		Slowest: bucketUpperBound(summary.slowest - 1),
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
		if diagnosisSignal(signal).relevant() {
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
