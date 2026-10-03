package diagnose

import (
	"math"
	"testing"
	"time"
)

func TestEvaluateReadiness(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		snapshot      Snapshot
		wantReadiness Readiness
		wantClass     Class
		wantReason    Reason
		wantExpiry    int64
	}{
		"cold": {
			snapshot:      Snapshot{Now: -1},
			wantReadiness: ReadinessCold,
			wantClass:     ClassNone,
			wantReason:    ReasonNoEvidence,
			wantExpiry:    int64(resultTTL),
		},
		"warming": {
			snapshot: Snapshot{
				Now:             int64(time.Second),
				EverRelevant:    true,
				RecentExpiresAt: int64(2 * time.Minute),
				Recent: Window{
					Attempts:          1,
					RelevantAttempts:  1,
					LastTick:          int64(time.Second),
					LastRelevantTick:  int64(time.Second),
					FirstRelevantTick: int64(time.Second),
				},
			},
			wantReadiness: ReadinessWarming,
			wantClass:     ClassNone,
			wantReason:    ReasonInsufficientEvidence,
			wantExpiry:    int64(11 * time.Second),
		},
		"recent ready": {
			snapshot: Snapshot{
				Now:             int64(30 * time.Second),
				EverRelevant:    true,
				RecentExpiresAt: int64(2 * time.Minute),
				Recent: Window{
					Attempts:          recentReadySamples,
					RelevantAttempts:  recentReadySamples,
					FirstRelevantTick: 0,
					LastRelevantTick:  int64(recentReadySpan),
					LastTick:          int64(recentReadySpan),
				},
			},
			wantReadiness: ReadinessReady,
			wantClass:     ClassHealthy,
			wantReason:    ReasonRecentEvidenceReady,
			wantExpiry:    int64(40 * time.Second),
		},
		"historical ready": {
			snapshot: Snapshot{
				Now:             int64(15 * time.Minute),
				EverRelevant:    true,
				RecentExpiresAt: int64(17 * time.Minute),
				Recent: Window{
					Attempts:          1,
					RelevantAttempts:  1,
					FirstRelevantTick: int64(15 * time.Minute),
					LastRelevantTick:  int64(15 * time.Minute),
					LastTick:          int64(15 * time.Minute),
				},
				Historical: Window{
					Attempts:          historicalReadySamples,
					RelevantAttempts:  historicalReadySamples,
					FirstRelevantTick: 0,
					LastRelevantTick:  int64(historicalReadySpan),
				},
			},
			wantReadiness: ReadinessReady,
			wantClass:     ClassNone,
			wantReason:    ReasonHistoricalBaselineReady,
			wantExpiry:    int64(15*time.Minute + resultTTL),
		},
		"stale": {
			snapshot: Snapshot{
				Now:          int64(20 * time.Minute),
				EverRelevant: true,
			},
			wantReadiness: ReadinessStale,
			wantClass:     ClassNone,
			wantReason:    ReasonEvidenceStale,
			wantExpiry:    int64(20*time.Minute + resultTTL),
		},
		"caller only": {
			snapshot: Snapshot{
				Now: int64(time.Minute),
				Recent: Window{
					Attempts: 20,
					LastTick: int64(time.Minute),
				},
			},
			wantReadiness: ReadinessCold,
			wantClass:     ClassNone,
			wantReason:    ReasonNoEvidence,
			wantExpiry:    int64(time.Minute + resultTTL),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := Evaluate(test.snapshot)
			if got.Readiness != test.wantReadiness {
				t.Errorf(
					"readiness = %v, want %v",
					got.Readiness,
					test.wantReadiness,
				)
			}
			if got.Class != test.wantClass {
				t.Errorf("class = %v, want %v", got.Class, test.wantClass)
			}
			if !hasReason(got, test.wantReason) {
				t.Errorf("reasons = %v, want %v", got.Reasons, test.wantReason)
			}
			if got.ExpiresAt != test.wantExpiry {
				t.Errorf(
					"expiry = %v, want %v",
					time.Duration(got.ExpiresAt),
					time.Duration(test.wantExpiry),
				)
			}
		})
	}
}

func TestEvaluateDiagnosisRuleOrder(t *testing.T) {
	t.Parallel()

	readyRecent := Window{
		Attempts:          20,
		RelevantAttempts:  20,
		FirstRelevantTick: 0,
		LastRelevantTick:  int64(recentReadySpan),
		LastTick:          int64(recentReadySpan),
		LatencySamples:    20,
	}
	tests := map[string]struct {
		recent     Window
		historical Window
		wantClass  Class
		wantReason Reason
	}{
		"client failure": {
			recent: Window{
				Attempts:         6,
				RelevantAttempts: 6,
				ClientFailures:   3,
				LastTick:         int64(time.Second),
				LastRelevantTick: int64(time.Second),
			},
			wantClass:  ClassClientError,
			wantReason: ReasonClientFailureRate,
		},
		"client failure precedes rate limiting": {
			recent: Window{
				Attempts:         10,
				RelevantAttempts: 10,
				ClientFailures:   5,
				RateLimited:      3,
				LastTick:         int64(time.Second),
				LastRelevantTick: int64(time.Second),
			},
			wantClass:  ClassClientError,
			wantReason: ReasonClientFailureRate,
		},
		"saturation": {
			recent: Window{
				Attempts:         10,
				RelevantAttempts: 10,
				RateLimited:      3,
				LastTick:         int64(time.Second),
				LastRelevantTick: int64(time.Second),
			},
			wantClass:  ClassSaturation,
			wantReason: ReasonRateLimitRate,
		},
		"dependency down": {
			recent: Window{
				Attempts:           10,
				RelevantAttempts:   10,
				DependencyFailures: 5,
				LastTick:           int64(time.Second),
				LastRelevantTick:   int64(time.Second),
			},
			wantClass:  ClassDependencyDown,
			wantReason: ReasonDependencyFailureRate,
		},
		"transient": {
			recent: Window{
				Attempts:           10,
				RelevantAttempts:   10,
				DependencyFailures: 1,
				LastTick:           int64(time.Second),
				LastRelevantTick:   int64(time.Second),
			},
			wantClass:  ClassTransient,
			wantReason: ReasonIsolatedDependencyFailure,
		},
		"healthy": {
			recent:     readyRecent,
			wantClass:  ClassHealthy,
			wantReason: ReasonWithinBaseline,
		},
		"insufficient": {
			recent: Window{
				Attempts:         2,
				RelevantAttempts: 2,
				LastTick:         int64(time.Second),
				LastRelevantTick: int64(time.Second),
			},
			wantClass:  ClassNone,
			wantReason: ReasonInsufficientRecentEvidence,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			snapshot := Snapshot{
				Recent:          test.recent,
				Historical:      test.historical,
				Now:             test.recent.LastTick,
				RecentExpiresAt: int64(2 * time.Minute),
				EverRelevant:    true,
			}
			got := Evaluate(snapshot)
			if got.Class != test.wantClass {
				t.Errorf("class = %v, want %v", got.Class, test.wantClass)
			}
			if !hasReason(got, test.wantReason) {
				t.Errorf("reasons = %v, want %v", got.Reasons, test.wantReason)
			}
		})
	}
}

func TestEvaluateDetectsDegradationAgainstHistory(t *testing.T) {
	t.Parallel()

	historical := Window{
		Attempts:          historicalCompareSamples,
		RelevantAttempts:  historicalCompareSamples,
		LatencySamples:    historicalCompareSamples,
		FirstRelevantTick: 0,
		LastRelevantTick:  int64(historicalReadySpan),
		P95LatencyBucket:  3,
	}
	baseRecent := Window{
		Attempts:          recentReadySamples,
		RelevantAttempts:  recentReadySamples,
		LatencySamples:    recentReadySamples,
		FirstRelevantTick: 0,
		LastRelevantTick:  int64(recentReadySpan),
		LastTick:          int64(recentReadySpan),
		P95LatencyBucket:  3,
	}

	tests := map[string]struct {
		update     func(*Window)
		wantReason Reason
	}{
		"failure rate": {
			update: func(recent *Window) {
				recent.DependencyFailures = 4
			},
			wantReason: ReasonFailureRateIncrease,
		},
		"latency": {
			update: func(recent *Window) {
				recent.P95LatencyBucket = 5
			},
			wantReason: ReasonLatencyIncrease,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			recent := baseRecent
			test.update(&recent)
			got := Evaluate(Snapshot{
				Recent:          recent,
				Historical:      historical,
				Now:             recent.LastTick,
				RecentExpiresAt: int64(2 * time.Minute),
				EverRelevant:    true,
			})
			if got.Class != ClassDegrading {
				t.Errorf("class = %v, want Degrading", got.Class)
			}
			if !hasReason(got, test.wantReason) {
				t.Errorf("reasons = %v, want %v", got.Reasons, test.wantReason)
			}
		})
	}
}

func TestEvaluateHandlesZeroAndSaturatedCounters(t *testing.T) {
	t.Parallel()

	if ratioAtLeast(1, 0, 50) {
		t.Fatal("zero denominator satisfied ratio")
	}
	if ratioAtLeast(1, 1, 101) {
		t.Fatal("invalid percentage satisfied ratio")
	}
	if got := basisPoints(1, 0); got != 0 {
		t.Errorf("zero-total basis points = %d, want 0", got)
	}
	if got := basisPoints(math.MaxUint64-1, math.MaxUint64); got != 9_999 {
		t.Errorf("saturated basis points = %d, want 9999", got)
	}
	if got := basisPoints(math.MaxUint64, math.MaxUint64); got != rateScale {
		t.Errorf("full saturated basis points = %d, want %d", got, rateScale)
	}
	if windowReady(
		Window{
			RelevantAttempts:  math.MaxUint64,
			FirstRelevantTick: 2,
			LastRelevantTick:  1,
		},
		1,
		0,
	) {
		t.Fatal("backward evidence span reported ready")
	}

	result := Evaluate(Snapshot{
		Now:             math.MaxInt64,
		RecentExpiresAt: math.MaxInt64,
		EverRelevant:    true,
		Recent: Window{
			Attempts:           math.MaxUint64,
			RelevantAttempts:   math.MaxUint64,
			DependencyFailures: math.MaxUint64,
			FirstRelevantTick:  0,
			LastRelevantTick:   math.MaxInt64,
			LastTick:           math.MaxInt64,
		},
	})
	if result.ExpiresAt != math.MaxInt64 {
		t.Errorf("saturated expiry = %d, want MaxInt64", result.ExpiresAt)
	}
	if result.Class != ClassDependencyDown {
		t.Errorf("saturated class = %v, want DependencyDown", result.Class)
	}

	expired := Evaluate(Snapshot{
		Now:             int64(2 * time.Minute),
		RecentExpiresAt: int64(time.Minute),
		EverRelevant:    true,
		Recent: Window{
			Attempts:          1,
			RelevantAttempts:  1,
			FirstRelevantTick: int64(time.Minute),
			LastRelevantTick:  int64(time.Minute),
		},
	})
	if expired.ExpiresAt != expired.EvaluatedAt {
		t.Errorf(
			"past evidence expiry = %d, want evaluation tick %d",
			expired.ExpiresAt,
			expired.EvaluatedAt,
		)
	}
	if got := saturatingAdd(math.MinInt64, -1); got != math.MinInt64 {
		t.Errorf("negative saturated addition = %d, want MinInt64", got)
	}
}

func TestEvaluateSummarizesWindowEvidence(t *testing.T) {
	t.Parallel()

	recent := Window{
		Attempts:           12,
		RelevantAttempts:   10,
		DependencyFailures: 3,
		RateLimited:        2,
		ClientFailures:     1,
		FirstRelevantTick:  int64(time.Second),
		LastRelevantTick:   int64(4 * time.Second),
		LastTick:           int64(5 * time.Second),
	}
	historical := Window{
		Attempts:           40,
		RelevantAttempts:   30,
		DependencyFailures: 4,
		RateLimited:        5,
		ClientFailures:     6,
		FirstRelevantTick:  int64(time.Minute),
		LastRelevantTick:   int64(3 * time.Minute),
	}
	wantRecent := Evidence{
		RelevantAttempts:   10,
		DependencyFailures: 3,
		RateLimited:        2,
		ClientFailures:     1,
		Span:               int64(3 * time.Second),
	}
	wantHistorical := Evidence{
		RelevantAttempts:   30,
		DependencyFailures: 4,
		RateLimited:        5,
		ClientFailures:     6,
		Span:               int64(2 * time.Minute),
	}

	tests := map[string]struct {
		snapshot       Snapshot
		wantRecent     Evidence
		wantHistorical Evidence
		wantReadiness  Readiness
	}{
		"cold caller-only traffic": {
			snapshot: Snapshot{
				Recent: Window{Attempts: 4, LastTick: int64(time.Second)},
				Now:    int64(time.Second),
			},
			wantReadiness: ReadinessCold,
		},
		"stale with historical evidence": {
			snapshot: Snapshot{
				Historical:   historical,
				Now:          int64(20 * time.Minute),
				EverRelevant: true,
			},
			wantHistorical: wantHistorical,
			wantReadiness:  ReadinessStale,
		},
		"evaluated": {
			snapshot: Snapshot{
				Recent:          recent,
				Historical:      historical,
				Now:             int64(5 * time.Second),
				RecentExpiresAt: int64(2 * time.Minute),
				EverRelevant:    true,
			},
			wantRecent:     wantRecent,
			wantHistorical: wantHistorical,
			wantReadiness:  ReadinessWarming,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := Evaluate(test.snapshot)
			if got.Readiness != test.wantReadiness {
				t.Errorf("readiness = %v, want %v", got.Readiness, test.wantReadiness)
			}
			if got.Recent != test.wantRecent {
				t.Errorf("recent evidence = %+v, want %+v", got.Recent, test.wantRecent)
			}
			if got.Historical != test.wantHistorical {
				t.Errorf(
					"historical evidence = %+v, want %+v",
					got.Historical,
					test.wantHistorical,
				)
			}
		})
	}
}

func TestEvaluateCarriesLatencySummary(t *testing.T) {
	t.Parallel()

	latency := Latency{Samples: 120, Slowest: 250 * time.Millisecond}
	ready := Window{
		Attempts:          30,
		RelevantAttempts:  30,
		LatencySamples:    30,
		FirstRelevantTick: int64(time.Second),
		LastRelevantTick:  int64(45 * time.Second),
		LastTick:          int64(45 * time.Second),
	}
	snapshots := map[string]Snapshot{
		"cold":  {Now: int64(time.Second)},
		"stale": {Now: int64(20 * time.Minute), EverRelevant: true},
		"ready": {
			Recent:          ready,
			Now:             int64(45 * time.Second),
			RecentExpiresAt: int64(2 * time.Minute),
			EverRelevant:    true,
		},
	}

	for name, snapshot := range snapshots {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			without := Evaluate(snapshot)
			snapshot.Latency = latency
			got := Evaluate(snapshot)
			if got.Latency != latency {
				t.Errorf("latency = %+v, want %+v", got.Latency, latency)
			}

			got.Latency = Latency{}
			if got != without {
				t.Errorf("result with latency = %+v, want %+v", got, without)
			}
		})
	}
}

func TestSummarizeBoundsEvidenceSpan(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		window Window
		want   int64
	}{
		"no relevant attempts": {
			window: Window{FirstRelevantTick: 1, LastRelevantTick: 2},
		},
		"single instant": {
			window: Window{
				RelevantAttempts:  1,
				FirstRelevantTick: 5,
				LastRelevantTick:  5,
			},
		},
		"backward": {
			window: Window{
				RelevantAttempts:  2,
				FirstRelevantTick: 5,
				LastRelevantTick:  1,
			},
		},
		"overflow": {
			window: Window{
				RelevantAttempts:  2,
				FirstRelevantTick: math.MinInt64,
				LastRelevantTick:  math.MaxInt64,
			},
			want: math.MaxInt64,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := summarize(test.window).Span; got != test.want {
				t.Errorf("span = %d, want %d", got, test.want)
			}
		})
	}
}

func TestResultReasonsRemainBounded(t *testing.T) {
	t.Parallel()

	var result Result
	for reason := ReasonNoEvidence; reason <= ReasonWithinBaseline; reason++ {
		result.addReason(reason)
	}
	if result.ReasonCount != maxReasons {
		t.Errorf("reason count = %d, want %d", result.ReasonCount, maxReasons)
	}
}

func FuzzEvaluateRemainsBounded(f *testing.F) {
	f.Add(
		uint64(20),
		uint64(20),
		uint64(0),
		uint64(0),
		uint64(0),
		int64(0),
		int64(30*time.Second),
		int64(30*time.Second),
		int64(2*time.Minute),
	)
	f.Add(
		uint64(math.MaxUint64),
		uint64(math.MaxUint64),
		uint64(math.MaxUint64),
		uint64(math.MaxUint64),
		uint64(math.MaxUint64),
		int64(math.MinInt64),
		int64(math.MaxInt64),
		int64(math.MaxInt64),
		int64(math.MaxInt64),
	)

	f.Fuzz(func(
		t *testing.T,
		attempts uint64,
		relevant uint64,
		rateLimited uint64,
		clientFailures uint64,
		dependencyFailures uint64,
		firstTick int64,
		lastTick int64,
		now int64,
		expiresAt int64,
	) {
		result := Evaluate(Snapshot{
			Recent: Window{
				Attempts:           attempts,
				RelevantAttempts:   relevant,
				RateLimited:        rateLimited,
				ClientFailures:     clientFailures,
				DependencyFailures: dependencyFailures,
				LatencySamples:     relevant,
				FirstRelevantTick:  firstTick,
				LastRelevantTick:   lastTick,
				LastTick:           lastTick,
				P95LatencyBucket:   uint8(attempts),
			},
			Historical: Window{
				Attempts:           attempts,
				RelevantAttempts:   relevant,
				DependencyFailures: dependencyFailures,
				LatencySamples:     relevant,
				FirstRelevantTick:  firstTick,
				LastRelevantTick:   lastTick,
				P95LatencyBucket:   uint8(relevant),
			},
			Now:             now,
			RecentExpiresAt: expiresAt,
			EverRelevant:    relevant > 0,
		})
		if result.Readiness > ReadinessStale {
			t.Fatalf("readiness = %d, outside closed set", result.Readiness)
		}
		if result.Class > ClassDegrading {
			t.Fatalf("class = %d, outside closed set", result.Class)
		}
		if result.ReasonCount > maxReasons {
			t.Fatalf("reason count = %d, want at most %d", result.ReasonCount, maxReasons)
		}
		if result.ExpiresAt < result.EvaluatedAt {
			t.Fatalf(
				"expiry %d precedes evaluation %d",
				result.ExpiresAt,
				result.EvaluatedAt,
			)
		}
		if result.Recent.Span < 0 || result.Historical.Span < 0 {
			t.Fatalf(
				"evidence spans = %d and %d, want non-negative",
				result.Recent.Span,
				result.Historical.Span,
			)
		}
	})
}

func BenchmarkEvaluate(b *testing.B) {
	snapshot := Snapshot{
		Now:             int64(30 * time.Second),
		RecentExpiresAt: int64(2 * time.Minute),
		EverRelevant:    true,
		Recent: Window{
			Attempts:           100,
			RelevantAttempts:   100,
			DependencyFailures: 10,
			LatencySamples:     100,
			FirstRelevantTick:  0,
			LastRelevantTick:   int64(30 * time.Second),
			P95LatencyBucket:   7,
		},
		Historical: Window{
			Attempts:           1_000,
			RelevantAttempts:   1_000,
			DependencyFailures: 10,
			LatencySamples:     1_000,
			FirstRelevantTick:  0,
			LastRelevantTick:   int64(20 * time.Minute),
			P95LatencyBucket:   4,
		},
	}

	b.ReportAllocs()
	for range b.N {
		benchmarkResult = Evaluate(snapshot)
	}
}

var benchmarkResult Result

func hasReason(result Result, reason Reason) bool {
	for index := uint8(0); index < result.ReasonCount; index++ {
		if result.Reasons[index] == reason {
			return true
		}
	}

	return false
}
