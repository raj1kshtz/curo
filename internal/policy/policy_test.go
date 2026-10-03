package policy

import (
	"testing"
	"time"

	"github.com/raj1kshtz/curo/internal/diagnose"
	"github.com/raj1kshtz/curo/internal/timeout"
)

var (
	testClasses = map[string]struct {
		class diagnose.Class
		want  Candidate
	}{
		"none":            {class: diagnose.ClassNone},
		"healthy":         {class: diagnose.ClassHealthy},
		"transient":       {class: diagnose.ClassTransient, want: CandidateRetry},
		"dependency down": {class: diagnose.ClassDependencyDown, want: CandidateBreakerOpen},
		"saturation":      {class: diagnose.ClassSaturation},
		"client error":    {class: diagnose.ClassClientError},
		"degrading":       {class: diagnose.ClassDegrading},
		"unknown":         {class: diagnose.Class(255)},
	}
	testReadinessStates = map[string]diagnose.Readiness{
		"cold":    diagnose.ReadinessCold,
		"warming": diagnose.ReadinessWarming,
		"ready":   diagnose.ReadinessReady,
		"stale":   diagnose.ReadinessStale,
	}
)

func TestEvaluateMapsReadyDiagnosesToCandidates(t *testing.T) {
	t.Parallel()

	for className, test := range testClasses {
		for readinessName, readiness := range testReadinessStates {
			t.Run(className+"/"+readinessName, func(t *testing.T) {
				t.Parallel()

				plan := Evaluate(diagnose.Result{
					EvaluatedAt: 10,
					ExpiresAt:   20,
					Readiness:   readiness,
					Class:       test.class,
				}, timeout.DefaultBounds)

				var wantCandidates Candidate
				wantReason := ReasonNone
				if test.want != 0 {
					if readiness == diagnose.ReadinessReady {
						wantCandidates = test.want
					} else {
						wantReason = ReasonReadinessRequired
					}
				}
				if plan.Candidates != wantCandidates {
					t.Errorf("candidates = %d, want %d", plan.Candidates, wantCandidates)
				}
				if plan.Reason != wantReason {
					t.Errorf("reason = %d, want %d", plan.Reason, wantReason)
				}
				if plan.Timeout != 0 {
					t.Errorf("timeout = %v, want 0 without latency evidence", plan.Timeout)
				}
				if plan.Version != Version {
					t.Errorf("version = %d, want %d", plan.Version, Version)
				}
				if plan.ExpiresAt != 20 {
					t.Errorf("expiry = %d, want 20", plan.ExpiresAt)
				}
			})
		}
	}
}

func TestEvaluateSelectsTimeoutIndependentlyOfDiagnosis(t *testing.T) {
	t.Parallel()

	latency := diagnose.Latency{
		Samples: timeout.MinimumSamples,
		Slowest: 250 * time.Millisecond,
	}
	for className, test := range testClasses {
		for readinessName, readiness := range testReadinessStates {
			t.Run(className+"/"+readinessName, func(t *testing.T) {
				t.Parallel()

				result := diagnose.Result{
					ExpiresAt: 20,
					Latency:   latency,
					Readiness: readiness,
					Class:     test.class,
				}
				diagnosis := Evaluate(result, timeout.Bounds{})
				plan := Evaluate(result, timeout.DefaultBounds)

				want := diagnosis
				want.Candidates |= CandidateTimeout
				want.Timeout = timeout.DefaultBounds.Minimum
				if plan != want {
					t.Errorf("plan = %+v, want %+v", plan, want)
				}
				if diagnosis.Candidates&CandidateTimeout != 0 || diagnosis.Timeout != 0 {
					t.Errorf("disabled bounds plan = %+v, want no timeout", diagnosis)
				}
			})
		}
	}
}

func TestEvaluateTimeoutFollowsSelect(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		latency diagnose.Latency
		bounds  timeout.Bounds
		want    time.Duration
	}{
		"too few samples": {
			latency: diagnose.Latency{
				Samples: timeout.MinimumSamples - 1,
				Slowest: time.Second,
			},
			bounds: timeout.DefaultBounds,
		},
		"disabled": {
			latency: diagnose.Latency{
				Samples: timeout.MinimumSamples,
				Slowest: time.Second,
			},
		},
		"three times the slowest bucket": {
			latency: diagnose.Latency{
				Samples: timeout.MinimumSamples,
				Slowest: 2500 * time.Millisecond,
			},
			bounds: timeout.DefaultBounds,
			want:   7500 * time.Millisecond,
		},
		"ceiling": {
			latency: diagnose.Latency{
				Samples: timeout.MinimumSamples,
				Slowest: time.Duration(1<<63 - 1),
			},
			bounds: timeout.DefaultBounds,
			want:   timeout.DefaultBounds.Maximum,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			plan := Evaluate(diagnose.Result{Latency: test.latency}, test.bounds)
			if plan.Timeout != test.want {
				t.Errorf("timeout = %v, want %v", plan.Timeout, test.want)
			}
			if selected := plan.Candidates&CandidateTimeout != 0; selected != (test.want > 0) {
				t.Errorf("CandidateTimeout selected = %t, want %t", selected, test.want > 0)
			}
		})
	}
}

func TestEvaluateZeroResultHasNoCandidates(t *testing.T) {
	t.Parallel()

	plan := Evaluate(diagnose.Result{}, timeout.DefaultBounds)
	if plan != (Plan{Version: Version}) {
		t.Errorf("zero result plan = %+v, want only the policy version", plan)
	}
}

func TestVersionThreeKeepsSaturationWithoutCandidates(t *testing.T) {
	t.Parallel()

	if Version != 3 {
		t.Fatalf("Version = %d, want 3", Version)
	}

	plan := Evaluate(diagnose.Result{
		Readiness: diagnose.ReadinessReady,
		Class:     diagnose.ClassSaturation,
	}, timeout.DefaultBounds)
	if plan.Candidates != 0 || plan.Reason != ReasonNone {
		t.Errorf("saturation plan = %+v, want no candidates and no policy reason", plan)
	}
}

func FuzzEvaluateTimeoutStaysWithinBounds(f *testing.F) {
	f.Add(uint64(100), int64(250*time.Millisecond), int64(2*time.Second), int64(30*time.Second), uint8(2), uint8(2))
	f.Add(uint64(99), int64(time.Second), int64(time.Second), int64(time.Second), uint8(3), uint8(1))
	f.Add(uint64(1<<64-1), int64(1<<63-1), int64(1), int64(1<<63-1), uint8(255), uint8(255))
	f.Add(uint64(500), int64(-1), int64(0), int64(0), uint8(0), uint8(3))

	f.Fuzz(func(
		t *testing.T,
		samples uint64,
		slowest, minimum, maximum int64,
		class, readiness uint8,
	) {
		bounds := timeout.Bounds{
			Minimum: time.Duration(minimum),
			Maximum: time.Duration(maximum),
		}
		result := diagnose.Result{
			Latency: diagnose.Latency{
				Samples: samples,
				Slowest: time.Duration(slowest),
			},
			Readiness: diagnose.Readiness(readiness),
			Class:     diagnose.Class(class),
		}

		plan := Evaluate(result, bounds)
		diagnosis := Evaluate(result, timeout.Bounds{})
		if selected := plan.Candidates&CandidateTimeout != 0; selected != (plan.Timeout > 0) {
			t.Fatalf("plan %+v: CandidateTimeout selected = %t with timeout %v", plan, selected, plan.Timeout)
		}
		if plan.Timeout > 0 &&
			(!bounds.Enabled() || plan.Timeout < bounds.Minimum || plan.Timeout > bounds.Maximum) {
			t.Fatalf("timeout %v outside bounds %+v", plan.Timeout, bounds)
		}
		if plan.Candidates&^CandidateTimeout != diagnosis.Candidates || plan.Reason != diagnosis.Reason {
			t.Fatalf("plan %+v changed the diagnosis candidates of %+v", plan, diagnosis)
		}
	})
}
