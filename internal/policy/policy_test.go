package policy

import (
	"testing"

	"github.com/raj1kshtz/curo/internal/diagnose"
)

func TestEvaluateMapsReadyDiagnosesToCandidates(t *testing.T) {
	t.Parallel()

	classes := map[string]struct {
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
	readinessStates := map[string]diagnose.Readiness{
		"cold":    diagnose.ReadinessCold,
		"warming": diagnose.ReadinessWarming,
		"ready":   diagnose.ReadinessReady,
		"stale":   diagnose.ReadinessStale,
	}

	for className, test := range classes {
		for readinessName, readiness := range readinessStates {
			t.Run(className+"/"+readinessName, func(t *testing.T) {
				t.Parallel()

				plan := Evaluate(diagnose.Result{
					EvaluatedAt: 10,
					ExpiresAt:   20,
					Readiness:   readiness,
					Class:       test.class,
				})

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

func TestEvaluateZeroResultHasNoCandidates(t *testing.T) {
	t.Parallel()

	plan := Evaluate(diagnose.Result{})
	if plan != (Plan{Version: Version}) {
		t.Errorf("zero result plan = %+v, want only the policy version", plan)
	}
}

func TestVersionTwoReportsSaturationWithoutCandidates(t *testing.T) {
	t.Parallel()

	if Version != 2 {
		t.Fatalf("Version = %d, want 2", Version)
	}

	plan := Evaluate(diagnose.Result{
		Readiness: diagnose.ReadinessReady,
		Class:     diagnose.ClassSaturation,
	})
	if plan.Candidates != 0 || plan.Reason != ReasonNone {
		t.Errorf("saturation plan = %+v, want no candidates and no policy reason", plan)
	}
}
