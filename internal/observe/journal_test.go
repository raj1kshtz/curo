package observe

import (
	"testing"
	"time"

	"github.com/raj1kshtz/curo/internal/diagnose"
	"github.com/raj1kshtz/curo/internal/policy"
)

func TestJournalRetainsNewestChangesInSequenceOrder(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		records   int
		wantFirst uint64
		wantCount int
	}{
		"empty":   {},
		"partial": {records: 3, wantFirst: 1, wantCount: 3},
		"full": {
			records:   changeCapacity,
			wantFirst: 1,
			wantCount: changeCapacity,
		},
		"wrapped": {
			records:   changeCapacity + 1,
			wantFirst: 2,
			wantCount: changeCapacity,
		},
		"wrapped twice": {
			records:   2*changeCapacity + 5,
			wantFirst: changeCapacity + 6,
			wantCount: changeCapacity,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			changes := &journal{}
			for index := range test.records {
				changes.record(
					testTargetKey("api.example"),
					diagnose.Result{EvaluatedAt: int64(index)},
					policy.Plan{Version: policy.Version},
				)
			}

			got := changes.snapshot()
			if len(got) != test.wantCount {
				t.Fatalf("retained changes = %d, want %d", len(got), test.wantCount)
			}
			for index, entry := range got {
				wantSequence := test.wantFirst + uint64(index)
				if entry.sequence != wantSequence {
					t.Fatalf(
						"change %d sequence = %d, want %d",
						index,
						entry.sequence,
						wantSequence,
					)
				}
				if entry.result.EvaluatedAt != int64(wantSequence-1) {
					t.Fatalf(
						"change %d evaluation = %d, want %d",
						index,
						entry.result.EvaluatedAt,
						wantSequence-1,
					)
				}
			}
		})
	}
}

func TestJournalSnapshotIsDetached(t *testing.T) {
	t.Parallel()

	changes := &journal{}
	changes.record(testTargetKey("api.example"), diagnose.Result{}, policy.Plan{})

	first := changes.snapshot()
	first[0].sequence = 99
	first[0].key.host = "mutated.example"

	second := changes.snapshot()
	if second[0].sequence != 1 || second[0].key.host != "api.example" {
		t.Errorf("snapshot mutation reached journal: %+v", second[0])
	}
}

func TestNilJournalIsInert(t *testing.T) {
	t.Parallel()

	var changes *journal
	changes.record(testTargetKey("api.example"), diagnose.Result{}, policy.Plan{})
	if got := changes.snapshot(); got != nil {
		t.Errorf("nil journal snapshot = %v, want nil", got)
	}
}

func TestDiagnosisRecordsOnlyCandidateChanges(t *testing.T) {
	t.Parallel()

	changes := &journal{}
	state := newTarget(0, true)
	state.key = testTargetKey("api.example")
	state.changes = changes

	failure := observation{
		outcome: outcomeHTTPFailure,
		signal:  diagnosisDependencyFailure,
	}
	for index := range 20 {
		recordObservation(t, state, time.Duration(index)*2*time.Second, failure)
	}

	warming, live := state.published()
	if !live {
		t.Fatal("tracked target was not live")
	}
	if warming.result.Readiness != diagnose.ReadinessWarming ||
		warming.result.Class != diagnose.ClassDependencyDown {
		t.Fatalf(
			"warming decision = %v/%v, want Warming/DependencyDown",
			warming.result.Readiness,
			warming.result.Class,
		)
	}
	if warming.plan.Candidates != 0 ||
		warming.plan.Reason != policy.ReasonReadinessRequired {
		t.Fatalf("warming plan = %+v, want readiness-gated empty plan", warming.plan)
	}
	if got := changes.snapshot(); len(got) != 0 {
		t.Fatalf("readiness-gated changes = %d, want 0", len(got))
	}

	ready := state.diagnosisAt(int64(40 * time.Second))
	if ready.Readiness != diagnose.ReadinessReady ||
		ready.Class != diagnose.ClassDependencyDown {
		t.Fatalf(
			"ready decision = %v/%v, want Ready/DependencyDown",
			ready.Readiness,
			ready.Class,
		)
	}
	assertChange(t, changes.snapshot(), 1, policy.CandidateBreakerOpen, diagnose.ClassDependencyDown)

	renewed := state.diagnosisAt(int64(50 * time.Second))
	if renewed.EvaluatedAt != int64(50*time.Second) {
		t.Fatalf("renewal evaluation = %v, want 50s", time.Duration(renewed.EvaluatedAt))
	}
	if got := changes.snapshot(); len(got) != 1 {
		t.Fatalf("changes after renewal = %d, want 1", len(got))
	}

	rateLimited := observation{
		outcome: outcomeHTTPFailure,
		signal:  diagnosisRateLimited,
	}
	for index := range 6 {
		recordObservation(t, state, 51*time.Second+time.Duration(index)*time.Second, rateLimited)
	}
	saturated := state.diagnosisAt(int64(60 * time.Second))
	if saturated.Class != diagnose.ClassSaturation {
		t.Fatalf("class after rate limiting = %v, want Saturation", saturated.Class)
	}
	assertChange(t, changes.snapshot(), 2, 0, diagnose.ClassSaturation)

	recordObservation(t, state, 10*time.Minute, observation{
		outcome: outcomeSuccess,
		signal:  diagnosisNeutral,
	})
	if got := changes.snapshot(); len(got) != 2 {
		t.Fatalf("changes after clearing an empty plan = %d, want 2", len(got))
	}

	retained := changes.snapshot()
	if retained[1].result.Readiness != diagnose.ReadinessReady ||
		retained[1].plan.Reason != policy.ReasonNone {
		t.Errorf(
			"saturation change = %v/%v, want Ready with no policy reason",
			retained[1].result.Readiness,
			retained[1].plan.Reason,
		)
	}
	for index, entry := range retained {
		if entry.key != state.key {
			t.Errorf("change %d key = %+v, want %+v", index, entry.key, state.key)
		}
	}
}

func TestRetiredTargetStopsEvaluatingAndRecordingChanges(t *testing.T) {
	t.Parallel()

	const idleTTL = 10 * time.Second

	changes := &journal{}
	registry := newRegistry(1, 1, idleTTL, changes)
	oldKey := testTargetKey("old.example")
	retired, _ := registry.get(oldKey, 0)
	if retired.key != oldKey || retired.changes != changes {
		t.Fatal("admitted target lacks its key or journal")
	}

	replacement, overflow := registry.get(testTargetKey("new.example"), int64(idleTTL))
	if overflow || replacement == retired {
		t.Fatal("expired target was not replaced")
	}
	if replacement.changes != changes {
		t.Fatal("replacement target lacks the journal")
	}
	if _, live := retired.published(); live {
		t.Fatal("retired target still publishes decisions")
	}

	failure := observation{
		outcome: outcomeHTTPFailure,
		signal:  diagnosisDependencyFailure,
	}
	for index := range 25 {
		recordObservation(
			t,
			retired,
			idleTTL+time.Duration(index)*2*time.Second,
			failure,
		)
	}

	if got := retired.diagnosisAt(int64(2 * time.Minute)); got != (diagnose.Result{}) {
		t.Errorf("retired diagnosis = %+v, want zero", got)
	}
	retired.mu.Lock()
	evaluated := retired.diagnosis.initialized
	retired.mu.Unlock()
	if evaluated {
		t.Error("retired target evaluated a diagnosis")
	}
	if got := changes.snapshot(); len(got) != 0 {
		t.Errorf("retired target changes = %d, want 0", len(got))
	}

	regular := registry.regularTargets()
	if len(regular) != 1 || regular[0] != replacement {
		t.Errorf("regular targets = %v, want only the replacement", regular)
	}
}

func recordObservation(
	t *testing.T,
	state *target,
	at time.Duration,
	value observation,
) {
	t.Helper()

	if !state.record(int64(at), value) {
		t.Fatalf("observation at %v was rejected", at)
	}
}

func assertChange(
	t *testing.T,
	changes []change,
	wantCount int,
	wantCandidates policy.Candidate,
	wantClass diagnose.Class,
) {
	t.Helper()

	if len(changes) != wantCount {
		t.Fatalf("changes = %d, want %d", len(changes), wantCount)
	}

	latest := changes[len(changes)-1]
	if latest.sequence != uint64(wantCount) {
		t.Errorf("latest sequence = %d, want %d", latest.sequence, wantCount)
	}
	if latest.plan.Candidates != wantCandidates {
		t.Errorf(
			"latest candidates = %d, want %d",
			latest.plan.Candidates,
			wantCandidates,
		)
	}
	if latest.result.Class != wantClass {
		t.Errorf("latest class = %v, want %v", latest.result.Class, wantClass)
	}
	if latest.plan.Version != policy.Version {
		t.Errorf("latest policy version = %d, want %d", latest.plan.Version, policy.Version)
	}
}
