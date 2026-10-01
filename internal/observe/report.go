package observe

import (
	"time"

	"github.com/raj1kshtz/curo/internal/diagnose"
	"github.com/raj1kshtz/curo/internal/policy"
)

// Report is a detached copy of published decisions and retained changes.
type Report struct {
	Targets []Decision
	Changes []Change
}

// Decision is a detached copy of one published target evaluation. Times are
// zero when the target has not been evaluated.
type Decision struct {
	EvaluatedAt time.Time
	ExpiresAt   time.Time
	Identity    Identity
	Result      diagnose.Result
	Plan        policy.Plan
}

// Change is one retained candidate change.
type Change struct {
	Decision Decision
	Sequence uint64
}

// Report returns retained changes and the latest published decision for each
// tracked regular target.
//
// Report never reads the clock or evaluates evidence. Changes are copied
// before targets, so a target decision is never older than a retained change
// recorded by the same target.
func (observer *Observer) Report() Report {
	if observer == nil {
		return Report{}
	}

	changes := observer.changes.snapshot()
	targets := observer.registry.regularTargets()

	var report Report
	if len(changes) > 0 {
		report.Changes = make([]Change, 0, len(changes))
	}
	for _, entry := range changes {
		report.Changes = append(report.Changes, Change{
			Decision: observer.decision(entry.key, publication{
				result:    entry.result,
				plan:      entry.plan,
				evaluated: true,
			}),
			Sequence: entry.sequence,
		})
	}

	if len(targets) > 0 {
		report.Targets = make([]Decision, 0, len(targets))
	}
	for _, state := range targets {
		published, live := state.published()
		if !live {
			continue
		}

		report.Targets = append(report.Targets, observer.decision(state.key, published))
	}

	return report
}

func (observer *Observer) decision(
	key targetKey,
	published publication,
) Decision {
	decision := Decision{
		Identity: key.identity(),
		Result:   published.result,
		Plan:     published.plan,
	}
	if published.evaluated {
		decision.EvaluatedAt = observer.at(published.result.EvaluatedAt)
		decision.ExpiresAt = observer.at(published.result.ExpiresAt)
	}

	return decision
}

func (observer *Observer) at(tick int64) time.Time {
	return observer.origin.Add(time.Duration(tick))
}
