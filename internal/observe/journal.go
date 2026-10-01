package observe

import (
	"sync"

	"github.com/raj1kshtz/curo/internal/diagnose"
	"github.com/raj1kshtz/curo/internal/policy"
)

const changeCapacity = 256

type change struct {
	key      targetKey
	result   diagnose.Result
	plan     policy.Plan
	sequence uint64
}

// journal retains the most recent candidate changes in a fixed ring. Its lock
// is a leaf: callers may hold a target lock, but journal never takes another.
type journal struct {
	entries [changeCapacity]change
	latest  uint64
	mu      sync.Mutex
}

func (journal *journal) record(
	key targetKey,
	result diagnose.Result,
	plan policy.Plan,
) {
	if journal == nil {
		return
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()

	journal.latest++
	journal.entries[(journal.latest-1)%changeCapacity] = change{
		key:      key,
		result:   result,
		plan:     plan,
		sequence: journal.latest,
	}
}

// snapshot returns retained changes in ascending sequence order.
func (journal *journal) snapshot() []change {
	if journal == nil {
		return nil
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()

	retained := min(journal.latest, changeCapacity)
	if retained == 0 {
		return nil
	}

	first := journal.latest - retained + 1
	changes := make([]change, 0, retained)
	for offset := range retained {
		changes = append(changes, journal.entries[(first+offset-1)%changeCapacity])
	}

	return changes
}
