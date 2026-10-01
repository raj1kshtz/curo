package curo

// Mode controls how much authority a Transport has over a request.
type Mode uint32

const (
	// Off delegates requests without collecting evidence or applying actions.
	Off Mode = iota

	// Observe permits evidence collection but does not permit request changes.
	Observe

	// Enforce permits eligible actions within configured safety bounds.
	Enforce
)

func (m Mode) valid() bool {
	return m >= Off && m <= Enforce
}
