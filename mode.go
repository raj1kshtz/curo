package curo

import (
	"fmt"
	"strings"
)

// Mode controls how much authority a Transport has over a request.
//
// A Mode prints and encodes as its name, and UnmarshalText accepts the name in
// any letter case, so flag.TextVar, encoding/json, and other text decoders can
// set a Mode from configuration or an admin control.
type Mode uint32

const (
	// Off delegates requests without collecting evidence or applying actions.
	Off Mode = iota

	// Observe permits evidence collection but does not permit request changes.
	Observe

	// Enforce permits eligible actions within fixed safety bounds: at most one
	// budgeted retry of a replay-safe request whose target has a current Retry
	// candidate, failing requests fast with ErrBreakerOpen while the target's
	// dependency breaker is open, and ending a read request with ErrTimeout
	// when response headers do not arrive within its target's adaptive
	// timeout.
	Enforce
)

// String returns the mode name: "Off", "Observe", or "Enforce". An invalid
// mode returns "Mode(N)", where N is its number.
func (m Mode) String() string {
	switch m {
	case Off:
		return "Off"
	case Observe:
		return "Observe"
	case Enforce:
		return "Enforce"
	default:
		return unknownName("Mode", uint64(m))
	}
}

// MarshalText encodes the mode as its name. It returns an error wrapping
// ErrInvalidMode for an invalid mode.
func (m Mode) MarshalText() ([]byte, error) {
	if !m.valid() {
		return nil, invalidModeError(m)
	}

	return []byte(m.String()), nil
}

// UnmarshalText sets the mode from its name in any letter case, such as "off",
// "Observe", or "ENFORCE". For any other text, it returns an error wrapping
// ErrInvalidMode and leaves the mode unchanged. A nil *Mode also returns an
// error wrapping ErrInvalidMode.
func (m *Mode) UnmarshalText(text []byte) error {
	if m == nil {
		return fmt.Errorf("%w: nil *Mode", ErrInvalidMode)
	}

	for _, mode := range [...]Mode{Off, Observe, Enforce} {
		if strings.EqualFold(string(text), mode.String()) {
			*m = mode
			return nil
		}
	}

	return fmt.Errorf("%w: %q", ErrInvalidMode, text)
}

func (m Mode) valid() bool {
	return m >= Off && m <= Enforce
}
