package timeout

import (
	"math"
	"testing"
	"time"
)

func TestBoundsEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		bounds  Bounds
		enabled bool
	}{
		{name: "zero", bounds: Bounds{}},
		{name: "default", bounds: DefaultBounds, enabled: true},
		{name: "equal", bounds: Bounds{Minimum: time.Second, Maximum: time.Second}, enabled: true},
		{name: "zero minimum", bounds: Bounds{Maximum: time.Second}},
		{name: "negative minimum", bounds: Bounds{Minimum: -1, Maximum: time.Second}},
		{name: "inverted", bounds: Bounds{Minimum: 2 * time.Second, Maximum: time.Second}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := test.bounds.Enabled(); got != test.enabled {
				t.Errorf("%+v.Enabled() = %t, want %t", test.bounds, got, test.enabled)
			}
		})
	}
}

func TestSelect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		samples uint64
		slowest time.Duration
		bounds  Bounds
		want    time.Duration
	}{
		{name: "disabled", samples: 1_000, slowest: time.Second},
		{name: "too few samples", samples: MinimumSamples - 1, slowest: time.Second, bounds: DefaultBounds},
		{name: "no slowest", samples: MinimumSamples, bounds: DefaultBounds},
		{name: "negative slowest", samples: MinimumSamples, slowest: -time.Second, bounds: DefaultBounds},
		{name: "floor", samples: MinimumSamples, slowest: 100 * time.Millisecond, bounds: DefaultBounds, want: 2 * time.Second},
		{name: "one second", samples: MinimumSamples, slowest: time.Second, bounds: DefaultBounds, want: 3 * time.Second},
		{name: "two and a half seconds", samples: 500, slowest: 2500 * time.Millisecond, bounds: DefaultBounds, want: 7500 * time.Millisecond},
		{name: "five seconds", samples: 500, slowest: 5 * time.Second, bounds: DefaultBounds, want: 15 * time.Second},
		{name: "at ceiling", samples: 500, slowest: 10 * time.Second, bounds: DefaultBounds, want: 30 * time.Second},
		{name: "above ceiling", samples: 500, slowest: 30 * time.Second, bounds: DefaultBounds, want: 30 * time.Second},
		{name: "overflow", samples: math.MaxUint64, slowest: math.MaxInt64, bounds: DefaultBounds, want: 30 * time.Second},
		{
			name:    "just above a third of the ceiling",
			samples: MinimumSamples,
			slowest: 10*time.Second + 1,
			bounds:  DefaultBounds,
			want:    30 * time.Second,
		},
		{
			name:    "custom bounds",
			samples: MinimumSamples,
			slowest: 50 * time.Millisecond,
			bounds:  Bounds{Minimum: 100 * time.Millisecond, Maximum: time.Second},
			want:    150 * time.Millisecond,
		},
		{
			name:    "fixed bounds",
			samples: MinimumSamples,
			slowest: time.Millisecond,
			bounds:  Bounds{Minimum: 5 * time.Second, Maximum: 5 * time.Second},
			want:    5 * time.Second,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := Select(test.samples, test.slowest, test.bounds)
			if got != test.want {
				t.Errorf(
					"Select(%d, %v, %+v) = %v, want %v",
					test.samples,
					test.slowest,
					test.bounds,
					got,
					test.want,
				)
			}
		})
	}
}

func FuzzSelectStaysWithinBounds(f *testing.F) {
	f.Add(uint64(MinimumSamples), int64(time.Second), int64(2*time.Second), int64(30*time.Second))
	f.Add(uint64(math.MaxUint64), int64(math.MaxInt64), int64(1), int64(math.MaxInt64))
	f.Add(uint64(0), int64(-1), int64(0), int64(0))

	f.Fuzz(func(t *testing.T, samples uint64, slowest, minimum, maximum int64) {
		bounds := Bounds{Minimum: time.Duration(minimum), Maximum: time.Duration(maximum)}
		got := Select(samples, time.Duration(slowest), bounds)
		if got == 0 {
			if bounds.Enabled() && samples >= MinimumSamples && slowest > 0 {
				t.Fatalf("Select(%d, %d, %+v) = 0 with eligible evidence", samples, slowest, bounds)
			}
			return
		}
		if !bounds.Enabled() || samples < MinimumSamples || slowest <= 0 {
			t.Fatalf("Select(%d, %d, %+v) = %v without eligible evidence", samples, slowest, bounds, got)
		}
		if got < bounds.Minimum || got > bounds.Maximum {
			t.Fatalf("Select(%d, %d, %+v) = %v outside bounds", samples, slowest, bounds, got)
		}
		if got < time.Duration(slowest) && got != bounds.Maximum {
			t.Fatalf("Select(%d, %d, %+v) = %v below the slowest latency", samples, slowest, bounds, got)
		}
	})
}
