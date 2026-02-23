package epoch

import (
	"testing"

	"runtime/race/kolkov/vectorclock"
)

// TestNewEpoch tests epoch creation and encoding.
func TestNewEpoch(t *testing.T) {
	tests := []struct {
		name      string
		tid       uint16
		clock     uint64
		wantEpoch uint64
	}{
		{
			name:      "zero epoch",
			tid:       0,
			clock:     0,
			wantEpoch: 0x0000000000000000,
		},
		{
			name:      "tid only",
			tid:       5,
			clock:     0,
			wantEpoch: 0x0005000000000000,
		},
		{
			name:      "clock only",
			tid:       0,
			clock:     0x1234,
			wantEpoch: 0x0000000000001234,
		},
		{
			name:      "tid and clock",
			tid:       42,
			clock:     0x123456,
			wantEpoch: 0x002A000000123456,
		},
		{
			name:      "max tid (65535)",
			tid:       65535,
			clock:     0,
			wantEpoch: 0xFFFF000000000000,
		},
		{
			name:      "max clock (48-bit)",
			tid:       0,
			clock:     0x0000FFFFFFFFFFFF,
			wantEpoch: 0x0000FFFFFFFFFFFF,
		},
		{
			name:      "max tid and max clock",
			tid:       65535,
			clock:     0x0000FFFFFFFFFFFF,
			wantEpoch: 0xFFFFFFFFFFFFFFFF,
		},
		{
			name:      "clock overflow (truncation)",
			tid:       1,
			clock:     0xFFFFFFFFFFFFFFFF, // Beyond 48 bits
			wantEpoch: 0x0001FFFFFFFFFFFF, // Should truncate to 48 bits
		},
		{
			name:      "large tid (1000 goroutines)",
			tid:       1000,
			clock:     500000,
			wantEpoch: 0x03E800000007A120,
		},
		{
			name:      "very large clock (1 billion operations)",
			tid:       10,
			clock:     1000000000,
			wantEpoch: 0x000A00003B9ACA00,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewEpoch(tt.tid, tt.clock)
			if uint64(got) != tt.wantEpoch {
				t.Errorf("NewEpoch(%d, 0x%X) = 0x%X, want 0x%X",
					tt.tid, tt.clock, uint64(got), tt.wantEpoch)
			}
		})
	}
}

// TestEpochDecode tests epoch decoding.
func TestEpochDecode(t *testing.T) {
	tests := []struct {
		name      string
		epoch     Epoch
		wantTID   uint16
		wantClock uint64
	}{
		{
			name:      "zero epoch",
			epoch:     0x0000000000000000,
			wantTID:   0,
			wantClock: 0,
		},
		{
			name:      "tid only",
			epoch:     0x0005000000000000,
			wantTID:   5,
			wantClock: 0,
		},
		{
			name:      "clock only",
			epoch:     0x0000000000001234,
			wantTID:   0,
			wantClock: 0x1234,
		},
		{
			name:      "tid and clock",
			epoch:     0x002A000000123456,
			wantTID:   42,
			wantClock: 0x123456,
		},
		{
			name:      "max tid (65535)",
			epoch:     0xFFFF000000000000,
			wantTID:   65535,
			wantClock: 0,
		},
		{
			name:      "max clock (48-bit)",
			epoch:     0x0000FFFFFFFFFFFF,
			wantTID:   0,
			wantClock: 0x0000FFFFFFFFFFFF,
		},
		{
			name:      "max epoch",
			epoch:     0xFFFFFFFFFFFFFFFF,
			wantTID:   65535,
			wantClock: 0x0000FFFFFFFFFFFF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTID, gotClock := tt.epoch.Decode()
			if gotTID != tt.wantTID {
				t.Errorf("Epoch(0x%X).Decode() tid = %d, want %d",
					tt.epoch, gotTID, tt.wantTID)
			}
			if gotClock != tt.wantClock {
				t.Errorf("Epoch(0x%X).Decode() clock = 0x%X, want 0x%X",
					tt.epoch, gotClock, tt.wantClock)
			}
		})
	}
}

// TestEpochRoundTrip tests that NewEpoch and Decode are inverse operations.
func TestEpochRoundTrip(t *testing.T) {
	tests := []struct {
		tid   uint16
		clock uint64
	}{
		{0, 0},
		{1, 100},
		{42, 0x123456},
		{65535, 0x0000FFFFFFFFFFFF},
		{32768, 0x800000000},
		{1000, 1000000000}, // 1000 goroutines, 1 billion operations
	}

	for _, tt := range tests {
		t.Run("roundtrip", func(t *testing.T) {
			epoch := NewEpoch(tt.tid, tt.clock)
			gotTID, gotClock := epoch.Decode()

			// Mask clock to 48 bits for comparison (handles overflow)
			wantClock := tt.clock & ClockMask

			if gotTID != tt.tid {
				t.Errorf("Round-trip TID: got %d, want %d", gotTID, tt.tid)
			}
			if gotClock != wantClock {
				t.Errorf("Round-trip clock: got 0x%X, want 0x%X", gotClock, wantClock)
			}
		})
	}
}

// TestEpochHappensBefore tests the critical happens-before check.
func TestEpochHappensBefore(t *testing.T) {
	tests := []struct {
		name  string
		epoch Epoch
		setup func() *vectorclock.VectorClock
		want  bool
	}{
		{
			name:  "epoch happens-before (clock <)",
			epoch: NewEpoch(3, 42),
			setup: func() *vectorclock.VectorClock {
				vc := vectorclock.New()
				vc.Set(0, 100)
				vc.Set(1, 100)
				vc.Set(2, 100)
				vc.Set(3, 45) // epoch's tid=3, clock=42 < 45
				return vc
			},
			want: true,
		},
		{
			name:  "epoch happens-before (clock ==)",
			epoch: NewEpoch(3, 42),
			setup: func() *vectorclock.VectorClock {
				vc := vectorclock.New()
				vc.Set(0, 100)
				vc.Set(1, 100)
				vc.Set(2, 100)
				vc.Set(3, 42) // epoch's tid=3, clock=42 == 42
				return vc
			},
			want: true,
		},
		{
			name:  "epoch NOT happens-before (clock >)",
			epoch: NewEpoch(3, 42),
			setup: func() *vectorclock.VectorClock {
				vc := vectorclock.New()
				vc.Set(0, 100)
				vc.Set(1, 100)
				vc.Set(2, 100)
				vc.Set(3, 41) // epoch's tid=3, clock=42 > 41
				return vc
			},
			want: false,
		},
		{
			name:  "zero epoch happens-before",
			epoch: NewEpoch(0, 0),
			setup: func() *vectorclock.VectorClock {
				vc := vectorclock.New()
				vc.Set(0, 10)
				return vc
			},
			want: true,
		},
		{
			name:  "max tid happens-before",
			epoch: NewEpoch(65535, 100),
			setup: func() *vectorclock.VectorClock {
				vc := vectorclock.New()
				vc.Set(65535, 200)
				return vc
			},
			want: true,
		},
		{
			name:  "max tid NOT happens-before",
			epoch: NewEpoch(65535, 100),
			setup: func() *vectorclock.VectorClock {
				vc := vectorclock.New()
				vc.Set(65535, 99)
				return vc
			},
			want: false,
		},
		{
			name:  "large tid (1000 goroutines)",
			epoch: NewEpoch(1000, 50000),
			setup: func() *vectorclock.VectorClock {
				vc := vectorclock.New()
				vc.Set(1000, 100000)
				return vc
			},
			want: true,
		},
		{
			name:  "epoch with uninitialized vc entry",
			epoch: NewEpoch(5, 0),
			setup: vectorclock.New, // All zeros
			want:  true,            // 0 <= 0
		},
		{
			name:  "epoch with uninitialized vc entry (non-zero clock)",
			epoch: NewEpoch(5, 1),
			setup: vectorclock.New, // All zeros
			want:  false,           // 1 > 0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := tt.setup()
			got := tt.epoch.HappensBefore(vc)
			if got != tt.want {
				tid, clock := tt.epoch.Decode()
				t.Errorf("Epoch(%d@%d).HappensBefore(vc[%d]=%d) = %v, want %v",
					clock, tid, tid, vc.Get(tid), got, tt.want)
			}
		})
	}
}

// TestEpochSame tests the same-epoch optimization check.
func TestEpochSame(t *testing.T) {
	tests := []struct {
		name string
		e1   Epoch
		e2   Epoch
		want bool
	}{
		{
			name: "identical epochs",
			e1:   NewEpoch(5, 100),
			e2:   NewEpoch(5, 100),
			want: true,
		},
		{
			name: "different tid",
			e1:   NewEpoch(5, 100),
			e2:   NewEpoch(6, 100),
			want: false,
		},
		{
			name: "different clock",
			e1:   NewEpoch(5, 100),
			e2:   NewEpoch(5, 101),
			want: false,
		},
		{
			name: "both zero",
			e1:   NewEpoch(0, 0),
			e2:   NewEpoch(0, 0),
			want: true,
		},
		{
			name: "max epochs identical",
			e1:   NewEpoch(65535, 0x0000FFFFFFFFFFFF),
			e2:   NewEpoch(65535, 0x0000FFFFFFFFFFFF),
			want: true,
		},
		{
			name: "completely different",
			e1:   NewEpoch(1, 100),
			e2:   NewEpoch(200, 50000),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.e1.Same(tt.e2)
			if got != tt.want {
				t.Errorf("Epoch(0x%X).Same(0x%X) = %v, want %v",
					tt.e1, tt.e2, got, tt.want)
			}

			// Test symmetry
			gotReverse := tt.e2.Same(tt.e1)
			if gotReverse != tt.want {
				t.Errorf("Epoch(0x%X).Same(0x%X) = %v, want %v (symmetry check)",
					tt.e2, tt.e1, gotReverse, tt.want)
			}
		})
	}
}

// BenchmarkEpochHappensBefore benchmarks the critical happens-before check.
func BenchmarkEpochHappensBefore(b *testing.B) {
	epoch := NewEpoch(42, 1000)
	vc := vectorclock.New()
	vc.Set(42, 2000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = epoch.HappensBefore(vc)
	}
}

// BenchmarkEpochDecode benchmarks epoch decoding.
func BenchmarkEpochDecode(b *testing.B) {
	epoch := NewEpoch(42, 0x123456)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = epoch.Decode()
	}
}

// BenchmarkNewEpoch benchmarks epoch creation.
func BenchmarkNewEpoch(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewEpoch(42, uint64(i)&ClockMask)
	}
}

// BenchmarkEpochSame benchmarks the same-epoch check.
func BenchmarkEpochSame(b *testing.B) {
	e1 := NewEpoch(42, 1000)
	e2 := NewEpoch(42, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = e1.Same(e2)
	}
}

// TestEpochString tests the String() method for debugging output.
func TestEpochString(t *testing.T) {
	tests := []struct {
		name  string
		epoch Epoch
		want  string
	}{
		{
			name:  "zero epoch",
			epoch: NewEpoch(0, 0),
			want:  "0@0",
		},
		{
			name:  "simple epoch",
			epoch: NewEpoch(5, 42),
			want:  "42@5",
		},
		{
			name:  "large clock",
			epoch: NewEpoch(3, 123456),
			want:  "123456@3",
		},
		{
			name:  "max tid",
			epoch: NewEpoch(65535, 100),
			want:  "100@65535",
		},
		{
			name:  "max clock (48-bit)",
			epoch: NewEpoch(1, 0x0000FFFFFFFFFFFF),
			want:  "281474976710655@1",
		},
		{
			name:  "large tid (1000 goroutines)",
			epoch: NewEpoch(1000, 500000),
			want:  "500000@1000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.epoch.String()
			if got != tt.want {
				t.Errorf("Epoch(0x%X).String() = %q, want %q",
					tt.epoch, got, tt.want)
			}
		})
	}
}

// TestTIDOverflow tests TID overflow detection and clamping (v0.2.0 Task 5).
func TestTIDOverflow(t *testing.T) {
	// Reset flags before test.
	ResetOverflowFlags()

	// Since TID is uint16, we cannot actually pass a value > MaxTID (65535).
	// This test verifies that values at MaxTID work correctly without overflow.
	e := NewEpoch(uint16(MaxTID), 1000)

	// Should NOT trigger overflow (MaxTID is valid).
	tidOverflow, _, _, _ := CheckOverflows() //nolint:dogsled // Only checking tidOverflow in this test
	if tidOverflow {
		t.Errorf("TID at MaxTID (%d) should NOT trigger overflow", MaxTID)
	}

	// Verify epoch is correctly created.
	tid, clock := e.Decode()
	if uint32(tid) != MaxTID {
		t.Errorf("NewEpoch(MaxTID, 1000).TID() = %d, want %d", tid, MaxTID)
	}
	if clock != 1000 {
		t.Errorf("NewEpoch(MaxTID, 1000).Clock() = %d, want 1000", clock)
	}
}

// TestClockOverflow tests clock overflow detection and clamping (v0.2.0 Task 5).
func TestClockOverflow(t *testing.T) {
	// Reset flags before test.
	ResetOverflowFlags()

	// Trigger clock overflow by passing MaxClock + 1.
	e := NewEpoch(1, MaxClock+1)

	// Check overflow flag is set.
	_, clockOverflow, _, _ := CheckOverflows() //nolint:dogsled // Only checking clockOverflow in this test
	if !clockOverflow {
		t.Errorf("Clock overflow should be detected for clock > MaxClock (%d)", MaxClock)
	}

	// Clock should be clamped to MaxClock (not wrapped to 0).
	_, clock := e.Decode()
	if clock != MaxClock {
		t.Errorf("NewEpoch(1, MaxClock+1).Clock() = %d, want %d (clamped)", clock, MaxClock)
	}
}

// TestTIDWarning verifies that NewEpoch does NOT set tidNearOverflow for high TID values.
//
// With FIFO TID recycling, high TID values (e.g., 60000) are NORMAL — TIDs cycle
// sequentially through all 65535 values before reuse. A TID value above MaxTIDWarning
// means nothing about pool exhaustion.
//
// Pool depletion warning is now handled in allocTID() when len(freeTIDs) < 100.
func TestTIDWarning(t *testing.T) {
	// Reset flags before test.
	ResetOverflowFlags()

	// Create epoch with TID above the old 90% threshold.
	// With FIFO recycling, this is a NORMAL TID value — should NOT set tidNearOverflow.
	_ = NewEpoch(uint16(MaxTIDWarning+1), 1000)

	// tidNearOverflow must NOT be set by NewEpoch — that was the false-warning bug.
	_, _, tidWarning, _ := CheckOverflows()
	if tidWarning {
		t.Errorf("NewEpoch must NOT set tidNearOverflow for high TID values (FIFO recycling makes high TIDs normal)")
	}

	// Confirm epoch was created correctly with the high TID value.
	e := NewEpoch(uint16(MaxTIDWarning+1), 1000)
	tid, clock := e.Decode()
	//nolint:gosec // G115: Safe conversion for test comparison.
	if uint32(tid) != MaxTIDWarning+1 {
		t.Errorf("Epoch TID = %d, want %d", tid, MaxTIDWarning+1)
	}
	if clock != 1000 {
		t.Errorf("Epoch clock = %d, want 1000", clock)
	}
}

// TestClockWarning tests clock warning threshold (90% of max) (v0.2.0 Task 5).
func TestClockWarning(t *testing.T) {
	// Reset flags before test.
	ResetOverflowFlags()

	// Trigger warning threshold (90% of MaxClock).
	_ = NewEpoch(1, MaxClockWarning+1)

	// Check warning flag is set.
	_, clockOverflow, _, clockWarning := CheckOverflows()
	if !clockWarning {
		t.Errorf("Clock warning should trigger at 90%% threshold (%d)", MaxClockWarning)
	}

	// Should NOT have triggered overflow yet.
	if clockOverflow {
		t.Errorf("Clock overflow should NOT trigger at warning threshold")
	}
}

// TestResetOverflowFlags tests that overflow flags can be reset (v0.2.0 Task 5).
func TestResetOverflowFlags(t *testing.T) {
	// Set flags by triggering clock overflows (clock warning and clock overflow).
	_ = NewEpoch(1, MaxClock+1)      // Clock overflow
	_ = NewEpoch(1, MaxClockWarning+1) // Clock warning

	// Verify clock flags are set.
	_, clockOverflow, _, clockWarning := CheckOverflows()
	if !clockOverflow {
		t.Errorf("Clock overflow should be set before reset")
	}
	if !clockWarning {
		t.Errorf("Clock warning should be set before reset")
	}

	// Set tidNearOverflow manually (simulating pool depletion warning from allocTID).
	tidNearOverflow.Store(1)

	_, _, tidWarning, _ := CheckOverflows()
	if !tidWarning {
		t.Errorf("TID warning should be set after manual store")
	}

	// Reset flags.
	ResetOverflowFlags()

	// Verify all flags are cleared.
	tidOverflow, clockOverflow, tidWarning, clockWarning := CheckOverflows()
	if tidOverflow || clockOverflow || tidWarning || clockWarning {
		t.Errorf("After reset, all flags should be false, got: tidOverflow=%v, clockOverflow=%v, tidWarning=%v, clockWarning=%v",
			tidOverflow, clockOverflow, tidWarning, clockWarning)
	}
}

// TestOverflowConstants verifies overflow detection constants are correct (v0.2.0 Task 5).
func TestOverflowConstants(t *testing.T) {
	// Verify MaxTID calculation.
	expectedMaxTID := uint32((1 << TIDBits) - 1) // 65,535
	if MaxTID != expectedMaxTID {
		t.Errorf("MaxTID = %d, want %d", MaxTID, expectedMaxTID)
	}

	// Verify MaxClock calculation.
	expectedMaxClock := uint64((1 << ClockBits) - 1) // 281,474,976,710,655
	if MaxClock != expectedMaxClock {
		t.Errorf("MaxClock = %d, want %d", MaxClock, expectedMaxClock)
	}

	// Verify MaxTIDWarning is 90% of MaxTID.
	expectedMaxTIDWarning := uint32((1 << TIDBits) * 9 / 10) // 58,982
	if MaxTIDWarning != expectedMaxTIDWarning {
		t.Errorf("MaxTIDWarning = %d, want %d (90%% of MaxTID)", MaxTIDWarning, expectedMaxTIDWarning)
	}

	// Verify MaxClockWarning is 90% of MaxClock.
	expectedMaxClockWarning := uint64((1 << ClockBits) * 9 / 10) // 253,327,479,039,589
	if MaxClockWarning != expectedMaxClockWarning {
		t.Errorf("MaxClockWarning = %d, want %d (90%% of MaxClock)", MaxClockWarning, expectedMaxClockWarning)
	}

	// Verify warning thresholds are less than max values.
	if MaxTIDWarning >= MaxTID {
		t.Errorf("MaxTIDWarning (%d) should be less than MaxTID (%d)", MaxTIDWarning, MaxTID)
	}
	if MaxClockWarning >= MaxClock {
		t.Errorf("MaxClockWarning (%d) should be less than MaxClock (%d)", MaxClockWarning, MaxClock)
	}
}
