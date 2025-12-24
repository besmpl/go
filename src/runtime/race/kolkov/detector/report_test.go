package detector

import (
	"bytes"
	"strings"
	"testing"

	"runtime/race/kolkov/epoch"
)

// TestAccessType_String tests the String() method of AccessType.
func TestAccessType_String(t *testing.T) {
	tests := []struct {
		name     string
		access   AccessType
		expected string
	}{
		{"Read", AccessRead, "Read"},
		{"Write", AccessWrite, "Write"},
		{"Unknown", AccessType(999), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.access.String()
			if got != tt.expected {
				t.Errorf("AccessType.String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// TestNewRaceReport tests the constructor for RaceReport.
//
//nolint:gocognit // Test function naturally complex due to table-driven test cases
func TestNewRaceReport(t *testing.T) {
	addr := uintptr(0x12345678)
	prevEpoch := epoch.NewEpoch(1, 10) // tid=1, clock=10
	currEpoch := epoch.NewEpoch(2, 20) // tid=2, clock=20

	tests := []struct {
		name             string
		raceType         string
		expectedCurrType AccessType
		expectedPrevType AccessType
		expectedCurrGID  uint32
		expectedPrevGID  uint32
	}{
		{
			name:             "write-write",
			raceType:         "write-write",
			expectedCurrType: AccessWrite,
			expectedPrevType: AccessWrite,
			expectedCurrGID:  2,
			expectedPrevGID:  1,
		},
		{
			name:             "read-write",
			raceType:         "read-write",
			expectedCurrType: AccessWrite,
			expectedPrevType: AccessRead,
			expectedCurrGID:  2,
			expectedPrevGID:  1,
		},
		{
			name:             "write-read",
			raceType:         "write-read",
			expectedCurrType: AccessRead,
			expectedPrevType: AccessWrite,
			expectedCurrGID:  2,
			expectedPrevGID:  1,
		},
		{
			name:             "unknown",
			raceType:         "unknown-type",
			expectedCurrType: AccessWrite,
			expectedPrevType: AccessWrite,
			expectedCurrGID:  2,
			expectedPrevGID:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := NewRaceReport(tt.raceType, addr, prevEpoch, currEpoch)

			// Check current access.
			if report.Current.Type != tt.expectedCurrType {
				t.Errorf("Current.Type = %v, want %v", report.Current.Type, tt.expectedCurrType)
			}
			if report.Current.Addr != addr {
				t.Errorf("Current.Addr = 0x%x, want 0x%x", report.Current.Addr, addr)
			}
			if report.Current.GoroutineID != tt.expectedCurrGID {
				t.Errorf("Current.GoroutineID = %d, want %d", report.Current.GoroutineID, tt.expectedCurrGID)
			}
			if report.Current.Epoch != currEpoch {
				t.Errorf("Current.Epoch = %v, want %v", report.Current.Epoch, currEpoch)
			}

			// Check previous access.
			if report.Previous.Type != tt.expectedPrevType {
				t.Errorf("Previous.Type = %v, want %v", report.Previous.Type, tt.expectedPrevType)
			}
			if report.Previous.Addr != addr {
				t.Errorf("Previous.Addr = 0x%x, want 0x%x", report.Previous.Addr, addr)
			}
			if report.Previous.GoroutineID != tt.expectedPrevGID {
				t.Errorf("Previous.GoroutineID = %d, want %d", report.Previous.GoroutineID, tt.expectedPrevGID)
			}
			if report.Previous.Epoch != prevEpoch {
				t.Errorf("Previous.Epoch = %v, want %v", report.Previous.Epoch, prevEpoch)
			}
		})
	}
}

// TestRaceReport_Format tests the Format() method.
func TestRaceReport_Format(t *testing.T) {
	addr := uintptr(0xabcdef123456)
	prevEpoch := epoch.NewEpoch(5, 100) // tid=5, clock=100
	currEpoch := epoch.NewEpoch(7, 200) // tid=7, clock=200

	tests := []struct {
		name         string
		raceType     string
		wantContains []string
	}{
		{
			name:     "write-write race",
			raceType: "write-write",
			wantContains: []string{
				"WARNING: DATA RACE",
				"Write at 0x0000abcdef123456 by goroutine 7:",
				"Previous Write at 0x0000abcdef123456 by goroutine 5:",
				// Phase 5 Task 5.2: Now has real stack traces
				"TestRaceReport_Format",                       // Should appear in current access stack
				"(previous access stack trace not available)", // Previous doesn't have stack
				"[epoch: 200@7]",
				"[epoch: 100@5]",
				"==================",
			},
		},
		{
			name:     "read-write race",
			raceType: "read-write",
			wantContains: []string{
				"WARNING: DATA RACE",
				"Write at 0x0000abcdef123456 by goroutine 7:",
				"Previous Read at 0x0000abcdef123456 by goroutine 5:",
			},
		},
		{
			name:     "write-read race",
			raceType: "write-read",
			wantContains: []string{
				"WARNING: DATA RACE",
				"Read at 0x0000abcdef123456 by goroutine 7:",
				"Previous Write at 0x0000abcdef123456 by goroutine 5:",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := NewRaceReport(tt.raceType, addr, prevEpoch, currEpoch)

			var buf bytes.Buffer
			report.Format(&buf)
			output := buf.String()

			// Check that all expected strings are present.
			for _, want := range tt.wantContains {
				if !strings.Contains(output, want) {
					t.Errorf("Format() output missing expected string %q\nGot:\n%s", want, output)
				}
			}

			// Check structure: should have header and footer separators.
			lines := strings.Split(output, "\n")
			if len(lines) < 5 {
				t.Errorf("Format() output too short, got %d lines", len(lines))
			}

			// First line should be separator.
			if lines[0] != "==================" {
				t.Errorf("First line = %q, want separator", lines[0])
			}

			// Second line should be warning.
			if lines[1] != "WARNING: DATA RACE" {
				t.Errorf("Second line = %q, want warning", lines[1])
			}
		})
	}
}

// TestRaceReport_String tests the String() method.
func TestRaceReport_String(t *testing.T) {
	addr := uintptr(0x12345678)
	prevEpoch := epoch.NewEpoch(1, 10) // tid=1, clock=10
	currEpoch := epoch.NewEpoch(2, 20) // tid=2, clock=20

	report := NewRaceReport("write-write", addr, prevEpoch, currEpoch)
	output := report.String()

	// Should contain key information.
	wantContains := []string{
		"WARNING: DATA RACE",
		"Write at 0x0000000012345678 by goroutine 2:",
		"Previous Write at 0x0000000012345678 by goroutine 1:",
		"==================",
	}

	for _, want := range wantContains {
		if !strings.Contains(output, want) {
			t.Errorf("String() missing expected string %q\nGot:\n%s", want, output)
		}
	}
}

// TestDetector_reportRaceV2 tests the new structured race reporting.
func TestDetector_reportRaceV2(t *testing.T) {
	d := NewDetector()

	addr := uintptr(0x1000)
	prevEpoch := epoch.NewEpoch(1, 5)  // tid=1, clock=5
	currEpoch := epoch.NewEpoch(2, 10) // tid=2, clock=10

	// Capture stderr output.
	// For this test, we'll check that racesDetected is incremented.
	// Full output testing is done in TestRaceReport_Format.

	initialCount := d.RacesDetected()
	if initialCount != 0 {
		t.Fatalf("Initial race count = %d, want 0", initialCount)
	}

	// Report a race (output goes to stderr, we won't capture it here).
	// Pass nil for VarState in tests (previous stack won't be shown).
	d.reportRaceV2("write-write", addr, nil, prevEpoch, currEpoch)

	// Check counter incremented.
	finalCount := d.RacesDetected()
	if finalCount != 1 {
		t.Errorf("After reportRaceV2, race count = %d, want 1", finalCount)
	}

	// Report another race.
	d.reportRaceV2("read-write", addr+8, nil, epoch.NewEpoch(3, 15), epoch.NewEpoch(4, 20))

	finalCount = d.RacesDetected()
	if finalCount != 2 {
		t.Errorf("After second reportRaceV2, race count = %d, want 2", finalCount)
	}
}

// BenchmarkNewRaceReport benchmarks race report creation.
func BenchmarkNewRaceReport(b *testing.B) {
	addr := uintptr(0x12345678)
	prevEpoch := epoch.NewEpoch(5, 100) // tid=5, clock=100
	currEpoch := epoch.NewEpoch(7, 200) // tid=7, clock=200

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewRaceReport("write-write", addr, prevEpoch, currEpoch)
	}
}

// BenchmarkRaceReport_Format benchmarks race report formatting.
func BenchmarkRaceReport_Format(b *testing.B) {
	addr := uintptr(0x12345678)
	prevEpoch := epoch.NewEpoch(5, 100) // tid=5, clock=100
	currEpoch := epoch.NewEpoch(7, 200) // tid=7, clock=200
	report := NewRaceReport("write-write", addr, prevEpoch, currEpoch)

	var buf bytes.Buffer

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		report.Format(&buf)
	}
}

// BenchmarkRaceReport_String benchmarks String() method.
func BenchmarkRaceReport_String(b *testing.B) {
	addr := uintptr(0x12345678)
	prevEpoch := epoch.NewEpoch(5, 100) // tid=5, clock=100
	currEpoch := epoch.NewEpoch(7, 200) // tid=7, clock=200
	report := NewRaceReport("write-write", addr, prevEpoch, currEpoch)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = report.String()
	}
}

// BenchmarkCaptureStackTrace benchmarks stack trace capture.
// Phase 5 Task 5.2: Measures overhead of runtime.Callers().
func BenchmarkCaptureStackTrace(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = captureStackTrace(5)
	}
}

// BenchmarkFormatStackTrace benchmarks stack trace formatting.
// Phase 5 Task 5.2: Measures overhead of runtime.CallersFrames() and formatting.
func BenchmarkFormatStackTrace(b *testing.B) {
	// Capture a real stack trace once
	pcs := captureStackTrace(2)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = formatStackTrace(pcs)
	}
}

// BenchmarkRaceReportWithStackTrace benchmarks full race report with stack trace.
// Phase 5 Task 5.2: Measures combined overhead (capture + format).
func BenchmarkRaceReportWithStackTrace(b *testing.B) {
	addr := uintptr(0x12345678)
	prevEpoch := epoch.NewEpoch(5, 100)
	currEpoch := epoch.NewEpoch(7, 200)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		report := NewRaceReport("write-write", addr, prevEpoch, currEpoch)
		var buf bytes.Buffer
		report.Format(&buf)
	}
}

// === Phase 5 Task 5.3: Deduplication Tests ===

// TestGenerateDeduplicationKey tests the deduplication key generation function.
func TestGenerateDeduplicationKey(t *testing.T) {
	tests := []struct {
		name     string
		raceType string
		addr     uintptr
		gid1     uint32
		gid2     uint32
		wantKey  string
	}{
		{
			name:     "write-write race with sorted IDs",
			raceType: "write-write",
			addr:     0x1234,
			gid1:     3,
			gid2:     5,
			wantKey:  "write-write:0x1234:3:5",
		},
		{
			name:     "write-write race with unsorted IDs (should sort)",
			raceType: "write-write",
			addr:     0x1234,
			gid1:     5,
			gid2:     3,
			wantKey:  "write-write:0x1234:3:5", // IDs sorted
		},
		{
			name:     "read-write race",
			raceType: "read-write",
			addr:     0xabcdef,
			gid1:     10,
			gid2:     20,
			wantKey:  "read-write:0xabcdef:10:20",
		},
		{
			name:     "write-read race",
			raceType: "write-read",
			addr:     0xffffff,
			gid1:     100,
			gid2:     50,
			wantKey:  "write-read:0xffffff:50:100", // IDs sorted
		},
		{
			name:     "same goroutine (edge case)",
			raceType: "write-write",
			addr:     0x5678,
			gid1:     7,
			gid2:     7,
			wantKey:  "write-write:0x5678:7:7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKey := generateDeduplicationKey(tt.raceType, tt.addr, tt.gid1, tt.gid2)
			if gotKey != tt.wantKey {
				t.Errorf("generateDeduplicationKey() = %q, want %q", gotKey, tt.wantKey)
			}
		})
	}
}

// TestNewRaceReport_DeduplicationKey tests that NewRaceReport generates correct dedup key.
func TestNewRaceReport_DeduplicationKey(t *testing.T) {
	addr := uintptr(0x1000)
	prevEpoch := epoch.NewEpoch(3, 10) // tid=3, clock=10
	currEpoch := epoch.NewEpoch(5, 20) // tid=5, clock=20

	report := NewRaceReport("write-write", addr, prevEpoch, currEpoch)

	// Expected key: "write-write:0x1000:3:5" (IDs sorted)
	expectedKey := "write-write:0x1000:3:5"
	if report.DeduplicationKey != expectedKey {
		t.Errorf("DeduplicationKey = %q, want %q", report.DeduplicationKey, expectedKey)
	}
}

// TestDetector_Deduplication_FirstRaceReported tests that first race is reported.
func TestDetector_Deduplication_FirstRaceReported(t *testing.T) {
	d := NewDetector()
	defer d.Reset()

	addr := uintptr(0x1000)
	prevEpoch := epoch.NewEpoch(1, 5)
	currEpoch := epoch.NewEpoch(2, 10)

	// First race should be reported (race counter increments).
	initialCount := d.RacesDetected()
	if initialCount != 0 {
		t.Fatalf("Initial race count = %d, want 0", initialCount)
	}

	d.reportRaceV2("write-write", addr, nil, prevEpoch, currEpoch)

	finalCount := d.RacesDetected()
	if finalCount != 1 {
		t.Errorf("After first reportRaceV2, race count = %d, want 1", finalCount)
	}
}

// TestDetector_Deduplication_DuplicateRaceSkipped tests that duplicate race is NOT reported.
func TestDetector_Deduplication_DuplicateRaceSkipped(t *testing.T) {
	d := NewDetector()
	defer d.Reset()

	addr := uintptr(0x1000)
	prevEpoch := epoch.NewEpoch(1, 5)
	currEpoch := epoch.NewEpoch(2, 10)

	// Report the same race twice.
	d.reportRaceV2("write-write", addr, nil, prevEpoch, currEpoch)
	d.reportRaceV2("write-write", addr, nil, prevEpoch, currEpoch)

	// Only first race should be counted.
	finalCount := d.RacesDetected()
	if finalCount != 1 {
		t.Errorf("After duplicate reportRaceV2, race count = %d, want 1 (duplicate should be skipped)", finalCount)
	}
}

// TestDetector_Deduplication_DifferentLocationReported tests that different locations are reported separately.
func TestDetector_Deduplication_DifferentLocationReported(t *testing.T) {
	d := NewDetector()
	defer d.Reset()

	addr1 := uintptr(0x1000)
	addr2 := uintptr(0x2000) // Different address
	prevEpoch := epoch.NewEpoch(1, 5)
	currEpoch := epoch.NewEpoch(2, 10)

	// Report races at two different addresses.
	d.reportRaceV2("write-write", addr1, nil, prevEpoch, currEpoch)
	d.reportRaceV2("write-write", addr2, nil, prevEpoch, currEpoch)

	// Both races should be counted (different locations).
	finalCount := d.RacesDetected()
	if finalCount != 2 {
		t.Errorf("After races at different locations, race count = %d, want 2", finalCount)
	}
}

// TestDetector_Deduplication_DifferentGoroutinesReported tests that different goroutine pairs are reported.
func TestDetector_Deduplication_DifferentGoroutinesReported(t *testing.T) {
	d := NewDetector()
	defer d.Reset()

	addr := uintptr(0x1000)

	// Race 1: G1 vs G2
	prevEpoch1 := epoch.NewEpoch(1, 5)
	currEpoch1 := epoch.NewEpoch(2, 10)
	d.reportRaceV2("write-write", addr, nil, prevEpoch1, currEpoch1)

	// Race 2: G1 vs G3 (different goroutine pair)
	prevEpoch2 := epoch.NewEpoch(1, 15)
	currEpoch2 := epoch.NewEpoch(3, 20)
	d.reportRaceV2("write-write", addr, nil, prevEpoch2, currEpoch2)

	// Both races should be counted (different goroutine pairs).
	finalCount := d.RacesDetected()
	if finalCount != 2 {
		t.Errorf("After races with different goroutine pairs, race count = %d, want 2", finalCount)
	}
}

// TestDetector_Deduplication_GoroutineOrderIrrelevant tests that goroutine order doesn't matter.
func TestDetector_Deduplication_GoroutineOrderIrrelevant(t *testing.T) {
	d := NewDetector()
	defer d.Reset()

	addr := uintptr(0x1000)

	// Race 1: G1 vs G2
	prevEpoch1 := epoch.NewEpoch(1, 5)
	currEpoch1 := epoch.NewEpoch(2, 10)
	d.reportRaceV2("write-write", addr, nil, prevEpoch1, currEpoch1)

	// Race 2: G2 vs G1 (same pair, reversed order)
	prevEpoch2 := epoch.NewEpoch(2, 15)
	currEpoch2 := epoch.NewEpoch(1, 20)
	d.reportRaceV2("write-write", addr, nil, prevEpoch2, currEpoch2)

	// Only first race should be counted (same goroutine pair).
	finalCount := d.RacesDetected()
	if finalCount != 1 {
		t.Errorf("After races with reversed goroutine order, race count = %d, want 1 (should be deduplicated)", finalCount)
	}
}

// TestDetector_Deduplication_DifferentRaceTypesReported tests that different race types are reported separately.
func TestDetector_Deduplication_DifferentRaceTypesReported(t *testing.T) {
	d := NewDetector()
	defer d.Reset()

	addr := uintptr(0x1000)
	prevEpoch := epoch.NewEpoch(1, 5)
	currEpoch := epoch.NewEpoch(2, 10)

	// Report different race types at same location with same goroutines.
	d.reportRaceV2("write-write", addr, nil, prevEpoch, currEpoch)
	d.reportRaceV2("read-write", addr, nil, prevEpoch, currEpoch)

	// Both races should be counted (different race types).
	finalCount := d.RacesDetected()
	if finalCount != 2 {
		t.Errorf("After races with different types, race count = %d, want 2", finalCount)
	}
}

// TestDetector_Reset_ClearsDeduplicationMap tests that Reset() clears the deduplication map.
func TestDetector_Reset_ClearsDeduplicationMap(t *testing.T) {
	d := NewDetector()

	addr := uintptr(0x1000)
	prevEpoch := epoch.NewEpoch(1, 5)
	currEpoch := epoch.NewEpoch(2, 10)

	// Report a race.
	d.reportRaceV2("write-write", addr, nil, prevEpoch, currEpoch)
	if d.RacesDetected() != 1 {
		t.Fatalf("Before reset, race count = %d, want 1", d.RacesDetected())
	}

	// Reset detector.
	d.Reset()

	// After reset, race count should be 0.
	if d.RacesDetected() != 0 {
		t.Errorf("After reset, race count = %d, want 0", d.RacesDetected())
	}

	// Report the same race again - should be counted (dedup map cleared).
	d.reportRaceV2("write-write", addr, nil, prevEpoch, currEpoch)
	if d.RacesDetected() != 1 {
		t.Errorf("After reset and re-report, race count = %d, want 1", d.RacesDetected())
	}
}

// BenchmarkGenerateDeduplicationKey benchmarks deduplication key generation.
func BenchmarkGenerateDeduplicationKey(b *testing.B) {
	addr := uintptr(0x12345678)
	gid1 := uint32(5)
	gid2 := uint32(10)
	raceType := "write-write"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = generateDeduplicationKey(raceType, addr, gid1, gid2)
	}
}

// BenchmarkDeduplicationCheck_FirstRace benchmarks first race detection (no dedup).
func BenchmarkDeduplicationCheck_FirstRace(b *testing.B) {
	d := NewDetector()
	defer d.Reset()

	addr := uintptr(0x12345678)
	prevEpoch := epoch.NewEpoch(5, 100)
	currEpoch := epoch.NewEpoch(7, 200)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Use different addresses to avoid deduplication.
		d.reportRaceV2("write-write", addr+uintptr(i), nil, prevEpoch, currEpoch)
	}
}

// BenchmarkDeduplicationCheck_DuplicateRace benchmarks duplicate race detection (with dedup).
func BenchmarkDeduplicationCheck_DuplicateRace(b *testing.B) {
	d := NewDetector()
	defer d.Reset()

	addr := uintptr(0x12345678)
	prevEpoch := epoch.NewEpoch(5, 100)
	currEpoch := epoch.NewEpoch(7, 200)

	// Report first race once.
	d.reportRaceV2("write-write", addr, nil, prevEpoch, currEpoch)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Report duplicate race (should be skipped).
		d.reportRaceV2("write-write", addr, nil, prevEpoch, currEpoch)
	}
}
