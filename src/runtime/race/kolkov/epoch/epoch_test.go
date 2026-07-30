package epoch

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"runtime/race/kolkov/vectorclock"
)

func TestNewEpochEncoding(t *testing.T) {
	tests := []struct {
		name  string
		tid   uint32
		clock uint64
		want  uint64
	}{
		{"zero", 0, 0, 0},
		{"tid only", 5, 0, 0x00000005_00000000},
		{"clock only", 0, 0x1234, 0x00000000_00001234},
		{"both", 42, 0x123456, 0x0000002A_00123456},
		{"dense boundary", 1023, 7, 0x000003FF_00000007},
		{"sparse boundary", 1024, 8, 0x00000400_00000008},
		{"past uint16", 65536, 9, 0x00010000_00000009},
		{"max", MaxTID, MaxClock, 0xFFFFFFFF_FFFFFFFF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := uint64(NewEpoch(tt.tid, tt.clock)); got != tt.want {
				t.Fatalf("NewEpoch(%d, %#x) = %#x, want %#x", tt.tid, tt.clock, got, tt.want)
			}
		})
	}
}

func TestEpochDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		tid   uint32
		clock uint64
	}{
		{0, 0},
		{1, 100},
		{1023, 0x123456},
		{1024, 500_000},
		{65535, 1_000_000_000},
		{65536, 2_000_000_000},
		{1<<20 + 7, MaxClock},
		{MaxTID, MaxClock},
	}
	for _, tt := range tests {
		e := NewEpoch(tt.tid, tt.clock)
		tid, clock := e.Decode()
		if tid != tt.tid || clock != tt.clock {
			t.Fatalf("NewEpoch(%d, %d).Decode() = (%d, %d)", tt.tid, tt.clock, tid, clock)
		}
	}
}

func TestEpochHappensBeforeDenseAndSparse(t *testing.T) {
	vc := vectorclock.New()
	vc.Set(7, 42)
	vc.Set(1<<20+7, 100)
	tests := []struct {
		e    Epoch
		want bool
	}{
		{NewEpoch(7, 42), true},
		{NewEpoch(7, 43), false},
		{NewEpoch(1<<20+7, 99), true},
		{NewEpoch(1<<20+7, 101), false},
		{NewEpoch(65536, 1), false},
		{NewEpoch(65536, 0), true},
	}
	for _, tt := range tests {
		if got := tt.e.HappensBefore(vc); got != tt.want {
			tid, clock := tt.e.Decode()
			t.Fatalf("%d@%d HappensBefore clock[%d]=%d: got %v, want %v", clock, tid, tid, vc.Get(tid), got, tt.want)
		}
	}
}

func TestEpochSame(t *testing.T) {
	e := NewEpoch(65536, 100)
	if !e.Same(e) {
		t.Fatal("epoch is not equal to itself")
	}
	if e.Same(NewEpoch(65537, 100)) || e.Same(NewEpoch(65536, 101)) {
		t.Fatal("epochs with different coordinates compare equal")
	}
}

func TestEpochString(t *testing.T) {
	tests := []struct {
		e    Epoch
		want string
	}{
		{NewEpoch(0, 0), "0@0"},
		{NewEpoch(5, 42), "42@5"},
		{NewEpoch(65536, MaxClock), "4294967295@65536"},
		{NewEpoch(MaxTID, MaxClock), "4294967295@4294967295"},
	}
	for _, tt := range tests {
		if got := tt.e.String(); got != tt.want {
			t.Fatalf("Epoch(%#x).String() = %q, want %q", tt.e, got, tt.want)
		}
	}
}

func TestOverflowFlags(t *testing.T) {
	ResetOverflowFlags()
	_ = NewEpoch(MaxTIDWarning+1, MaxClockWarning+1)
	tidOverflow, clockOverflow, tidWarning, clockWarning := CheckOverflows()
	if tidOverflow || clockOverflow || !tidWarning || !clockWarning {
		t.Fatalf("near-limit flags = (%v,%v,%v,%v), want (false,false,true,true)", tidOverflow, clockOverflow, tidWarning, clockWarning)
	}

	ResetOverflowFlags()
	if a, b, c, d := CheckOverflows(); a || b || c || d {
		t.Fatalf("ResetOverflowFlags left flags set: (%v,%v,%v,%v)", a, b, c, d)
	}
}

func TestClockOverflowFailsClosed(t *testing.T) {
	if os.Getenv("KOLKOV_TEST_EPOCH_OVERFLOW") == "1" {
		NewEpoch(1, MaxClock+1)
		t.Fatal("NewEpoch returned after logical clock overflow")
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestClockOverflowFailsClosed$")
	cmd.Env = append(os.Environ(), "KOLKOV_TEST_EPOCH_OVERFLOW=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("overflow subprocess succeeded:\n%s", out)
	}
	if !strings.Contains(string(out), "fatal error: race detector logical clock overflow") {
		t.Fatalf("overflow subprocess did not fail closed:\n%s", out)
	}
}

func TestNextClockIsCheckOnly(t *testing.T) {
	ResetOverflowFlags()
	if got := NextClock(41); got != 42 {
		t.Fatalf("NextClock(41) = %d, want 42", got)
	}
	_, overflow, _, near := CheckOverflows()
	if overflow || near {
		t.Fatalf("ordinary preflight changed overflow flags: overflow=%v near=%v", overflow, near)
	}
}

func TestNextClockOverflowFailsClosed(t *testing.T) {
	if os.Getenv("KOLKOV_NEXT_CLOCK_OVERFLOW") == "1" {
		NextClock(MaxClock)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestNextClockOverflowFailsClosed$")
	cmd.Env = append(os.Environ(), "KOLKOV_NEXT_CLOCK_OVERFLOW=1")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("NextClock(MaxClock) succeeded; output:\n%s", output)
	}
	if !strings.Contains(string(output), "race detector logical clock overflow") {
		t.Fatalf("overflow output did not contain fail-closed diagnostic:\n%s", output)
	}
}

func TestOverflowConstants(t *testing.T) {
	if TIDBits != 32 || ClockBits != 32 || MaxTID != ^uint32(0) || MaxClock != uint64(^uint32(0)) {
		t.Fatalf("unexpected epoch geometry: tidBits=%d clockBits=%d maxTID=%d maxClock=%d", TIDBits, ClockBits, MaxTID, MaxClock)
	}
	wantWarning := uint64(1<<32) * 9 / 10
	if uint64(MaxTIDWarning) != wantWarning {
		t.Fatalf("MaxTIDWarning = %d, want %d", MaxTIDWarning, wantWarning)
	}
	if MaxClockWarning != wantWarning {
		t.Fatalf("MaxClockWarning = %d, want %d", MaxClockWarning, wantWarning)
	}
}

func BenchmarkEpochHappensBefore(b *testing.B) {
	e := NewEpoch(42, 1000)
	vc := vectorclock.New()
	vc.Set(42, 2000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = e.HappensBefore(vc)
	}
}

func BenchmarkEpochDecode(b *testing.B) {
	e := NewEpoch(42, 0x123456)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = e.Decode()
	}
}

func BenchmarkNewEpoch(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewEpoch(42, uint64(i)&ClockMask)
	}
}

func BenchmarkEpochSame(b *testing.B) {
	e := NewEpoch(42, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = e.Same(e)
	}
}
