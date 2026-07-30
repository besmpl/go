package api

import (
	"bytes"
	"os"
	"os/exec"
	"testing"
	"unsafe"

	"runtime/race/kolkov/goroutine"
	"runtime/race/kolkov/vectorclock"
)

func TestTIDAllocationIsMonotonic(t *testing.T) {
	base := nextTID.Load()
	for offset := uint32(1); offset <= 10; offset++ {
		want := base + offset
		tid, startClock := allocTID()
		if tid != want || startClock != 1 {
			t.Fatalf("allocation %d = (%d, %d), want (%d, 1)", want, tid, startClock, want)
		}
	}
}

func raiseNextTIDTo(min uint32) uint32 {
	for {
		current := nextTID.Load()
		if current >= min || nextTID.CompareAndSwap(current, min) {
			return max(current, min)
		}
	}
}

func TestTIDAllocationCrossesStorageBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		start uint32
		want  []uint32
	}{
		{"dense to sparse", vectorclock.DenseThreads - 2, []uint32{vectorclock.DenseThreads - 1, vectorclock.DenseThreads, vectorclock.DenseThreads + 1}},
		{"past uint16", 65534, []uint32{65535, 65536, 65537}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := raiseNextTIDTo(tt.start)
			for i, boundaryWant := range tt.want {
				want := start + uint32(i) + 1
				if start == tt.start {
					want = boundaryWant
				}
				tid, _ := allocTID()
				if tid != want {
					t.Fatalf("allocTID() = %d, want %d", tid, want)
				}
				ctx := goroutine.Alloc(tid)
				if ctx.C.Get(tid) != 1 {
					t.Fatalf("clock[%d] = %d, want 1", tid, ctx.C.Get(tid))
				}
				ctx.C.Release()
			}
		})
	}
}

func TestTIDAllocationPast65535Lifetimes(t *testing.T) {
	const lifetimes = 70_000
	base := nextTID.Load()
	for offset := uint32(1); offset <= lifetimes; offset++ {
		want := base + offset
		tid, _ := allocTID()
		if tid != want {
			t.Fatalf("lifetime allocation = %d, want %d", tid, want)
		}
		ctx := goroutine.Alloc(tid)
		if got := ctx.C.Get(tid); got != 1 {
			t.Fatalf("lifetime %d clock = %d, want 1", tid, got)
		}
		ctx.C.Release()
	}
	if got := nextTID.Load(); got != base+lifetimes {
		t.Fatalf("last issued TID = %d, want %d", got, base+lifetimes)
	}
}

func TestTIDExhaustionFailsBeforeSentinel(t *testing.T) {
	if os.Getenv("KOLKOV_TID_EXHAUSTION") == "1" {
		nextTID.store64(uint64(^uint32(0)) - 1)
		tid, _ := allocTID()
		if tid != ^uint32(0) || nextTID.load64() != uint64(^uint32(0)) {
			panic("maximum representable TID was not issued exactly once")
		}
		allocTID()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestTIDExhaustionFailsBeforeSentinel$")
	cmd.Env = append(os.Environ(), "KOLKOV_TID_EXHAUSTION=1")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("TID exhaustion succeeded; output:\n%s", output)
	}
	if !bytes.Contains(output, []byte("race detector exhausted logical goroutine IDs")) {
		t.Fatalf("TID exhaustion output did not contain fail-closed diagnostic:\n%s", output)
	}
}

func TestTIDReservationBoundaryDoesNotPublishSentinel(t *testing.T) {
	var highWater tidHighWater
	highWater.store64(uint64(^uint32(0)) - 1)
	if tid, ok := highWater.reserve(); !ok || tid != ^uint32(0) {
		t.Fatalf("last representable reservation = (%d,%v), want (%d,true)", tid, ok, ^uint32(0))
	}
	before := highWater.load64()
	if tid, ok := highWater.reserve(); ok || tid != 0 {
		t.Fatalf("exhausted reservation = (%d,%v), want (0,false)", tid, ok)
	}
	if after := highWater.load64(); after != before || after >= exhaustedTIDHighWater {
		t.Fatalf("failed reservation published state: before=%d after=%d sentinel=%d", before, after, exhaustedTIDHighWater)
	}
}

func TestContextEndDoesNotRecycleTID(t *testing.T) {
	const gid = int64(1 << 50)
	enabled.Store(1)
	contextsMap.Delete(gid)
	firstTID, _ := allocTID()
	first := goroutine.Alloc(firstTID)
	contextsMap.Store(gid, first)
	raceGoEndFromRuntime(gid)
	if _, ok := contextsMap.Load(gid); ok {
		t.Fatal("ended context remains rooted")
	}
	if first.C != nil {
		t.Fatal("ended context still owns its vector clock")
	}

	secondTID, _ := allocTID()
	if secondTID != firstTID+1 {
		t.Fatalf("TID after context end = %d, want %d", secondTID, firstTID+1)
	}
}

func TestDetachedContextLifecycle(t *testing.T) {
	enabled.Store(1)
	parentTID, _ := allocTID()
	parent := goroutine.Alloc(parentTID)
	defer parent.C.Release()
	parent.C.Set(65536, 9)

	ptr := raceContextStartFromRuntime(0x1234, uintptr(unsafe.Pointer(parent)))
	if ptr <= 1 {
		t.Fatalf("raceContextStartFromRuntime returned %#x", ptr)
	}
	detachedContextsMu.lock()
	child := detachedContexts[ptr]
	detachedContextsMu.unlock()
	if child == nil {
		t.Fatal("temporary context is not GC-rooted")
	}
	if child.TID != parentTID+1 || child.C.Get(parentTID) != 1 || child.C.Get(65536) != 9 || child.C.Get(child.TID) != 1 {
		t.Fatalf("temporary context did not inherit parent: child tid=%d clock=%s", child.TID, child.C)
	}
	if parent.C.Get(parentTID) != 2 {
		t.Fatalf("parent clock after temporary fork = %d, want 2", parent.C.Get(parentTID))
	}
	raceContextEndFromRuntime(ptr)
	if child.C != nil {
		t.Fatal("ended temporary context still owns its vector clock")
	}
	detachedContextsMu.lock()
	_, rooted := detachedContexts[ptr]
	detachedContextsMu.unlock()
	if rooted {
		t.Fatal("ended temporary context remains GC-rooted")
	}
	next, _ := allocTID()
	if next != child.TID+1 {
		t.Fatalf("temporary context ID was reused: next=%d ended=%d", next, child.TID)
	}
}

func BenchmarkAllocTID(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = allocTID()
	}
}
