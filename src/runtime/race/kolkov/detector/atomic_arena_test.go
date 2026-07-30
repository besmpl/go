package detector

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"

	"runtime/race/kolkov/goroutine"
)

func TestAtomicArenaExhaustionFailsClosed(t *testing.T) {
	const helperEnv = "GO_RACE_ATOMIC_ARENA_EXHAUSTION"
	if mode := os.Getenv(helperEnv); mode != "" {
		a := newAtomicHistoryArena()
		switch mode {
		case "state-handle":
			a.nextState = ^uint32(0)
			a.newState(0x1000)
		case "generation":
			a.generation = ^uint32(0)
			a.reset()
		case "frontier-generation":
			n := a.allocFrontier()
			a.freeFrontierNode(n)
			n.generation.Store(^uint64(0))
			a.allocFrontier()
		default:
			os.Exit(3)
		}
		os.Exit(4)
	}

	for _, test := range []struct {
		mode, message string
	}{
		{"state-handle", "race detector exhausted atomic-state handles"},
		{"generation", "race detector atomic arena generation overflow"},
		{"frontier-generation", "race detector atomic-frontier generation overflow"},
	} {
		t.Run(test.mode, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestAtomicArenaExhaustionFailsClosed$")
			cmd.Env = append(os.Environ(), helperEnv+"="+test.mode)
			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("exhaustion helper succeeded; output:\n%s", output)
			}
			if !strings.Contains(string(output), test.message) {
				t.Fatalf("exhaustion output missing %q:\n%s", test.message, output)
			}
		})
	}
}

func TestAtomicArenaInlineFrontierSpillsWithoutEviction(t *testing.T) {
	a := newAtomicHistoryArena()
	h := atomicHistory{arena: a}
	const witnesses = atomicInlineFrontier + 19
	for i := uint32(1); i <= witnesses; i++ {
		ctx := goroutine.Alloc(80_000 + i)
		recordAtomicAccess(&h, ctx, uintptr(0x9000+i), 0xff, false)
	}
	if got := atomicHistoryCardinality(h); got != witnesses {
		t.Fatalf("frontier cardinality = %d, want %d", got, witnesses)
	}
	for i := uint32(1); i <= witnesses; i++ {
		access, ok := atomicHistoryAccess(h, 80_000+i)
		if !ok || access.clocks[7] != 1 || access.pcs[7] != uintptr(0x9000+i) {
			t.Fatalf("witness %d = (%+v,%v), want exact lane-7 witness", i, access, ok)
		}
	}
	if stats := a.Stats(); stats.History != witnesses-atomicInlineFrontier || stats.PeakHistory != stats.History {
		t.Fatalf("arena overflow live nodes = %d, want %d", stats.History, witnesses-atomicInlineFrontier)
	}
}

func TestAtomicArenaForcedTypedSlabRefillSurvivesGCAndStackGrowth(t *testing.T) {
	a := newAtomicHistoryArena()
	states := make([]*atomicState, atomicStateSlabSize*3+1)
	for i := range states {
		states[i] = a.newState(uintptr(0x100000 + i*8))
		ctx := goroutine.Alloc(uint32(90_000 + i))
		recordAtomicAccess(&states[i].writes, ctx, uintptr(0xa000+i), 1, false)
	}
	var grow func(int) uintptr
	grow = func(depth int) uintptr {
		var pad [512]byte
		pad[0] = byte(depth)
		if depth == 0 {
			runtime.GC()
			return uintptr(pad[0])
		}
		return uintptr(pad[0]) + grow(depth-1)
	}
	_ = grow(32)
	for i, state := range states {
		if state.base != uintptr(0x100000+i*8) || state.handle.generation != a.generation {
			t.Fatalf("state %d moved or lost its generation: base=%#x handle=%+v", i, state.base, state.handle)
		}
		if access, ok := atomicHistoryAccess(state.writes, uint32(90_000+i)); !ok || access.pcs[0] != uintptr(0xa000+i) {
			t.Fatalf("state %d lost typed history after GC/stack growth", i)
		}
	}
	if got := a.Stats().Refills; got < 4 { // state slabs plus at least one supporting slab
		t.Fatalf("forced allocation used %d typed slab refills, want at least 4", got)
	}
}

func TestAtomicArenaResetInvalidatesHandlesAndReusesTypedStorage(t *testing.T) {
	a := newAtomicHistoryArena()
	old := a.newState(0x2000)
	oldHandle := old.handle
	a.reset()
	fresh := a.newState(0x3000)
	if fresh != old {
		t.Fatalf("quiescent reset did not reuse first typed state slot: old=%p fresh=%p", old, fresh)
	}
	if fresh.handle.generation == oldHandle.generation || fresh.handle.index != 1 {
		t.Fatalf("fresh handle = %+v, stale = %+v", fresh.handle, oldHandle)
	}
}

func TestAtomicArenaPinAccounting(t *testing.T) {
	a := newAtomicHistoryArena()
	a.pin()
	a.pin()
	a.unpin()
	a.unpin()
	stats := a.Stats()
	if stats.Pinned != 0 || stats.PeakPinned != 2 {
		t.Fatalf("pin accounting = live %d peak %d, want 0/2", stats.Pinned, stats.PeakPinned)
	}
}

func TestAtomicArenaSidecarRetirementReleasesEveryObject(t *testing.T) {
	a := newAtomicHistoryArena()
	s := a.newState(0x4000)
	a.retainState(s)

	// Force every out-of-line object class to become live on one sidecar.
	for tid := uint32(1); tid <= atomicInlineFrontier+5; tid++ {
		ctx := goroutine.Alloc(100_000 + tid)
		recordAtomicAccess(&s.writes, ctx, uintptr(0xb000+tid), 1, false)
	}
	writer := goroutine.Alloc(110_000)
	s.publishRelease(writer, 1)
	reader := goroutine.Alloc(110_001)
	s.registerReadFrontier(reader, 1, 0xc000, false, nil)

	before := a.Stats()
	if before.States != 1 || before.History == 0 || before.Releases != 1 || before.Ranges == 0 || before.Frontiers != 1 {
		t.Fatalf("live sidecar accounting before retirement = %+v", before)
	}
	a.releaseState(s)
	after := a.Stats()
	if after.States != 0 || after.History != 0 || after.Releases != 0 || after.Ranges != 0 || after.Frontiers != 0 {
		t.Fatalf("retired sidecar retained arena objects: %+v", after)
	}

	// The typed state slot is immediately reusable, but its diagnostic identity
	// must change so a reclaimed address never denotes the old generation.
	fresh := a.newState(0x5000)
	if fresh != s {
		t.Fatalf("retired typed state slot was not reused: old=%p fresh=%p", s, fresh)
	}
	if fresh.handle.index == 0 || fresh.handle.index == 1 {
		t.Fatalf("reused state handle = %+v, want a fresh identity", fresh.handle)
	}
}

func TestAtomicArenaClearChurnPlateaus(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(120_000)
	const addr = uintptr(0x51000)

	var refills uint64
	var previous atomicArenaHandle
	for i := 0; i < atomicStateSlabSize*8; i++ {
		completeAtomicSize(d, addr, 8, ctx, false, true, uintptr(0xd000+i))
		state := atomicHistoryForTest(t, d, addr)
		if state.handle == previous {
			t.Fatalf("clear churn iteration %d reused handle %+v", i, state.handle)
		}
		previous = state.handle
		d.ClearShadowRange(addr, 8)
		stats := d.atomicArena.Stats()
		if stats.States != 0 || stats.History != 0 || stats.Releases != 0 || stats.Ranges != 0 || stats.Frontiers != 0 {
			t.Fatalf("clear churn iteration %d retained live arena objects: %+v", i, stats)
		}
		if i == 0 {
			refills = stats.Refills
		} else if stats.Refills != refills {
			t.Fatalf("clear churn grew typed slabs at iteration %d: refills=%d, warmed=%d", i, stats.Refills, refills)
		}
	}
}

func TestAtomicArenaPartialClearRetainsSurvivingBinding(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(125_000)
	const addr = uintptr(0x52000)
	completeAtomicSize(d, addr, 8, ctx, false, true, 0xd800)
	original := atomicHistoryForTest(t, d, addr)

	d.ClearShadowRange(addr, 4)
	if got := atomicHistoryForTest(t, d, addr+4); got != original {
		t.Fatalf("partial clear changed surviving sidecar: got %p, want %p", got, original)
	}
	if stats := d.atomicArena.Stats(); stats.States != 1 || stats.Releases != 1 {
		t.Fatalf("partial clear retired surviving sidecar metadata: %+v", stats)
	}

	d.ClearShadowRange(addr+4, 4)
	if stats := d.atomicArena.Stats(); stats.States != 0 || stats.Releases != 0 || stats.Ranges != 0 {
		t.Fatalf("final partial clear retained sidecar metadata: %+v", stats)
	}
}

func TestAtomicArenaRepeatedResetReusesAllSlabs(t *testing.T) {
	a := newAtomicHistoryArena()
	fill := func() {
		for i := 0; i < atomicStateSlabSize*3+1; i++ {
			a.newState(uintptr(0x6000 + i*8))
		}
		for i := 0; i < atomicHistorySlabSize*2+1; i++ {
			a.allocHistoryNode()
		}
		for i := 0; i < atomicReleaseSlabSize*2+1; i++ {
			a.allocRelease()
		}
		for i := 0; i < atomicRangeSlabSize*2+1; i++ {
			a.allocRange()
		}
		for i := 0; i < atomicFrontierSlabSize*2+1; i++ {
			a.allocFrontier()
		}
	}

	fill()
	want := [5]int{len(a.states), len(a.histories), len(a.releases), len(a.ranges), len(a.frontiers)}
	for cycle := 0; cycle < 5; cycle++ {
		a.reset()
		fill()
		got := [5]int{len(a.states), len(a.histories), len(a.releases), len(a.ranges), len(a.frontiers)}
		if got != want {
			t.Fatalf("reset cycle %d grew typed slab sets: got %v, warmed %v", cycle, got, want)
		}
	}
}
