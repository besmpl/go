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

func TestFinalizerHandoffInvalidatesStrongAtomicReleaseProof(t *testing.T) {
	const (
		targetGoid = int64(1<<52 + 401)
		sourceGoid = int64(1<<52 + 402)
	)
	targetTID, targetStart := allocTID()
	sourceTID, sourceStart := allocTID()
	target := goroutine.AllocWithStartClock(targetTID, targetStart)
	source := goroutine.AllocWithStartClock(sourceTID, sourceStart)
	source.IncrementClock()
	contextsMap.Store(targetGoid, target)
	contextsMap.Store(sourceGoid, source)
	lifecycleMu.lock()
	delete(pendingTIDs, targetTID)
	delete(pendingTIDs, sourceTID)
	lifecycleMu.unlock()
	t.Cleanup(func() {
		raceGoEndFromRuntime(targetGoid)
		raceGoEndFromRuntime(sourceGoid)
	})

	var releaseRoot byte
	release := unsafe.Pointer(&releaseRoot)
	target.RecordAtomicRelease(release, 9, 12, 0xff, true)
	before := target.ForeignGeneration
	raceFinalizerGoFromRuntime(uintptr(unsafe.Pointer(target)))
	if target.ForeignGeneration != before+1 {
		t.Fatalf("finalizer foreign generation = %d, want %d", target.ForeignGeneration, before+1)
	}
	seen, strong, ok := target.LookupAtomicRelease(release, 9, 0xff)
	if !ok || strong || seen != 12 {
		t.Fatalf("finalizer cache proof = (%d,%v,%v), want weak version 12", seen, strong, ok)
	}
}

func TestGoSetChildIDBindsSpawnAcrossDisable(t *testing.T) {
	const (
		parentGoid = int64(1<<52 + 101)
		childGoid  = int64(1<<52 + 102)
	)

	oldEnabled := enabled.Load()
	enabled.Store(1)
	contextsMap.Delete(parentGoid)
	contextsMap.Delete(childGoid)

	parentTID, startClock := allocTID()
	parent := goroutine.AllocWithStartClock(parentTID, startClock)
	parent.C.Set(65536, 9)
	contextsMap.Store(parentGoid, parent)

	var spawnID uintptr
	t.Cleanup(func() {
		enabled.Store(1)
		if clock := consumeSpawnContextByID(spawnID, childGoid); clock != nil {
			clock.Release()
		}
		raceGoEndFromRuntime(childGoid)
		raceGoEndFromRuntime(parentGoid)
		enabled.Store(oldEnabled)
	})

	spawnID = raceGoStartFromRuntime(0x1234, parentGoid)
	if spawnID == 0 {
		t.Fatal("runtime go-start returned no spawn token")
	}

	// Model a concurrent Disable call landing between the runtime's go-start
	// and child-binding callbacks.
	Disable()
	childPtr := raceGoSetChildIDWithCtx(childGoid, spawnID)
	if childPtr <= 1 {
		t.Fatalf("child binding while disabled returned %#x", childPtr)
	}

	child, ok := contextsMap.Load(childGoid)
	if !ok {
		t.Fatal("child binding while disabled did not root its context")
	}
	if child.C.Get(parentTID) != 1 || child.C.Get(65536) != 9 {
		t.Fatalf("child binding lost the pre-disable spawn clock: %s", child.C)
	}
	if clock := consumeSpawnContextByID(spawnID, childGoid); clock != nil {
		clock.Release()
		t.Fatal("child binding left the spawn token pending")
	}
}

func TestGoSetChildIDBindsIndependentContextWhileDisabled(t *testing.T) {
	const (
		parentGoid = int64(1<<52 + 111)
		childGoid  = int64(1<<52 + 112)
	)

	oldEnabled := enabled.Load()
	enabled.Store(1)
	contextsMap.Delete(parentGoid)
	contextsMap.Delete(childGoid)

	parentTID, startClock := allocTID()
	parent := goroutine.AllocWithStartClock(parentTID, startClock)
	parent.C.Set(65536, 9)
	contextsMap.Store(parentGoid, parent)
	t.Cleanup(func() {
		enabled.Store(1)
		raceGoEndFromRuntime(childGoid)
		raceGoEndFromRuntime(parentGoid)
		enabled.Store(oldEnabled)
	})

	Disable()
	spawnID := raceGoStartFromRuntime(0x1234, parentGoid)
	if spawnID != 0 {
		t.Fatalf("disabled go-start returned spawn token %#x", spawnID)
	}
	childPtr := raceGoSetChildIDWithCtx(childGoid, spawnID)
	if childPtr <= 1 {
		t.Fatalf("disabled child binding returned %#x", childPtr)
	}

	child, ok := contextsMap.Load(childGoid)
	if !ok {
		t.Fatal("disabled child binding did not root its context")
	}
	if child.C.Get(parentTID) != 0 || child.C.Get(65536) != 0 {
		t.Fatalf("disabled go-start unexpectedly published a fork edge: %s", child.C)
	}
}

func TestGoEndReleasesContextWhileDisabled(t *testing.T) {
	const goid = int64(1<<52 + 201)

	oldEnabled := enabled.Load()
	enabled.Store(1)
	contextsMap.Delete(goid)

	tid, startClock := allocTID()
	ctx := goroutine.AllocWithStartClock(tid, startClock)
	contextsMap.Store(goid, ctx)
	t.Cleanup(func() {
		enabled.Store(1)
		raceGoEndFromRuntime(goid)
		enabled.Store(oldEnabled)
	})

	Disable()
	raceGoEndFromRuntime(goid)

	if _, ok := contextsMap.Load(goid); ok {
		t.Fatal("go-end while disabled left the context rooted")
	}
	if ctx.C != nil {
		t.Fatal("go-end while disabled retained the vector clock")
	}
}

func TestAPIGoEndReleasesContextWhileDisabled(t *testing.T) {
	oldEnabled := enabled.Load()
	enabled.Store(1)
	defer enabled.Store(oldEnabled)

	type result struct {
		rooted    bool
		ownsClock bool
	}
	done := make(chan result, 1)
	go func() {
		goid := getGoroutineID()
		tid, startClock := allocTID()
		ctx := goroutine.AllocWithStartClock(tid, startClock)
		contextsMap.Store(goid, ctx)

		Disable()
		racegoend()
		_, rooted := contextsMap.Load(goid)
		ownsClock := ctx.C != nil

		// Keep cleanup effective against the pre-fix implementation.
		enabled.Store(1)
		raceGoEndFromRuntime(goid)
		done <- result{rooted: rooted, ownsClock: ownsClock}
	}()

	got := <-done
	if got.rooted {
		t.Fatal("API go-end while disabled left the context rooted")
	}
	if got.ownsClock {
		t.Fatal("API go-end while disabled retained the vector clock")
	}
}

func TestClearShadowMaintainsAddressLifecycleWhileDisabled(t *testing.T) {
	oldEnabled := enabled.Load()
	enabled.Store(1)
	det.Reset()

	first := goroutine.Alloc(100_001)
	second := goroutine.Alloc(100_002)
	t.Cleanup(func() {
		first.C.Release()
		second.C.Release()
		det.Reset()
		enabled.Store(oldEnabled)
	})

	const addr = uintptr(0x7f00_1234_5000)
	det.OnWrite(addr, first, 0x101)
	if shadow.Get(addr) == nil {
		t.Fatal("first address lifetime did not create shadow state")
	}

	// Model an allocator free/reallocation occurring while user event
	// recording is globally disabled.
	Disable()
	raceClearShadow(addr, 1)
	if shadow.Get(addr) != nil {
		t.Fatal("disabled allocator clear retained the old address lifetime")
	}

	Reset()
	det.OnWrite(addr, second, 0x202)
	if got := det.RacesDetected(); got != 0 {
		t.Fatalf("reused address inherited stale race history: got %d races", got)
	}
}

func TestDirtyDisabledReenablesOnlyThroughQuiescentDrain(t *testing.T) {
	Reset()
	const rootedGID = int64(1<<52 + 501)
	rooted := allocRegisteredContext(rootedGID)
	detachedTID, detachedStart := allocTID()
	detached := goroutine.AllocWithStartClock(detachedTID, detachedStart)
	detachedPtr := uintptr(unsafe.Pointer(detached))
	lifecycleMu.lock()
	detachedContexts = map[uintptr]*goroutine.RaceContext{detachedPtr: detached}
	delete(pendingTIDs, detachedTID)
	lifecycleMu.unlock()
	spawnClock := vectorclock.NewFromPool()
	spawnClock.Set(rooted.TID, 7)
	info := &spawnInfo{parentClock: spawnClock}
	spawnContextsMu.lock()
	spawnContextsSlice = append(spawnContextsSlice, info)
	spawnContextsMu.unlock()
	_, _ = allocTID() // leave one unpublished reservation for Reset to drain
	highWater := nextTID.load64()

	Disable()
	if got := detectorLifecycle(lifecycleState.Load()); got != lifecycleDirtyDisabled || enabled.Load() != 0 {
		t.Fatalf("Disable state = (%d,%d), want dirty-disabled", got, enabled.Load())
	}
	Reset()
	Enable()
	if got := detectorLifecycle(lifecycleState.Load()); got != lifecycleEnabled || enabled.Load() != 1 {
		t.Fatalf("Enable state = (%d,%d), want enabled after drain", got, enabled.Load())
	}
	if rooted.C != nil || detached.C != nil || info.parentClock != nil {
		t.Fatal("quiescent drain retained a rooted, detached, or pending spawn clock")
	}
	if _, ok := contextsMap.Load(rootedGID); ok {
		t.Fatal("quiescent drain retained rooted context ownership")
	}
	lifecycleMu.lock()
	gotDetached, gotPending := len(detachedContexts), len(pendingTIDs)
	lifecycleMu.unlock()
	spawnContextsMu.lock()
	gotSpawns := len(spawnContextsSlice)
	spawnContextsMu.unlock()
	if gotDetached != 0 || gotPending != 0 || gotSpawns != 0 {
		t.Fatalf("drain retained registries: detached=%d pending=%d spawns=%d", gotDetached, gotPending, gotSpawns)
	}
	if nextTID.load64() != highWater {
		t.Fatalf("Reset rewound TID high-water: got %d want %d", nextTID.load64(), highWater)
	}
}

func TestDirtyDisabledEnableFailsClosed(t *testing.T) {
	if os.Getenv("KOLKOV_DIRTY_ENABLE") == "1" {
		Reset()
		Disable()
		Enable()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestDirtyDisabledEnableFailsClosed$")
	cmd.Env = append(os.Environ(), "KOLKOV_DIRTY_ENABLE=1")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("Enable resumed dirty lifecycle; output:\n%s", output)
	}
	if !bytes.Contains(output, []byte("race detector dirty state requires quiescent Reset before Enable")) {
		t.Fatalf("dirty Enable output did not contain fail-closed diagnostic:\n%s", output)
	}
}

func TestRepeatedInitFiniResetNeverReusesTID(t *testing.T) {
	var previous uint64
	var priorContexts []*goroutine.RaceContext
	for cycle := 0; cycle < 3; cycle++ {
		Init()
		if got := detectorLifecycle(lifecycleState.Load()); got != lifecycleEnabled {
			t.Fatalf("cycle %d Init state = %d, want enabled", cycle, got)
		}
		ctx, ok := contextsMap.Load(getGoroutineID())
		if !ok || ctx == nil || ctx.C == nil {
			t.Fatalf("cycle %d Init did not root caller context", cycle)
		}
		if uint64(ctx.TID) <= previous {
			t.Fatalf("cycle %d reused TID %d after %d", cycle, ctx.TID, previous)
		}
		for _, prior := range priorContexts {
			if prior.C != nil {
				t.Fatalf("cycle %d retained prior context clock", cycle)
			}
		}
		previous = uint64(ctx.TID)
		priorContexts = append(priorContexts, ctx)
		Fini()
		if got := detectorLifecycle(lifecycleState.Load()); got != lifecycleDirtyDisabled {
			t.Fatalf("cycle %d Fini state = %d, want dirty-disabled", cycle, got)
		}
	}
	Reset()
	for _, prior := range priorContexts {
		if prior.C != nil {
			t.Fatal("final Reset retained a context clock")
		}
	}
	if nextTID.load64() < previous {
		t.Fatalf("final Reset rewound high-water below %d", previous)
	}
}

func TestChildBindingWaitsForLifecycleInitialization(t *testing.T) {
	const childGoid = int64(1<<52 + 301)

	oldInitState := apiInitCalled.Load()
	oldEnabled := enabled.Load()
	apiInitCalled.Store(1) // initialization in progress
	enabled.Store(0)
	contextsMap.Delete(childGoid)
	t.Cleanup(func() {
		raceGoEndFromRuntime(childGoid)
		apiInitCalled.Store(oldInitState)
		enabled.Store(oldEnabled)
	})

	if got := raceGoSetChildIDWithCtx(childGoid, 0); got != 0 {
		t.Fatalf("child binding before lifecycle initialization returned %#x", got)
	}
	if _, ok := contextsMap.Load(childGoid); ok {
		t.Fatal("child binding before lifecycle initialization rooted a context")
	}
}

func TestClearShadowWaitsForLifecycleInitialization(t *testing.T) {
	oldInitState := apiInitCalled.Load()
	oldEnabled := enabled.Load()
	oldDetector := det
	apiInitCalled.Store(1) // initialization in progress
	enabled.Store(0)
	det = nil
	t.Cleanup(func() {
		det = oldDetector
		apiInitCalled.Store(oldInitState)
		enabled.Store(oldEnabled)
	})

	// There cannot be stale state before the detector exists. The allocator
	// callback must wait rather than dereferencing a partially initialized
	// detector during package initialization.
	raceClearShadow(0x1234, 1)
}
