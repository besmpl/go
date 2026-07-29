package api

import (
	"testing"
	"unsafe"

	"runtime/race/kolkov/detector"
	"runtime/race/kolkov/goroutine"
)

func resetLifecycleRegistryForTest() {
	lifecycleMu.lock()
	contextsMap.resetLocked()
	detachedContexts = nil
	pendingTIDs = nil
	lifecycleMu.unlock()
}

func allocRegisteredContext(gid int64) *goroutine.RaceContext {
	tid, startClock := allocTID()
	ctx := goroutine.AllocWithStartClock(tid, startClock)
	contextsMap.Store(gid, ctx)
	return ctx
}

func TestFinalizerHandoffMergesLiveDetachedAndRetiresFinishedIDs(t *testing.T) {
	resetLifecycleRegistryForTest()

	target := allocRegisteredContext(1)
	live := allocRegisteredContext(2)
	detachedTID, startClock := allocTID()
	detached := goroutine.AllocWithStartClock(detachedTID, startClock)
	finished := allocRegisteredContext(4)
	for i := 0; i < 2; i++ {
		live.IncrementClock()
	}
	for i := 0; i < 3; i++ {
		detached.IncrementClock()
	}
	for i := 0; i < 4; i++ {
		finished.IncrementClock()
	}

	detachedContextsMu.lock()
	detachedContexts = map[uintptr]*goroutine.RaceContext{
		uintptr(unsafe.Pointer(detached)): detached,
	}
	delete(pendingTIDs, detached.TID)
	detachedContextsMu.unlock()
	if got, ok := contextsMap.LoadAndDelete(4); !ok || got != finished {
		t.Fatal("failed to end finished test context")
	}
	finishedTID := finished.TID
	finished.C.Release()
	finished.C = nil

	before := target.C.Get(target.TID)
	raceFinalizerGoFromRuntime(uintptr(unsafe.Pointer(target)))
	if got, want := target.C.Get(target.TID), before+1; got != want {
		t.Fatalf("target clock = %d, want %d", got, want)
	}
	for _, ctx := range []*goroutine.RaceContext{live, detached} {
		if got, want := target.C.Get(ctx.TID), ctx.C.Get(ctx.TID); got != want {
			t.Fatalf("merged clock[%d] = %d, want %d", ctx.TID, got, want)
		}
	}
	if !target.C.IsRetired(finishedTID) || target.C.Get(finishedTID) != ^uint32(0) {
		t.Fatalf("finished TID %d was not retired in finalizer snapshot", finishedTID)
	}

	resetLifecycleRegistryForTest()
	for _, ctx := range []*goroutine.RaceContext{target, live, detached} {
		ctx.C.Release()
		ctx.C = nil
	}
}

func TestFinalizerHandoffInvalidatesObservedSourceReadCache(t *testing.T) {
	resetLifecycleRegistryForTest()
	target := allocRegisteredContext(50)
	source := allocRegisteredContext(51)
	d := detector.NewDetector()
	const (
		addr    = uintptr(0xF1000)
		readPC  = uintptr(0x101)
		writePC = uintptr(0x202)
	)

	// The source read is initially represented in both shadow memory and its
	// redundant-read cache.
	d.OnRead(addr, source, readPC)
	d.OnRead(addr, source, readPC)
	slot := (addr >> 3) & (goroutine.ReadCacheSlots - 1)
	if got := source.ReadCache[slot]; got != addr {
		t.Fatalf("source read cache = %#x, want %#x", got, addr)
	}
	_, sourceClock64 := source.GetEpoch().Decode()
	sourceClock := uint32(sourceClock64)

	raceFinalizerGoFromRuntime(uintptr(unsafe.Pointer(target)))
	if got := source.ReadCacheInvalidatedClock.Load(); got != sourceClock {
		t.Fatalf("source invalidation clock = %d, want %d", got, sourceClock)
	}
	if got := source.ReadCache[slot]; got != addr {
		t.Fatalf("handoff externally mutated source-owned cache: got %#x, want %#x", got, addr)
	}

	// The handoff orders the finalizer write after the represented source read,
	// so this write correctly reports no race and replaces that read history.
	d.OnWrite(addr, target, writePC)
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("handoff-ordered finalizer write reported %d races, want 0", got)
	}

	// A later source read has no edge back from the finalizer. The runtime must
	// reject its still-present cache entry; entering the detector then exposes
	// the concurrent finalizer write.
	d.OnRead(addr, source, readPC+1)
	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("post-finalizer source read reported %d races, want 1", got)
	}

	resetLifecycleRegistryForTest()
	for _, ctx := range []*goroutine.RaceContext{target, source} {
		ctx.C.Release()
		ctx.C = nil
	}
}

func TestFinalizerHandoffTreatsAllocatedUnpublishedTIDAsLiveHole(t *testing.T) {
	resetLifecycleRegistryForTest()
	target := allocRegisteredContext(10)
	pendingTID, startClock := allocTID()

	raceFinalizerGoFromRuntime(uintptr(unsafe.Pointer(target)))
	if target.C.IsRetired(pendingTID) {
		t.Fatalf("allocated-but-unpublished TID %d was retired", pendingTID)
	}

	pending := goroutine.AllocWithStartClock(pendingTID, startClock)
	pending.IncrementClock()
	contextsMap.Store(11, pending)
	raceFinalizerGoFromRuntime(uintptr(unsafe.Pointer(target)))
	if got, want := target.C.Get(pendingTID), pending.C.Get(pendingTID); got != want {
		t.Fatalf("published pending clock[%d] = %d, want %d", pendingTID, got, want)
	}

	resetLifecycleRegistryForTest()
	for _, ctx := range []*goroutine.RaceContext{target, pending} {
		ctx.C.Release()
		ctx.C = nil
	}
}

func TestFinalizerHandoffCannotObserveEndBetweenDeleteAndRetirement(t *testing.T) {
	resetLifecycleRegistryForTest()
	target := allocRegisteredContext(30)
	ending := allocRegisteredContext(31)
	ending.IncrementClock()

	// Force the old failure window: the context has been deleted, but its
	// VectorClock has not yet been released. The finalizer cannot enter until
	// this same lifecycle critical section completes, then reconstructs the
	// ended identity from the high-water/live-set complement.
	lifecycleMu.lock()
	ended, ok := contextsMap.loadAndDeleteLocked(31)
	if !ok || ended != ending {
		lifecycleMu.unlock()
		t.Fatal("failed to delete ending context under lifecycle lock")
	}
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		raceFinalizerGoFromRuntime(uintptr(unsafe.Pointer(target)))
		close(done)
	}()
	<-started
	select {
	case <-done:
		lifecycleMu.unlock()
		t.Fatal("finalizer crossed an in-progress lifecycle transition")
	default:
	}
	lifecycleMu.unlock()
	<-done

	if !target.C.IsRetired(ending.TID) {
		t.Fatalf("ended TID %d was lost at finalizer handoff", ending.TID)
	}
	ending.C.Release()
	ending.C = nil
	resetLifecycleRegistryForTest()
	target.C.Release()
	target.C = nil
}

func TestFinalizerRetirementStorageTracksLiveHolesNotHistory(t *testing.T) {
	resetLifecycleRegistryForTest()
	target := allocRegisteredContext(20)
	const finishedCount = 256
	for i := 0; i < finishedCount; i++ {
		ctx := allocRegisteredContext(int64(100 + i))
		ended, ok := contextsMap.LoadAndDelete(int64(100 + i))
		if !ok || ended != ctx {
			t.Fatal("failed to end historical context")
		}
		ctx.C.Release()
		ctx.C = nil
	}
	live := allocRegisteredContext(1000)
	for i := 0; i < finishedCount; i++ {
		ctx := allocRegisteredContext(int64(2000 + i))
		ended, ok := contextsMap.LoadAndDelete(int64(2000 + i))
		if !ok || ended != ctx {
			t.Fatal("failed to end second historical context")
		}
		ended.C.Release()
		ended.C = nil
	}

	raceFinalizerGoFromRuntime(uintptr(unsafe.Pointer(target)))
	rangeCount := 0
	target.C.RangeRetired(func(_, _ uint32) bool {
		rangeCount++
		return true
	})
	if rangeCount > 3 { // Prefix history, gap around target, and gap around live.
		t.Fatalf("retirement used %d intervals for two live holes", rangeCount)
	}
	if target.C.IsRetired(target.TID) || target.C.IsRetired(live.TID) {
		t.Fatal("live hole was included in retired complement")
	}

	resetLifecycleRegistryForTest()
	for _, ctx := range []*goroutine.RaceContext{target, live} {
		ctx.C.Release()
		ctx.C = nil
	}
}

func TestContextsMapDeletePreservesCollidingEntry(t *testing.T) {
	var contexts contextsMapType
	first := new(goroutine.RaceContext)
	second := new(goroutine.RaceContext)

	// These GIDs had the same home bucket in the old fixed probe table.
	const firstGID, secondGID int64 = 6, 10952
	contexts.Store(firstGID, first)
	contexts.Store(secondGID, second)

	if got, ok := contexts.LoadAndDelete(firstGID); !ok || got != first {
		t.Fatalf("LoadAndDelete(%d) = (%p, %v), want (%p, true)", firstGID, got, ok, first)
	}
	if got, ok := contexts.Load(secondGID); !ok || got != second {
		t.Fatalf("deleting colliding GID hid live context: Load(%d) = (%p, %v), want (%p, true)", secondGID, got, ok, second)
	}
}

func TestContextsMapDoesNotOverwriteCollisionOverflow(t *testing.T) {
	var contexts contextsMapType
	// All values shared one home bucket in the old table, whose eight-probe
	// overflow path overwrote a live GC root when the ninth value was stored.
	gids := [...]int64{1, 17712, 35423, 46369, 64080, 75026, 92737, 110448, 121394}
	want := make([]*goroutine.RaceContext, len(gids))
	for i, gid := range gids {
		want[i] = new(goroutine.RaceContext)
		contexts.Store(gid, want[i])
	}
	for i, gid := range gids {
		if got, ok := contexts.Load(gid); !ok || got != want[i] {
			t.Fatalf("Load(%d) after collision overflow = (%p, %v), want (%p, true)", gid, got, ok, want[i])
		}
	}
}
