package detector

import (
	"testing"
	"time"
	"unsafe"

	"runtime/race/kolkov/goroutine"
)

func completeCachedLoadSlowForTest(d *Detector, addr uintptr, ctx *goroutine.RaceContext, pc uintptr) {
	completeCachedLoadSlowSizedForTest(d, addr, 8, ctx, pc)
}

func completeCachedLoadSlowSizedForTest(d *Detector, addr, size uintptr, ctx *goroutine.RaceContext, pc uintptr) {
	var token AtomicToken
	d.AtomicBeginLoad(addr, size, ctx, &token)
	d.AtomicEndLoad(addr, size, ctx, &token, pc)
}

func warmCachedLoadForTest(t *testing.T, d *Detector, addr uintptr, ctx *goroutine.RaceContext, pc uintptr) {
	warmCachedLoadSizedForTest(t, d, addr, 8, ctx, pc)
}

func warmCachedLoadSizedForTest(t *testing.T, d *Detector, addr, size uintptr, ctx *goroutine.RaceContext, pc uintptr) {
	t.Helper()
	for i := 0; i < 3; i++ {
		completeCachedLoadSlowSizedForTest(d, addr, size, ctx, pc)
		var token AtomicToken
		if revision, generation, ok := d.AtomicBeginLoadFast(addr, size, ctx, pc, &token); ok {
			if !d.AtomicEndLoadFast(ctx, &token, revision, generation) {
				t.Fatal("stable warmed cached load failed validation")
			}
			return
		}
	}
	t.Fatal("locked loads did not seed exact cached load")
}

func TestAtomicCachedLoadRejectsWriterRevisionChange(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(70_001)
	writer := goroutine.Alloc(70_002)
	const addr = uintptr(0x51000)
	const pc = uintptr(0x8100)
	warmCachedLoadForTest(t, d, addr, reader, pc)

	var loadToken AtomicToken
	revision, generation, ok := d.AtomicBeginLoadFast(addr, 8, reader, pc, &loadToken)
	if !ok {
		t.Fatal("warmed cached load missed")
	}
	var writeToken AtomicToken
	d.AtomicBeginPlain(addr, 8, writer, false, true, &writeToken)
	d.AtomicEnd(addr, 8, writer, &writeToken, 0x8101, true)
	if d.AtomicEndLoadFast(reader, &loadToken, revision, generation) {
		t.Fatal("cached load accepted a hardware window crossed by a writer")
	}
}

func TestAtomicCachedLoadRevisionOnlyMissRetainsFrontier(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(70_003)
	writer := goroutine.Alloc(70_004)
	const addr = uintptr(0x51080)
	const pc = uintptr(0x8102)
	warmCachedLoadForTest(t, d, addr, reader, pc)

	cacheIndex := -1
	for i := range reader.AtomicLoadCache {
		if reader.AtomicLoadCache[i].PC == pc {
			cacheIndex = i
			break
		}
	}
	if cacheIndex < 0 {
		t.Fatal("warmed load has no cache entry")
	}
	before := reader.AtomicLoadCache[cacheIndex]
	state := atomicHistoryForTest(t, d, addr)

	for i := 0; i < 16; i++ {
		var token AtomicToken
		d.AtomicBeginPlain(addr, 8, writer, false, true, &token)
		d.AtomicEnd(addr, 8, writer, &token, pc+1, true)

		if _, _, ok := d.AtomicBeginLoadFast(addr, 8, reader, pc, &token); ok {
			t.Fatal("stale writer revision unexpectedly hit")
		}
		if got := reader.AtomicLoadCache[cacheIndex]; got != before {
			t.Fatalf("revision-only miss mutated cache entry: got %+v want %+v", got, before)
		}
		state.mu.lock()
		frontier := state.readFrontiers
		count := 0
		for node := frontier; node != nil; node = node.next {
			count++
		}
		state.mu.unlock()
		if count != 1 || unsafe.Pointer(frontier) != before.Frontier {
			t.Fatalf("revision-only miss changed frontier list: head=%p count=%d want=%p/1", frontier, count, before.Frontier)
		}

		completeCachedLoadSlowForTest(d, addr, reader, pc)
		after := reader.AtomicLoadCache[cacheIndex]
		if after.Frontier != before.Frontier || after.Generation != before.Generation {
			t.Fatalf("locked refresh replaced frontier: before=%p/%d after=%p/%d", before.Frontier, before.Generation, after.Frontier, after.Generation)
		}
		if after.Revision == before.Revision {
			t.Fatal("locked refresh did not record the latest writer revision")
		}
		before = after
	}
}

func TestAtomicCachedLoadOddRevisionMissRetainsFrontier(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(70_005)
	const addr = uintptr(0x510c0)
	const pc = uintptr(0x8104)
	warmCachedLoadForTest(t, d, addr, reader, pc)
	before := ctxCacheEntryForTest(t, d, reader, addr, pc)
	state := atomicHistoryForTest(t, d, addr)
	writer := goroutine.AllocWithParentClock(70_006, reader.C, 1)

	var writeToken AtomicToken
	d.AtomicBeginPlain(addr, 8, writer, false, true, &writeToken)
	if state.writerRevision.Load()&1 == 0 {
		d.AtomicEnd(addr, 8, writer, &writeToken, pc+1, true)
		t.Fatal("writer transaction did not publish an odd revision")
	}

	done := make(chan bool, 1)
	go func() {
		var loadToken AtomicToken
		_, _, ok := d.AtomicBeginLoadFast(addr, 8, reader, pc, &loadToken)
		done <- ok
	}()
	var hit bool
	select {
	case hit = <-done:
	case <-time.After(time.Second):
		d.AtomicEnd(addr, 8, writer, &writeToken, pc+1, true)
		<-done
		t.Fatal("odd-revision miss blocked trying to deactivate its active frontier")
	}
	if hit {
		d.AtomicEnd(addr, 8, writer, &writeToken, pc+1, true)
		t.Fatal("odd writer revision unexpectedly hit")
	}
	afterMiss := ctxCacheEntryForTest(t, d, reader, addr, pc)
	if afterMiss != before {
		d.AtomicEnd(addr, 8, writer, &writeToken, pc+1, true)
		t.Fatalf("odd-revision miss mutated cache entry: got %+v want %+v", afterMiss, before)
	}
	frontier := state.readFrontiers
	if frontier == nil || unsafe.Pointer(frontier) != before.Frontier || frontier.next != nil {
		d.AtomicEnd(addr, 8, writer, &writeToken, pc+1, true)
		t.Fatalf("odd-revision miss changed frontier list: head=%p want=%p", frontier, before.Frontier)
	}

	// The writer is happens-after the cached read, so its completion prunes the
	// retained node. The existing zero-mask fallback must still relink that same
	// slot-owned node rather than treating the earlier revision miss as eviction.
	d.AtomicEnd(addr, 8, writer, &writeToken, pc+1, true)
	state.mu.lock()
	prunedHead := state.readFrontiers
	state.mu.unlock()
	if prunedHead != nil || frontier.mask.Load() != 0 {
		t.Fatalf("HB writer did not fully prune retained frontier: head=%p mask=%#x", prunedHead, frontier.mask.Load())
	}

	completeCachedLoadSlowForTest(d, addr, reader, pc)
	afterRefresh := ctxCacheEntryForTest(t, d, reader, addr, pc)
	if afterRefresh.Frontier != before.Frontier || afterRefresh.Generation <= before.Generation || afterRefresh.Revision == before.Revision {
		t.Fatalf("locked refresh did not reuse frontier at latest revision: before=%p/%d/%d after=%p/%d/%d", before.Frontier, before.Generation, before.Revision, afterRefresh.Frontier, afterRefresh.Generation, afterRefresh.Revision)
	}
}

func TestAtomicCachedLoadPublishesLatestReadFrontier(t *testing.T) {
	tests := []struct {
		name  string
		write func(*Detector, uintptr, *goroutine.RaceContext)
	}{
		{name: "scalar", write: func(d *Detector, addr uintptr, ctx *goroutine.RaceContext) { d.OnWrite(addr, ctx, 0x8111) }},
		{name: "sized", write: func(d *Detector, addr uintptr, ctx *goroutine.RaceContext) { d.OnWriteSized(addr, 8, ctx, 0x8111) }},
		{name: "range", write: func(d *Detector, addr uintptr, ctx *goroutine.RaceContext) { d.OnWriteRange(addr, 8, ctx, 0x8111) }},
		{name: "partial range", write: func(d *Detector, addr uintptr, ctx *goroutine.RaceContext) { d.OnWriteRange(addr+4, 4, ctx, 0x8111) }},
		{name: "wide range", write: func(d *Detector, addr uintptr, ctx *goroutine.RaceContext) { d.OnWriteRange(addr-4, 16, ctx, 0x8111) }},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := NewDetector()
			reader := goroutine.Alloc(uint32(70_011 + i*2))
			addr := uintptr(0x51100 + i*0x100)
			const loadPC = uintptr(0x8110)
			warmCachedLoadForTest(t, d, addr, reader, loadPC)

			writer := goroutine.AllocWithParentClock(uint32(70_012+i*2), reader.C, 1)
			reader.IncrementClock()
			var token AtomicToken
			revision, generation, ok := d.AtomicBeginLoadFast(addr, 8, reader, loadPC, &token)
			if !ok || !d.AtomicEndLoadFast(reader, &token, revision, generation) {
				t.Fatal("new-epoch cached load did not complete")
			}
			test.write(d, addr, writer)
			if got := d.RacesDetected(); got != 1 {
				t.Fatalf("ordinary writer saw %d races, want latest cached read frontier conflict", got)
			}
		})
	}
}

func TestAtomicCachedLoadFrontierOnlyStillRacesWithOrdinaryWrite(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(70_019)
	const addr = uintptr(0x511c0)
	const loadPC = uintptr(0x8118)
	warmCachedLoadForTest(t, d, addr, reader, loadPC)

	writer := goroutine.AllocWithParentClock(70_020, reader.C, 1)
	reader.IncrementClock()
	var token AtomicToken
	revision, generation, ok := d.AtomicBeginLoadFast(addr, 8, reader, loadPC, &token)
	if !ok || !d.AtomicEndLoadFast(reader, &token, revision, generation) {
		t.Fatal("new-epoch cached load did not complete")
	}
	state := atomicHistoryForTest(t, d, addr)
	state.mu.lock()
	canonicalReads := atomicHistoryCardinality(state.reads)
	registered := state.readFrontiers != nil
	state.mu.unlock()
	if canonicalReads != 0 || !registered {
		t.Fatalf("cached load history = %d canonical reads, registered=%v; want frontier only", canonicalReads, registered)
	}

	d.OnWrite(addr, writer, loadPC+1)
	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("ordinary writer saw %d races, want frontier-only cached read conflict", got)
	}
}

func TestAtomicCachedLoadClearDrainsPublication(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(70_021)
	const addr = uintptr(0x51200)
	const pc = uintptr(0x8120)
	warmCachedLoadForTest(t, d, addr, ctx, pc)

	var token AtomicToken
	revision, generation, ok := d.AtomicBeginLoadFast(addr, 8, ctx, pc, &token)
	if !ok {
		t.Fatal("warmed cached load missed")
	}
	done := make(chan struct{})
	go func() {
		d.ClearShadowRange(addr, 8)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("clear completed before cached read-frontier publication")
	case <-time.After(10 * time.Millisecond):
	}
	if !d.AtomicEndLoadFast(ctx, &token, revision, generation) {
		t.Fatal("clear changed writer revision of retained old generation")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("clear did not finish after cached load released capability")
	}
	if _, _, ok := d.AtomicBeginLoadFast(addr, 8, ctx, pc, &token); ok {
		t.Fatal("cleared lifecycle accepted old cached load identity")
	}
}

func TestAtomicCachedLoadFrontierPrunesAfterHBWriter(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(70_031)
	const addr = uintptr(0x51300)
	const pc = uintptr(0x8130)
	warmCachedLoadForTest(t, d, addr, reader, pc)
	state := atomicHistoryForTest(t, d, addr)
	if state.readFrontiers == nil {
		t.Fatal("warm load registered no frontier")
	}

	writer := goroutine.AllocWithParentClock(70_032, reader.C, 1)
	var token AtomicToken
	d.AtomicBeginPlain(addr, 8, writer, false, true, &token)
	d.AtomicEnd(addr, 8, writer, &token, pc+1, true)
	state.mu.lock()
	frontiers := state.readFrontiers
	state.mu.unlock()
	if frontiers != nil {
		t.Fatal("HB-dominated cached frontier remained linked after writer")
	}
	if _, _, ok := d.AtomicBeginLoadFast(addr, 8, reader, pc, &token); ok {
		t.Fatal("writer revision accepted a pruned cached frontier")
	}
	DeactivateAtomicLoadCache(reader)
	if stats := d.atomicArena.Stats(); stats.Frontiers != 0 {
		t.Fatalf("cache teardown retained %d fully pruned frontiers, want 0", stats.Frontiers)
	}
}

func TestAtomicCachedLoadPrunedFrontierRecycleAdvancesGeneration(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(70_033)
	const (
		addr = uintptr(0x51340)
		pc   = uintptr(0x8132)
	)
	warmCachedLoadForTest(t, d, addr, reader, pc)
	stale := ctxCacheEntryForTest(t, d, reader, addr, pc)

	writer := goroutine.AllocWithParentClock(70_034, reader.C, 1)
	var token AtomicToken
	d.AtomicBeginPlain(addr, 8, writer, false, true, &token)
	d.AtomicEnd(addr, 8, writer, &token, pc+1, true)
	DeactivateAtomicLoadCache(reader)

	freshReader := goroutine.Alloc(70_035)
	const freshAddr = uintptr(0x51380)
	warmCachedLoadForTest(t, d, freshAddr, freshReader, pc+2)
	fresh := ctxCacheEntryForTest(t, d, freshReader, freshAddr, pc+2)
	if fresh.Frontier != stale.Frontier {
		t.Fatalf("freed frontier was not recycled: stale=%p fresh=%p", stale.Frontier, fresh.Frontier)
	}
	if fresh.Generation <= stale.Generation {
		t.Fatalf("recycled frontier generation = %d, want greater than stale %d", fresh.Generation, stale.Generation)
	}
	DeactivateAtomicLoadCache(freshReader)
}

func TestAtomicCachedLoadPrunedFrontierStateSurvivesConcurrentClear(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(70_036)
	const (
		addr = uintptr(0x513a0)
		pc   = uintptr(0x8134)
	)
	warmCachedLoadForTest(t, d, addr, reader, pc)
	oldState := atomicHistoryForTest(t, d, addr)

	// Hold the exact capability across the hardware window. Clear closes that
	// capability first and waits; completing the HB writer then fully prunes the
	// cached frontier before clear drops the shadow binding's state ownership.
	writer := goroutine.AllocWithParentClock(70_037, reader.C, 1)
	var token AtomicToken
	d.AtomicBeginPlain(addr, 8, writer, false, true, &token)
	done := make(chan struct{})
	go func() {
		d.ClearShadowRange(addr, 8)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("clear completed before the retained writer capability drained")
	case <-time.After(10 * time.Millisecond):
	}
	d.AtomicEnd(addr, 8, writer, &token, pc+1, true)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("clear did not finish after the writer released its capability")
	}
	if stats := d.atomicArena.Stats(); stats.States != 1 || stats.Frontiers != 1 {
		t.Fatalf("cache ownership did not retain the pruned state after clear: %+v", stats)
	}

	// The cleared address now receives a distinct sidecar. Tearing down the old
	// cache entry must lock and retire only oldState, never the fresh generation.
	freshReader := goroutine.Alloc(70_038)
	warmCachedLoadForTest(t, d, addr, freshReader, pc+2)
	freshState := atomicHistoryForTest(t, d, addr)
	if freshState == oldState {
		t.Fatal("clear reused a state still owned by the old cache entry")
	}
	freshEntry := ctxCacheEntryForTest(t, d, freshReader, addr, pc+2)
	DeactivateAtomicLoadCache(reader)
	freshState.mu.lock()
	freshHead := freshState.readFrontiers
	freshState.mu.unlock()
	if freshHead == nil || unsafe.Pointer(freshHead) != freshEntry.Frontier {
		t.Fatalf("old cache teardown detached fresh frontier: head=%p want=%p", freshHead, freshEntry.Frontier)
	}
	if stats := d.atomicArena.Stats(); stats.States != 1 || stats.Frontiers != 1 {
		t.Fatalf("old cache teardown disturbed fresh state ownership: %+v", stats)
	}
	DeactivateAtomicLoadCache(freshReader)
	d.ClearShadowRange(addr, 8)
	if stats := d.atomicArena.Stats(); stats.States != 0 || stats.Frontiers != 0 {
		t.Fatalf("clear lifecycle retained state or frontier ownership: %+v", stats)
	}
}

func TestAtomicCachedLoadResetDropsStaleStateOwnership(t *testing.T) {
	d := NewDetector()
	oldReader := goroutine.Alloc(70_039)
	const (
		addr = uintptr(0x513b0)
		pc   = uintptr(0x8138)
	)
	warmCachedLoadForTest(t, d, addr, oldReader, pc)
	oldEntry := ctxCacheEntryForTest(t, d, oldReader, addr, pc)

	// Direct detector users may reset without visiting external contexts. Arena
	// reset invalidates the old cache ownership and may immediately reuse both
	// physical slots for a fresh generation.
	d.Reset()
	freshReader := goroutine.Alloc(70_040)
	warmCachedLoadForTest(t, d, addr, freshReader, pc+1)
	freshEntry := ctxCacheEntryForTest(t, d, freshReader, addr, pc+1)
	if freshEntry.State != oldEntry.State || freshEntry.Frontier != oldEntry.Frontier {
		t.Fatalf("reset did not exercise physical state/frontier reuse: old=%p/%p fresh=%p/%p",
			oldEntry.State, oldEntry.Frontier, freshEntry.State, freshEntry.Frontier)
	}
	if freshEntry.StateGeneration == oldEntry.StateGeneration {
		t.Fatalf("reset reused state generation %d", freshEntry.StateGeneration)
	}

	DeactivateAtomicLoadCache(oldReader)
	freshState := atomicHistoryForTest(t, d, addr)
	freshState.mu.lock()
	freshHead := freshState.readFrontiers
	freshState.mu.unlock()
	if freshHead == nil || unsafe.Pointer(freshHead) != freshEntry.Frontier {
		t.Fatalf("stale reset cache detached fresh frontier: head=%p want=%p", freshHead, freshEntry.Frontier)
	}
	if stats := d.atomicArena.Stats(); stats.States != 1 || stats.Frontiers != 1 {
		t.Fatalf("stale reset cache released fresh arena ownership: %+v", stats)
	}
	DeactivateAtomicLoadCache(freshReader)
	d.ClearShadowRange(addr, 8)
	if stats := d.atomicArena.Stats(); stats.States != 0 || stats.Frontiers != 0 {
		t.Fatalf("reset lifecycle retained state or frontier ownership: %+v", stats)
	}
}

func TestAtomicCachedLoadPrunedFrontierArenaPlateaus(t *testing.T) {
	d := NewDetector()
	const (
		addr   = uintptr(0x513c0)
		pc     = uintptr(0x8135)
		cycles = atomicFrontierSlabSize*2 + 1
	)

	for i := 0; i < cycles; i++ {
		reader := goroutine.Alloc(uint32(71_100 + i*2))
		warmCachedLoadForTest(t, d, addr, reader, pc)
		writer := goroutine.AllocWithParentClock(uint32(71_101+i*2), reader.C, 1)
		var token AtomicToken
		d.AtomicBeginPlain(addr, 8, writer, false, true, &token)
		d.AtomicEnd(addr, 8, writer, &token, pc+1, true)
		d.ClearShadowRange(addr, 8)
		if stats := d.atomicArena.Stats(); stats.States != 1 || stats.Frontiers != 1 {
			t.Fatalf("cycle %d lost cache-owned pruned state during clear: %+v", i, stats)
		}
		DeactivateAtomicLoadCache(reader)
		stats := d.atomicArena.Stats()
		if stats.States != 0 || stats.History != 0 || stats.Releases != 0 || stats.Ranges != 0 || stats.Frontiers != 0 {
			t.Fatalf("cycle %d retained live arena objects: %+v", i, stats)
		}
	}
	if got := len(d.atomicArena.states); got != 1 {
		t.Fatalf("pruned state churn allocated %d slabs, want 1", got)
	}
	if got := len(d.atomicArena.frontiers); got != 1 {
		t.Fatalf("pruned frontier churn allocated %d slabs, want 1", got)
	}
	if peak := d.atomicArena.Stats().PeakFrontiers; peak != 1 {
		t.Fatalf("pruned frontier churn peak = %d, want 1", peak)
	}
}

func TestAtomicCachedLoadEvictionFoldsLatestFrontier(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(70_041)
	const pc = uintptr(0x8140)
	addrs := [4]uintptr{0x51400, 0x51500, 0x51600, 0x51700}
	for _, addr := range addrs[:goroutine.AtomicLoadCacheSlots] {
		warmCachedLoadForTest(t, d, addr, reader, pc)
	}
	writer := goroutine.AllocWithParentClock(70_042, reader.C, 1)
	reader.IncrementClock()
	var token AtomicToken
	revision, generation, ok := d.AtomicBeginLoadFast(addrs[0], 8, reader, pc, &token)
	if !ok || !d.AtomicEndLoadFast(reader, &token, revision, generation) {
		t.Fatal("latest cached load did not complete")
	}
	state := atomicHistoryForTest(t, d, addrs[0])
	state.mu.lock()
	canonicalBefore := atomicHistoryCardinality(state.reads)
	state.mu.unlock()
	if canonicalBefore != 0 {
		t.Fatalf("cached load recorded %d canonical reads before eviction, want frontier only", canonicalBefore)
	}
	// The fourth key misses before it has shadow state; miss preparation evicts
	// slot zero and must fold addr[0]'s new epoch before unlinking its node.
	if _, _, ok := d.AtomicBeginLoadFast(addrs[3], 8, reader, pc, &token); ok {
		t.Fatal("unmaterialized fourth address unexpectedly hit")
	}
	state.mu.lock()
	folded, foldedOK := atomicHistoryAccess(state.reads, reader.TID)
	frontiers := state.readFrontiers
	state.mu.unlock()
	if !foldedOK || folded.clocks[0] != uint32(reader.GetEpoch()) || frontiers != nil {
		t.Fatalf("evicted frontier fold = read=%+v/%v frontiers=%p, want latest canonical witness", folded, foldedOK, frontiers)
	}
	d.OnWrite(addrs[0], writer, pc+1)
	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("post-eviction writer saw %d races, want folded cached read", got)
	}
}

func TestAtomicCachedLoadRetainsThreeIdentityWorkingSet(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(70_045)
	const addr = uintptr(0x51440)
	wordSize := unsafe.Sizeof(uintptr(0))
	identities := [...]struct {
		addr uintptr
		pc   uintptr
	}{
		// atomic.Value.Store and atomic.Value.Load read the same type word at
		// distinct call sites; Load then reads the adjacent data word.
		{addr: addr, pc: 0x8145},
		{addr: addr, pc: 0x8146},
		{addr: addr + wordSize, pc: 0x8147},
	}

	// atomic.Value-style loops repeatedly load a small, fixed set of exact
	// identities. Keep all three enrolled so the steady state stays on the
	// cache-only path instead of folding and rebuilding a frontier each cycle.
	var frontiers [len(identities)]unsafe.Pointer
	for i, identity := range identities {
		warmCachedLoadSizedForTest(t, d, identity.addr, wordSize, reader, identity.pc)
		frontiers[i] = ctxCacheEntryForTest(t, d, reader, identity.addr, identity.pc).Frontier
	}

	for cycle := 0; cycle < 32; cycle++ {
		for i, identity := range identities {
			var token AtomicToken
			revision, generation, ok := d.AtomicBeginLoadFast(identity.addr, wordSize, reader, identity.pc, &token)
			if !ok {
				t.Fatalf("cycle %d identity %d missed the resident three-entry working set", cycle, i)
			}
			if !d.AtomicEndLoadFast(reader, &token, revision, generation) {
				t.Fatalf("cycle %d identity %d failed cached-load validation", cycle, i)
			}
			if got := ctxCacheEntryForTest(t, d, reader, identity.addr, identity.pc).Frontier; got != frontiers[i] {
				t.Fatalf("cycle %d identity %d frontier = %p, want resident %p", cycle, i, got, frontiers[i])
			}
		}
	}
}

func TestAtomicCachedLoadGoEndDeactivationFoldsLatestFrontier(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(70_051)
	const addr = uintptr(0x51700)
	const pc = uintptr(0x8150)
	warmCachedLoadForTest(t, d, addr, reader, pc)
	writer := goroutine.AllocWithParentClock(70_052, reader.C, 1)
	reader.IncrementClock()
	var token AtomicToken
	revision, generation, ok := d.AtomicBeginLoadFast(addr, 8, reader, pc, &token)
	if !ok || !d.AtomicEndLoadFast(reader, &token, revision, generation) {
		t.Fatal("latest cached load did not complete")
	}
	wantClock := uint32(reader.GetEpoch())
	reader.IncrementClock()
	state := atomicHistoryForTest(t, d, addr)
	state.mu.lock()
	canonicalBefore := atomicHistoryCardinality(state.reads)
	state.mu.unlock()
	if canonicalBefore != 0 {
		t.Fatalf("cached load recorded %d canonical reads before deactivation, want frontier only", canonicalBefore)
	}
	DeactivateAtomicLoadCache(reader)
	state.mu.lock()
	folded, foldedOK := atomicHistoryAccess(state.reads, reader.TID)
	frontiers := state.readFrontiers
	state.mu.unlock()
	if !foldedOK || folded.clocks[0] != wantClock || frontiers != nil {
		t.Fatalf("deactivated frontier fold = read=%+v/%v frontiers=%p, want cached witness clock %d", folded, foldedOK, frontiers, wantClock)
	}
	d.OnWrite(addr, writer, pc+1)
	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("post-go-end writer saw %d races, want folded cached read", got)
	}
}

func TestAtomicCachedLoadEnrollmentPrunesHBDominatedFoldedCohort(t *testing.T) {
	d := NewDetector()
	parent := goroutine.Alloc(70_053)
	const (
		addr       = uintptr(0x51740)
		pc         = uintptr(0x8154)
		cohortSize = 64
		firstTID   = uint32(72_000)
		secondTID  = uint32(73_000)
	)

	// Every first-cohort reader is a sibling. Fold all of their cached-load
	// witnesses, then join their clocks into the parent just as a phase-ending
	// WaitGroup would before starting the next cohort.
	readers := make([]*goroutine.RaceContext, cohortSize)
	for i := uint32(0); i < cohortSize; i++ {
		readers[i] = goroutine.AllocWithParentClock(firstTID+i, parent.C, 1)
	}
	for _, reader := range readers {
		warmCachedLoadForTest(t, d, addr, reader, pc)
		DeactivateAtomicLoadCache(reader)
		parent.C.Join(reader.C)
		reader.C.Release()
	}
	state := atomicHistoryForTest(t, d, addr)
	state.mu.lock()
	for i := uint32(0); i < cohortSize; i++ {
		if _, ok := state.reads.user.find(firstTID + i); !ok {
			state.mu.unlock()
			t.Fatalf("first-cohort TID %d was not folded", firstTID+i)
		}
	}
	state.mu.unlock()

	// Second-cohort siblings all inherit the joined first cohort. Their first
	// locked loads enroll cached frontiers at the exact read-time clocks;
	// geometric pruning must eventually discard every dominated old witness
	// while retaining the mutually concurrent current cohort.
	for i := uint32(0); i < cohortSize; i++ {
		reader := goroutine.AllocWithParentClock(secondTID+i, parent.C, 1)
		warmCachedLoadForTest(t, d, addr, reader, pc)
		DeactivateAtomicLoadCache(reader)
		reader.C.Release()
	}
	state.mu.lock()
	defer state.mu.unlock()
	for i := uint32(0); i < cohortSize; i++ {
		if _, ok := state.reads.user.find(firstTID + i); ok {
			t.Fatalf("HB-dominated first-cohort TID %d survived enrollment pruning", firstTID+i)
		}
		if _, ok := state.reads.user.find(secondTID + i); !ok {
			t.Fatalf("concurrent second-cohort TID %d was pruned", secondTID+i)
		}
	}
}

func TestAtomicCachedLoadEnrollmentRetainsConcurrentFoldedReaders(t *testing.T) {
	d := NewDetector()
	parent := goroutine.Alloc(70_054)
	const (
		addr       = uintptr(0x51760)
		pc         = uintptr(0x8156)
		cohortSize = 64
		firstTID   = uint32(74_000)
	)

	// Sequential test execution does not create HB: every reader inherits the
	// same parent clock and none imports a sibling. Enrollment pruning must
	// retain all of these concurrent witnesses.
	for i := uint32(0); i < cohortSize; i++ {
		reader := goroutine.AllocWithParentClock(firstTID+i, parent.C, 1)
		warmCachedLoadForTest(t, d, addr, reader, pc)
		DeactivateAtomicLoadCache(reader)
		reader.C.Release()
	}
	state := atomicHistoryForTest(t, d, addr)
	state.mu.lock()
	defer state.mu.unlock()
	if got := state.reads.user.live; got != cohortSize {
		t.Fatalf("concurrent folded history size = %d, want %d", got, cohortSize)
	}
	for i := uint32(0); i < cohortSize; i++ {
		if _, ok := state.reads.user.find(firstTID + i); !ok {
			t.Fatalf("concurrent folded TID %d was pruned", firstTID+i)
		}
	}
}

func TestAtomicCachedLoadTeardownDoesNotPruneFromPostLoadAcquire(t *testing.T) {
	d := NewDetector()
	parent := goroutine.Alloc(70_055)
	older := goroutine.AllocWithParentClock(75_000, parent.C, 1)
	current := goroutine.AllocWithParentClock(75_001, parent.C, 1)
	const (
		addr = uintptr(0x51770)
		pc   = uintptr(0x8157)
	)

	warmCachedLoadForTest(t, d, addr, older, pc)
	DeactivateAtomicLoadCache(older)
	warmCachedLoadForTest(t, d, addr, current, pc)

	// The older read was concurrent with current's cached load. Importing the
	// older context afterward cannot retroactively make it happen before that
	// load, so teardown must not use current's later clock to prune the witness.
	current.C.Join(older.C)
	current.NoteForeignImport()
	DeactivateAtomicLoadCache(current)

	state := atomicHistoryForTest(t, d, addr)
	state.mu.lock()
	defer state.mu.unlock()
	if _, ok := state.reads.user.find(older.TID); !ok {
		t.Fatal("post-load acquire caused teardown to prune a concurrent older read")
	}
	if _, ok := state.reads.user.find(current.TID); !ok {
		t.Fatal("current cached read was not folded at teardown")
	}
}

func TestAtomicNoncachedReadsKeepCanonicalHistory(t *testing.T) {
	for i, test := range []struct {
		name        string
		synchronize bool
	}{
		{name: "synchronized", synchronize: true},
		{name: "non-sync", synchronize: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			d := NewDetector()
			ctx := goroutine.Alloc(uint32(70_055 + i))
			addr := uintptr(0x51780 + i*0x40)
			pc := uintptr(0x8158 + i)
			var token AtomicToken
			d.AtomicBeginPlain(addr, 8, ctx, test.synchronize, test.synchronize, &token)
			d.AtomicEndMode(addr, 8, ctx, &token, pc, false, test.synchronize)

			state := atomicHistoryForTest(t, d, addr)
			state.mu.lock()
			access, ok := atomicHistoryAccess(state.reads, ctx.TID)
			frontiers := state.readFrontiers
			state.mu.unlock()
			if !ok || access.clocks[0] != uint32(ctx.GetEpoch()) || access.pcs[0] != pc || frontiers != nil {
				t.Fatalf("noncached read history = %+v/%v frontiers=%p, want canonical witness", access, ok, frontiers)
			}
		})
	}
}

func TestAtomicCachedLoadWithoutFrontierKeepsCanonicalHistory(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(70_057)
	const pc = uintptr(0x815a)
	for _, addr := range []uintptr{0x51800, 0x51840, 0x51880} {
		warmCachedLoadForTest(t, d, addr, ctx, pc)
	}

	// Direct detector callers need not run FastBegin miss preparation. With all
	// cache slots occupied, a locked cached load therefore has no frontier slot
	// and must retain its witness in the canonical history instead.
	const addr = uintptr(0x518c0)
	completeCachedLoadSlowForTest(d, addr, ctx, pc)
	state := atomicHistoryForTest(t, d, addr)
	state.mu.lock()
	access, ok := atomicHistoryAccess(state.reads, ctx.TID)
	frontiers := state.readFrontiers
	state.mu.unlock()
	if !ok || access.clocks[0] != uint32(ctx.GetEpoch()) || access.pcs[0] != pc || frontiers != nil {
		t.Fatalf("frontierless cached read history = %+v/%v frontiers=%p, want canonical witness", access, ok, frontiers)
	}
}

func TestAtomicCachedLoadReusesContextOwnedNode(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(70_061)
	const addr = uintptr(0x51800)
	var firstPointers [goroutine.AtomicLoadCacheSlots]unsafe.Pointer
	for i := 0; i < goroutine.AtomicLoadCacheSlots; i++ {
		warmCachedLoadForTest(t, d, addr, ctx, uintptr(0x8160+i))
		firstPointers[i] = ctx.AtomicLoadCache[i].Frontier
	}
	var token AtomicToken
	if _, _, ok := d.AtomicBeginLoadFast(addr, 8, ctx, 0x8170, &token); ok {
		t.Fatal("new PC unexpectedly hit a full cache")
	}
	completeCachedLoadSlowForTest(d, addr, ctx, 0x8170)
	state := atomicHistoryForTest(t, d, addr)
	state.mu.lock()
	count := 0
	for node := state.readFrontiers; node != nil; node = node.next {
		count++
	}
	state.mu.unlock()
	if count != goroutine.AtomicLoadCacheSlots {
		t.Fatalf("active frontier nodes = %d, want cache bound %d", count, goroutine.AtomicLoadCacheSlots)
	}
	reused := false
	for i := range ctx.AtomicLoadCache {
		for _, old := range firstPointers {
			if ctx.AtomicLoadCache[i].Frontier == old {
				reused = true
			}
		}
	}
	if !reused {
		t.Fatal("cache eviction allocated instead of reusing a context-owned node")
	}
}

func TestAtomicCachedLoadRefreshBalancesStateOwnership(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(70_063)
	const addr = uintptr(0x51840)
	for i := 0; i < goroutine.AtomicLoadCacheSlots; i++ {
		warmCachedLoadForTest(t, d, addr, ctx, uintptr(0x8171+i))
	}
	for cycle := 0; cycle < 32; cycle++ {
		for i := 0; i < goroutine.AtomicLoadCacheSlots; i++ {
			// Locked refreshes update an existing same-state entry. They must keep
			// its one ownership reference rather than retaining on every refresh.
			completeCachedLoadSlowForTest(d, addr, ctx, uintptr(0x8171+i))
		}
	}
	d.ClearShadowRange(addr, 8)
	if stats := d.atomicArena.Stats(); stats.States != 1 || stats.Frontiers != goroutine.AtomicLoadCacheSlots {
		t.Fatalf("cache did not retain one cleared state with %d frontiers: %+v", goroutine.AtomicLoadCacheSlots, stats)
	}
	DeactivateAtomicLoadCache(ctx)
	stats := d.atomicArena.Stats()
	if stats.States != 0 || stats.History != 0 || stats.Releases != 0 || stats.Ranges != 0 || stats.Frontiers != 0 {
		t.Fatalf("cache teardown retained arena ownership after refresh churn: %+v", stats)
	}
}

func TestAtomicCachedLoadDescriptorRegenerationDoesNotAliasFrontier(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(70_065)
	escaper := goroutine.Alloc(70_066)
	const (
		addr    = uintptr(0x51880)
		other   = uintptr(0x51a00)
		pc      = uintptr(0x8175)
		otherPC = uintptr(0x8176)
	)
	warmCachedLoadForTest(t, d, addr, reader, pc)
	oldFast := ctxCacheEntryForTest(t, d, reader, addr, pc).Fast

	// A general transaction closes F1 while retaining its atomicState and read
	// frontier. Two compatible loads then pass probation and publish F2. The F2
	// cache entry must own a different node rather than finding F1's node by TID.
	var token AtomicToken
	d.AtomicBegin(addr, 8, escaper, true, &token)
	d.AtomicEnd(addr, 8, escaper, &token, pc+10, false)
	for i := 0; i < 2; i++ {
		if _, _, ok := d.AtomicBeginLoadFast(addr, 8, reader, pc, &token); ok {
			t.Fatal("escaped capability unexpectedly remained cache-fast")
		}
		completeCachedLoadSlowForTest(d, addr, reader, pc)
	}
	var fresh goroutine.AtomicLoadCacheEntry
	for _, entry := range reader.AtomicLoadCache {
		if entry.PC == pc && entry.Fast != nil && entry.Fast != oldFast {
			fresh = entry
			break
		}
	}
	if fresh.Fast == nil {
		t.Fatal("compatible loads did not publish a fresh capability generation")
	}

	// Fill the third cache slot so the unrelated miss evicts F1's slot and
	// reuses its detached node in another state. An old implementation let F2
	// alias that node; its ensuing generation miss then repurposed the still-live
	// other-state node and linked one mutable object into both lists.
	warmCachedLoadForTest(t, d, other-0x40, reader, otherPC-1)
	if _, _, ok := d.AtomicBeginLoadFast(other, 8, reader, otherPC, &token); ok {
		t.Fatal("unmaterialized other address unexpectedly hit")
	}
	completeCachedLoadSlowForTest(d, other, reader, otherPC)
	if revision, generation, ok := d.AtomicBeginLoadFast(addr, 8, reader, pc, &token); ok {
		if !d.AtomicEndLoadFast(reader, &token, revision, generation) {
			t.Fatal("fresh descriptor cache hit failed validation")
		}
	} else {
		completeCachedLoadSlowForTest(d, addr, reader, pc)
	}

	addrState := atomicHistoryForTest(t, d, addr)
	otherState := atomicHistoryForTest(t, d, other)
	addrState.mu.lock()
	addrNode := addrState.readFrontiers
	addrNext := (*atomicReadFrontier)(nil)
	if addrNode != nil {
		addrNext = addrNode.next
	}
	addrState.mu.unlock()
	otherState.mu.lock()
	otherNode := otherState.readFrontiers
	otherNext := (*atomicReadFrontier)(nil)
	if otherNode != nil {
		otherNext = otherNode.next
	}
	otherState.mu.unlock()
	if addrNode == nil || otherNode == nil || addrNode == otherNode {
		t.Fatalf("frontier ownership corrupted: addr=%p other=%p", addrNode, otherNode)
	}
	if addrNext != nil || otherNext != nil {
		t.Fatalf("unexpected duplicate/self-linked frontiers: addr next=%p other next=%p", addrNext, otherNext)
	}
}

func TestAtomicCachedLoadStaleGenerationCannotDonateActiveSpare(t *testing.T) {
	a := newAtomicHistoryArena()
	oldState := a.newState(0x51a80)
	liveState := a.newState(0x51ac0)
	a.retainState(oldState)
	a.retainState(liveState)
	node := a.allocFrontier()
	node.tid = 70_067
	node.pc = 0x8177
	node.mask.Store(0xff)
	node.clock.Store(1)
	liveState.readFrontiers = node
	entry := goroutine.AtomicLoadCacheEntry{
		State: unsafe.Pointer(oldState), Frontier: unsafe.Pointer(node), Generation: node.generation.Load() - 1,
		StateGeneration: oldState.handle.generation,
	}
	ctx := goroutine.Alloc(70_067)
	ctx.AtomicLoadCache[0] = entry
	prepareAtomicLoadCache(ctx, nil, nil, 0xff, 0x8177, false)
	if spare := ctx.AtomicLoadCache[0].Frontier; spare != nil {
		t.Fatalf("stale generation donated active node %p as a spare", spare)
	}
	if liveState.readFrontiers != node || node.mask.Load() != 0xff {
		t.Fatal("stale detach mutated node owned by another state")
	}
	if stats := a.Stats(); stats.States != 1 || stats.Frontiers != 1 {
		t.Fatalf("stale cache eviction ownership balance = %+v, want one live state/frontier", stats)
	}
	liveEntry := goroutine.AtomicLoadCacheEntry{
		State: unsafe.Pointer(liveState), Frontier: unsafe.Pointer(node), Generation: node.generation.Load(),
		StateGeneration: liveState.handle.generation,
	}
	spare := deactivateAtomicLoadEntry(liveEntry)
	if spare != node {
		t.Fatalf("live cache owner detached %p, want %p", spare, node)
	}
	a.freeFrontierNode(spare)
	if stats := a.Stats(); stats.States != 0 || stats.Frontiers != 0 {
		t.Fatalf("stale-generation test retained arena ownership: %+v", stats)
	}
}

func TestAtomicCachedLoadPrunedInFlightNodeReusesOwnSlot(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(70_068)
	const addr = uintptr(0x51b00)
	const pc = uintptr(0x8178)
	warmCachedLoadForTest(t, d, addr, reader, pc)
	before := ctxCacheEntryForTest(t, d, reader, addr, pc)

	var loadToken AtomicToken
	revision, generation, ok := d.AtomicBeginLoadFast(addr, 8, reader, pc, &loadToken)
	if !ok {
		t.Fatal("warmed cached load missed")
	}
	writer := goroutine.AllocWithParentClock(70_069, reader.C, 1)
	var writeToken AtomicToken
	d.AtomicBeginPlain(addr, 8, writer, false, true, &writeToken)
	d.AtomicEnd(addr, 8, writer, &writeToken, pc+1, true)
	if d.AtomicEndLoadFast(reader, &loadToken, revision, generation) {
		t.Fatal("cached load crossed an HB writer which pruned its node")
	}
	completeCachedLoadSlowForTest(d, addr, reader, pc)
	after := ctxCacheEntryForTest(t, d, reader, addr, pc)
	if after.Frontier != before.Frontier || after.Generation <= before.Generation {
		t.Fatalf("pruned slot was not safely reused: before=%p/%d after=%p/%d", before.Frontier, before.Generation, after.Frontier, after.Generation)
	}
	state := atomicHistoryForTest(t, d, addr)
	state.mu.lock()
	node := state.readFrontiers
	state.mu.unlock()
	if node == nil || unsafe.Pointer(node) != after.Frontier || node.next != nil {
		t.Fatalf("reused frontier list corrupted: head=%p cache=%p", node, after.Frontier)
	}
}

func ctxCacheEntryForTest(t *testing.T, d *Detector, ctx *goroutine.RaceContext, addr, pc uintptr) goroutine.AtomicLoadCacheEntry {
	t.Helper()
	state := atomicHistoryForTest(t, d, addr)
	for _, entry := range ctx.AtomicLoadCache {
		if entry.State == unsafe.Pointer(state) && entry.PC == pc && entry.Frontier != nil {
			return entry
		}
	}
	t.Fatalf("no atomic load cache entry for address %#x pc %#x", addr, pc)
	return goroutine.AtomicLoadCacheEntry{}
}

func TestAtomicCachedLoadPartialPruneEvictionFoldsSurvivingLanes(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(70_071)
	const (
		addr     = uintptr(0x51900)
		other    = uintptr(0x51a00)
		padding  = uintptr(0x51b00)
		eviction = uintptr(0x51c00)
		pc       = uintptr(0x8180)
	)
	warmCachedLoadForTest(t, d, addr, reader, pc)
	warmCachedLoadForTest(t, d, other, reader, pc)
	warmCachedLoadForTest(t, d, padding, reader, pc)
	for i, entry := range reader.AtomicLoadCache {
		if entry.Fast == nil || entry.State == nil || entry.Frontier == nil {
			t.Fatalf("cache slot %d is not filled before eviction: %+v", i, entry)
		}
	}
	before := ctxCacheEntryForTest(t, d, reader, addr, pc)
	victim := reader.AtomicLoadCache[reader.AtomicLoadCacheNext%goroutine.AtomicLoadCacheSlots]
	if victim != before {
		t.Fatalf("next eviction victim = %+v, want partial frontier entry %+v", victim, before)
	}
	ordinaryWriter := goroutine.AllocWithParentClock(70_072, reader.C, 1)
	reader.IncrementClock()
	var token AtomicToken
	revision, generation, ok := d.AtomicBeginLoadFast(addr, 8, reader, pc, &token)
	if !ok || !d.AtomicEndLoadFast(reader, &token, revision, generation) {
		t.Fatal("latest cached load did not complete")
	}
	atomicWriter := goroutine.AllocWithParentClock(70_073, reader.C, 1)
	d.AtomicBeginPlain(addr, 4, atomicWriter, false, true, &token)
	d.AtomicEnd(addr, 4, atomicWriter, &token, pc+1, true)
	frontier := (*atomicReadFrontier)(before.Frontier)
	if got := uint8(frontier.mask.Load()); got != 0xf0 {
		t.Fatalf("low-half HB write left frontier mask %#x, want high-half mask %#x", got, uint8(0xf0))
	}

	// The low-half HB write deactivates only those lanes. With all slots full,
	// an unrelated miss selects this partial node as the round-robin victim and
	// must fold its surviving high half before unlinking it.
	if _, _, ok := d.AtomicBeginLoadFast(eviction, 8, reader, pc, &token); ok {
		t.Fatal("unmaterialized eviction address unexpectedly hit")
	}
	state := atomicHistoryForTest(t, d, addr)
	state.mu.lock()
	folded, foldedOK := atomicHistoryAccess(state.reads, reader.TID)
	frontiers := state.readFrontiers
	state.mu.unlock()
	if frontiers != nil || frontier.mask.Load() != 0 {
		t.Fatalf("evicted partial frontier remained linked: head=%p mask=%#x", frontiers, frontier.mask.Load())
	}
	for lane := uint8(0); lane < AtomicTokenSlots; lane++ {
		want := uint32(0)
		if lane >= 4 {
			want = uint32(reader.GetEpoch())
		}
		if !foldedOK || folded.clocks[lane] != want {
			t.Fatalf("folded lane %d clock = %d/%v, want %d", lane, folded.clocks[lane], foldedOK, want)
		}
	}
	d.OnWrite(addr+4, ordinaryWriter, pc+2)
	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("high-half writer saw %d races, want folded surviving read", got)
	}
}

func TestAtomicCachedLoadFastMissReusesFullyPrunedNode(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(71_001)
	const addr = uintptr(0x52100)
	const pc = uintptr(0x8210)
	warmCachedLoadForTest(t, d, addr, reader, pc)
	before := ctxCacheEntryForTest(t, d, reader, addr, pc)

	writer := goroutine.AllocWithParentClock(71_002, reader.C, 1)
	var token AtomicToken
	d.AtomicBeginPlain(addr, 8, writer, false, true, &token)
	d.AtomicEnd(addr, 8, writer, &token, pc+1, true)
	if got := (*atomicReadFrontier)(before.Frontier).mask.Load(); got != 0 {
		t.Fatalf("HB writer left frontier mask %#x, want fully pruned", got)
	}

	// Exercise the runtime sequence: speculative FastBegin miss, followed by
	// the trusted locked load. The miss must preserve the slot-owned detached
	// node instead of dropping it and allocating a replacement.
	if _, _, ok := d.AtomicBeginLoadFast(addr, 8, reader, pc, &token); ok {
		t.Fatal("writer revision unexpectedly retained cached hit")
	}
	completeCachedLoadSlowForTest(d, addr, reader, pc)
	after := ctxCacheEntryForTest(t, d, reader, addr, pc)
	if after.Frontier != before.Frontier || after.Generation <= before.Generation {
		t.Fatalf("pruned FastBegin miss did not reuse node: before=%p/%d after=%p/%d", before.Frontier, before.Generation, after.Frontier, after.Generation)
	}
	if revision, generation, ok := d.AtomicBeginLoadFast(addr, 8, reader, pc, &token); !ok || !d.AtomicEndLoadFast(reader, &token, revision, generation) {
		t.Fatal("relinked node did not produce stable cached hit")
	}
}
