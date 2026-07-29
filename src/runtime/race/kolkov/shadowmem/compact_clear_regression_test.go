//go:build amd64 || arm64

package shadowmem

import (
	"runtime"
	"testing"
	"time"

	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/vectorclock"
)

func compactClearClock(current epoch.Epoch) *vectorclock.VectorClock {
	clock := vectorclock.New()
	tid, value := current.Decode()
	clock.Set(tid, uint32(value))
	return clock
}

func TestPageTableEmptyCompactClearRetainsLifecycleUntilReaccess(t *testing.T) {
	pt := NewPageTableShadow()
	const (
		base   = uintptr(1) << 40
		seed   = base + 17
		target = base + 101
	)
	seedEpoch := epoch.NewEpoch(37, 3)
	if !pt.TryCompactWrite(seed, seedEpoch, compactClearClock(seedEpoch), 0x7801) {
		t.Fatal("compact metadata setup failed")
	}
	view, ok := pt.blockFor(seed, false)
	if !ok {
		t.Fatal("compact block setup disappeared")
	}
	groups := view.history.compact.Load()
	if groups == nil || view.history.state.Load() != nil {
		t.Fatalf("setup compact=%p default=%p, want compact metadata without inherited default", groups, view.history.state.Load())
	}
	before := groups.lifecycle
	pt.ClearRange(target, 1)
	pt.ClearRange(target, 1)
	if groups.lifecycle != before {
		t.Fatalf("empty repeated clear advanced lifecycle from %v to %v", before, groups.lifecycle)
	}
	if slot := pt.GetSlot(target); slot != nil {
		t.Fatalf("empty compact clear materialized slot %p", slot)
	}
	if state, authoritative := groups.lookupExact(target); state != nil || !authoritative {
		t.Fatalf("empty compact clear lookup=(%p,%v), want authoritative zero", state, authoritative)
	}

	accessEpoch := epoch.NewEpoch(38, 5)
	if !pt.TryCompactWrite(target, accessEpoch, compactClearClock(accessEpoch), 0x7802) {
		t.Fatal("post-clear compact reaccess failed")
	}
	accessed := pt.Get(target)
	if accessed == nil || accessed.GetW() != accessEpoch || accessed.GetLifecycleID() != before.uint64() {
		t.Fatalf("post-clear history=%v, want W=%v lifecycle=%d", accessed, accessEpoch, before.uint64())
	}
	pt.ClearRange(target, 1)
	if groups.lifecycle == before {
		t.Fatal("clear after represented reaccess did not advance lifecycle")
	}
	freshEpoch := epoch.NewEpoch(39, 7)
	if !pt.TryCompactWrite(target, freshEpoch, compactClearClock(freshEpoch), 0x7803) {
		t.Fatal("fresh compact reaccess failed")
	}
	fresh := pt.Get(target)
	if fresh == nil || fresh.GetW() != freshEpoch || fresh.GetLifecycleID() == accessed.GetLifecycleID() {
		t.Fatalf("fresh history=%v, want W=%v and lifecycle distinct from %v", fresh, freshEpoch, accessed.GetLifecycleID())
	}
}

func TestPageTablePartialCompactDefaultClearAllocatesNothing(t *testing.T) {
	pt := NewPageTableShadow()
	const (
		base      = uintptr(1) << 40
		clearRuns = 32
	)
	current := epoch.NewEpoch(21, 7)
	clock := compactClearClock(current)

	// Each measured clear starts in a different unmaterialized word. Lane 3
	// has exact compact history and lane 4 inherits the block default, so one
	// two-byte clear exercises both sources without setup allocations entering
	// the measurement. AllocsPerRun performs one unmeasured warm-up call.
	addresses := make([]uintptr, clearRuns+1)
	for i := range addresses {
		addr := base + uintptr(i)*8 + 3
		addresses[i] = addr
		if !pt.TryCompactWrite(addr, current, clock, 0x1111) {
			t.Fatalf("compact setup %d failed", i)
		}
	}
	pt.AccessRange(base, rangeBlockSize, func(_ uintptr, _ uint8, _ *VarState) {})

	oldCompact := pt.Get(addresses[0])
	oldDefault := pt.Get(addresses[0] - 1)
	if oldCompact == nil || oldDefault == nil || oldCompact == oldDefault {
		t.Fatalf("setup histories: compact=%p default=%p", oldCompact, oldDefault)
	}
	for _, addr := range addresses {
		if slot := pt.GetSlot(addr); slot != nil {
			t.Fatalf("setup word %#x materialized slot %p", addr&^uintptr(7), slot)
		}
	}

	next := 0
	allocs := testing.AllocsPerRun(clearRuns, func() {
		pt.ClearRange(addresses[next], 2)
		next++
	})
	if allocs != 0 {
		t.Fatalf("partial compact/default clear allocated %.2f objects/op", allocs)
	}
	if next != len(addresses) {
		t.Fatalf("clear calls = %d, want %d including warm-up", next, len(addresses))
	}

	for _, addr := range addresses {
		if slot := pt.GetSlot(addr); slot != nil {
			t.Fatalf("allocation-free clear materialized word %#x: %p", addr&^uintptr(7), slot)
		}
		if got := pt.Get(addr); got != nil {
			t.Fatalf("cleared compact lane %#x = %p, want nil", addr, got)
		}
		if got := pt.Get(addr + 1); got != nil {
			t.Fatalf("cleared default lane %#x = %p, want nil", addr+1, got)
		}
		if got := pt.Get(addr - 1); got != oldDefault {
			t.Fatalf("left adjacent lane %#x = %p, want default %p", addr-1, got, oldDefault)
		}
		if got := pt.Get(addr + 2); got != oldDefault {
			t.Fatalf("right adjacent lane %#x = %p, want default %p", addr+2, got, oldDefault)
		}
	}

	freshEpoch := epoch.NewEpoch(22, 9)
	if !pt.TryCompactWrite(addresses[0], freshEpoch, compactClearClock(freshEpoch), 0x2222) {
		t.Fatal("first access after tombstone did not adopt fresh compact history")
	}
	fresh := pt.Get(addresses[0])
	if fresh == nil || fresh.GetW() != freshEpoch {
		t.Fatalf("fresh compact state = %v, want W=%v", fresh, freshEpoch)
	}
	if fresh.GetLifecycleID() == oldCompact.GetLifecycleID() ||
		fresh.GetLifecycleID() == oldDefault.GetLifecycleID() {
		t.Fatalf("fresh lifecycle %d reused cleared generation (compact=%d default=%d)",
			fresh.GetLifecycleID(), oldCompact.GetLifecycleID(), oldDefault.GetLifecycleID())
	}
	if got := pt.Get(addresses[0] + 1); got != nil {
		t.Fatalf("adjacent tombstone changed during adoption: %p", got)
	}
}

func TestPageTableFullClearRetiresDefaultBeforeCompact(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldProcs)

	pt := NewPageTableShadow()
	const (
		base = uintptr(1) << 40
		addr = base + 0x123
	)
	current := epoch.NewEpoch(23, 11)
	if !pt.TryCompactWrite(addr, current, compactClearClock(current), 0x3333) {
		t.Fatal("compact setup failed")
	}
	pt.AccessRange(base, rangeBlockSize, func(_ uintptr, _ uint8, _ *VarState) {})

	compactState := pt.Get(addr)
	defaultState := pt.Get(base)
	view, ok := pt.blockFor(addr, false)
	if !ok || compactState == nil || defaultState == nil || compactState == defaultState {
		t.Fatalf("setup view=%v compact=%p default=%p", ok, compactState, defaultState)
	}
	compact := view.history.compact.Load()
	if compact == nil || pt.GetSlot(addr) != nil {
		t.Fatalf("setup compact=%p slot=%p", compact, pt.GetSlot(addr))
	}

	// Pin the old default transaction. Correct full-clear ordering must block
	// before compact.reset; the old ordering reset membership first and then
	// blocked here, leaving a stable window in which Get fell through to the
	// unrelated old default.
	defaultState.LockAccess()
	done := make(chan struct{})
	go func() {
		pt.ClearRange(base, rangeBlockSize)
		close(done)
	}()

	deadline := time.Now().Add(time.Second)
	for view.history.mu.state.Load() == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if view.history.mu.state.Load() == 0 {
		defaultState.UnlockAccess()
		<-done
		t.Fatal("full clear did not acquire the block lock")
	}

	var stale *VarState
	observeUntil := time.Now().Add(25 * time.Millisecond)
	for time.Now().Before(observeUntil) {
		_, authoritative := compact.lookupExact(addr)
		if !authoritative && view.history.state.Load() == defaultState {
			stale = pt.Get(addr)
			break
		}
		runtime.Gosched()
	}
	if stale == nil {
		if got := pt.Get(addr); got != compactState {
			stale = got
		}
	}

	defaultState.UnlockAccess()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("full clear did not finish after releasing the default transaction")
	}
	if stale != nil {
		t.Fatalf("Get observed %p after compact retirement while old default %p remained published", stale, defaultState)
	}
	if got := pt.Get(addr); got != nil {
		t.Fatalf("full clear left state %p", got)
	}
	if view.history.state.Load() != nil {
		t.Fatal("full clear retained block default")
	}
	if _, authoritative := compact.lookupExact(addr); authoritative {
		t.Fatal("full clear retained compact membership or tombstone")
	}
}

func TestPageTableTombstoneLookupAdoptionAndMaterialization(t *testing.T) {
	pt := NewPageTableShadow()
	const (
		base = uintptr(1) << 40
		word = base + 0x180
		addr = word + 2
	)
	defaultWrite := epoch.NewEpoch(24, 13)
	pt.AccessRange(base, rangeBlockSize, func(_ uintptr, _ uint8, state *VarState) {
		state.SetW(defaultWrite)
	})
	defaultState := pt.Get(word)
	view, ok := pt.blockFor(addr, false)
	if !ok || defaultState == nil {
		t.Fatalf("default setup view=%v state=%p", ok, defaultState)
	}
	compact := view.history.compact.Load()
	if compact == nil {
		t.Fatal("block default was published without compact metadata")
	}

	pt.ClearRange(addr, 1)
	if slot := pt.GetSlot(addr); slot != nil {
		t.Fatalf("partial clear materialized slot %p", slot)
	}
	if state, authoritative := compact.lookupExact(addr); state != nil || !authoritative {
		t.Fatalf("tombstone lookup = (%p,%v), want authoritative nil", state, authoritative)
	}
	if state, authoritative := compact.lookupExact(addr - 1); state != nil || authoritative {
		t.Fatalf("uncovered lookup = (%p,%v), want non-authoritative miss", state, authoritative)
	}
	if got := pt.Get(addr); got != nil {
		t.Fatalf("page-table tombstone fell through to default: %p", got)
	}
	if got := pt.Get(addr - 1); got != defaultState {
		t.Fatalf("uncovered lane = %p, want default %p", got, defaultState)
	}

	adoptEpoch := epoch.NewEpoch(25, 17)
	if !pt.TryCompactWrite(addr, adoptEpoch, compactClearClock(adoptEpoch), 0x4444) {
		t.Fatal("tombstoned lane did not admit compact adoption over a live default")
	}
	adopted := pt.Get(addr)
	if adopted == nil || adopted.GetW() != adoptEpoch || compact.isTombstone(addr) {
		t.Fatalf("adopted state=%v tombstone=%v, want W=%v and retired tombstone",
			adopted, compact.isTombstone(addr), adoptEpoch)
	}
	if adopted.GetLifecycleID() == defaultState.GetLifecycleID() {
		t.Fatal("compact adoption reused the pre-clear default lifecycle")
	}

	// Leave a second authoritative-zero lane in the same word, then force
	// normal materialization. Publication must retain the adopted lane, exclude
	// the tombstone from the default clone, and only then retire compact sources.
	pt.ClearRange(addr+1, 1)
	if state, authoritative := compact.lookupExact(addr + 1); state != nil || !authoritative {
		t.Fatalf("second tombstone lookup = (%p,%v)", state, authoritative)
	}
	slot := pt.GetOrCreateSlot(word)
	materialized := slot.State(uint8(addr & 7))
	if materialized == nil || materialized == adopted || materialized.GetW() != adoptEpoch ||
		materialized.GetLifecycleID() != adopted.GetLifecycleID() {
		t.Fatalf("materialized adopted history=%v (source %p lifecycle %d)",
			materialized, adopted, adopted.GetLifecycleID())
	}
	if got := slot.State(uint8((addr + 1) & 7)); got != nil {
		t.Fatalf("materialized tombstone lane = %p, want nil", got)
	}
	if got := slot.State(0); got == nil || got.GetW() != defaultWrite {
		t.Fatalf("materialized uncovered lane = %v, want default W=%v", got, defaultWrite)
	}
	if _, authoritative := compact.lookupExact(addr); authoritative {
		t.Fatal("materialization retained adopted compact membership")
	}
	if _, authoritative := compact.lookupExact(addr + 1); authoritative {
		t.Fatal("materialization retained compact tombstone")
	}
	if got := pt.Get(addr + 1); got != nil {
		t.Fatalf("materialized nil lane fell through to block default: %p", got)
	}

	fresh := pt.GetOrCreate(addr + 1)
	if fresh == nil || fresh == adopted || fresh == defaultState ||
		fresh.GetLifecycleID() == adopted.GetLifecycleID() ||
		fresh.GetLifecycleID() == defaultState.GetLifecycleID() {
		t.Fatalf("post-materialization lifecycle = state %p id %d; adopted=%p/%d default=%p/%d",
			fresh, fresh.GetLifecycleID(), adopted, adopted.GetLifecycleID(),
			defaultState, defaultState.GetLifecycleID())
	}
}

func TestPageTableFullBlockCallbacksFollowAddressOrder(t *testing.T) {
	pt := NewPageTableShadow()
	const (
		base         = uintptr(1) << 40
		materialized = base + 0x21
		compactAddr  = base + 0x303
	)
	compactEpoch := epoch.NewEpoch(26, 19)
	if !pt.TryCompactWrite(compactAddr, compactEpoch, compactClearClock(compactEpoch), 0x5555) {
		t.Fatal("later compact setup failed")
	}
	lowState := pt.GetOrCreate(materialized)
	if lowState == nil {
		t.Fatal("lower materialized setup failed")
	}
	if pt.GetSlot(materialized) == nil || pt.GetSlot(compactAddr) != nil {
		t.Fatalf("setup slots: lower=%p compact=%p", pt.GetSlot(materialized), pt.GetSlot(compactAddr))
	}

	var representatives []uintptr
	emptyMask := false
	pt.AccessRange(base, rangeBlockSize, func(word uintptr, mask uint8, _ *VarState) {
		if mask == 0 {
			emptyMask = true
			return
		}
		representatives = append(representatives, word+uintptr(firstLane(mask)))
	})
	if emptyMask {
		t.Fatal("full-block callback used an empty mask")
	}
	if len(representatives) < 4 {
		t.Fatalf("callbacks = %v, want default, materialized groups, and compact", representatives)
	}
	for i := 1; i < len(representatives); i++ {
		if representatives[i] < representatives[i-1] {
			t.Fatalf("callback order regressed at %d: %v", i, representatives)
		}
	}
	if representatives[0] != base {
		t.Fatalf("first callback representative = %#x, want lowest default address %#x", representatives[0], base)
	}
	foundMaterialized := false
	foundCompact := false
	for _, representative := range representatives {
		foundMaterialized = foundMaterialized || representative == materialized
		foundCompact = foundCompact || representative == compactAddr
	}
	if !foundMaterialized || !foundCompact {
		t.Fatalf("callbacks %v omitted materialized=%#x or compact=%#x", representatives, materialized, compactAddr)
	}
}

func TestPageTableFullBlockAccessTransitionsTombstoneExactly(t *testing.T) {
	pt := NewPageTableShadow()
	const (
		base        = uintptr(1) << 40
		word        = base + 0x280
		compactAddr = word + 2
		tombAddr    = word + 5
	)
	compactWrite := epoch.NewEpoch(27, 23)
	defaultWrite := epoch.NewEpoch(28, 29)
	if !pt.TryCompactWrite(compactAddr, compactWrite, compactClearClock(compactWrite), 0x6666) {
		t.Fatal("compact setup failed")
	}
	pt.AccessRange(base, rangeBlockSize, func(_ uintptr, _ uint8, _ *VarState) {})

	oldCompact := pt.Get(compactAddr)
	oldDefault := pt.Get(tombAddr - 1)
	if oldCompact == nil || oldDefault == nil || oldCompact == oldDefault {
		t.Fatalf("setup histories: compact=%p default=%p", oldCompact, oldDefault)
	}
	oldDefault.LockAccess()
	oldDefault.SetW(defaultWrite)
	oldDefault.UnlockAccess()
	if slot := pt.GetSlot(tombAddr); slot != nil {
		t.Fatalf("setup word unexpectedly materialized: %p", slot)
	}

	pt.ClearRange(tombAddr, 1)
	view, ok := pt.blockFor(tombAddr, false)
	if !ok {
		t.Fatal("cleared block disappeared")
	}
	compact := view.history.compact.Load()
	if compact == nil {
		t.Fatal("clear lost compact metadata")
	}
	if state, authoritative := compact.lookupExact(tombAddr); state != nil || !authoritative {
		t.Fatalf("pre-range tombstone = (%p,%v), want authoritative nil", state, authoritative)
	}
	if got := pt.Get(compactAddr); got != oldCompact {
		t.Fatalf("clear changed adjacent compact history: got %p want %p", got, oldCompact)
	}
	if got := pt.Get(tombAddr - 1); got != oldDefault {
		t.Fatalf("clear changed adjacent default history: got %p want %p", got, oldDefault)
	}

	const transitionedPC = uintptr(0x7777)
	tombWrite := epoch.NewEpoch(29, 31)
	tombVisits := 0
	var tombMask uint8
	var tombInitialWrite epoch.Epoch
	pt.AccessRange(base, rangeBlockSize, func(callbackWord uintptr, mask uint8, state *VarState) {
		state.SetReadPC(transitionedPC)
		if callbackWord == word && mask&(uint8(1)<<(tombAddr&7)) != 0 {
			tombVisits++
			tombMask = mask
			tombInitialWrite = state.GetW()
			state.SetW(tombWrite)
		}
	})

	if tombVisits != 1 || tombMask != uint8(1)<<(tombAddr&7) || tombInitialWrite != 0 {
		t.Fatalf("tombstone transition: visits=%d mask=%#x initialW=%v, want one exact zero callback",
			tombVisits, tombMask, tombInitialWrite)
	}
	slot := pt.GetSlot(tombAddr)
	if slot == nil {
		t.Fatal("full-block access did not materialize tombstone word")
	}
	tombState := pt.Get(tombAddr)
	if tombState == nil || tombState.GetW() != tombWrite || tombState.GetReadPC() != transitionedPC {
		t.Fatalf("transitioned tombstone state = %v, want W=%v readPC=%#x", tombState, tombWrite, transitionedPC)
	}
	if tombState.GetLifecycleID() == oldCompact.GetLifecycleID() ||
		tombState.GetLifecycleID() == oldDefault.GetLifecycleID() {
		t.Fatalf("transitioned tombstone reused old lifecycle %d (compact=%d default=%d)",
			tombState.GetLifecycleID(), oldCompact.GetLifecycleID(), oldDefault.GetLifecycleID())
	}
	compactState := pt.Get(compactAddr)
	if compactState == nil || compactState.GetW() != compactWrite || compactState.GetReadPC() != transitionedPC ||
		compactState.GetLifecycleID() != oldCompact.GetLifecycleID() {
		t.Fatalf("adjacent compact history after range = %v, want W=%v lifecycle=%d readPC=%#x",
			compactState, compactWrite, oldCompact.GetLifecycleID(), transitionedPC)
	}
	defaultState := pt.Get(tombAddr - 1)
	if defaultState == nil || defaultState.GetW() != defaultWrite || defaultState.GetReadPC() != transitionedPC ||
		defaultState.GetLifecycleID() != oldDefault.GetLifecycleID() {
		t.Fatalf("adjacent default history after range = %v, want W=%v lifecycle=%d readPC=%#x",
			defaultState, defaultWrite, oldDefault.GetLifecycleID(), transitionedPC)
	}
	if _, authoritative := compact.lookupExact(tombAddr); authoritative || compact.isTombstone(tombAddr) {
		t.Fatal("full-block access retained tombstone after slot publication")
	}
	if _, authoritative := compact.lookupExact(compactAddr); authoritative {
		t.Fatal("full-block access retained compact membership after word materialization")
	}
}
