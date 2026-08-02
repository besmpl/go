//go:build amd64 || arm64

package shadowmem

import (
	"testing"
	"unsafe"

	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/vectorclock"
)

type ordinaryFastSnapshot struct {
	write           epoch.Epoch
	read            epoch.Epoch
	exclusiveWriter int64
	writePC         uintptr
	readPC          uintptr
	writeCount      uint32
	lifecycle       uint64
}

func snapshotOrdinaryFastState(state *VarState) ordinaryFastSnapshot {
	state.mu.lock()
	defer state.mu.unlock()
	return ordinaryFastSnapshot{
		write:           epoch.Epoch(state.W.Load()),
		read:            epoch.Epoch(state.readEpoch0.Load()),
		exclusiveWriter: state.exclusiveWriter.Load(),
		writePC:         state.writePC.Load(),
		readPC:          state.readPC.Load(),
		writeCount:      state.writeCount,
		lifecycle:       state.GetLifecycleID(),
	}
}

func snapshotOrdinaryFastSemantics(state *VarState) ordinaryFastSnapshot {
	snapshot := snapshotOrdinaryFastState(state)
	snapshot.lifecycle = 0
	return snapshot
}

func ordinaryFastClock(events ...epoch.Epoch) *vectorclock.VectorClock {
	clock := vectorclock.New()
	for _, event := range events {
		tid, value := event.Decode()
		clock.Set(tid, uint32(value))
	}
	return clock
}

func materializedExactState(t *testing.T, pt *PageTableShadow, addr, size uintptr) *VarState {
	t.Helper()
	mask, ok := ordinaryFastWordMask(addr, size)
	if !ok {
		t.Fatalf("invalid test scalar %#x/%d", addr, size)
	}
	slot := pt.GetOrCreateSlot(addr)
	var state *VarState
	slot.AccessGroups(mask, func(_ uint8, candidate *VarState) { state = candidate })
	if state == nil || slot.referenceMask(state) != mask {
		t.Fatalf("materialized test state mask=%#x, want %#x", slot.referenceMask(state), mask)
	}
	return state
}

func compactExactState(t *testing.T, pt *PageTableShadow, addr uintptr) *VarState {
	t.Helper()
	view, ok := pt.blockFor(addr, false)
	if !ok {
		t.Fatal("compact block is absent")
	}
	compact := view.history.compact.Load()
	if compact == nil || compact.palette.Load() != nil {
		t.Fatal("sparse compact representation is absent")
	}
	group, overlap := compact.lookupGroup(addr)
	if overlap || group == nil || group.state.Load() == nil {
		t.Fatal("exact compact state is absent")
	}
	return group.state.Load()
}

func TestOrdinaryFastMaterializedWidthsAndPublication(t *testing.T) {
	for _, size := range []uintptr{1, 2, 4, 8} {
		t.Run(string(rune('0'+size)), func(t *testing.T) {
			pt := NewPageTableShadow()
			addr := uintptr(1)<<40 + 0x180
			state := materializedExactState(t, pt, addr, size)
			current := epoch.NewEpoch(17, 9)
			clock := ordinaryFastClock(current)

			if !pt.TryOrdinaryWrite(addr, size, current, clock, 0x1100+size) {
				t.Fatal("first materialized write missed")
			}
			got := snapshotOrdinaryFastState(state)
			if got.write != current || got.exclusiveWriter != 17 || got.writeCount != 1 || got.writePC != 0x1100+size {
				t.Fatalf("write publication = %+v", got)
			}

			result, cached := pt.TryOrdinaryRead(addr, size, current, clock, 0x1200+size)
			if result != OrdinaryFastHandledCacheable || cached != state {
				t.Fatalf("read result = (%v, %p), want cacheable %p", result, cached, state)
			}
			got = snapshotOrdinaryFastState(state)
			if got.read != current || got.readPC != 0x1200+size {
				t.Fatalf("read publication = %+v", got)
			}

			if !pt.TryOrdinaryWrite(addr, size, current, clock, 0x1300+size) {
				t.Fatal("same-reader W-R-W missed")
			}
			got = snapshotOrdinaryFastState(state)
			if got.write != current || got.read != 0 || got.writeCount != 2 || got.writePC != 0x1300+size {
				t.Fatalf("W-R-W publication = %+v", got)
			}
		})
	}
}

func TestOrdinaryFastCompactMaterializedEquivalence(t *testing.T) {
	const (
		addr = uintptr(1)<<40 + 0x240
		size = uintptr(4)
	)
	materialized := NewPageTableShadow()
	materialState := materializedExactState(t, materialized, addr, size)
	compact := NewPageTableShadow()
	current := epoch.NewEpoch(23, 11)
	clock := ordinaryFastClock(current)
	if !materialized.TryOrdinaryWrite(addr, size, current, clock, 0x2100) ||
		!compact.TryCompactWriteRange(addr, size, current, clock, 0x2100) {
		t.Fatal("failed to seed equivalent histories")
	}

	if result, cached := materialized.TryOrdinaryRead(addr, size, current, clock, 0x2200); result != OrdinaryFastHandledCacheable || cached != materialState {
		t.Fatalf("materialized read = (%v, %p)", result, cached)
	}
	if result, cached := compact.TryOrdinaryRead(addr, size, current, clock, 0x2200); result != OrdinaryFastHandled || cached != nil {
		t.Fatalf("changed compact read = (%v, %p), want handled and uncached", result, cached)
	}
	compactState := compactExactState(t, compact, addr)
	if got, want := snapshotOrdinaryFastSemantics(compactState), snapshotOrdinaryFastSemantics(materialState); got != want {
		t.Fatalf("post-read compact=%+v materialized=%+v", got, want)
	}

	if !materialized.TryOrdinaryWrite(addr, size, current, clock, 0x2300) ||
		!compact.TryOrdinaryWrite(addr, size, current, clock, 0x2300) {
		t.Fatal("equivalent W-R-W missed")
	}
	if got, want := snapshotOrdinaryFastSemantics(compactExactState(t, compact, addr)), snapshotOrdinaryFastSemantics(materialState); got != want {
		t.Fatalf("post-write compact=%+v materialized=%+v", got, want)
	}
}

func TestOrdinaryFastCompactExposedGenerationIsImmutable(t *testing.T) {
	pt := NewPageTableShadow()
	const addr = uintptr(1)<<40 + 0x2c0
	current := epoch.NewEpoch(29, 7)
	clock := ordinaryFastClock(current)
	if !pt.TryCompactWrite(addr, current, clock, 0x2900) {
		t.Fatal("compact seed write missed")
	}
	if result, state := pt.TryOrdinaryRead(addr, 1, current, clock, 0x2901); result != OrdinaryFastHandled || state != nil {
		t.Fatalf("changed read = (%v, %p), want handled and uncached", result, state)
	}
	result, exposed := pt.TryOrdinaryRead(addr, 1, current, clock, 0x2901)
	if result != OrdinaryFastHandledCacheable || exposed == nil {
		t.Fatalf("no-op read = (%v, %p), want cacheable", result, exposed)
	}
	before := snapshotOrdinaryFastState(exposed)
	if pt.TryOrdinaryWrite(addr, 1, current, clock, 0x2902) {
		t.Fatal("fast write mutated an exposed compact generation")
	}
	if got := snapshotOrdinaryFastState(exposed); got != before {
		t.Fatalf("exposed generation changed on miss: got %+v want %+v", got, before)
	}
}

func TestOrdinaryFastCompactSharedNoopReadIsCacheable(t *testing.T) {
	pt := NewPageTableShadow()
	const (
		first  = uintptr(1)<<40 + 0x2e0
		second = first + 32
		pc     = uintptr(0x2e01)
	)
	current := epoch.NewEpoch(30, 7)
	clock := ordinaryFastClock(current)
	if got := pt.TryCompactRead(first, current, clock, pc); got != CompactReadHandled {
		t.Fatalf("first seed read = %v", got)
	}
	if got := pt.TryCompactRead(second, current, clock, pc); got != CompactReadHandled {
		t.Fatalf("second seed read = %v", got)
	}
	shared := compactExactState(t, pt, first)
	if compactExactState(t, pt, second) != shared {
		t.Fatal("equivalent reads did not share one compact generation")
	}
	before := snapshotOrdinaryFastState(shared)
	result, cached := pt.TryOrdinaryRead(first, 1, current, clock, pc)
	if result != OrdinaryFastHandledCacheable || cached != shared {
		t.Fatalf("shared no-op read = (%v, %p), want cacheable %p", result, cached, shared)
	}
	if got := snapshotOrdinaryFastState(shared); got != before {
		t.Fatalf("shared no-op changed state: got %+v want %+v", got, before)
	}
	if pt.TryOrdinaryWrite(first, 1, current, clock, pc+1) {
		t.Fatal("shared compact write bypassed canonical copy-on-write")
	}
	if got := snapshotOrdinaryFastState(shared); got != before {
		t.Fatalf("shared write miss changed state: got %+v want %+v", got, before)
	}
}

func TestOrdinaryFastMissesAreSemanticallyUnchanged(t *testing.T) {
	pt := NewPageTableShadow()
	const addr = uintptr(1)<<40 + 0x301
	state := materializedExactState(t, pt, addr, 2)
	writer := epoch.NewEpoch(31, 7)
	if !pt.TryOrdinaryWrite(addr, 2, writer, ordinaryFastClock(writer), 0x3100) {
		t.Fatal("seed write missed")
	}

	before := snapshotOrdinaryFastState(state)
	concurrent := epoch.NewEpoch(32, 1)
	if result, cached := pt.TryOrdinaryRead(addr, 2, concurrent, ordinaryFastClock(concurrent), 0x3200); result != OrdinaryFastMiss || cached != nil {
		t.Fatalf("conflicting read = (%v, %p)", result, cached)
	}
	if got := snapshotOrdinaryFastState(state); got != before {
		t.Fatalf("conflict miss changed state: got %+v want %+v", got, before)
	}

	// A partial access would require COW from the established two-byte class.
	if result, _ := pt.TryOrdinaryRead(addr, 1, writer, ordinaryFastClock(writer), 0x3300); result != OrdinaryFastMiss {
		t.Fatalf("COW read result=%v", result)
	}
	if got := snapshotOrdinaryFastState(state); got != before {
		t.Fatalf("COW miss changed state: got %+v want %+v", got, before)
	}

	state.LockAccess()
	if pt.TryOrdinaryWrite(addr, 2, writer, ordinaryFastClock(writer), 0x3400) {
		t.Fatal("contended state lock was not a miss")
	}
	state.UnlockAccess()
	if got := snapshotOrdinaryFastState(state); got != before {
		t.Fatalf("contention miss changed state: got %+v want %+v", got, before)
	}
}

func TestOrdinaryFastRejectsAtomicOverlayAndAddressReuse(t *testing.T) {
	pt := NewPageTableShadow()
	const addr = uintptr(1)<<40 + 0x388
	state := materializedExactState(t, pt, addr, 1)
	current := epoch.NewEpoch(41, 3)
	state.LockAccess()
	state.SetAtomicState(unsafe.Pointer(new(byte)))
	state.UnlockAccess()
	before := snapshotOrdinaryFastState(state)
	if result, _ := pt.TryOrdinaryRead(addr, 1, current, ordinaryFastClock(current), 0x4100); result != OrdinaryFastMiss {
		t.Fatalf("atomic-overlay read result=%v", result)
	}
	if got := snapshotOrdinaryFastState(state); got != before {
		t.Fatalf("overlay miss changed state: got %+v want %+v", got, before)
	}

	pt.ClearRange(addr, 1)
	if result, _ := pt.TryOrdinaryRead(addr, 1, current, ordinaryFastClock(current), 0x4200); result != OrdinaryFastMiss {
		t.Fatalf("cleared lane read result=%v", result)
	}
	reused := pt.GetOrCreate(addr)
	if reused == state || reused.GetLifecycleID() == before.lifecycle {
		t.Fatalf("address reuse retained old generation: old=%p/%d new=%p/%d", state, before.lifecycle, reused, reused.GetLifecycleID())
	}
}

func TestOrdinaryFastCompactRedirectAndPaletteMiss(t *testing.T) {
	pt := NewPageTableShadow()
	const base = uintptr(1)<<40 + 0x500
	current := epoch.NewEpoch(51, 5)
	clock := ordinaryFastClock(current)
	if pt.TryOrdinaryWrite(base, 1, current, clock, 0x5000) {
		t.Fatal("ordinary fast path performed first compact publication")
	}
	if got := pt.TryCompactRead(base, current, clock, 0x5100); got != CompactReadHandled {
		t.Fatalf("first compact read=%v", got)
	}
	if got := pt.TryCompactRead(base+8, current, clock, 0x5200); got != CompactReadHandled {
		t.Fatalf("second compact read=%v", got)
	}
	// Updating the first read PC to 0x5200 would converge with the second
	// descriptor and require membership redirection.
	if result, _ := pt.TryOrdinaryRead(base, 1, current, clock, 0x5200); result != OrdinaryFastMiss {
		t.Fatalf("redirecting compact read=%v", result)
	}

	dense := NewPageTableShadow()
	for i := uintptr(0); i < compactPaletteBitmapCrossover+1; i++ {
		if !dense.TryCompactWrite(base+i*17, current, clock, 0x6000+i) {
			t.Fatalf("compact palette seed %d missed", i)
		}
	}
	view, _ := dense.blockFor(base, false)
	if compact := view.history.compact.Load(); compact == nil || compact.palette.Load() == nil {
		t.Fatal("test did not establish dense compact representation")
	}
	if result, _ := dense.TryOrdinaryRead(base, 1, current, clock, 0x6100); result != OrdinaryFastMiss {
		t.Fatalf("dense compact read=%v", result)
	}
}

func TestOrdinaryFastCompactContentionAndPartialClear(t *testing.T) {
	pt := NewPageTableShadow()
	const base = uintptr(1)<<40 + 0x580
	current := epoch.NewEpoch(55, 7)
	clock := ordinaryFastClock(current)
	if !pt.TryCompactWriteRange(base, 2, current, clock, 0x5500) {
		t.Fatal("compact range seed missed")
	}
	state := compactExactState(t, pt, base)
	before := snapshotOrdinaryFastState(state)
	view, ok := pt.blockFor(base, false)
	if !ok {
		t.Fatal("compact block is absent")
	}
	view.history.mu.lock()
	if result, cached := pt.TryOrdinaryRead(base, 2, current, clock, 0x5600); result != OrdinaryFastMiss || cached != nil {
		view.history.mu.unlock()
		t.Fatalf("contended compact read = (%v, %p)", result, cached)
	}
	view.history.mu.unlock()
	if got := snapshotOrdinaryFastState(state); got != before {
		t.Fatalf("compact contention miss changed state: got %+v want %+v", got, before)
	}

	pt.ClearRange(base, 1)
	if result, cached := pt.TryOrdinaryRead(base, 1, current, clock, 0x5700); result != OrdinaryFastMiss || cached != nil {
		t.Fatalf("cleared compact lane read = (%v, %p)", result, cached)
	}
	if got := compactExactState(t, pt, base+1); got != state {
		t.Fatalf("partial clear changed sibling state: got %p want %p", got, state)
	}
	if result, cached := pt.TryOrdinaryRead(base+1, 1, current, clock, 0x5800); result != OrdinaryFastHandled || cached != nil {
		t.Fatalf("changed compact sibling read = (%v, %p), want handled and uncached", result, cached)
	}

	if !pt.TryCompactWrite(base, current, clock, 0x5900) {
		t.Fatal("fresh compact transition from tombstone missed")
	}
	fresh := compactExactState(t, pt, base)
	if fresh == state || fresh.GetLifecycleID() == before.lifecycle {
		t.Fatalf("compact reuse retained old generation: old=%p/%d new=%p/%d", state, before.lifecycle, fresh, fresh.GetLifecycleID())
	}
}

func TestOrdinaryFastWarmedMaterializedZeroAllocation(t *testing.T) {
	pt := NewPageTableShadow()
	const addr = uintptr(1)<<40 + 0x680
	state := materializedExactState(t, pt, addr, 8)
	current := epoch.NewEpoch(61, 13)
	clock := ordinaryFastClock(current)
	if result, _ := pt.TryOrdinaryRead(addr, 8, current, clock, 0x6100); result != OrdinaryFastHandledCacheable {
		t.Fatalf("warmup read=%v", result)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		result, cached := pt.TryOrdinaryRead(addr, 8, current, clock, 0x6100)
		if result != OrdinaryFastHandledCacheable || cached != state {
			panic("warmed ordinary read missed")
		}
	}); allocs != 0 {
		t.Fatalf("warmed ordinary read allocations=%v", allocs)
	}
}
