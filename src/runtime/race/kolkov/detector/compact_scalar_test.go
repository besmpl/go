//go:build amd64 || arm64

package detector

import (
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	"runtime/race/kolkov/goroutine"
	"runtime/race/kolkov/shadowmem"
)

func TestCompactScalarSameKeySharesHistoryWithoutMaterializingSlots(t *testing.T) {
	const (
		base      = uintptr(0x400000)
		accesses  = 2048
		writePC   = uintptr(0xc001)
		readPC    = uintptr(0xc002)
		maxStates = 1
	)

	d := NewDetector()
	ctx := goroutine.Alloc(201)
	defer ctx.C.Release()

	for i := uintptr(0); i < accesses; i++ {
		d.OnWrite(base+i, ctx, writePC)
	}
	for i := uintptr(0); i < accesses; i++ {
		d.OnRead(base+i, ctx, readPC)
	}
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("same-context compact accesses reported %d races", got)
	}

	states := make(map[*shadowmem.VarState]struct{})
	for i := uintptr(0); i < accesses; i++ {
		addr := base + i
		state := d.ShadowGet(addr)
		if state == nil {
			t.Fatalf("address %#x has no compact history", addr)
		}
		states[state] = struct{}{}
		if state.GetW() != ctx.GetEpoch() ||
			state.GetReadEpoch() != ctx.GetEpoch() ||
			state.GetReaderCount() != 1 ||
			state.GetWritePC() != writePC ||
			state.GetReadPC() != readPC ||
			state.GetExclusiveWriter() != int64(ctx.TID) ||
			state.GetWriteCount() != 1 ||
			state.GetLifecycleID() == 0 {
			t.Fatalf("address %#x has incorrect compact state: %s", addr, state)
		}
	}
	if len(states) > maxStates {
		t.Fatalf("%d same-key addresses retained %d represented states, want at most %d", accesses, len(states), maxStates)
	}

	materialized := 0
	for offset := uintptr(0); offset < accesses; offset += 8 {
		if d.rangeMemory.GetSlot(base+offset) != nil {
			materialized++
		}
	}
	if materialized != 0 {
		t.Fatalf("%d same-key addresses materialized %d word slots, want 0", accesses, materialized)
	}
}

func TestCompactScalarWeakHintMaterializesOnlyOnMatchingSecondRead(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(205)
	defer ctx.C.Release()
	const (
		addr    = uintptr(0x408002)
		oneShot = addr + 8
		pc      = uintptr(0xc081)
	)

	d.OnReadSized(addr, 4, ctx, pc)
	d.OnReadSized(oneShot, 4, ctx, pc)
	if d.rangeMemory.GetSlot(addr) != nil || d.rangeMemory.GetSlot(oneShot) != nil {
		t.Fatal("first compact reads materialized a word slot")
	}
	ctx.IncrementClock()
	if !ctx.HasReadHintSized(oneShot, 4) {
		t.Fatal("synchronization did not retain the most recent exact hint")
	}
	d.OnReadSized(oneShot, 4, ctx, pc+1)
	if d.rangeMemory.GetSlot(oneShot) == nil {
		t.Fatal("matching second-epoch read did not materialize its word")
	}
	if d.rangeMemory.GetSlot(addr) != nil {
		t.Fatal("colliding one-shot read was materialized")
	}
	index := (oneShot >> 3) & (goroutine.ReadCacheSlots - 1)
	state := d.ShadowGet(oneShot)
	if state == nil || ctx.ReadCache[index] != oneShot ||
		ctx.ReadCacheStates[index] != unsafe.Pointer(state) || ctx.ReadCacheWidths[index] != 4 {
		t.Fatalf("second read cache = (%#x,%p,%d), state=%p",
			ctx.ReadCache[index], ctx.ReadCacheStates[index], ctx.ReadCacheWidths[index], state)
	}
}

func TestCompactScalarWeakHintRejectsAtomicOverlay(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(206)
	defer ctx.C.Release()
	const addr = uintptr(0x409000)

	d.OnReadSized(addr, 8, ctx, 0xc091)
	completeAtomicSize(d, addr, 8, ctx, false, false, 0xc092)
	ctx.IncrementClock()
	d.OnReadSized(addr, 8, ctx, 0xc093)

	state := d.ShadowGet(addr)
	state.LockAccess()
	atomicHistory := existingAtomicStateLocked(state)
	_, recorded := atomicHistoryAccess(atomicHistory.plainReads, ctx.TID)
	state.UnlockAccess()
	if !recorded {
		t.Fatal("weak-hint read bypassed the established atomic/plain transition")
	}
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("same-context atomic/plain sequence reported %d races", got)
	}
}

func TestCompactScalarBlockingWeakHintPublishesAccessCacheAndReport(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(209)
	writer := goroutine.Alloc(210)
	defer reader.C.Release()
	defer writer.C.Release()
	const addr = uintptr(0x409102)

	// The second epoch materializes one exact four-lane state. The writer has
	// observed that read, so only the later reader is concurrent and reports.
	d.OnReadSized(addr, 4, reader, 0xc094)
	reader.IncrementClock()
	d.OnReadSized(addr, 4, reader, 0xc095)
	state := d.ShadowGet(addr)
	if state == nil || d.rangeMemory.GetSlot(addr) == nil {
		t.Fatal("setup did not materialize exact hinted state")
	}
	writer.C.Set(reader.TID, 2)
	d.OnWriteSized(addr, 4, writer, 0xc096)
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("ordered setup reported %d races", got)
	}

	reader.IncrementClock()
	state.LockAccess()
	entered := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(entered)
		d.OnReadSized(addr, 4, reader, 0xc097)
		close(done)
	}()
	<-entered
	select {
	case <-done:
		state.UnlockAccess()
		t.Fatal("weak-hint read completed while its access lock was held")
	case <-time.After(10 * time.Millisecond):
	}
	state.UnlockAccess()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("weak-hint read did not complete after its access lock was released")
	}

	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("blocking weak-hint conflict reported %d races, want 1", got)
	}
	if state.GetReadEpoch() != reader.GetEpoch() || state.GetReadPC() != 0xc097 {
		t.Fatalf("blocking weak-hint read was not published: state=%s", state)
	}
	index := (addr >> 3) & (goroutine.ReadCacheSlots - 1)
	if reader.ReadCache[index] != addr ||
		reader.ReadCacheStates[index] != unsafe.Pointer(state) ||
		reader.ReadCacheWidths[index] != 4 {
		t.Fatalf("blocking weak-hint cache = (%#x,%p,%d), want (%#x,%p,4)",
			reader.ReadCache[index], reader.ReadCacheStates[index], reader.ReadCacheWidths[index], addr, state)
	}
}

func TestCompactScalarCollidingHintDoesNotMaterialize(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(207)
	defer ctx.C.Release()
	const (
		addr     = uintptr(0x40a001)
		collider = addr + 32
	)

	d.OnRead(addr, ctx, 0xc0a1)
	d.OnRead(addr, ctx, 0xc0a1)
	d.OnRead(collider, ctx, 0xc0a2)
	d.OnRead(collider, ctx, 0xc0a2)
	ctx.IncrementClock()
	if ctx.HasReadHintSized(addr, 1) || !ctx.HasReadHintSized(collider, 1) {
		index := (addr >> 3) & (goroutine.ReadCacheSlots - 1)
		t.Fatalf("direct-mapped collision entry = (%#x,%d), want (%#x,weak 1)",
			ctx.ReadCache[index], ctx.ReadCacheWidths[index], collider)
	}
	d.OnRead(addr, ctx, 0xc0a3)
	if slot := d.rangeMemory.GetSlot(addr); slot != nil {
		t.Fatalf("collided hint materialized slot %p", slot)
	}
}

func TestWeakHintPartialReadSplitsUniformWord(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(208)
	defer ctx.C.Release()
	const addr = uintptr(0x40b000)

	d.OnReadSized(addr, 8, ctx, 0xc0b1)
	ctx.IncrementClock()
	d.OnReadSized(addr, 8, ctx, 0xc0b2)
	shared := d.ShadowGet(addr)
	oldEpoch := ctx.GetEpoch()
	if slot := d.rangeMemory.GetSlot(addr); slot == nil || slot.State(7) != shared {
		t.Fatal("setup did not materialize one uniform eight-lane state")
	}

	ctx.RecordAddressOnlyReadRange(addr, 4)
	ctx.IncrementClock()
	d.OnReadSized(addr, 4, ctx, 0xc0b3)
	slot := d.rangeMemory.GetSlot(addr)
	lower, upper := slot.State(0), slot.State(4)
	if lower == nil || upper == nil || lower == upper {
		t.Fatalf("partial hinted read did not split shared word: lower=%p upper=%p", lower, upper)
	}
	if lower.GetReadEpoch() != ctx.GetEpoch() {
		t.Fatalf("lower lanes retained epoch %v, want %v", lower.GetReadEpoch(), ctx.GetEpoch())
	}
	if upper.GetReadEpoch() != oldEpoch {
		t.Fatalf("upper lanes changed epoch to %v, want %v", upper.GetReadEpoch(), oldEpoch)
	}
}

func TestCompactScalarConflictsFallBackAndReport(t *testing.T) {
	t.Run("write-read", func(t *testing.T) {
		d := NewDetector()
		writer := goroutine.Alloc(211)
		reader := goroutine.Alloc(212)
		defer writer.C.Release()
		defer reader.C.Release()
		const addr = uintptr(0x410003)

		d.OnWrite(addr, writer, 0xc101)
		if slot := d.rangeMemory.GetSlot(addr); slot != nil {
			t.Fatalf("initial compact write materialized slot %p", slot)
		}
		var report *RaceReport
		d.reportObserver = func(observed *RaceReport) { report = observed }
		d.OnRead(addr, reader, 0xc102)

		if got := d.RacesDetected(); got != 1 {
			t.Fatalf("compact write/read conflict reported %d races, want 1", got)
		}
		if report == nil || report.Current.Type != AccessRead || report.Previous.Type != AccessWrite {
			t.Fatalf("compact write/read report = %#v, want previous write and current read", report)
		}
		if slot := d.rangeMemory.GetSlot(addr); slot == nil {
			t.Fatal("conflicting compact read did not fall back to a materialized slot")
		}
		state := d.ShadowGet(addr)
		if state == nil || state.GetW() != writer.GetEpoch() || state.GetReadEpoch() != reader.GetEpoch() {
			t.Fatalf("conflicting read did not publish complete history: %v", state)
		}
	})

	t.Run("write-write", func(t *testing.T) {
		d := NewDetector()
		first := goroutine.Alloc(213)
		second := goroutine.Alloc(214)
		defer first.C.Release()
		defer second.C.Release()
		const addr = uintptr(0x420005)

		d.OnWrite(addr, first, 0xc201)
		if slot := d.rangeMemory.GetSlot(addr); slot != nil {
			t.Fatalf("initial compact write materialized slot %p", slot)
		}
		var report *RaceReport
		d.reportObserver = func(observed *RaceReport) { report = observed }
		d.OnWrite(addr, second, 0xc202)

		if got := d.RacesDetected(); got != 1 {
			t.Fatalf("compact write/write conflict reported %d races, want 1", got)
		}
		if report == nil || report.Current.Type != AccessWrite || report.Previous.Type != AccessWrite {
			t.Fatalf("compact write/write report = %#v, want two writes", report)
		}
		if slot := d.rangeMemory.GetSlot(addr); slot == nil {
			t.Fatal("conflicting compact write did not fall back to a materialized slot")
		}
		state := d.ShadowGet(addr)
		if state == nil || state.GetW() != second.GetEpoch() || state.GetWritePC() != 0xc202 {
			t.Fatalf("conflicting write did not publish the current history: %v", state)
		}
	})
}

func TestCompactScalarMatchesForcedMaterializedTransitions(t *testing.T) {
	const (
		addr        = uintptr(0x430006)
		anchorCount = 6
	)
	compact := NewDetector()
	forced := NewDetector()
	var anchors [anchorCount]uintptr
	for i := range anchors {
		anchors[i] = addr + uintptr(i)*8
		if slot := forced.rangeMemory.GetOrCreateSlot(anchors[i]); slot == nil {
			t.Fatalf("failed to force reference address %d materialization", i)
		}
	}

	compactCtx := goroutine.Alloc(221)
	forcedCtx := goroutine.Alloc(221)
	defer compactCtx.C.Release()
	defer forcedCtx.C.Release()

	type operation struct {
		write bool
		pc    uintptr
	}
	transitions := []operation{
		{write: true, pc: 0xc302},
		{write: false, pc: 0xc303},
		{write: false, pc: 0xc304},
	}

	// Seed a compressing class with equal anchors and compare every compact
	// descriptor transition with the materialized oracle.
	for _, anchor := range anchors {
		compact.OnWrite(anchor, compactCtx, 0xc301)
		forced.OnWrite(anchor, forcedCtx, 0xc301)
		if slot := compact.rangeMemory.GetSlot(anchor); slot != nil {
			t.Fatalf("initial shared-class write at %#x materialized slot %p", anchor, slot)
		}
	}
	if got, want := compact.ShadowGet(addr), forced.ShadowGet(addr); got == nil || want == nil {
		t.Fatalf("initial shared-class state is nil: compact=%p forced=%p", got, want)
	} else {
		assertCompactScalarEquivalent(t, 0, got, want)
	}

	var compactLifecycle, forcedLifecycle uint64
	compactLifecycle = compact.ShadowGet(addr).GetLifecycleID()
	forcedLifecycle = forced.ShadowGet(addr).GetLifecycleID()
	if compactLifecycle == 0 || forcedLifecycle == 0 {
		t.Fatalf("initial state has zero lifecycle: compact=%d forced=%d", compactLifecycle, forcedLifecycle)
	}

	step := 0
	active := len(anchors)
	apply := func(op operation) {
		step++
		// Move every member except one. Each call therefore starts with at
		// least two members in its source class, and the target class remains
		// compact for the next transition.
		for i := 0; i < active-1; i++ {
			anchor := anchors[i]
			if op.write {
				compact.OnWrite(anchor, compactCtx, op.pc)
				forced.OnWrite(anchor, forcedCtx, op.pc)
			} else {
				compact.OnRead(anchor, compactCtx, op.pc)
				forced.OnRead(anchor, forcedCtx, op.pc)
			}
			if slot := compact.rangeMemory.GetSlot(anchor); slot != nil {
				t.Fatalf("step %d shared transition at %#x materialized slot %p", step, anchor, slot)
			}
		}
		active--
		got, want := compact.ShadowGet(addr), forced.ShadowGet(addr)
		assertCompactScalarEquivalent(t, step, got, want)
		if got.GetLifecycleID() != compactLifecycle || want.GetLifecycleID() != forcedLifecycle {
			t.Fatalf("step %d changed address lifecycle: compact=%d/%d forced=%d/%d",
				step, got.GetLifecycleID(), compactLifecycle, want.GetLifecycleID(), forcedLifecycle)
		}
	}

	for _, op := range transitions {
		apply(op)
	}
	compactCtx.IncrementClock()
	forcedCtx.IncrementClock()
	apply(operation{write: false, pc: 0xc305})

	// Materializing an exact compact member must preserve both its complete
	// ordinary history and its allocator-lifetime identity.
	if slot := compact.rangeMemory.GetOrCreateSlot(addr); slot == nil {
		t.Fatal("failed to materialize compact address")
	}
	got, want := compact.ShadowGet(addr), forced.ShadowGet(addr)
	assertCompactScalarEquivalent(t, step+1, got, want)
	if got.GetLifecycleID() != compactLifecycle {
		t.Fatalf("materialization changed compact lifecycle from %d to %d", compactLifecycle, got.GetLifecycleID())
	}
	if got.GetLifecycleID() == 0 || want.GetLifecycleID() != forcedLifecycle {
		t.Fatalf("materialized lifecycle mismatch: compact=%d forced=%d/%d", got.GetLifecycleID(), want.GetLifecycleID(), forcedLifecycle)
	}
	if compact.RacesDetected() != 0 || forced.RacesDetected() != 0 {
		t.Fatalf("equivalent conflict-free transitions reported races: compact=%d forced=%d",
			compact.RacesDetected(), forced.RacesDetected())
	}
}

func TestCompactScalarRepeatedIdenticalAccessPolicy(t *testing.T) {
	t.Run("write", func(t *testing.T) {
		d := NewDetector()
		ctx := goroutine.Alloc(225)
		defer ctx.C.Release()
		const (
			addr = uintptr(0x438003)
			pc   = uintptr(0xc381)
		)

		d.OnWrite(addr, ctx, pc)
		before := d.ShadowGet(addr)
		if before == nil {
			t.Fatal("first compact write did not publish history")
		}
		lifecycle := before.GetLifecycleID()
		if slot := d.rangeMemory.GetSlot(addr); slot != nil {
			t.Fatalf("first compact write materialized slot %p", slot)
		}

		// An identical same-epoch transition is the adaptive hotness signal.
		// Compact preflight deliberately falls through so future hooks use the
		// normal materialized state and runtime fast paths.
		d.OnWrite(addr, ctx, pc)
		slot := d.rangeMemory.GetSlot(addr)
		if slot == nil {
			t.Fatal("second identical write did not promote the exact word")
		}
		after := d.ShadowGet(addr)
		if after == nil || slot.State(uint8(addr&7)) != after {
			t.Fatalf("promoted write state = %p, slot state = %p", after, slot.State(uint8(addr&7)))
		}
		if after.GetLifecycleID() != lifecycle {
			t.Fatalf("hot-write promotion changed lifecycle from %d to %d", lifecycle, after.GetLifecycleID())
		}
		if after.GetW() != ctx.GetEpoch() || after.GetWritePC() != pc ||
			after.GetExclusiveWriter() != int64(ctx.TID) || after.GetWriteCount() != 1 {
			t.Fatalf("hot-write promotion changed history: %s", after)
		}
		if got := d.RacesDetected(); got != 0 {
			t.Fatalf("same-context hot write reported %d races", got)
		}
	})

	t.Run("read remains compact and caches only exact no-op", func(t *testing.T) {
		d := NewDetector()
		ctx := goroutine.Alloc(226)
		defer ctx.C.Release()
		const (
			addr = uintptr(0x439005)
			pc   = uintptr(0xc391)
		)

		d.OnRead(addr, ctx, pc)
		before := d.ShadowGet(addr)
		if before == nil {
			t.Fatal("first compact read did not publish history")
		}
		lifecycle := before.GetLifecycleID()
		cacheSlot := (addr >> 3) & (goroutine.ReadCacheSlots - 1)
		if slot := d.rangeMemory.GetSlot(addr); slot != nil {
			t.Fatalf("first compact read materialized slot %p", slot)
		}
		if ctx.ReadCache[cacheSlot] != 0 || ctx.ReadCacheStates[cacheSlot] != nil {
			t.Fatalf("changed compact read cache = (%#x,%p), want empty",
				ctx.ReadCache[cacheSlot], ctx.ReadCacheStates[cacheSlot])
		}

		d.OnRead(addr, ctx, pc)
		if slot := d.rangeMemory.GetSlot(addr); slot != nil {
			t.Fatalf("second identical read materialized slot %p", slot)
		}
		after := d.ShadowGet(addr)
		if after != before {
			t.Fatalf("compact read no-op state = %p, want exact immutable source %p", after, before)
		}
		if after.GetLifecycleID() != lifecycle {
			t.Fatalf("compact read no-op changed lifecycle from %d to %d", lifecycle, after.GetLifecycleID())
		}
		if after.GetReadEpoch() != ctx.GetEpoch() || after.GetReadPC() != pc || after.GetReaderCount() != 1 {
			t.Fatalf("compact read no-op changed history: %s", after)
		}
		if ctx.ReadCache[cacheSlot] != addr || ctx.ReadCacheStates[cacheSlot] != nil {
			t.Fatalf("compact read no-op cache = (%#x,%p), want (%#x,nil)",
				ctx.ReadCache[cacheSlot], ctx.ReadCacheStates[cacheSlot], addr)
		}
		allocs := testing.AllocsPerRun(1000, func() {
			d.OnRead(addr, ctx, pc)
		})
		if allocs != 0 {
			t.Fatalf("repeated compact read allocated %.2f objects/op", allocs)
		}
		if slot := d.rangeMemory.GetSlot(addr); slot != nil {
			t.Fatalf("allocation check materialized slot %p", slot)
		}
		// A same-epoch read from a different call site is not a semantic no-op:
		// readPC is exact reporting metadata and must publish a fresh descriptor.
		const nextPC = uintptr(0xc392)
		d.OnRead(addr, ctx, nextPC)
		next := d.ShadowGet(addr)
		if next == nil || next == after || next.GetReadPC() != nextPC || next.GetReadEpoch() != ctx.GetEpoch() {
			t.Fatalf("changed-PC compact read did not publish exact metadata: before=%p after=%v", after, next)
		}
		if slot := d.rangeMemory.GetSlot(addr); slot != nil {
			t.Fatalf("changed-PC compact read materialized slot %p", slot)
		}
		if ctx.ReadCache[cacheSlot] != addr || ctx.ReadCacheStates[cacheSlot] != nil {
			t.Fatalf("changed-PC read rooted cache state: (%#x,%p)",
				ctx.ReadCache[cacheSlot], ctx.ReadCacheStates[cacheSlot])
		}
		if got := d.RacesDetected(); got != 0 {
			t.Fatalf("same-context hot read reported %d races", got)
		}
	})

	t.Run("shared class changing PC stays compact", func(t *testing.T) {
		d := NewDetector()
		ctx := goroutine.Alloc(227)
		defer ctx.C.Release()
		const (
			addr    = uintptr(0x43a007)
			peer    = addr + 8
			firstPC = uintptr(0xc3a0)
			nextPC  = uintptr(0xc3a1)
		)

		d.OnWrite(addr, ctx, firstPC)
		d.OnWrite(peer, ctx, firstPC)
		before, peerState := d.ShadowGet(addr), d.ShadowGet(peer)
		if before == nil || before != peerState {
			t.Fatalf("equal anchors did not share compact state: addr=%p peer=%p", before, peerState)
		}
		lifecycle := before.GetLifecycleID()

		d.OnWrite(addr, ctx, nextPC)
		after := d.ShadowGet(addr)
		if after == nil || after.GetWritePC() != nextPC || after.GetW() != ctx.GetEpoch() {
			t.Fatalf("shared-class changing-PC state is incorrect: %v", after)
		}
		if after.GetLifecycleID() != lifecycle {
			t.Fatalf("shared-class changing PC changed lifecycle from %d to %d", lifecycle, after.GetLifecycleID())
		}
		if after == d.ShadowGet(peer) || d.ShadowGet(peer).GetWritePC() != firstPC {
			t.Fatalf("changing one member also changed its peer: addr=%p peer=%v", after, d.ShadowGet(peer))
		}
		if slot := d.rangeMemory.GetSlot(addr); slot != nil {
			t.Fatalf("shared-class changing PC materialized slot %p", slot)
		}
		if got := d.RacesDetected(); got != 0 {
			t.Fatalf("shared-class changing PC reported %d races", got)
		}
	})

	t.Run("sole changing PC stays compact", func(t *testing.T) {
		d := NewDetector()
		ctx := goroutine.Alloc(229)
		defer ctx.C.Release()
		const addr = uintptr(0x43c004)

		d.OnWrite(addr, ctx, 0xc3c1)
		before := d.ShadowGet(addr)
		if before == nil {
			t.Fatal("initial sole compact write did not publish history")
		}
		lifecycle := before.GetLifecycleID()

		d.OnWrite(addr, ctx, 0xc3c2)
		slot := d.rangeMemory.GetSlot(addr)
		if slot != nil {
			t.Fatalf("sole-member descriptor change materialized slot %p", slot)
		}
		after := d.ShadowGet(addr)
		if after == nil || after == before || after.GetW() != ctx.GetEpoch() ||
			after.GetWritePC() != 0xc3c2 || after.GetWriteCount() != 1 ||
			after.GetExclusiveWriter() != int64(ctx.TID) || after.GetReaderCount() != 0 {
			t.Fatalf("sole changing-PC compact transition has incorrect history: %v", after)
		}
		if after.GetLifecycleID() != lifecycle {
			t.Fatalf("sole changing-PC transition changed lifecycle from %d to %d", lifecycle, after.GetLifecycleID())
		}
		if before.GetW() != ctx.GetEpoch() || before.GetWritePC() != 0xc3c1 ||
			before.GetWriteCount() != 1 || before.GetLifecycleID() != lifecycle {
			t.Fatalf("sole changing-PC transition mutated old compact state: %v", before)
		}
		if got := d.RacesDetected(); got != 0 {
			t.Fatalf("sole changing-PC transition reported %d races", got)
		}
	})

	t.Run("same-reader write after read stays compact", func(t *testing.T) {
		d := NewDetector()
		ctx := goroutine.Alloc(228)
		defer ctx.C.Release()
		const (
			addr = uintptr(0x43b002)
			peer = addr + 8
		)

		d.OnWrite(addr, ctx, 0xc3b1)
		d.OnWrite(peer, ctx, 0xc3b1)
		d.OnRead(addr, ctx, 0xc3b2)
		before := d.ShadowGet(addr)
		if before == nil || before.GetW() != ctx.GetEpoch() || before.GetReadEpoch() != ctx.GetEpoch() {
			t.Fatalf("compact write/read setup has incorrect state: %v", before)
		}
		lifecycle := before.GetLifecycleID()
		if slot := d.rangeMemory.GetSlot(addr); slot != nil {
			t.Fatalf("write/read setup materialized slot %p", slot)
		}

		// A read by this same logical thread can remain compact: afterWrite still
		// proves the represented read and write happen before the current clock.
		d.OnWrite(addr, ctx, 0xc3b3)
		slot := d.rangeMemory.GetSlot(addr)
		if slot != nil {
			t.Fatalf("same-reader write after compact read materialized slot %p", slot)
		}
		after := d.ShadowGet(addr)
		if after == nil || after == before {
			t.Fatalf("same-reader compact cycle state = %p, before %p", after, before)
		}
		if after.GetLifecycleID() != lifecycle {
			t.Fatalf("same-reader compact cycle changed lifecycle from %d to %d", lifecycle, after.GetLifecycleID())
		}
		if after.GetW() != ctx.GetEpoch() || after.GetReaderCount() != 0 ||
			after.GetWritePC() != 0xc3b3 || after.GetWriteCount() != 2 ||
			after.GetExclusiveWriter() != int64(ctx.TID) {
			t.Fatalf("same-reader compact cycle changed write semantics: %s", after)
		}
		peerState := d.ShadowGet(peer)
		if peerState == nil || peerState == after || peerState.GetWritePC() != 0xc3b1 ||
			peerState.GetWriteCount() != 1 || peerState.GetReaderCount() != 0 {
			t.Fatalf("same-reader compact cycle changed peer history: %v", peerState)
		}
		if got := d.RacesDetected(); got != 0 {
			t.Fatalf("same-reader compact cycle reported %d races", got)
		}
	})
}

func TestCompactScalarRWMutexMarkerMaterializesSidecarWithoutCaching(t *testing.T) {
	markerPC := reflect.ValueOf((*sync.RWMutex).RLock).Pointer() + 1
	if !rwMutexMarkerPC(markerPC) {
		t.Fatalf("RWMutex marker PC %#x was not classified as a marker", markerPC)
	}

	d := NewDetector()
	ctx := goroutine.Alloc(231)
	defer ctx.C.Release()
	const addr = uintptr(0x440003)

	d.OnRead(addr, ctx, 0xc401)
	before := d.ShadowGet(addr)
	if before == nil {
		t.Fatal("ordinary compact read did not publish history")
	}
	beforeLifecycle := before.GetLifecycleID()
	if slot := d.rangeMemory.GetSlot(addr); slot != nil {
		t.Fatalf("ordinary compact read materialized slot %p", slot)
	}
	cacheSlot := (addr >> 3) & (goroutine.ReadCacheSlots - 1)
	cachedState := ctx.ReadCacheStates[cacheSlot]
	if ctx.ReadCache[cacheSlot] != 0 || cachedState != nil {
		t.Fatalf("changed ordinary compact read cache = (%#x,%p), want empty",
			ctx.ReadCache[cacheSlot], cachedState)
	}

	d.OnRead(addr, ctx, markerPC)
	slot := d.rangeMemory.GetSlot(addr)
	if slot == nil {
		t.Fatal("RWMutex marker did not force exact slot materialization")
	}
	after := d.ShadowGet(addr)
	if after == nil || slot.State(uint8(addr&7)) != after {
		t.Fatalf("marker materialized state = %p, slot state = %p", after, slot.State(uint8(addr&7)))
	}
	if after.GetLifecycleID() != beforeLifecycle {
		t.Fatalf("marker materialization changed lifecycle from %d to %d", beforeLifecycle, after.GetLifecycleID())
	}
	after.LockAccess()
	atomicSidecar := after.GetAtomicState()
	after.UnlockAccess()
	if atomicSidecar == nil {
		t.Fatal("RWMutex marker did not attach its atomic classification sidecar")
	}
	if ctx.ReadCache[cacheSlot] != 0 || ctx.ReadCacheStates[cacheSlot] != cachedState {
		t.Fatalf("RWMutex marker rewrote empty cache entry to (%#x,%p)",
			ctx.ReadCache[cacheSlot], ctx.ReadCacheStates[cacheSlot])
	}
	if ctx.ReadCacheStates[cacheSlot] == unsafe.Pointer(after) {
		t.Fatal("RWMutex marker published its materialized classification state into the user-read cache")
	}
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("same-context marker transition reported %d races", got)
	}
}

func TestCompactScalarGroupOverflowFallsBackSoundly(t *testing.T) {
	const (
		base    = uintptr(0x450000)
		entries = 64
	)
	d := NewDetector()
	ctx := goroutine.Alloc(241)
	defer ctx.C.Release()

	var compactCount, materializedCount int
	var fallbackAddr uintptr
	for i := uintptr(0); i < entries; i++ {
		addr := base + i*8
		pc := uintptr(0xc500) + i
		d.OnWrite(addr, ctx, pc)
		state := d.ShadowGet(addr)
		if state == nil || state.GetW() != ctx.GetEpoch() || state.GetWritePC() != pc ||
			state.GetExclusiveWriter() != int64(ctx.TID) || state.GetWriteCount() != 1 {
			t.Fatalf("overflow entry %d at %#x has incorrect state: %v", i, addr, state)
		}
		if d.rangeMemory.GetSlot(addr) == nil {
			compactCount++
		} else {
			materializedCount++
			if fallbackAddr == 0 {
				fallbackAddr = addr
			}
		}
	}
	if compactCount == 0 || materializedCount == 0 {
		t.Fatalf("distinct-key population did not exercise both paths: compact=%d materialized=%d", compactCount, materializedCount)
	}
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("conflict-free overflow population reported %d races", got)
	}

	conflicting := goroutine.Alloc(242)
	defer conflicting.C.Release()
	var report *RaceReport
	d.reportObserver = func(observed *RaceReport) { report = observed }
	d.OnWrite(fallbackAddr, conflicting, 0xc5ff)
	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("materialized overflow conflict reported %d races, want 1", got)
	}
	if report == nil || report.Current.Addr != fallbackAddr ||
		report.Current.Type != AccessWrite || report.Previous.Type != AccessWrite {
		t.Fatalf("materialized overflow report = %#v, want write/write at %#x", report, fallbackAddr)
	}
	if state := d.ShadowGet(fallbackAddr); state == nil || state.GetW() != conflicting.GetEpoch() {
		t.Fatalf("overflow fallback did not publish conflicting write: %v", state)
	}
}

func TestCompactScalarPartialClearDoesNotAllocate(t *testing.T) {
	const (
		base         = uintptr(0x460000)
		measuredRuns = 64
		seeded       = measuredRuns + 2 // AllocsPerRun performs one warmup call.
	)
	d := NewDetector()
	ctx := goroutine.Alloc(251)
	defer ctx.C.Release()

	for i := uintptr(0); i < seeded; i++ {
		d.OnWrite(base+i, ctx, 0xc601)
		if slot := d.rangeMemory.GetSlot(base + i); slot != nil {
			t.Fatalf("compact clear setup at %#x materialized slot %p", base+i, slot)
		}
	}
	before := d.ShadowGet(base)
	if before == nil || before.GetLifecycleID() == 0 {
		t.Fatalf("compact clear setup has invalid state: %v", before)
	}
	oldLifecycle := before.GetLifecycleID()

	var next uintptr
	if allocs := testing.AllocsPerRun(measuredRuns, func() {
		d.ClearShadowRange(base+next, 1)
		next++
	}); allocs != 0 {
		t.Fatalf("compact partial clear allocated %.2f objects per call", allocs)
	}
	if next != measuredRuns+1 {
		t.Fatalf("AllocsPerRun invoked clear %d times, want %d", next, measuredRuns+1)
	}
	for i := uintptr(0); i < next; i++ {
		if state := d.ShadowGet(base + i); state != nil {
			t.Fatalf("cleared compact address %#x retained state %p", base+i, state)
		}
	}
	if state := d.ShadowGet(base + next); state == nil || state.GetW() != ctx.GetEpoch() {
		t.Fatalf("adjacent uncleared compact history was lost: %v", state)
	}
	for offset := uintptr(0); offset < next; offset += 8 {
		if slot := d.rangeMemory.GetSlot(base + offset); slot != nil {
			t.Fatalf("allocation-free clear materialized word at %#x: %p", base+offset, slot)
		}
	}

	// Reusing a tombstoned address must start a new allocator lifetime without
	// reconnecting it to the retained compact class.
	reused := goroutine.Alloc(252)
	defer reused.C.Release()
	d.OnWrite(base, reused, 0xc602)
	after := d.ShadowGet(base)
	if after == nil || after.GetW() != reused.GetEpoch() {
		t.Fatalf("reused compact address has incorrect history: %v", after)
	}
	if after.GetLifecycleID() == 0 || after.GetLifecycleID() == oldLifecycle {
		t.Fatalf("reused compact address lifecycle = %d, want nonzero and different from %d",
			after.GetLifecycleID(), oldLifecycle)
	}
	if slot := d.rangeMemory.GetSlot(base); slot != nil {
		t.Fatalf("first access after compact clear materialized slot %p", slot)
	}
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("compact clear/reuse reported %d stale races", got)
	}
}

func assertCompactScalarEquivalent(t *testing.T, step int, got, want *shadowmem.VarState) {
	t.Helper()
	if got == nil || want == nil {
		if got != want {
			t.Fatalf("step %d nil state mismatch: compact=%p forced=%p", step, got, want)
		}
		return
	}
	if got.GetW() != want.GetW() ||
		got.GetReadEpoch() != want.GetReadEpoch() ||
		!reflect.DeepEqual(got.GetReadEpochs(), want.GetReadEpochs()) ||
		got.GetReaderCount() != want.GetReaderCount() ||
		got.IsPromoted() != want.IsPromoted() ||
		got.GetWritePC() != want.GetWritePC() ||
		got.GetReadPC() != want.GetReadPC() ||
		got.GetExclusiveWriter() != want.GetExclusiveWriter() ||
		got.GetWriteCount() != want.GetWriteCount() ||
		got.GetWriteStack() != want.GetWriteStack() ||
		got.GetReadStack() != want.GetReadStack() {
		t.Fatalf("step %d compact/forced state mismatch:\n compact %s\n forced  %s", step, got, want)
	}
	if got.GetAtomicState() != nil || want.GetAtomicState() != nil {
		t.Fatalf("step %d ordinary compact/forced transition acquired atomic state", step)
	}
	if (got.GetLifecycleID() == 0) != (want.GetLifecycleID() == 0) {
		t.Fatalf("step %d lifecycle presence mismatch: compact=%d forced=%d",
			step, got.GetLifecycleID(), want.GetLifecycleID())
	}
}
