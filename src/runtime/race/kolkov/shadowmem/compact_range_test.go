//go:build amd64 || arm64

package shadowmem

import (
	"testing"

	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/vectorclock"
)

func compactRangeTestClock(entries ...epoch.Epoch) *vectorclock.VectorClock {
	clock := vectorclock.New()
	for _, entry := range entries {
		tid, value := entry.Decode()
		clock.Set(tid, uint32(value))
	}
	return clock
}

func TestCompactRangeVirginExactMembershipAndNeighbors(t *testing.T) {
	for _, test := range []struct {
		name   string
		offset uintptr
		size   uintptr
	}{
		{name: "one-aligned", offset: 64, size: 1},
		{name: "one-unaligned", offset: 67, size: 1},
		{name: "sixteen", offset: 61, size: 16},
		{name: "thirty-two", offset: 4061, size: 32},
	} {
		t.Run(test.name, func(t *testing.T) {
			pt := NewPageTableShadow()
			base := uintptr(0x2000000)
			current := epoch.NewEpoch(7, 11)
			if !pt.TryCompactWriteRange(base+test.offset, test.size, current, compactRangeTestClock(current), 0x1010) {
				t.Fatal("virgin range did not compact")
			}
			state := pt.Get(base + test.offset)
			if state == nil || state.GetW() != current {
				t.Fatalf("first selected state = %v, want W=%v", state, current)
			}
			for offset := test.offset; offset < test.offset+test.size; offset++ {
				if got := pt.Get(base + offset); got != state {
					t.Fatalf("selected offset %d state = %p, want shared %p", offset, got, state)
				}
				if slot := pt.GetSlot(base + offset); slot != nil {
					t.Fatalf("selected offset %d materialized slot %p", offset, slot)
				}
			}
			if test.offset != 0 && pt.Get(base+test.offset-1) != nil {
				t.Fatal("left neighbor inherited compact range")
			}
			if test.offset+test.size < rangeBlockSize && pt.Get(base+test.offset+test.size) != nil {
				t.Fatal("right neighbor inherited compact range")
			}
		})
	}
}

func TestCompactRangeSplitConvergeAndReadWrite(t *testing.T) {
	pt := NewPageTableShadow()
	const base = uintptr(0x2100000)
	first := epoch.NewEpoch(8, 3)
	second := epoch.NewEpoch(8, 4)
	clock := compactRangeTestClock(first, second)

	if !pt.TryCompactWriteRange(base+32, 32, first, clock, 0x2010) {
		t.Fatal("initial write range failed")
	}
	writeState := pt.Get(base + 32)
	if !pt.TryCompactReadRange(base+32, 16, second, clock, 0x2011) {
		t.Fatal("first split read failed")
	}
	readState := pt.Get(base + 32)
	if readState == writeState || pt.Get(base+48) != writeState {
		t.Fatal("read range did not split exact membership")
	}
	if !pt.TryCompactReadRange(base+48, 16, second, clock, 0x2011) {
		t.Fatal("converging read failed")
	}
	for offset := uintptr(32); offset < 64; offset++ {
		if got := pt.Get(base + offset); got != readState {
			t.Fatalf("converged offset %d state = %p, want %p", offset, got, readState)
		}
	}

	// Range write-after-read deliberately remains compact when afterWrite proves
	// the read frontier ordered; scalar compaction may promote this pattern.
	if !pt.TryCompactWriteRange(base+32, 32, second, clock, 0x2012) {
		t.Fatal("ordered range write-after-read failed")
	}
	for offset := uintptr(32); offset < 64; offset++ {
		state := pt.Get(base + offset)
		if state == nil || state.GetW() != second || state.GetReaderCount() != 0 {
			t.Fatalf("offset %d write-after-read state = %v", offset, state)
		}
		if pt.GetSlot(base+offset) != nil {
			t.Fatalf("offset %d write-after-read materialized", offset)
		}
	}
}

func TestCompactRangeConvergentSourcesAndDestinationAliasAcrossWords(t *testing.T) {
	for _, test := range []struct {
		name           string
		finalPC        uintptr
		existingTarget bool
	}{
		{name: "new-destination", finalPC: 0x2112},
		{name: "existing-source-destination", finalPC: 0x2110, existingTarget: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			pt := NewPageTableShadow()
			const base = uintptr(0x2110000)
			current := epoch.NewEpoch(18, 7)
			clock := compactRangeTestClock(current)

			// The adjacent sources differ only in read PC. Their union crosses a
			// bitmap-word boundary, so the final transition must snapshot both
			// source masks before it publishes either destination mask.
			if !pt.TryCompactReadRange(base+60, 8, current, clock, 0x2110) {
				t.Fatal("first source read failed")
			}
			first := pt.Get(base + 60)
			if !pt.TryCompactReadRange(base+68, 8, current, clock, 0x2111) {
				t.Fatal("second source read failed")
			}
			second := pt.Get(base + 68)
			if first == nil || second == nil || first == second {
				t.Fatalf("sources did not have distinct histories: first=%p second=%p", first, second)
			}

			if !pt.TryCompactReadRange(base+60, 16, current, clock, test.finalPC) {
				t.Fatal("convergent source transition failed")
			}
			destination := pt.Get(base + 60)
			if test.existingTarget && destination != first {
				t.Fatalf("destination = %p, want existing source group %p", destination, first)
			}
			if !test.existingTarget && (destination == first || destination == second) {
				t.Fatalf("new convergent destination reused a source: destination=%p first=%p second=%p", destination, first, second)
			}
			for offset := uintptr(60); offset < 76; offset++ {
				if got := pt.Get(base + offset); got != destination {
					t.Fatalf("offset %d state = %p, want converged %p", offset, got, destination)
				}
			}
			if pt.Get(base+59) != nil || pt.Get(base+76) != nil {
				t.Fatal("convergent transition changed an unselected neighbor")
			}
		})
	}
}

func TestCompactRangeTombstoneAndDefaultMixAcrossWords(t *testing.T) {
	pt := NewPageTableShadow()
	const base = uintptr(0x2120000)
	old := epoch.NewEpoch(19, 1)
	if !pt.TryCompactReadRange(base+128, 1, old, compactRangeTestClock(old), 0x2120) {
		t.Fatal("failed to create compact block header")
	}
	oldState := pt.Get(base + 128)

	// Clearing the middle of the selected range creates an authoritative-zero
	// tombstone class, while its two neighbors still inherit the fresh block
	// default. The combined range crosses a bitmap-word boundary.
	pt.ClearRange(base+62, 4)
	for offset := uintptr(62); offset < 66; offset++ {
		if state := pt.Get(base + offset); state != nil {
			t.Fatalf("cleared offset %d state = %p, want tombstone zero", offset, state)
		}
	}
	fresh := epoch.NewEpoch(20, 1)
	if !pt.TryCompactReadRange(base+60, 8, fresh, compactRangeTestClock(fresh), 0x2121) {
		t.Fatal("mixed tombstone/default range did not compact")
	}
	destination := pt.Get(base + 60)
	if destination == nil || destination.GetReadEpoch() != fresh {
		t.Fatalf("mixed destination = %v, want read %v", destination, fresh)
	}
	for offset := uintptr(60); offset < 68; offset++ {
		if got := pt.Get(base + offset); got != destination {
			t.Fatalf("mixed offset %d state = %p, want %p", offset, got, destination)
		}
	}
	if pt.Get(base+59) != nil || pt.Get(base+68) != nil {
		t.Fatal("mixed transition changed an unselected default neighbor")
	}
	if got := pt.Get(base + 128); got != oldState {
		t.Fatalf("mixed transition changed old-lifecycle source: got %p want %p", got, oldState)
	}
}

func TestCompactRangeDefaultAndTombstoneLifecycle(t *testing.T) {
	pt := NewPageTableShadow()
	const base = uintptr(0x2200000)
	write := epoch.NewEpoch(9, 2)
	read := epoch.NewEpoch(9, 3)
	clock := compactRangeTestClock(write, read)
	pt.AccessRange(base, rangeBlockSize, func(_ uintptr, _ uint8, state *VarState) {
		state.SetW(write)
		state.SetExclusiveWriter(9)
		state.IncrementWriteCount()
		state.SetWritePC(0x3010)
	})
	defaultState := pt.Get(base)

	if !pt.TryCompactReadRange(base+101, 7, read, clock, 0x3011) {
		t.Fatal("partial default read did not compact")
	}
	selected := pt.Get(base + 101)
	if selected == nil || selected == defaultState || selected.GetReadEpoch() != read {
		t.Fatalf("default override = %v, default=%p", selected, defaultState)
	}
	if got := pt.Get(base + 100); got != defaultState {
		t.Fatalf("uncovered default changed: got %p want %p", got, defaultState)
	}

	pt.ClearRange(base+103, 2)
	if pt.Get(base+103) != nil || pt.Get(base+104) != nil {
		t.Fatal("partial clear failed to publish exact zero")
	}
	fresh := epoch.NewEpoch(10, 1)
	if !pt.TryCompactReadRange(base+103, 2, fresh, compactRangeTestClock(fresh), 0x3012) {
		t.Fatal("tombstone read did not return to compact history")
	}
	if state := pt.Get(base + 103); state == nil || state.GetReadEpoch() != fresh || state.GetLifecycleID() == selected.GetLifecycleID() {
		t.Fatalf("fresh tombstone lifecycle was not isolated: %v", state)
	}
	if got := pt.Get(base + 102); got != selected {
		t.Fatalf("neighbor of recycled tombstone changed: got %p want %p", got, selected)
	}
}

func TestCompactRangeCapacityUpgradesAtomically(t *testing.T) {
	pt := NewPageTableShadow()
	const base = uintptr(0x2300000)
	current := epoch.NewEpoch(11, 1)
	clock := compactRangeTestClock(current)
	for i := 0; i < compactGroupCapacity; i++ {
		if !pt.TryCompactWrite(base+uintptr(i), current, clock, uintptr(0x4000+i)) {
			t.Fatalf("seed descriptor %d failed", i)
		}
	}
	view, _ := pt.blockFor(base, false)
	compact := view.history.compact.Load()
	if !pt.TryCompactWriteRange(base+100, 4, current, clock, 0x4fff) {
		t.Fatal("capacity range did not upgrade")
	}
	palette := compact.palette.Load()
	if palette == nil {
		t.Fatal("dense palette was not published")
	}
	for offset := uintptr(100); offset < 104; offset++ {
		descriptor, ok := palette.descriptor(offset)
		if !ok || descriptor.history.writePC != 0x4fff {
			t.Fatalf("offset %d descriptor = (%+v,%v)", offset, descriptor, ok)
		}
	}
}

func TestCompactRangeReleasesRuntimePathLocks(t *testing.T) {
	pt := NewPageTableShadow()
	const base = uintptr(0x2308000)
	writer := epoch.NewEpoch(17, 1)
	writerClock := compactRangeTestClock(writer)

	// Establish a materialized block default. Compact range transitions lock
	// both this state and the owning block while runtime range hooks execute on
	// the system stack.
	pt.AccessRange(base, rangeBlockSize, func(_ uintptr, _ uint8, state *VarState) {
		state.SetW(writer)
		state.SetExclusiveWriter(17)
		state.IncrementWriteCount()
		state.SetWritePC(0x4080)
	})
	view, _ := pt.blockFor(base, false)
	defaultState := view.history.state.Load()
	if defaultState == nil {
		t.Fatal("block default was not materialized")
	}
	checkUnlocked := func(stage string) {
		if got := defaultState.accessMu.state.Load(); got != 0 {
			t.Fatalf("%s left default access lock in state %d", stage, got)
		}
		if got := view.history.mu.state.Load(); got != 0 {
			t.Fatalf("%s left block lock in state %d", stage, got)
		}
	}

	reader := epoch.NewEpoch(17, 2)
	orderedClock := compactRangeTestClock(writer, reader)
	if !pt.TryCompactReadRange(base+128, 8, reader, orderedClock, 0x4081) {
		t.Fatal("ordered sparse range read failed")
	}
	checkUnlocked("sparse success")

	concurrent := epoch.NewEpoch(18, 1)
	if pt.TryCompactReadRange(base+256, 8, concurrent, compactRangeTestClock(concurrent), 0x4082) {
		t.Fatal("concurrent sparse range read unexpectedly succeeded")
	}
	checkUnlocked("sparse rejection")

	// Give distinct bytes enough exact history classes to upgrade this same
	// block to a dense palette without replacing its materialized default.
	for i := 0; i <= compactPaletteBitmapCrossover; i++ {
		if !pt.TryCompactWriteRange(base+512+uintptr(i), 1, writer, writerClock, uintptr(0x4090+i)) {
			t.Fatalf("dense seed %d failed", i)
		}
	}
	compact := view.history.compact.Load()
	if compact == nil || compact.palette.Load() == nil {
		t.Fatal("dense palette was not published")
	}
	if !pt.TryCompactReadRange(base+1024, 8, reader, orderedClock, 0x40a0) {
		t.Fatal("ordered dense range read failed")
	}
	checkUnlocked("dense success")
	if pt.TryCompactReadRange(base+1032, 8, concurrent, compactRangeTestClock(concurrent), 0x40a1) {
		t.Fatal("concurrent dense range read unexpectedly succeeded")
	}
	checkUnlocked("dense rejection")
}

func TestCompactRangeMultiDestinationDenseSteadyStateDoesNotAllocate(t *testing.T) {
	pt := NewPageTableShadow()
	const base = uintptr(0x2310000)
	current := epoch.NewEpoch(16, 1)
	clock := compactRangeTestClock(current)
	for i := 0; i < compactGroupCapacity-1; i++ {
		if !pt.TryCompactWrite(base+uintptr(i), current, clock, uintptr(0x4100+i)) {
			t.Fatalf("seed descriptor %d failed", i)
		}
	}
	view, _ := pt.blockFor(base, false)
	compact := view.history.compact.Load()
	if !pt.TryCompactReadRange(base, 2, current, clock, 0x41ff) || compact.palette.Load() == nil {
		t.Fatal("first multi-destination access did not upgrade")
	}
	failed := false
	if allocs := testing.AllocsPerRun(1000, func() {
		if !pt.TryCompactReadRange(base, 2, current, clock, 0x41ff) {
			failed = true
		}
	}); allocs != 0 {
		t.Fatalf("dense steady state allocated %.2f objects", allocs)
	}
	if failed {
		t.Fatal("dense steady-state access failed")
	}
}

func TestCompactRangeRejectsConflictAndMaterializedWord(t *testing.T) {
	pt := NewPageTableShadow()
	const base = uintptr(0x2400000)
	writer := epoch.NewEpoch(12, 1)
	if !pt.TryCompactWriteRange(base+7, 17, writer, compactRangeTestClock(writer), 0x5010) {
		t.Fatal("seed write failed")
	}
	before := pt.Get(base + 7)
	reader := epoch.NewEpoch(13, 1)
	if pt.TryCompactReadRange(base+7, 17, reader, compactRangeTestClock(reader), 0x5011) {
		t.Fatal("concurrent read compacted")
	}
	if got := pt.Get(base + 7); got != before || got.GetW() != writer {
		t.Fatalf("conflict changed compact source: %p W=%v", got, got.GetW())
	}

	pt.GetOrCreateSlot(base + 80)
	if pt.TryCompactWriteRange(base+79, 4, writer, compactRangeTestClock(writer), 0x5012) {
		t.Fatal("range intersecting a materialized word compacted")
	}
}

func TestCompactRangeRepeatedAccessDoesNotAllocate(t *testing.T) {
	pt := NewPageTableShadow()
	const (
		base = uintptr(0x2500000)
		size = uintptr(32)
	)
	current := epoch.NewEpoch(14, 1)
	clock := compactRangeTestClock(current)
	if !pt.TryCompactReadRange(base+3, size, current, clock, 0x6010) {
		t.Fatal("warm read range failed")
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if !pt.TryCompactReadRange(base+3, size, current, clock, 0x6010) {
			t.Fatal("repeated read range fell back")
		}
	}); allocs != 0 {
		t.Fatalf("repeated compact range allocated %.2f objects", allocs)
	}
}

func TestCompactRangeHistoryChurnRecyclesEmptyGroups(t *testing.T) {
	pt := NewPageTableShadow()
	const (
		base   = uintptr(0x2510000)
		offset = uintptr(17)
		size   = uintptr(64)
	)
	clock := vectorclock.New()
	sequence := uint64(1)
	transition := func() {
		current := epoch.NewEpoch(14, sequence)
		clock.Set(14, uint32(sequence))
		sequence++
		if !pt.TryCompactWriteRange(base+offset, size, current, clock, uintptr(sequence)) ||
			!pt.TryCompactReadRange(base+offset, size, current, clock, uintptr(sequence+1)) {
			panic("range history churn fell back")
		}
	}
	for i := 0; i < 2*compactGroupCapacity; i++ {
		transition()
	}
	view, _ := pt.blockFor(base, false)
	if got := compactActiveGroups(view.history.compact.Load()); got != 1 {
		t.Fatalf("active groups after range churn = %d, want 1", got)
	}
	for addr := base + offset; addr < base+offset+size; addr += 8 {
		if slot := pt.GetSlot(addr); slot != nil {
			t.Fatalf("range churn materialized slot at %#x: %p", addr, slot)
		}
	}
	if allocs := testing.AllocsPerRun(1000, transition); allocs != 0 {
		t.Fatalf("warmed unexposed range churn allocated %.2f objects/op", allocs)
	}
}

func TestCompactRangeReservationDoesNotRecycleExactDestination(t *testing.T) {
	groups := newCompactGroups()
	current := epoch.NewEpoch(15, 1)
	clock := compactRangeTestClock(current)

	// Anchors 0 and 1 are the two selected source classes. Warm and then empty
	// the exact post-read descriptor for anchor 0.
	if _, ok := groups.tryWrite(0, current, clock, 10); !ok {
		t.Fatal("first source setup failed")
	}
	if _, ok := groups.tryWrite(1, current, clock, 20); !ok {
		t.Fatal("second source setup failed")
	}
	if _, ok := groups.tryWrite(100, current, clock, 10); !ok {
		t.Fatal("exact-destination warm write failed")
	}
	warmed, ok := groups.tryRead(100, current, clock, 30)
	if !ok {
		t.Fatal("exact-destination warm read failed")
	}
	// Keep the warmed destination live while filling physical bitmap slots, so
	// crossover recycling cannot legitimately use it as an empty donor.
	if _, ok := groups.tryWrite(101, current, clock, 10); !ok {
		t.Fatal("exact-destination keeper write failed")
	}
	if joined, ok := groups.tryRead(101, current, clock, 30); !ok || joined != warmed {
		t.Fatal("exact-destination keeper did not join warmed group")
	}
	if _, ok := groups.tryRead(100, current, clock, 31); !ok {
		t.Fatal("exact-destination retirement read failed")
	}
	warmedDescriptor, descriptorOK := compactDescriptorFromState(warmed)
	if !descriptorOK {
		t.Fatal("warmed state was not representable")
	}
	warmedGroup := groups.findDescriptor(warmedDescriptor, nil)
	if warmedGroup == nil || warmedGroup.empty() {
		t.Fatalf("warmed exact destination = %p, empty=%v", warmedGroup, warmedGroup != nil && warmedGroup.empty())
	}

	// Fill the table, then create one other empty cache. The range needs the
	// warmed exact destination plus that recyclable cache. Failing to reserve the
	// exact slot aliases two unequal post-transition descriptors.
	var admissionBlocker *compactGroup
	for i := 0; i < compactGroupCapacity-5; i++ {
		if _, ok := groups.tryWrite(uintptr(200+i), current, clock, uintptr(100+i)); !ok {
			t.Fatalf("fill group %d failed", i)
		}
		if admissionBlocker == nil && groups.allocatedGroupCount() == compactPaletteBitmapCrossover {
			admissionBlocker, _ = groups.lookupGroup(200)
			admissionBlocker.joinable = false
		}
	}
	if admissionBlocker == nil {
		t.Fatal("did not install bitmap admission blocker")
	}
	groups.retireRange(101, 1)
	groups.retireRange(201, 1)
	if !warmedGroup.empty() {
		t.Fatal("warmed destination keeper did not retire")
	}
	if groups.reusableSlot() < 0 {
		t.Fatal("expected recyclable empty cache")
	}
	if !groups.tryRange(0, 2, nil, current, clock, 30, false) {
		t.Fatal("two-destination range failed despite sufficient live capacity")
	}
	admissionBlocker.joinable = true
	first, overlapFirst := groups.lookupGroup(0)
	second, overlapSecond := groups.lookupGroup(1)
	if first == nil || second == nil || first == second || overlapFirst || overlapSecond {
		t.Fatalf("range destinations aliased: first=%p/%v second=%p/%v", first, overlapFirst, second, overlapSecond)
	}
	if first != warmedGroup || first.descriptor.history.writePC != 10 || second.descriptor.history.writePC != 20 {
		t.Fatalf("range descriptors = first %+v/%p second %+v; warmed=%p",
			first.descriptor.history, first, second.descriptor.history, warmedGroup)
	}
}
