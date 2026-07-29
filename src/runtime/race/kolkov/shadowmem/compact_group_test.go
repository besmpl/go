//go:build amd64 || arm64

package shadowmem

import (
	"testing"

	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/vectorclock"
)

func compactTestEpoch(tid uint32, clock uint64) epoch.Epoch {
	return epoch.NewEpoch(tid, clock)
}

func compactTestClock(entries ...epoch.Epoch) *vectorclock.VectorClock {
	clock := vectorclock.New()
	for _, entry := range entries {
		tid, value := entry.Decode()
		clock.Set(tid, uint32(value))
	}
	return clock
}

func compactActiveGroups(groups *compactGroups) int {
	count := 0
	for i := range groups.groups {
		group := groups.groups[i].Load()
		if group != nil && !group.empty() {
			count++
		}
	}
	return count
}

// compactTestKeepBitmapThroughCapacity makes speculative dense admission fail
// while a bitmap-specific regression fills the physical 16-slot table. Restore
// the returned group's joinability before exercising the behavior under test.
func compactTestKeepBitmapThroughCapacity(t *testing.T, groups *compactGroups, anchor uintptr) *compactGroup {
	t.Helper()
	group, overlap := groups.lookupGroup(anchor)
	if group == nil || overlap {
		t.Fatalf("bitmap admission blocker at %d = %p overlap=%v", anchor, group, overlap)
	}
	group.joinable = false
	return group
}

func TestCompactClearLifecyclePolicyParity(t *testing.T) {
	for _, dense := range []bool{false, true} {
		name := "bitmap"
		if dense {
			name = "palette"
		}
		t.Run(name, func(t *testing.T) {
			newGroups := func() *compactGroups {
				groups := newCompactGroups()
				if dense {
					groups.palette.Store(new(compactPalette))
				}
				groups.activate()
				groups.ensureLifecycle()
				return groups
			}

			t.Run("empty known clear", func(t *testing.T) {
				groups := newGroups()
				before := groups.lifecycle
				groups.clearRangeKnownDefault(101, 1, false)
				if groups.lifecycle != before {
					t.Fatalf("virgin clear advanced lifecycle from %v to %v", before, groups.lifecycle)
				}
				groups.clearRangeKnownDefault(101, 1, true)
				if groups.lifecycle != before {
					t.Fatalf("repeated tombstone clear advanced lifecycle from %v to %v", before, groups.lifecycle)
				}
				if state, authoritative := groups.lookupExact(101); state != nil || !authoritative {
					t.Fatalf("known clear lookup=(%p,%v), want authoritative zero", state, authoritative)
				}
			})

			t.Run("conservative direct clear", func(t *testing.T) {
				groups := newGroups()
				before := groups.lifecycle
				groups.clearRange(102, 1)
				if groups.lifecycle == before {
					t.Fatal("direct clear did not advance lifecycle")
				}
				before = groups.lifecycle
				groups.clearRange(102, 1)
				if groups.lifecycle == before {
					t.Fatal("repeated direct clear did not advance lifecycle")
				}
			})

			t.Run("inherited default", func(t *testing.T) {
				groups := newGroups()
				before := groups.lifecycle
				groups.clearRangeKnownDefault(103, 1, true)
				if groups.lifecycle == before {
					t.Fatal("selected inherited default did not advance lifecycle")
				}
			})

			t.Run("represented history", func(t *testing.T) {
				groups := newGroups()
				current := compactTestEpoch(36, 7)
				if _, ok := groups.tryWrite(104, current, compactTestClock(current), 0x7701); !ok {
					t.Fatal("represented-history setup failed")
				}
				before := groups.lifecycle
				groups.clearRangeKnownDefault(104, 1, false)
				if groups.lifecycle == before {
					t.Fatal("represented-history clear did not advance lifecycle")
				}
			})
		})
	}
}

func TestCompactGroupsExactMembershipAndHistoryClasses(t *testing.T) {
	groups := newCompactGroups()
	current := compactTestEpoch(7, 11)
	clock := compactTestClock(current)
	const blockBase = uintptr(0x12345000)

	writeOnly, ok := groups.tryWrite(blockBase+17, current, clock, 101)
	if !ok {
		t.Fatal("fresh write was not compacted")
	}
	joinedWrite, ok := groups.tryWrite(blockBase+4095, current, clock, 101)
	if !ok || joinedWrite != writeOnly {
		t.Fatalf("equal fresh write did not join: state=%p ok=%v, want %p", joinedWrite, ok, writeOnly)
	}
	readOnly, ok := groups.tryRead(blockBase+64, current, clock, 202)
	if !ok {
		t.Fatal("fresh read was not compacted")
	}
	writeRead, ok := groups.tryWrite(blockBase+130, current, clock, 101)
	if !ok {
		t.Fatal("fresh W half of W+R was not compacted")
	}
	writeRead, ok = groups.tryRead(blockBase+130, current, clock, 202)
	if !ok {
		t.Fatal("W+R transition was not compacted")
	}

	if got := groups.lookup(blockBase + 17); got != writeOnly {
		t.Fatalf("write lookup = %p, want %p", got, writeOnly)
	}
	if got := groups.lookup(blockBase + 4095); got != writeOnly {
		t.Fatalf("masked absolute lookup = %p, want %p", got, writeOnly)
	}
	if got := groups.lookup(blockBase + 18); got != nil {
		t.Fatalf("adjacent uninstrumented anchor unexpectedly mapped to %p", got)
	}
	if writeOnly == readOnly || writeOnly == writeRead || readOnly == writeRead {
		t.Fatalf("W/R/WR classes aliased: W=%p R=%p WR=%p", writeOnly, readOnly, writeRead)
	}
	if got := compactActiveGroups(groups); got != 3 {
		t.Fatalf("active compact groups = %d, want 3", got)
	}

	writeDescriptor, ok := compactDescriptorFromState(writeOnly)
	if !ok || writeDescriptor.history.write != current || writeDescriptor.history.read != 0 {
		t.Fatalf("W key = %+v, ok=%v", writeDescriptor.history, ok)
	}
	readDescriptor, ok := compactDescriptorFromState(readOnly)
	if !ok || readDescriptor.history.write != 0 || readDescriptor.history.read != current {
		t.Fatalf("R key = %+v, ok=%v", readDescriptor.history, ok)
	}
	wrDescriptor, ok := compactDescriptorFromState(writeRead)
	if !ok || wrDescriptor.history.write != current || wrDescriptor.history.read != current {
		t.Fatalf("WR key = %+v, ok=%v", wrDescriptor.history, ok)
	}
}

func TestCompactHistorySemanticKeyAndLifecycleGeneration(t *testing.T) {
	first := newCompactGroups()
	current := compactTestEpoch(3, 9)
	clock := compactTestClock(current)
	stateA, ok := first.tryWrite(1, current, clock, 55)
	if !ok {
		t.Fatal("first compact write failed")
	}
	stateB, ok := first.tryWrite(2, current, clock, 55)
	if !ok || stateB != stateA {
		t.Fatalf("virgin anchors in one generation did not join: A=%p B=%p ok=%v", stateA, stateB, ok)
	}
	descriptorA, ok := compactDescriptorFromState(stateA)
	if !ok {
		t.Fatal("supported compact state rejected")
	}
	if descriptorA.lifecycle != first.lifecycle || stateA.lifecycleID != first.lifecycle {
		t.Fatal("published state did not preserve its block generation")
	}

	second := newCompactGroups()
	stateC, ok := second.tryWrite(3, current, clock, 55)
	if !ok {
		t.Fatal("second-generation compact write failed")
	}
	descriptorC, ok := compactDescriptorFromState(stateC)
	if !ok {
		t.Fatal("second supported state rejected")
	}
	if descriptorA.history != descriptorC.history {
		t.Fatalf("allocator lifecycle leaked into semantic key: A=%+v C=%+v", descriptorA.history, descriptorC.history)
	}
	if descriptorA == descriptorC || descriptorA.lifecycle == descriptorC.lifecycle {
		t.Fatal("published descriptor failed to distinguish allocator lifetimes")
	}

	oldGeneration := first.lifecycle
	first.reset()
	if first.lifecycle == oldGeneration {
		t.Fatal("full reset did not advance compact allocator generation")
	}
	stateAfterReset, ok := first.tryWrite(1, current, clock, 55)
	if !ok || stateAfterReset == stateA {
		t.Fatalf("post-reset state reused old pointer: state=%p old=%p ok=%v", stateAfterReset, stateA, ok)
	}
	if stateAfterReset.lifecycleID != first.lifecycle || stateAfterReset.lifecycleID == oldGeneration {
		t.Fatal("post-reset state did not preserve the new generation")
	}
}

func TestCompactTrueNoopAndWriteAfterReadPolicies(t *testing.T) {
	current := compactTestEpoch(4, 20)
	clock := compactTestClock(current)

	writes := newCompactGroups()
	write, ok := writes.tryWrite(9, current, clock, 100)
	if !ok {
		t.Fatal("initial write failed")
	}
	if got, ok := writes.tryWrite(9, current, clock, 100); ok || got != nil {
		t.Fatalf("true write no-op remained compact: state=%p ok=%v", got, ok)
	}
	if got := writes.lookup(9); got != write {
		t.Fatalf("write no-op mutated mapping: got=%p want=%p", got, write)
	}

	reads := newCompactGroups()
	read, ok := reads.tryRead(10, current, clock, 200)
	if !ok {
		t.Fatal("initial read failed")
	}
	readGroup, overlap := reads.lookupGroup(10)
	if readGroup == nil || overlap {
		t.Fatalf("initial read group = %p, overlap=%v", readGroup, overlap)
	}
	beforeRevision := reads.revision.Load()
	if got, ok := reads.tryRead(10, current, clock, 200); !ok || got != read {
		t.Fatalf("true read no-op = (%p,%v), want exact source (%p,true)", got, ok, read)
	}
	if got := reads.lookup(10); got != read {
		t.Fatalf("read no-op mutated mapping: got=%p want=%p", got, read)
	}
	if got, overlap := reads.lookupGroup(10); got != readGroup || overlap || reads.revision.Load() != beforeRevision {
		t.Fatalf("read no-op mutated compact publication: group=%p/%p overlap=%v revision=%d/%d",
			got, readGroup, overlap, reads.revision.Load(), beforeRevision)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		got, compacted := reads.tryRead(10, current, clock, 200)
		if !compacted || got != read {
			panic("compact read no-op changed result")
		}
	})
	if allocs != 0 {
		t.Fatalf("compact read no-op allocated %.2f objects/op", allocs)
	}

	if got, ok := reads.tryWrite(10, current, clock, 300); ok || got != nil {
		t.Fatalf("write-after-read remained compact: state=%p ok=%v", got, ok)
	}
	if got := reads.lookup(10); got != read {
		t.Fatalf("write-after-read fallback mutated source: got=%p want=%p", got, read)
	}
}

func TestCompactSingletonWriteReadPipelineCachesExactStates(t *testing.T) {
	groups := newCompactGroups()
	current := compactTestEpoch(4, 21)
	clock := compactTestClock(current)

	write, ok := groups.tryWrite(9, current, clock, 100)
	if !ok {
		t.Fatal("singleton write failed")
	}
	writeGroup, overlap := groups.lookupGroup(9)
	if writeGroup == nil || overlap {
		t.Fatalf("singleton write group = %p, overlap=%v", writeGroup, overlap)
	}
	writeRead, ok := groups.tryRead(9, current, clock, 200)
	if !ok || writeRead == nil || writeRead == write {
		t.Fatalf("singleton W->R transition = %p, ok=%v, write=%p", writeRead, ok, write)
	}
	if !writeGroup.empty() || !writeGroup.joinable || writeGroup.retired || writeGroup.state.Load() != write {
		t.Fatalf("emptied W descriptor was not cached: empty=%v joinable=%v retired=%v state=%p want=%p",
			writeGroup.empty(), writeGroup.joinable, writeGroup.retired, writeGroup.state.Load(), write)
	}
	if got := groups.lookup(9); got != writeRead {
		t.Fatalf("singleton pipeline lookup = %p, want %p", got, writeRead)
	}

	next := uintptr(128)
	reusedExactStates := true
	allocs := testing.AllocsPerRun(100, func() {
		anchor := next
		next += 8
		gotWrite, writeOK := groups.tryWrite(anchor, current, clock, 100)
		gotWriteRead, readOK := groups.tryRead(anchor, current, clock, 200)
		if !writeOK || !readOK || gotWrite != write || gotWriteRead != writeRead {
			reusedExactStates = false
		}
	})
	if !reusedExactStates {
		t.Fatal("later W->R anchors did not reuse the warmed W/WR states")
	}
	if allocs != 0 {
		t.Fatalf("warmed singleton W->R pipeline allocated %.2f objects/op", allocs)
	}
}

func TestCompactSimpleReadTransitionsAndConflictFailClosed(t *testing.T) {
	groups := newCompactGroups()
	first := compactTestEpoch(1, 5)
	second := compactTestEpoch(1, 8)
	clock := compactTestClock(first, second)

	state, ok := groups.tryRead(31, first, clock, 10)
	if !ok {
		t.Fatal("fresh read failed")
	}
	if joined, ok := groups.tryRead(32, first, clock, 10); !ok || joined != state {
		t.Fatal("second anchor did not form a compact class before transition")
	}
	state, ok = groups.tryRead(31, second, clock, 11)
	if !ok {
		t.Fatal("same-TID read replacement failed")
	}
	descriptor, _ := compactDescriptorFromState(state)
	if descriptor.history.read != second || descriptor.history.readPC != 11 {
		t.Fatalf("read replacement key = %+v", descriptor.history)
	}

	concurrent := compactTestEpoch(2, 1)
	before := groups.lookup(31)
	if got, ok := groups.tryRead(31, concurrent, compactTestClock(concurrent), 12); ok || got != nil {
		t.Fatalf("concurrent read did not fail closed: state=%p ok=%v", got, ok)
	}
	if got := groups.lookup(31); got != before {
		t.Fatalf("failed transition changed source: got=%p want=%p", got, before)
	}

	writes := newCompactGroups()
	write, ok := writes.tryWrite(40, first, compactTestClock(first), 20)
	if !ok {
		t.Fatal("fresh write failed")
	}
	if got, ok := writes.tryRead(40, concurrent, compactTestClock(concurrent), 21); ok || got != nil {
		t.Fatalf("concurrent W/R conflict did not fail closed: state=%p ok=%v", got, ok)
	}
	if got := writes.lookup(40); got != write {
		t.Fatalf("conflict fallback changed write state: got=%p want=%p", got, write)
	}
}

func TestCompactCapacityOverflowUpgradesToDensePalette(t *testing.T) {
	groups := newCompactGroups()
	current := compactTestEpoch(9, 1)
	clock := compactTestClock(current)
	for i := 0; i < compactGroupCapacity; i++ {
		if _, ok := groups.tryWrite(uintptr(i*8), current, clock, uintptr(100+i)); !ok {
			t.Fatalf("unique key %d overflowed early", i)
		}
	}
	if _, ok := groups.tryWrite(3000, current, clock, 999); !ok {
		t.Fatal("seventeenth live history did not upgrade")
	}
	palette := groups.palette.Load()
	if palette == nil {
		t.Fatal("dense palette was not published")
	}
	descriptor, ok := palette.descriptor(3000)
	if !ok || descriptor.history.write != current || descriptor.history.writePC != 999 {
		t.Fatalf("upgraded descriptor = (%+v,%v)", descriptor, ok)
	}
	groups.retireWord(0)
	if palette.owner(0) != compactPaletteDefault || palette.owner(3000) < compactPaletteFirstShape {
		t.Fatal("dense retirement changed the wrong anchor")
	}
}

func TestCompactScalarHistoryChurnRecyclesEmptyGroups(t *testing.T) {
	groups := newCompactGroups()
	current := compactTestEpoch(9, 4)
	clock := compactTestClock(current)
	pc := uintptr(1)
	for ; pc <= 2*compactGroupCapacity; pc++ {
		if _, ok := groups.tryWrite(0, current, clock, pc); !ok {
			t.Fatalf("history %d fell back with one live class", pc)
		}
	}
	if got := compactActiveGroups(groups); got != 1 {
		t.Fatalf("active groups after churn = %d, want 1", got)
	}

	if allocs := testing.AllocsPerRun(1000, func() {
		pc++
		if _, ok := groups.tryWrite(0, current, clock, pc); !ok {
			panic("warmed history churn fell back")
		}
	}); allocs != 0 {
		t.Fatalf("warmed unexposed history churn allocated %.2f objects/op", allocs)
	}
}

func TestCompactScalarVirginAdmissionRecyclesHistoricalCache(t *testing.T) {
	groups := newCompactGroups()
	current := compactTestEpoch(9, 6)
	clock := compactTestClock(current)
	var admissionBlocker *compactGroup
	for i := 0; i < compactGroupCapacity; i++ {
		if _, ok := groups.tryWrite(uintptr(i), current, clock, uintptr(100+i)); !ok {
			t.Fatalf("unique live class %d failed", i)
		}
		if i+1 == compactPaletteBitmapCrossover {
			admissionBlocker = compactTestKeepBitmapThroughCapacity(t, groups, 0)
		}
	}
	admissionBlocker.joinable = true
	// Converge all live anchors into one descriptor. This leaves a full table
	// containing one live class and fifteen historical empty caches, with no nil
	// or retired slot for the old admission policy to use.
	for i := 0; i < compactGroupCapacity; i++ {
		if _, ok := groups.tryWrite(uintptr(i), current, clock, 999); !ok {
			t.Fatalf("class %d did not converge", i)
		}
	}
	if got := compactActiveGroups(groups); got != 1 {
		t.Fatalf("active groups after convergence = %d, want 1", got)
	}
	for i := range groups.groups {
		group := groups.groups[i].Load()
		if group == nil || group.retired {
			t.Fatalf("slot %d = %p retired=%v, want non-retired historical cache", i, group, group != nil && group.retired)
		}
	}

	const virgin = uintptr(3000)
	if _, ok := groups.tryWrite(virgin, current, clock, 1000); !ok {
		t.Fatal("virgin anchor was rejected despite only two live classes")
	}
	if got := compactActiveGroups(groups); got != 2 || groups.lookup(virgin) == nil {
		t.Fatalf("virgin admission active groups=%d state=%p, want 2/non-nil", got, groups.lookup(virgin))
	}
}

func TestCompactEmptyGroupRecyclePreservesExposedState(t *testing.T) {
	groups := newCompactGroups()
	current := compactTestEpoch(9, 5)
	clock := compactTestClock(current)
	first, ok := groups.tryWrite(0, current, clock, 1)
	if !ok {
		t.Fatal("initial history failed")
	}
	retained := groups.lookup(0)
	retainedDescriptor, retainedOK := compactDescriptorFromState(retained)
	retainedGroup, overlap := groups.lookupGroup(0)
	if retained != first || !retainedOK || retainedGroup == nil || overlap {
		t.Fatalf("retained setup = state %p/%p descriptorOK=%v group=%p overlap=%v",
			retained, first, retainedOK, retainedGroup, overlap)
	}

	// Fill every slot with a distinct historical descriptor. The next change
	// must recycle the first, now-empty group which owns retained.
	for pc := uintptr(2); pc <= compactGroupCapacity+1; pc++ {
		if _, ok := groups.tryWrite(0, current, clock, pc); !ok {
			t.Fatalf("history %d fell back", pc)
		}
	}
	if groups.groups[0].Load() != retainedGroup {
		t.Fatalf("empty group object was replaced: got %p want %p", groups.groups[0].Load(), retainedGroup)
	}
	if replacement := retainedGroup.state.Load(); replacement == retained {
		t.Fatalf("exposed state was reused in place: retained=%p replacement=%p", retained, replacement)
	}
	if descriptor, ok := compactDescriptorFromState(retained); !ok || descriptor != retainedDescriptor {
		t.Fatalf("retained state changed: descriptor=%+v ok=%v want=%+v", descriptor, ok, retainedDescriptor)
	}
}

func TestCompactCapacityFullSoleSourceCOWReplacement(t *testing.T) {
	groups := newCompactGroups()
	current := compactTestEpoch(9, 2)
	clock := compactTestClock(current)
	var admissionBlocker *compactGroup
	for i := 0; i < compactGroupCapacity; i++ {
		if _, ok := groups.tryWrite(uintptr(i*8), current, clock, uintptr(100+i)); !ok {
			t.Fatalf("unique key %d overflowed early", i)
		}
		if i+1 == compactPaletteBitmapCrossover {
			admissionBlocker = compactTestKeepBitmapThroughCapacity(t, groups, 0)
		}
	}
	admissionBlocker.joinable = true

	const anchor = uintptr(0)
	source, overlap := groups.lookupGroup(anchor)
	oldState := groups.lookup(anchor)
	oldDescriptor, oldOK := compactDescriptorFromState(oldState)
	if source == nil || overlap || oldState == nil || !oldOK || !source.soleMember(anchor) {
		t.Fatalf("full-capacity source setup = group=%p overlap=%v state=%p descriptorOK=%v sole=%v",
			source, overlap, oldState, oldOK, source != nil && source.soleMember(anchor))
	}

	const nextPC = uintptr(999)
	state, ok := groups.tryWrite(anchor, current, clock, nextPC)
	if !ok || state == nil || state == oldState {
		t.Fatalf("full-capacity sole replacement = %p, ok=%v, old=%p", state, ok, oldState)
	}
	after, afterOverlap := groups.lookupGroup(anchor)
	if after != source || afterOverlap || after.state.Load() != state || !after.joinable || after.retired ||
		!after.soleMember(anchor) || compactActiveGroups(groups) != compactGroupCapacity {
		t.Fatalf("replacement changed group/membership: before=%p after=%p overlap=%v state=%p joinable=%v retired=%v sole=%v active=%d",
			source, after, afterOverlap, after.state.Load(), after.joinable, after.retired,
			after.soleMember(anchor), compactActiveGroups(groups))
	}
	descriptor, descriptorOK := compactDescriptorFromState(state)
	if !descriptorOK || descriptor != after.descriptor || descriptor.lifecycle != oldDescriptor.lifecycle ||
		descriptor.history.write != current || descriptor.history.writePC != nextPC || descriptor.history.writeCount != 1 {
		t.Fatalf("replacement descriptor = %+v, ok=%v; group=%+v old=%+v", descriptor, descriptorOK, after.descriptor, oldDescriptor)
	}
	if immutable, immutableOK := compactDescriptorFromState(oldState); !immutableOK || immutable != oldDescriptor {
		t.Fatalf("old immutable state changed: descriptor=%+v ok=%v, want %+v", immutable, immutableOK, oldDescriptor)
	}
	for i := 1; i < compactGroupCapacity; i++ {
		other := groups.lookup(uintptr(i * 8))
		if other == nil || other.writePC.Load() != uintptr(100+i) {
			t.Fatalf("replacement changed unrelated key %d: %v", i, other)
		}
	}
}

func TestCompactCapacityFullUnexposedSoleSourceReusesState(t *testing.T) {
	groups := newCompactGroups()
	current := compactTestEpoch(9, 3)
	clock := compactTestClock(current)
	var retained *VarState
	var admissionBlocker *compactGroup
	for i := 0; i < compactGroupCapacity; i++ {
		state, ok := groups.tryWrite(uintptr(i*8), current, clock, uintptr(200+i))
		if !ok {
			t.Fatalf("unique key %d overflowed early", i)
		}
		if i == 0 {
			retained = state
		}
		if i+1 == compactPaletteBitmapCrossover {
			admissionBlocker = compactTestKeepBitmapThroughCapacity(t, groups, 0)
		}
	}
	admissionBlocker.joinable = true

	pc := uintptr(700)
	allocs := testing.AllocsPerRun(100, func() {
		pc ^= 1
		state, ok := groups.tryWrite(0, current, clock, pc)
		if !ok || state != retained {
			panic("unexposed sole state was not reused")
		}
	})
	if allocs != 0 {
		t.Fatalf("unexposed full-capacity transition allocated %.2f objects/op", allocs)
	}
}

func TestCompactRangeCallbackExposurePreservesRetainedState(t *testing.T) {
	pt := NewPageTableShadow()
	const base = uintptr(0x520000)
	current := compactTestEpoch(10, 1)
	clock := compactTestClock(current)
	var admissionBlocker *compactGroup
	for i := 0; i < compactGroupCapacity; i++ {
		if !pt.TryCompactWrite(base+uintptr(i*8), current, clock, uintptr(300+i)) {
			t.Fatalf("unique key %d overflowed early", i)
		}
		if i+1 == compactPaletteBitmapCrossover {
			view, _ := pt.blockFor(base, false)
			admissionBlocker = compactTestKeepBitmapThroughCapacity(t, view.history.compact.Load(), 0)
		}
	}
	admissionBlocker.joinable = true

	var retained *VarState
	pt.AccessRange(base, rangeBlockSize, func(word uintptr, _ uint8, state *VarState) {
		if word == base && state.GetWritePC() == 300 {
			retained = state
		}
	})
	if retained == nil {
		t.Fatal("full-block callback did not expose the selected compact state")
	}
	if !pt.TryCompactWrite(base, current, clock, 999) {
		t.Fatal("full-capacity sole transition fell back")
	}
	replacement := pt.Get(base)
	if replacement == nil || replacement == retained {
		t.Fatalf("exposed callback state was reused: retained=%p replacement=%p", retained, replacement)
	}
	if retained.GetWritePC() != 300 || replacement.GetWritePC() != 999 {
		t.Fatalf("callback state mutated: retainedPC=%#x replacementPC=%#x",
			retained.GetWritePC(), replacement.GetWritePC())
	}
}

func TestCompactRemovalCoverageAndMaterialization(t *testing.T) {
	groups := newCompactGroups()
	current := compactTestEpoch(5, 2)
	clock := compactTestClock(current)
	first, _ := groups.tryWrite(64, current, clock, 1)
	second, _ := groups.tryWrite(65, current, clock, 2)
	joined, _ := groups.tryWrite(67, current, clock, 1)
	if first != joined || first == second {
		t.Fatalf("test setup grouping failed: first=%p joined=%p second=%p", first, joined, second)
	}
	if got := groups.coveredWord(64); got != 0x0b {
		t.Fatalf("covered word = %#x, want 0x0b", got)
	}

	slot := &ShadowSlot{}
	groups.materializeWord(64, slot)
	if slot.State(0) == nil || slot.State(1) == nil || slot.State(3) == nil {
		t.Fatal("materialization omitted covered lanes")
	}
	if slot.State(0) != slot.State(3) {
		t.Fatal("one compact group was cloned more than once per word")
	}
	if slot.State(0) == first || slot.State(1) == second {
		t.Fatal("materialization reused immutable compact state directly")
	}
	if slot.State(0).lifecycleID != first.lifecycleID || slot.State(1).lifecycleID != second.lifecycleID {
		t.Fatal("materialization did not preserve lifecycle")
	}
	if slot.State(2) != nil {
		t.Fatalf("uncovered lane materialized as %p", slot.State(2))
	}

	groups.retireRange(65, 2)
	if got := groups.coveredWord(64); got != 0x09 {
		t.Fatalf("coverage after exact range removal = %#x, want 0x09", got)
	}
	if groups.lookup(65) != nil || groups.lookup(66) != nil || groups.lookup(64) == nil || groups.lookup(67) == nil {
		t.Fatal("range removal changed the wrong exact anchors")
	}
	groups.retireWord(64)
	if got := groups.coveredWord(64); got != 0 || groups.lookup(64) != nil || groups.lookup(67) != nil {
		t.Fatalf("word retirement left compact coverage %#x", got)
	}
}

func TestCompactClearPublishesExactTombstonesAndFreshGeneration(t *testing.T) {
	groups := newCompactGroups()
	current := compactTestEpoch(6, 4)
	clock := compactTestClock(current)
	old, ok := groups.tryWrite(70, current, clock, 11)
	if !ok {
		t.Fatal("setup compact write failed")
	}
	joined, ok := groups.tryWrite(71, current, clock, 11)
	if !ok || joined != old {
		t.Fatal("setup compact join failed")
	}
	oldGeneration := groups.lifecycle

	groups.clearRange(70, 1)
	if state, authoritative := groups.lookupExact(70); state != nil || !authoritative {
		t.Fatalf("cleared anchor lookup = (%p,%v), want authoritative zero", state, authoritative)
	}
	if got := groups.lookup(71); got != old {
		t.Fatalf("adjacent compact anchor changed to %p, want %p", got, old)
	}
	if state, authoritative := groups.lookupExact(72); state != nil || authoritative {
		t.Fatalf("never-represented anchor lookup = (%p,%v), want miss", state, authoritative)
	}
	if got := groups.wordTombstoneMask(64); got != 1<<6 {
		t.Fatalf("word tombstone mask = %#x, want %#x", got, uint8(1<<6))
	}
	if groups.lifecycle == oldGeneration {
		t.Fatal("partial clear did not advance virgin generation")
	}

	freshGeneration := groups.lifecycle
	fresh, ok := groups.tryWrite(70, current, clock, 11)
	if !ok {
		t.Fatal("fresh transition from tombstone failed")
	}
	if fresh == old || fresh.lifecycleID != freshGeneration || fresh.lifecycleID == old.lifecycleID {
		t.Fatalf("tombstone reaccess state=%p lifecycle=%v; old=%p lifecycle=%v current=%v",
			fresh, fresh.lifecycleID, old, old.lifecycleID, freshGeneration)
	}
	if state, authoritative := groups.lookupExact(70); state != fresh || !authoritative || groups.isTombstone(70) {
		t.Fatalf("reaccess lookup = (%p,%v), tombstone=%v", state, authoritative, groups.isTombstone(70))
	}
}

func TestCompactTombstoneRetiresOnlyAfterSlotPublicationHook(t *testing.T) {
	groups := newCompactGroups()
	groups.clearRange(80, 3)
	if got := groups.wordTombstoneMask(80); got != 0x07 {
		t.Fatalf("setup tombstones = %#x, want 0x07", got)
	}

	// materializeWord intentionally does not retire authoritative-zero lanes;
	// the page table publishes its complete slot before this explicit hook.
	slot := &ShadowSlot{}
	groups.materializeWord(80, slot)
	if got := groups.wordTombstoneMask(80); got != 0x07 {
		t.Fatalf("unpublished materialization retired tombstones: %#x", got)
	}
	groups.retireWord(80)
	if got := groups.wordTombstoneMask(80); got != 0 {
		t.Fatalf("published word retirement left tombstones %#x", got)
	}
	if state, authoritative := groups.lookupExact(80); state != nil || authoritative {
		t.Fatalf("retired compact source still authoritative: (%p,%v)", state, authoritative)
	}
}

func TestCompactLookupFailsClosedDuringTombstoneToMembershipPublication(t *testing.T) {
	groups := newCompactGroups()
	const anchor = uintptr(91)
	groups.clearRange(anchor, 1)
	descriptor := compactHistoryDescriptor{
		history: compactHistoryKey{
			write:           compactTestEpoch(3, 7),
			exclusiveWriter: 3,
			writePC:         55,
			writeCount:      1,
		},
		lifecycle: groups.lifecycle,
	}
	state := compactStateFromDescriptor(descriptor)
	group := &compactGroup{descriptor: descriptor, joinable: true}
	group.state.Store(state)
	compactSetMember(group, anchor) // Still unpublished.

	groups.beginMutation()
	if got, authoritative := groups.lookupExact(anchor); got != nil || !authoritative {
		groups.endMutation()
		t.Fatalf("odd-revision lookup = (%p,%v), want conservative (nil,true)", got, authoritative)
	}

	// Destination becomes visible before the tombstone is retired. The reader
	// may conservatively miss during publication, but the next stable lookup
	// must return the destination rather than a synthetic default.
	groups.groups[0].Store(group)
	groups.clearTombstone(anchor)
	groups.endMutation()
	if got, authoritative := groups.lookupExact(anchor); got != state || !authoritative {
		t.Fatalf("stable lookup = (%p,%v), want (%p,true)", got, authoritative, state)
	}
}

func TestCompactActiveGatePublicationAndResetPermanence(t *testing.T) {
	membership := newCompactGroups()
	if got := membership.active.Load(); got != 0 {
		t.Fatalf("new header active = %d, want 0", got)
	}
	current := compactTestEpoch(13, 17)
	if _, ok := membership.tryWrite(111, current, compactTestClock(current), 23); !ok {
		t.Fatal("first compact membership publication failed")
	}
	if got := membership.active.Load(); got != 1 {
		t.Fatalf("membership header active = %d, want 1", got)
	}
	membership.reset()
	if got := membership.active.Load(); got != 1 {
		t.Fatalf("reset cleared permanent active gate: got %d, want 1", got)
	}
	if _, authoritative := membership.lookupExact(111); authoritative {
		t.Fatal("reset retained membership despite permanent active gate")
	}

	tombstone := newCompactGroups()
	if got := tombstone.active.Load(); got != 0 {
		t.Fatalf("new tombstone header active = %d, want 0", got)
	}
	tombstone.publishTombstoneRange(222, 1)
	if got := tombstone.active.Load(); got != 1 {
		t.Fatalf("tombstone header active = %d, want 1", got)
	}
	if state, authoritative := tombstone.lookupExact(222); state != nil || !authoritative {
		t.Fatalf("published tombstone = (%p,%v), want (nil,true)", state, authoritative)
	}
}

func TestCompactLookupNeverSynthesizesAbsenceAcrossMoves(t *testing.T) {
	groups := newCompactGroups()
	const anchor = uintptr(123)
	groups.clearRange(anchor, 1)
	current := compactTestEpoch(5, 6)
	clock := compactTestClock(current)
	stop := make(chan struct{})
	errors := make(chan struct{}, 1)
	done := make(chan struct{}, 4)
	for reader := 0; reader < cap(done); reader++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, authoritative := groups.lookupExact(anchor); !authoritative {
					select {
					case errors <- struct{}{}:
					default:
					}
					return
				}
			}
		}()
	}
	for i := 0; i < 1000; i++ {
		if _, ok := groups.tryWrite(anchor, current, clock, uintptr(i+1)); !ok {
			t.Fatalf("tombstone transition %d failed", i)
		}
		groups.clearRange(anchor, 1)
	}
	close(stop)
	for reader := 0; reader < cap(done); reader++ {
		<-done
	}
	select {
	case <-errors:
		t.Fatal("lock-free lookup synthesized a non-authoritative gap")
	default:
	}
}

func TestCompactClearRangeAllocatesNothing(t *testing.T) {
	groups := newCompactGroups()
	current := compactTestEpoch(8, 3)
	clock := compactTestClock(current)
	for i := uintptr(0); i < 64; i++ {
		if _, ok := groups.tryWrite(i, current, clock, 1); !ok {
			t.Fatalf("setup write %d failed", i)
		}
	}
	allocs := testing.AllocsPerRun(100, func() {
		groups.clearRange(0, 64)
	})
	if allocs != 0 {
		t.Fatalf("allocator clear allocated %.2f objects/op", allocs)
	}
	for i := uintptr(0); i < 64; i++ {
		if state, authoritative := groups.lookupExact(i); state != nil || !authoritative {
			t.Fatalf("cleared anchor %d = (%p,%v), want tombstone", i, state, authoritative)
		}
	}
}

func TestCompactResetAllocatesNothingAndClearsTombstones(t *testing.T) {
	groups := newCompactGroups()
	groups.clearRange(7, 2)
	allocs := testing.AllocsPerRun(100, func() {
		groups.reset()
	})
	if allocs != 0 {
		t.Fatalf("full compact reset allocated %.2f objects/op", allocs)
	}
	if groups.wordTombstoneMask(0) != 0 || groups.lookup(7) != nil {
		t.Fatal("full reset retained compact ownership")
	}
}

func TestCompactDescriptorRejectsUnsupportedStates(t *testing.T) {
	tests := []struct {
		name  string
		state func() *VarState
	}{
		{
			name: "atomic sidecar",
			state: func() *VarState {
				state := NewVarState()
				state.atomicState.Store(&AtomicFastPath{})
				return state
			},
		},
		{
			name: "multiple readers",
			state: func() *VarState {
				state := NewVarState()
				state.SetReadEpoch(compactTestEpoch(1, 1))
				state.AddReader(compactTestEpoch(2, 1))
				return state
			},
		},
		{
			name: "promoted readers",
			state: func() *VarState {
				state := NewVarState()
				state.SetReadEpoch(compactTestEpoch(1, 1))
				state.PromoteToReadClock(compactTestEpoch(2, 1), nil)
				return state
			},
		},
		{
			name: "write stack",
			state: func() *VarState {
				state := NewVarState()
				state.SetWriteStack(1)
				return state
			},
		},
		{
			name: "read stack",
			state: func() *VarState {
				state := NewVarState()
				state.SetReadStack(1)
				return state
			},
		},
		{
			name: "inconsistent read mirror",
			state: func() *VarState {
				state := NewVarState()
				state.readEpoch0.Store(uint64(compactTestEpoch(1, 1)))
				return state
			},
		},
		{
			name: "unsupported owner",
			state: func() *VarState {
				state := NewVarState()
				state.exclusiveWriter.Store(-2)
				return state
			},
		},
		{
			name: "zero lifecycle",
			state: func() *VarState {
				return &VarState{}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if descriptor, ok := compactDescriptorFromState(test.state()); ok {
				t.Fatalf("unsupported state admitted as %+v", descriptor)
			}
		})
	}
}

func TestCompactDescriptorDiscriminatesEverySupportedField(t *testing.T) {
	base := compactHistoryKey{
		write:           compactTestEpoch(1, 2),
		read:            compactTestEpoch(1, 3),
		exclusiveWriter: 1,
		writePC:         4,
		readPC:          5,
		writeCount:      6,
	}
	variants := []compactHistoryKey{
		{write: compactTestEpoch(1, 9), read: base.read, exclusiveWriter: base.exclusiveWriter, writePC: base.writePC, readPC: base.readPC, writeCount: base.writeCount},
		{write: base.write, read: compactTestEpoch(1, 9), exclusiveWriter: base.exclusiveWriter, writePC: base.writePC, readPC: base.readPC, writeCount: base.writeCount},
		{write: base.write, read: base.read, exclusiveWriter: -1, writePC: base.writePC, readPC: base.readPC, writeCount: base.writeCount},
		{write: base.write, read: base.read, exclusiveWriter: base.exclusiveWriter, writePC: 9, readPC: base.readPC, writeCount: base.writeCount},
		{write: base.write, read: base.read, exclusiveWriter: base.exclusiveWriter, writePC: base.writePC, readPC: 9, writeCount: base.writeCount},
		{write: base.write, read: base.read, exclusiveWriter: base.exclusiveWriter, writePC: base.writePC, readPC: base.readPC, writeCount: 9},
	}
	for i, variant := range variants {
		if variant == base {
			t.Fatalf("variant %d omitted a supported field from exact equality", i)
		}
	}
}

func TestCompactSnapshotOrderDelayedCommitAndConvergentMerge(t *testing.T) {
	groups := newCompactGroups()
	current := compactTestEpoch(2, 10)
	clock := compactTestClock(current)
	// Insert in reverse address order and distinguish initial PCs.
	second, _ := groups.tryWrite(200, current, clock, 2)
	first, _ := groups.tryWrite(8, current, clock, 1)
	if second == first {
		t.Fatal("test setup did not create two source groups")
	}

	var accesses [compactGroupCapacity]compactAccess
	count := groups.snapshotAccesses(0x5000, &accesses)
	if count != 2 || accesses[0].anchor != 0x5008 || accesses[1].anchor != 0x50c8 {
		t.Fatalf("snapshot order/count = %d [%#x, %#x]", count, accesses[0].anchor, accesses[1].anchor)
	}
	if groups.lookup(8) != first || groups.lookup(200) != second {
		t.Fatal("snapshot published COW states before callbacks completed")
	}
	for i := 0; i < count; i++ {
		accesses[i].state.SetWritePC(99)
	}
	groups.commitAccesses(&accesses, count)
	if got := compactActiveGroups(groups); got != 1 {
		t.Fatalf("convergent full transitions left %d active groups, want 1", got)
	}
	if groups.lookup(8) == nil || groups.lookup(8) != groups.lookup(200) {
		t.Fatal("convergent histories did not merge exact memberships")
	}
	if descriptor, ok := compactDescriptorFromState(groups.lookup(8)); !ok || descriptor.history.writePC != 99 {
		t.Fatalf("committed state descriptor = %+v, ok=%v", descriptor, ok)
	}
}

func TestCompactFullRangeRetainsUnsupportedImmutableGroupUntilScalarFallback(t *testing.T) {
	groups := newCompactGroups()
	first := compactTestEpoch(1, 3)
	clock := compactTestClock(first)
	state, ok := groups.tryRead(24, first, clock, 10)
	if !ok {
		t.Fatal("first compact read failed")
	}
	if joined, ok := groups.tryRead(25, first, clock, 10); !ok || joined != state {
		t.Fatal("second exact anchor did not join setup group")
	}

	second := compactTestEpoch(2, 4)
	groups.accessAll(0x7000, func(_ uintptr, _ uint8, state *VarState) {
		if !state.AddReader(second) {
			t.Fatal("test transition unexpectedly required promotion")
		}
	})
	published := groups.lookup(24)
	group, overlap := groups.lookupGroup(24)
	if published == nil || group == nil || overlap || group.joinable || published.GetReaderCount() != 2 {
		t.Fatalf("unsupported immutable group = state=%p group=%p overlap=%v joinable=%v readers=%d",
			published, group, overlap, group != nil && group.joinable, published.GetReaderCount())
	}
	if groups.lookup(25) != published {
		t.Fatal("full transition split equivalent exact members")
	}

	// A second full transition must preserve the nonjoinable group rather than
	// dropping its history. A scalar compact preflight then fails unchanged so
	// the page table can materialize an exact ordinary oracle.
	groups.accessAll(0x7000, func(_ uintptr, _ uint8, state *VarState) {
		state.SetReadPC(99)
	})
	published = groups.lookup(24)
	if published == nil || published.GetReaderCount() != 2 || published.GetReadPC() != 99 {
		t.Fatalf("second full transition lost unsupported history: %v", published)
	}
	before := published
	if got, ok := groups.tryRead(24, first, clock, 100); ok || got != nil || groups.lookup(24) != before {
		t.Fatalf("scalar unsupported fallback mutated compact history: state=%p ok=%v", got, ok)
	}
	slot := &ShadowSlot{}
	groups.materializeWord(24, slot)
	materialized := slot.State(0)
	if materialized == nil || materialized == before || materialized.GetReaderCount() != 2 || materialized.GetReadPC() != 99 {
		t.Fatalf("materialized unsupported oracle = %v (compact %p)", materialized, before)
	}
	reads := materialized.GetReadEpochs()
	if len(reads) != 2 || reads[0] != first || reads[1] != second {
		t.Fatalf("materialized readers = %v, want [%v %v]", reads, first, second)
	}
}

func TestCompactExistingGroupJoinAllocatesNothing(t *testing.T) {
	groups := newCompactGroups()
	current := compactTestEpoch(1, 1)
	clock := compactTestClock(current)
	if _, ok := groups.tryWrite(1, current, clock, 7); !ok {
		t.Fatal("setup write failed")
	}
	allocs := testing.AllocsPerRun(100, func() {
		// Retire the just-added virgin bit so every iteration measures the same
		// existing-key join rather than the deliberate no-op fallback.
		if _, ok := groups.tryWrite(2, current, clock, 7); !ok {
			t.Fatal("existing-key join failed")
		}
		groups.retireRange(2, 1)
	})
	if allocs != 0 {
		t.Fatalf("existing destination join allocated %.2f objects/op", allocs)
	}
}
