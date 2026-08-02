package vectorclock

import "testing"

func TestVectorClockCausalViewLogicalUnionAndLowering(t *testing.T) {
	anchor := New()
	anchor.Set(1, 3)
	anchor.Set(7, 9)
	anchor.RetireRange(20, 22)
	lineage := NewClockLineage(anchor)
	view, appended := lineage.AppendPinned(1, 5)
	if !appended {
		t.Fatal("lineage update was unexpectedly dominated")
	}
	defer view.Release()
	defer lineage.Release()
	anchor.Release()

	vc := New()
	defer vc.Release()
	baseClock := New()
	baseClock.Set(1, 4)
	baseClock.Set(8, 11)
	vc.JoinSnapshot(baseClock.Freeze())
	baseClock.Release()
	vc.Set(9, 13)
	if !vc.TryJoinCausal(view) {
		t.Fatal("failed to adopt causal view")
	}

	for tid, want := range map[uint32]uint32{1: 5, 7: 9, 8: 11, 9: 13, 20: ^uint32(0), 22: ^uint32(0)} {
		if got := vc.Get(tid); got != want {
			t.Fatalf("Get(%d) = %d, want %d", tid, got, want)
		}
	}

	// Lowering must remove the contribution from both immutable roots rather
	// than leave it visible behind a smaller owned value.
	vc.Set(1, 2)
	if got := vc.Get(1); got != 2 {
		t.Fatalf("lowered Get(1) = %d, want 2", got)
	}
	vc.Increment(7)
	if got := vc.Get(7); got != 10 {
		t.Fatalf("Increment causal coordinate = %d, want 10", got)
	}
	vc.Set(20, 1)
	if !vc.IsRetired(20) {
		t.Fatal("finite Set resurrected a causal retirement")
	}
}

func TestVectorClockCausalFiniteMaxUintCanBeLowered(t *testing.T) {
	anchor := New()
	anchor.Set(6, ^uint32(0))
	lineage := NewClockLineage(anchor)
	view := lineage.Pin()
	anchor.Release()
	defer view.Release()
	defer lineage.Release()

	vc := New()
	defer vc.Release()
	vc.JoinCausal(view)
	if vc.IsRetired(6) {
		t.Fatal("finite MaxUint32 coordinate became retirement")
	}
	vc.Set(6, 4)
	if got := vc.Get(6); got != 4 {
		t.Fatalf("lowered finite MaxUint32 = %d, want 4", got)
	}
}

func TestVectorClockCausalViewCopyLifecycle(t *testing.T) {
	anchor := New()
	anchor.Set(5, 17)
	lineage := NewClockLineage(anchor)
	view := lineage.Pin()
	anchor.Release()

	source := New()
	if !source.TryJoinCausal(view) {
		t.Fatal("failed to install source view")
	}
	clone := source.Clone()
	copyClock := New()
	copyClock.CopyFrom(source)
	tryCopy := New()
	if !tryCopy.TryCopyFrom(source) {
		t.Fatal("allocation-free causal copy failed")
	}

	// Drop every original owner. Each VectorClock copy must retain an
	// independent pin to the immutable segment.
	source.Release()
	view.Release()
	lineage.Release()
	for name, vc := range map[string]*VectorClock{"clone": clone, "copy": copyClock, "try-copy": tryCopy} {
		if got := vc.Get(5); got != 17 {
			t.Fatalf("%s lost causal pin: got %d, want 17", name, got)
		}
		vc.Reset()
		if got := vc.Get(5); got != 0 {
			t.Fatalf("%s reset retained causal state: got %d", name, got)
		}
		vc.Release()
	}
}

func TestVectorClockTryJoinCausalSameFamilyAndForeignFallback(t *testing.T) {
	emptyAnchor := New()
	firstLineage := NewClockLineage(emptyAnchor)
	emptyAnchor.Release()
	old, _ := firstLineage.AppendPinned(1, 1)
	newer, _ := firstLineage.AppendPinned(2, 2)
	defer old.Release()
	defer newer.Release()
	defer firstLineage.Release()

	vc := New()
	defer vc.Release()
	vc.Set(30, 3)
	if !vc.TryJoinCausal(old) || !vc.TryJoinCausal(newer) {
		t.Fatal("same-family causal advance failed")
	}
	if got := vc.Get(1); got != 1 {
		t.Fatalf("older family coordinate lost: got %d", got)
	}
	if got := vc.Get(2); got != 2 {
		t.Fatalf("new family coordinate missing: got %d", got)
	}
	if got := vc.Get(30); got != 3 {
		t.Fatalf("owned overlay lost: got %d", got)
	}

	foreignAnchor := New()
	foreignAnchor.Set(4, 4)
	foreignLineage := NewClockLineage(foreignAnchor)
	foreign := foreignLineage.Pin()
	foreignAnchor.Release()
	defer foreign.Release()
	defer foreignLineage.Release()
	if !vc.TryJoinCausal(foreign) {
		t.Fatal("second unrelated root did not fit on nonallocating path")
	}
	if got := vc.Get(4); got != 4 {
		t.Fatalf("unrelated inline root missing: got %d", got)
	}
	vc.JoinCausal(foreign)
	for tid, want := range map[uint32]uint32{1: 1, 2: 2, 4: 4, 30: 3} {
		if got := vc.Get(tid); got != want {
			t.Fatalf("foreign fallback Get(%d) = %d, want %d", tid, got, want)
		}
	}
}

func TestVectorClockTryJoinCausalSameFamilyAllocatesNothing(t *testing.T) {
	anchor := New()
	lineage := NewClockLineage(anchor)
	anchor.Release()
	old, _ := lineage.AppendPinned(1, 1)
	newer, _ := lineage.AppendPinned(2, 2)
	defer old.Release()
	defer newer.Release()
	defer lineage.Release()

	allocs := testing.AllocsPerRun(1000, func() {
		var vc VectorClock
		if !vc.TryJoinCausal(old) || !vc.TryJoinCausal(newer) {
			panic("same-family join failed")
		}
		vc.Reset()
	})
	if allocs != 0 {
		t.Fatalf("same-family TryJoinCausal allocated: %v allocs/run", allocs)
	}
}

func TestVectorClockCausalGenericOperationsMatchMaterializedOracle(t *testing.T) {
	anchor := New()
	anchor.JoinRange(100, 103, 7)
	anchor.RetireRange(200, 205)
	lineage := NewClockLineage(anchor)
	view, _ := lineage.AppendPinned(2, 8)
	defer view.Release()
	defer lineage.Release()
	anchor.Release()

	got := New()
	defer got.Release()
	got.Set(3, 9)
	got.JoinCausal(view)
	oracle := view.materialize()
	defer oracle.Release()
	oracle.Set(3, 9)

	requireSameClock(t, oracle, got)
	if !got.LessOrEqual(oracle) || !oracle.LessOrEqual(got) {
		t.Fatal("causal and materialized clocks are not equal")
	}
	frozen := got.Freeze()
	if frozen == nil || got.causal.Valid() {
		t.Fatal("Freeze did not lower and release causal root")
	}
	requireSameClock(t, oracle, got)

	observed := oracle.Clone()
	got.PruneLessOrEqual(observed)
	observed.Release()
	if got.GetMaxTID() != 205 { // retirement is never pruned
		t.Fatalf("prune lost retirement max TID: %d", got.GetMaxTID())
	}
	if got.Get(2) != 0 || got.Get(100) != 0 {
		t.Fatal("prune retained observed finite causal coordinates")
	}
}

func TestPruneSmallEventFrontierUsesCausalPointProof(t *testing.T) {
	anchor := New()
	for tid := uint32(100); tid < 108; tid++ {
		anchor.Set(tid, tid+10)
	}
	lineage := NewClockLineage(anchor)
	view := lineage.Pin()
	observed := New()
	if !observed.TryJoinCausal(view) {
		t.Fatal("failed to install observed causal root")
	}
	events := New()
	for tid := uint32(100); tid < 108; tid++ {
		events.Set(tid, tid)
	}
	refs := view.segment.refs.Load()

	events.PruneLessOrEqual(observed)
	if got := view.segment.refs.Load(); got != refs {
		t.Fatalf("causal point proof changed segment refs: got %d want %d", got, refs)
	}
	for tid := uint32(100); tid < 108; tid++ {
		if got := events.Get(tid); got != 0 {
			t.Fatalf("observed event %d remained at %d", tid, got)
		}
	}
	if !observed.causal.Valid() {
		t.Fatal("pruning materialized the observed causal root")
	}

	events.Release()
	observed.Release()
	view.Release()
	lineage.Release()
	anchor.Release()
}

func TestPruneWideSingletonEventSetKeepsCausalRootStructural(t *testing.T) {
	const eventsN = 512
	const missing = 233
	anchor := New()
	events := New()
	for i := uint32(0); i < eventsN; i++ {
		tid := uint32(100_000 + i*3)
		events.Set(tid, i+1)
		if i != missing {
			anchor.Set(tid, i+1)
		}
	}
	lineage := NewClockLineage(anchor)
	view := lineage.Pin()
	observed := New()
	if !observed.TryJoinCausal(view) {
		t.Fatal("failed to install observed causal root")
	}
	refs := view.segment.refs.Load()

	events.PruneEventSetLessOrEqual(observed)
	if got := view.segment.refs.Load(); got != refs {
		t.Fatalf("wide event pruning changed segment refs: got %d want %d", got, refs)
	}
	for i := uint32(0); i < eventsN; i++ {
		tid := uint32(100_000 + i*3)
		want := uint32(0)
		if i == missing {
			want = i + 1
		}
		if got := events.Get(tid); got != want {
			t.Fatalf("event %d = %d, want %d", i, got, want)
		}
	}
	if !observed.causal.Valid() {
		t.Fatal("wide event pruning materialized observed causal root")
	}

	events.Release()
	observed.Release()
	view.Release()
	lineage.Release()
	anchor.Release()
}

func TestPruneCoalescedEventSetKeepsCausalRootStructural(t *testing.T) {
	const first = uint32(100_000)
	const eventsN = uint32(512)
	const observedN = uint32(233)

	// Equal reader epochs are canonically coalesced even though each coordinate
	// remains an independent read event.
	events := New()
	events.JoinRange(first, first+eventsN-1, 7)
	anchor := New()
	anchor.JoinRange(first, first+observedN-1, 7)
	lineage := NewClockLineage(anchor)
	view := lineage.Pin()
	observed := New()
	if !observed.TryJoinCausal(view) {
		t.Fatal("failed to install observed causal root")
	}
	refs := view.segment.refs.Load()

	events.PruneEventSetLessOrEqual(observed)
	if got := view.segment.refs.Load(); got != refs {
		t.Fatalf("coalesced event pruning changed segment refs: got %d want %d", got, refs)
	}
	if !observed.causal.Valid() {
		t.Fatal("coalesced event pruning materialized observed causal root")
	}
	if len(events.sparseRuns) != 1 {
		t.Fatalf("coalesced event pruning produced %d runs, want 1: %+v", len(events.sparseRuns), events.sparseRuns)
	}
	want := finiteRun{First: first + observedN, Last: first + eventsN - 1, Clock: 7}
	if got := events.sparseRuns[0]; got != want {
		t.Fatalf("retained run = %+v, want %+v", got, want)
	}

	events.Release()
	observed.Release()
	view.Release()
	lineage.Release()
	anchor.Release()
}

func TestPruneCoalescedEventSetSplitsExactly(t *testing.T) {
	const first = uint32(100_000)
	const eventsN = uint32(64)
	events := New()
	events.JoinRange(first, first+eventsN-1, 11)
	anchor := New()
	for offset := uint32(0); offset < eventsN; offset += 2 {
		anchor.Set(first+offset, 11)
	}
	lineage := NewClockLineage(anchor)
	view := lineage.Pin()
	observed := New()
	if !observed.TryJoinCausal(view) {
		t.Fatal("failed to install observed causal root")
	}

	events.PruneEventSetLessOrEqual(observed)
	if len(events.sparseRuns) != int(eventsN/2) {
		t.Fatalf("split event pruning produced %d runs, want %d", len(events.sparseRuns), eventsN/2)
	}
	for offset := uint32(0); offset < eventsN; offset++ {
		want := uint32(0)
		if offset&1 != 0 {
			want = 11
		}
		if got := events.Get(first + offset); got != want {
			t.Fatalf("event %d = %d, want %d", offset, got, want)
		}
	}
	if !observed.causal.Valid() {
		t.Fatal("split event pruning materialized observed causal root")
	}

	events.Release()
	observed.Release()
	view.Release()
	lineage.Release()
	anchor.Release()
}

func TestPrunePathologicallyWideCoalescedEventSetFallsBackExactly(t *testing.T) {
	const first = uint32(100_000)
	const eventsN = uint32(64*1024 + 1)
	const observedN = uint32(40_000)
	events := New()
	events.JoinRange(first, first+eventsN-1, 13)
	anchor := New()
	anchor.JoinRange(first, first+observedN-1, 13)
	lineage := NewClockLineage(anchor)
	view := lineage.Pin()
	observed := New()
	if !observed.TryJoinCausal(view) {
		t.Fatal("failed to install observed causal root")
	}

	events.PruneEventSetLessOrEqual(observed)
	want := finiteRun{First: first + observedN, Last: first + eventsN - 1, Clock: 13}
	if len(events.sparseRuns) != 1 || events.sparseRuns[0] != want {
		t.Fatalf("wide fallback result = %+v, want [%+v]", events.sparseRuns, want)
	}
	if !observed.causal.Valid() {
		t.Fatal("wide fallback changed the observing clock representation")
	}

	events.Release()
	observed.Release()
	view.Release()
	lineage.Release()
	anchor.Release()
}

func TestPruneCoalescedEventSetUsesDenseAnchorBlocksExactly(t *testing.T) {
	const first = uint32(DenseThreads)
	const eventsN = uint32(1024)
	events := New()
	events.JoinRange(first, first+eventsN-1, 7)
	anchor := New()
	for offset := uint32(0); offset < eventsN; offset++ {
		clock := uint32(7)
		if offset&1 != 0 {
			clock = 9
		}
		anchor.Set(first+offset, clock)
	}
	lineage := NewClockLineage(anchor)
	view := lineage.Pin()
	observed := New()
	if !observed.TryJoinCausal(view) {
		t.Fatal("failed to install dense causal anchor")
	}
	refs := view.segment.refs.Load()

	events.PruneEventSetLessOrEqual(observed)
	if len(events.sparseRuns) != 0 || len(events.denseTail) != 0 {
		t.Fatalf("fully dominated dense-anchor event set survived: dense=%d sparse=%+v", len(events.denseTail), events.sparseRuns)
	}
	if !observed.causal.Valid() || view.segment.refs.Load() != refs {
		t.Fatal("dense block proof changed observer ownership or representation")
	}

	events.Release()
	observed.Release()
	view.Release()
	lineage.Release()
	anchor.Release()
}

func TestPruneCoalescedEventSetDenseAnchorGapFallsBackExactly(t *testing.T) {
	const first = uint32(DenseThreads)
	const eventsN = uint32(128)
	const gap = first + 73
	events := New()
	events.JoinRange(first, first+eventsN-1, 5)
	anchor := New()
	for offset := uint32(0); offset < eventsN; offset++ {
		if tid := first + offset; tid != gap {
			anchor.Set(tid, 5)
		}
	}
	lineage := NewClockLineage(anchor)
	view := lineage.Pin()
	observed := New()
	if !observed.TryJoinCausal(view) {
		t.Fatal("failed to install gapped dense causal anchor")
	}

	events.PruneEventSetLessOrEqual(observed)
	for tid := first; tid < first+eventsN; tid++ {
		want := uint32(0)
		if tid == gap {
			want = 5
		}
		if got := events.Get(tid); got != want {
			t.Fatalf("gapped dense-anchor survivor TID %d = %d, want %d", tid, got, want)
		}
	}

	events.Release()
	observed.Release()
	view.Release()
	lineage.Release()
	anchor.Release()
}

func TestPruneCoalescedEventSetBlockProofRespectsPinnedVersions(t *testing.T) {
	const first = uint32(DenseThreads)
	const eventsN = uint32(64)
	const gap = first + 41
	anchor := New()
	for tid := first; tid < first+eventsN; tid++ {
		if tid != gap {
			anchor.Set(tid, 5)
		}
	}
	lineage := NewClockLineage(anchor)
	old := lineage.Pin()
	if _, appended := lineage.Append(gap, 5); !appended {
		t.Fatal("failed to append the anchor gap")
	}
	latest := lineage.Pin()
	if latest.anchorDominatesAlignedBlock(first, 5) {
		t.Fatal("mutable head incorrectly strengthened the immutable anchor proof")
	}
	if !lineage.Rotate() {
		t.Fatal("failed to rotate the completed block into a new anchor")
	}
	current := lineage.Pin()

	prune := func(view CausalView) *VectorClock {
		events := New()
		events.JoinRange(first, first+eventsN-1, 5)
		observed := New()
		if !observed.TryJoinCausal(view) {
			t.Fatal("failed to install causal view")
		}
		events.PruneEventSetLessOrEqual(observed)
		observed.Release()
		return events
	}

	oldEvents := prune(old)
	for tid := first; tid < first+eventsN; tid++ {
		want := uint32(0)
		if tid == gap {
			want = 5
		}
		if got := oldEvents.Get(tid); got != want {
			t.Fatalf("historical survivor TID %d = %d, want %d", tid, got, want)
		}
	}
	latestEvents := prune(latest)
	for tid := first; tid < first+eventsN; tid++ {
		if got := latestEvents.Get(tid); got != 0 {
			t.Fatalf("latest pre-rotation survivor TID %d = %d, want 0", tid, got)
		}
	}
	currentEvents := prune(current)
	for tid := first; tid < first+eventsN; tid++ {
		if got := currentEvents.Get(tid); got != 0 {
			t.Fatalf("current survivor TID %d = %d, want 0", tid, got)
		}
	}

	oldEvents.Release()
	latestEvents.Release()
	currentEvents.Release()
	current.Release()
	latest.Release()
	old.Release()
	lineage.Release()
	anchor.Release()
}

func BenchmarkVectorClockTryJoinCausalSameFamily(b *testing.B) {
	anchor := New()
	lineage := NewClockLineage(anchor)
	anchor.Release()
	old, _ := lineage.AppendPinned(1, 1)
	newer, _ := lineage.AppendPinned(2, 2)
	b.Cleanup(func() {
		old.Release()
		newer.Release()
		lineage.Release()
	})

	if old.segment != newer.segment {
		b.Fatal("benchmark views unexpectedly crossed a segment")
	}
	var vc VectorClock
	if !vc.TryJoinCausal(old) {
		b.Fatal("failed to install old view")
	}
	b.Cleanup(vc.Reset)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Rewind only the version of this benchmark-owned same-segment pin so
		// each iteration measures the real O(1) advance rather than a dominated
		// no-op. Segment ownership never changes.
		vc.causal.roots[0].version = old.version
		vc.TryJoinCausal(newer)
	}
}
