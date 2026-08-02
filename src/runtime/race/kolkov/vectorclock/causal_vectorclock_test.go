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
