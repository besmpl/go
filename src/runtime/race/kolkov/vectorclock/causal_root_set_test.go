package vectorclock

import "testing"

func causalTestView(t *testing.T, tid, clock uint32) (*ClockLineage, CausalView) {
	t.Helper()
	anchor := New()
	anchor.Set(tid, clock)
	lineage := NewClockLineage(anchor)
	view := lineage.Pin()
	anchor.Release()
	return lineage, view
}

func TestCausalRootSetRetainsUnrelatedRootsToCapacity(t *testing.T) {
	var lineages [CausalRootCapacity]*ClockLineage
	var views [CausalRootCapacity]CausalView
	for i := range views {
		lineages[i], views[i] = causalTestView(t, uint32(i+1), uint32(10+i))
		defer lineages[i].Release()
		defer views[i].Release()
	}

	vc := New()
	defer vc.Release()
	for i, view := range views {
		if !vc.TryJoinCausal(view) {
			t.Fatalf("unrelated root %d did not fit inline", i)
		}
	}
	var borrowed [CausalRootCapacity]CausalView
	if got := vc.BorrowCausalRoots(&borrowed); got != CausalRootCapacity {
		t.Fatalf("root count = %d, want %d", got, CausalRootCapacity)
	}
	for i := range views {
		if got := vc.Get(uint32(i + 1)); got != uint32(10+i) {
			t.Fatalf("Get(%d) = %d, want %d", i+1, got, 10+i)
		}
	}
	allocs := testing.AllocsPerRun(1000, func() {
		var inline VectorClock
		for _, view := range views {
			if !inline.TryJoinCausal(view) {
				panic("inline root join failed")
			}
		}
		inline.Reset()
	})
	if allocs != 0 {
		t.Fatalf("%d unrelated inline roots allocated: %v allocs/run", CausalRootCapacity, allocs)
	}
}

func TestCausalRootSetSameFamilyAdvanceReleasesDominatedSegment(t *testing.T) {
	anchor := New()
	lineage := NewClockLineage(anchor)
	old := lineage.Pin()
	anchor.Release()

	lineage.lock()
	lineage.rotateLocked()
	lineage.unlock()
	newer, changed := lineage.AppendPinned(9, 2)
	if !changed || old.segment == newer.segment {
		t.Fatal("test setup did not cross lineage segments")
	}
	defer old.Release()
	defer newer.Release()
	defer lineage.Release()

	vc := New()
	if !vc.TryJoinCausal(old) {
		t.Fatal("failed to retain old root")
	}
	oldRefs := old.segment.refs.Load()
	if !vc.TryJoinCausal(newer) {
		t.Fatal("same-family advance failed")
	}
	if got := old.segment.refs.Load(); got != oldRefs-1 {
		t.Fatalf("dominated segment refs = %d, want %d", got, oldRefs-1)
	}
	if vc.causal.count != 1 || vc.causal.roots[0].segment != newer.segment {
		t.Fatal("advance did not replace the dominated family root")
	}
	vc.Release()
}

func TestCausalRootSetOverflowIsAllOrNothingThenMaterializesExactly(t *testing.T) {
	var lineages [CausalRootCapacity + 1]*ClockLineage
	var views [CausalRootCapacity + 1]CausalView
	for i := range views {
		lineages[i], views[i] = causalTestView(t, uint32(100+i), uint32(20+i))
		defer lineages[i].Release()
		defer views[i].Release()
	}

	vc := New()
	defer vc.Release()
	for i := 0; i < CausalRootCapacity; i++ {
		vc.JoinCausal(views[i])
	}
	if vc.TryJoinCausal(views[CausalRootCapacity]) {
		t.Fatal("overflowing TryJoinCausal unexpectedly succeeded")
	}
	if vc.causal.count != CausalRootCapacity || vc.Get(uint32(100+CausalRootCapacity)) != 0 {
		t.Fatal("failed overflow TryJoinCausal mutated the clock")
	}

	vc.JoinCausal(views[CausalRootCapacity])
	if vc.causal.count != 1 || !vc.causal.roots[0].SameFamily(views[CausalRootCapacity]) {
		t.Fatal("overflow fallback did not retain only the incoming root")
	}
	for i := range views {
		if got := vc.Get(uint32(100 + i)); got != uint32(20+i) {
			t.Fatalf("overflow fallback lost root %d: got %d", i, got)
		}
	}
}

func TestVectorClockTryJoinCausalRootSetDirectly(t *testing.T) {
	firstLineage, first := causalTestView(t, 7, 11)
	secondLineage, second := causalTestView(t, 9, 13)
	defer firstLineage.Release()
	defer secondLineage.Release()
	defer first.Release()
	defer second.Release()

	source := New()
	defer source.Release()
	if !source.TryJoinCausal(first) || !source.TryJoinCausal(second) {
		t.Fatal("failed to build causal source")
	}
	var nilDestination *VectorClock
	if nilDestination.TryJoin(source) {
		t.Fatal("nil destination joined a nonempty causal source")
	}
	destination := New()
	if !destination.TryJoin(source) {
		t.Fatal("direct causal-root-set join failed")
	}
	if destination.causal.count != 2 || destination.Get(7) != 11 || destination.Get(9) != 13 {
		t.Fatal("direct causal-root-set join lost a source family")
	}
	destination.Release()

	if allocs := testing.AllocsPerRun(1000, func() {
		var clock VectorClock
		if !clock.TryJoin(source) {
			panic("direct causal-root-set join failed")
		}
		clock.Reset()
	}); allocs != 0 {
		t.Fatalf("direct causal-root-set join allocated: %.2f objects", allocs)
	}
}

func TestVectorClockTryJoinCausalRootSetAdvancesFamilyLifecycle(t *testing.T) {
	anchor := New()
	lineage := NewClockLineage(anchor)
	old := lineage.Pin()
	anchor.Release()
	if !lineage.Rotate() {
		t.Fatal("failed to rotate lineage")
	}
	newer, appended := lineage.AppendPinned(17, 3)
	if !appended || newer.segment == old.segment {
		t.Fatal("failed to build a newer cross-segment view")
	}
	defer lineage.Release()
	defer old.Release()
	defer newer.Release()

	destination := New()
	defer destination.Release()
	source := New()
	defer source.Release()
	if !destination.TryJoinCausal(old) || !source.TryJoinCausal(newer) {
		t.Fatal("failed to build same-family clocks")
	}
	oldRefs := old.segment.refs.Load()
	newRefs := newer.segment.refs.Load()
	if !destination.TryJoin(source) {
		t.Fatal("direct root-set join did not advance the family")
	}
	if destination.causal.count != 1 || destination.causal.roots[0].segment != newer.segment ||
		old.segment.refs.Load() != oldRefs-1 || newer.segment.refs.Load() != newRefs+1 {
		t.Fatal("direct root-set family advance broke reference ownership")
	}
}

func TestVectorClockTryJoinCausalRootSetOverflowIsAllOrNothing(t *testing.T) {
	var lineages [CausalRootCapacity + 1]*ClockLineage
	var views [CausalRootCapacity + 1]CausalView
	for i := range views {
		lineages[i], views[i] = causalTestView(t, uint32(200+i), uint32(30+i))
		defer lineages[i].Release()
		defer views[i].Release()
	}

	destination := New()
	defer destination.Release()
	for i := 0; i < CausalRootCapacity; i++ {
		if !destination.TryJoinCausal(views[i]) {
			t.Fatalf("failed to fill destination root %d", i)
		}
	}
	source := New()
	defer source.Release()
	if !source.TryJoinCausal(views[CausalRootCapacity]) {
		t.Fatal("failed to build overflowing source")
	}
	refs := views[CausalRootCapacity].segment.refs.Load()
	if destination.TryJoin(source) {
		t.Fatal("overflowing direct root-set join unexpectedly succeeded")
	}
	if destination.causal.count != CausalRootCapacity ||
		destination.Get(uint32(200+CausalRootCapacity)) != 0 ||
		views[CausalRootCapacity].segment.refs.Load() != refs {
		t.Fatal("failed direct root-set join mutated state or ownership")
	}
}

func TestCausalRootSetCloneResetReferenceLifecycle(t *testing.T) {
	lineage, view := causalTestView(t, 7, 70)
	defer lineage.Release()
	defer view.Release()

	vc := New()
	vc.JoinCausal(view)
	refs := view.segment.refs.Load()
	clone := vc.Clone()
	if got := view.segment.refs.Load(); got != refs+1 {
		t.Fatalf("clone refs = %d, want %d", got, refs+1)
	}
	clone.Reset()
	if got := view.segment.refs.Load(); got != refs {
		t.Fatalf("reset refs = %d, want %d", got, refs)
	}
	clone.Release()
	vc.Release()
	if got := view.segment.refs.Load(); got != refs-1 {
		t.Fatalf("release refs = %d, want %d", got, refs-1)
	}
}

func TestCausalRootSetLogicalParityAndSetResidual(t *testing.T) {
	leftLineage, left := causalTestView(t, 11, 3)
	rightLineage, right := causalTestView(t, 12, 4)
	defer leftLineage.Release()
	defer rightLineage.Release()
	defer left.Release()
	defer right.Release()

	vc := New()
	defer vc.Release()
	vc.JoinCausal(left)
	vc.JoinCausal(right)
	vc.Set(13, 5)
	oracle := left.materialize()
	defer oracle.Release()
	rightClock := right.materialize()
	oracle.Join(rightClock)
	rightClock.Release()
	oracle.Set(13, 5)
	if !vc.LessOrEqual(oracle) || !oracle.LessOrEqual(vc) {
		t.Fatal("inline causal root set differs from materialized oracle")
	}

	views := [CausalRootCapacity]CausalView{left, right}
	if !vc.ResidualLessOrEqualCausalSet(&views, 2, 13) {
		t.Fatal("set residual proof rejected its exact two-root union")
	}
}
