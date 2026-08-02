package vectorclock

import "testing"

func TestReleaseProjectionCapturesBaseRootsAndOwnedOverlayExactly(t *testing.T) {
	source := New()
	source.Set(3, 7)
	source.Freeze()
	lineages := make([]*ClockLineage, CausalRootCapacity)
	views := make([]CausalView, CausalRootCapacity)
	for i := range lineages {
		anchor := New()
		anchor.Set(uint32(100+i), uint32(20+i))
		lineages[i] = NewClockLineage(anchor)
		views[i] = lineages[i].Pin()
		if !source.TryJoinCausal(views[i]) {
			t.Fatalf("root %d did not fit", i)
		}
		anchor.Release()
	}
	source.Set(1<<20, 99)
	source.RetireRange(1<<21, 1<<21+2)

	var projection ReleaseProjection
	if !TryPinReleaseProjection(source, &projection) {
		t.Fatal("small exact projection fell back")
	}
	var finite []FiniteRange
	var retired []RetiredRange
	projection.AppendRanges(&finite, &retired)
	finite, retired = CanonicalizeReleaseRanges(finite, retired)
	got := New()
	got.JoinCanonicalRanges(finite)
	got.RetireRanges(retired)
	if !got.LessOrEqual(source) || !source.LessOrEqual(got) {
		t.Fatalf("projection = %v, want %v", got, source)
	}
	projection.Release()
	for i := range views {
		views[i].Release()
		lineages[i].Release()
	}
	source.Release()
	got.Release()
}

func TestReleaseProjectionReleaseDropsEveryRetainedRoot(t *testing.T) {
	anchor := New()
	anchor.Set(77, 5)
	lineage := NewClockLineage(anchor)
	view := lineage.Pin()
	clock := New()
	if !clock.TryJoinCausal(view) {
		t.Fatal("failed to install causal root")
	}
	before := view.segment.refs.Load()
	var projection ReleaseProjection
	if !TryPinReleaseProjection(clock, &projection) {
		t.Fatal("root-only projection fell back")
	}
	if got := view.segment.refs.Load(); got != before+1 {
		t.Fatalf("refs after capture = %d, want %d", got, before+1)
	}
	projection.Release()
	if got := view.segment.refs.Load(); got != before {
		t.Fatalf("refs after release = %d, want %d", got, before)
	}
	view.Release()
	clock.Release()
	lineage.Release()
	anchor.Release()
}

func TestReleaseProjectionSharesDenseTailAndJoinsExactly(t *testing.T) {
	source := New()
	for i := uint32(0); i < 256; i++ {
		source.Set(DenseThreads+i, i&1+1)
	}
	const ownTID = DenseThreads + 1000
	source.Set(ownTID, 9)
	want := source.Clone()

	projection := PinReleaseProjection(source)
	defer projection.Release()
	if len(source.denseTail) == 0 || len(projection.denseTail) != len(source.denseTail) ||
		&projection.denseTail[0] != &source.denseTail[0] || !source.denseTailShared {
		t.Fatal("projection did not retain dense frontier copy-on-write")
	}

	var noRoots [CausalRootCapacity]CausalView
	if !source.ResidualLessOrEqualReleaseProjection(&noRoots, 0, &projection, ownTID) {
		t.Fatal("projection did not structurally dominate its source residual")
	}

	joined := New()
	projection.JoinInto(joined)
	if !joined.LessOrEqual(want) || !want.LessOrEqual(joined) {
		t.Fatal("projection JoinInto changed exact clock")
	}

	old := projection.get(DenseThreads + 100)
	source.Set(DenseThreads+100, old+10)
	if got := projection.get(DenseThreads + 100); got != old {
		t.Fatalf("source mutation changed projection: got %d want %d", got, old)
	}
	source.Set(ownTID+1, 11)
	if source.ResidualLessOrEqualReleaseProjection(&noRoots, 0, &projection, ownTID) {
		t.Fatal("projection accepted uncovered foreign sparse residual")
	}

	joined.Release()
	want.Release()
	source.Release()
}

func TestReleaseProjectionPreservesPreparedDenseOwnerWrite(t *testing.T) {
	source := New()
	for i := uint32(0); i < 256; i++ {
		source.Set(DenseThreads+i, i&1+1)
	}
	const ownerTID = DenseThreads + 100
	old := source.Get(ownerTID)
	source.PrepareKnownMonotonicSet(ownerTID)
	if !source.CanSetKnownMonotonicAlive(ownerTID) {
		t.Fatal("prepared dense owner write is not allocation-free")
	}

	projection := PinReleaseProjectionForPreparedOwner(source, ownerTID)
	defer projection.Release()
	if len(source.denseTail) == 0 || len(projection.denseTail) != len(source.denseTail) {
		t.Fatal("test did not capture a dense owner coordinate")
	}
	if &projection.denseTail[0] == &source.denseTail[0] || source.denseTailShared {
		t.Fatal("owner-aware projection invalidated the prepared dense write")
	}

	source.SetKnownMonotonicAlive(ownerTID, old+1)
	if got := projection.get(ownerTID); got != old {
		t.Fatalf("prepared source write changed projection: got %d want %d", got, old)
	}
	if got := source.Get(ownerTID); got != old+1 {
		t.Fatalf("prepared source write = %d, want %d", got, old+1)
	}
	var noRoots [CausalRootCapacity]CausalView
	if !source.ResidualLessOrEqualReleaseProjection(&noRoots, 0, &projection, ownerTID) {
		t.Fatal("copied dense projection did not prove unchanged foreign residual")
	}
	foreignTID := uint32(ownerTID + 1)
	source.Set(foreignTID, source.Get(foreignTID)+1)
	if source.ResidualLessOrEqualReleaseProjection(&noRoots, 0, &projection, ownerTID) {
		t.Fatal("copied dense projection accepted newer foreign residual")
	}
	source.Release()
}

func TestTryReleaseProjectionRejectsPreparedDenseOwner(t *testing.T) {
	source := New()
	for i := uint32(0); i < 256; i++ {
		source.Set(DenseThreads+i, i&1+1)
	}
	const ownerTID = DenseThreads + 100
	source.PrepareKnownMonotonicSet(ownerTID)
	var projection ReleaseProjection
	if TryPinReleaseProjectionForPreparedOwner(source, ownerTID, &projection) {
		t.Fatal("allocation-free capture shared the prepared dense owner")
	}
	if source.denseTailShared {
		t.Fatal("failed owner-aware capture changed source sharing state")
	}
	source.SetKnownMonotonicAlive(ownerTID, source.Get(ownerTID)+1)
	source.Release()
}

func TestReleaseProjectionProvesWideSparseResidualByCanonicalMerge(t *testing.T) {
	const ownTID = uint32(1 << 22)
	source := New()
	for i := uint32(0); i < uint32(maxCausalResidualProofRun)+128; i++ {
		source.Set(100_000+i*3, i+1)
	}
	source.Set(ownTID, 10)
	projection := PinReleaseProjection(source)
	defer projection.Release()
	if len(projection.dynamicFinite) <= int(maxCausalResidualProofRun) {
		t.Fatal("test did not create a residual wider than the pointwise proof bound")
	}

	var noRoots [CausalRootCapacity]CausalView
	source.Set(ownTID, 11)
	if !source.ResidualLessOrEqualReleaseProjection(&noRoots, 0, &projection, ownTID) {
		t.Fatal("canonical merge did not prove a wide unchanged foreign residual")
	}
	source.Set(100_000+63*3, 10_000)
	if source.ResidualLessOrEqualReleaseProjection(&noRoots, 0, &projection, ownTID) {
		t.Fatal("canonical merge accepted a newer foreign sparse coordinate")
	}
	source.Release()
}

func TestPinReleaseProjectionLargeOverlayPreservesCausalRootsAndOwnedRanges(t *testing.T) {
	const causalCoordinates = 2048
	anchor := New()
	for i := uint32(0); i < causalCoordinates; i++ {
		anchor.Set(10_000+i*2, i+1)
	}
	lineage := NewClockLineage(anchor)
	view := lineage.Pin()

	baseClock := New()
	baseClock.Set(700, 11)
	base := baseClock.Freeze()
	source := New()
	for i := uint32(0); i < ReleaseProjectionFiniteCapacity+2; i++ {
		source.Set(1<<20+i*2, 100+i)
	}
	for i := uint32(0); i < ReleaseProjectionRetiredCapacity+2; i++ {
		tid := uint32(1<<21) + i*3
		source.RetireRange(tid, tid)
	}
	if !source.TryJoinSnapshot(base) {
		t.Fatal("failed to install immutable base")
	}
	if !source.TryJoinCausal(view) {
		t.Fatal("failed to install large causal root")
	}
	oracle := source.Clone()

	refsBefore := view.segment.refs.Load()
	projection := PinReleaseProjection(source)
	if projection.base != base {
		t.Fatal("large projection replaced its immutable base")
	}
	if projection.rootN != 1 || projection.roots[0].segment != view.segment {
		t.Fatal("large projection materialized rather than retaining its causal root")
	}
	if got := view.segment.refs.Load(); got != refsBefore+1 {
		t.Fatalf("refs after large capture = %d, want %d", got, refsBefore+1)
	}
	if len(projection.dynamicFinite) != ReleaseProjectionFiniteCapacity+2 || projection.finiteN != 0 {
		t.Fatalf("dynamic finite ranges = %d inline = %d", len(projection.dynamicFinite), projection.finiteN)
	}
	if len(projection.dynamicRetired) != ReleaseProjectionRetiredCapacity+2 || projection.retiredN != 0 {
		t.Fatalf("dynamic retired ranges = %d inline = %d", len(projection.dynamicRetired), projection.retiredN)
	}

	// Mutate both source backing stores after capture. The projection must keep
	// the earlier range values rather than aliasing mutable VectorClock state.
	for i := uint32(0); i < ReleaseProjectionFiniteCapacity+2; i++ {
		source.Set(1<<20+i*2, 1000+i)
	}
	for i := uint32(0); i < ReleaseProjectionRetiredCapacity+1; i++ {
		source.RetireRange(uint32(1<<21)+i*3+1, uint32(1<<21)+i*3+2)
	}
	refsAfterMutation := view.segment.refs.Load()

	var bases []*ClockSnapshot
	var roots []CausalView
	var finite []FiniteRange
	var retired []RetiredRange
	projection.Drain(&bases, &roots, &finite, &retired)
	if projection.base != nil || projection.rootN != 0 || projection.dynamicFinite != nil || projection.dynamicRetired != nil {
		t.Fatal("drain did not empty large projection")
	}
	if len(bases) != 1 || bases[0] != base || len(roots) != 1 {
		t.Fatalf("drained components = %d bases, %d roots", len(bases), len(roots))
	}
	if got := view.segment.refs.Load(); got != refsAfterMutation {
		t.Fatalf("drain changed transferred root refs = %d, want %d", got, refsAfterMutation)
	}
	AppendCoalescedReleaseComponents(bases, roots, &finite, &retired)
	if got := view.segment.refs.Load(); got != refsAfterMutation-1 {
		t.Fatalf("consuming drained root left refs = %d, want %d", got, refsAfterMutation-1)
	}
	finite, retired = CanonicalizeReleaseRanges(finite, retired)
	got := New()
	got.JoinCanonicalRanges(finite)
	got.RetireRanges(retired)
	if !got.LessOrEqual(oracle) || !oracle.LessOrEqual(got) {
		t.Fatal("large projection changed after source mutation")
	}
	if source.LessOrEqual(oracle) {
		t.Fatal("source mutation did not distinguish the captured value")
	}

	got.Release()
	oracle.Release()
	source.Release()
	view.Release()
	lineage.Release()
	anchor.Release()
	baseClock.Release()
}

func TestCoalesceReleaseRootsKeepsOneMaximumVersionPerFamily(t *testing.T) {
	anchor := New()
	lineage := NewClockLineage(anchor)
	const versions = 1000
	roots := make([]CausalView, 0, versions)
	for i := uint32(1); i <= versions; i++ {
		view, ok := lineage.AppendPinned(91, i)
		if !ok {
			t.Fatalf("append %d was not published", i)
		}
		roots = append(roots, view)
	}
	coalesced := coalesceReleaseRoots(roots)
	if len(coalesced) != 1 {
		t.Fatalf("coalesced roots = %d, want 1", len(coalesced))
	}
	if got := coalesced[0].Get(91); got != versions {
		t.Fatalf("maximum version clock = %d, want %d", got, versions)
	}
	coalesced[0].Release()
	lineage.Release()
	anchor.Release()
}

func TestCanonicalizeReleaseRangesPointwiseMaximumAndRetirement(t *testing.T) {
	finite := []FiniteRange{
		{First: 5, Last: 12, Clock: 2},
		{First: 0, Last: 7, Clock: 1},
		{First: 7, Last: 9, Clock: 4},
		{First: 13, Last: 15, Clock: 2},
	}
	retired := []RetiredRange{{First: 8, Last: 8}, {First: 14, Last: 20}}
	finite, retired = CanonicalizeReleaseRanges(finite, retired)
	got := New()
	got.JoinCanonicalRanges(finite)
	got.RetireRanges(retired)
	for tid := uint32(0); tid <= 20; tid++ {
		wantClock := uint32(0)
		for _, r := range []FiniteRange{{0, 7, 1}, {5, 12, 2}, {7, 9, 4}, {13, 15, 2}} {
			if tid >= r.First && tid <= r.Last && r.Clock > wantClock {
				wantClock = r.Clock
			}
		}
		wantRetired := tid == 8 || tid >= 14
		if got.IsRetired(tid) != wantRetired {
			t.Fatalf("tid %d retirement = %v, want %v", tid, got.IsRetired(tid), wantRetired)
		}
		if !wantRetired && got.Get(tid) != wantClock {
			t.Fatalf("tid %d clock = %d, want %d", tid, got.Get(tid), wantClock)
		}
	}
}

func BenchmarkPinReleaseProjectionLargeOverlay(b *testing.B) {
	anchor := New()
	for i := uint32(0); i < 4096; i++ {
		anchor.Set(10_000+i*2, i+1)
	}
	lineage := NewClockLineage(anchor)
	view := lineage.Pin()
	source := New()
	for i := uint32(0); i < ReleaseProjectionFiniteCapacity+4; i++ {
		source.Set(1<<20+i*2, 100+i)
	}
	for i := uint32(0); i < ReleaseProjectionRetiredCapacity+2; i++ {
		tid := uint32(1<<21) + i*2
		source.RetireRange(tid, tid)
	}
	if !source.TryJoinCausal(view) {
		b.Fatal("failed to install large causal root")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		projection := PinReleaseProjection(source)
		projection.Release()
	}
	b.StopTimer()
	source.Release()
	view.Release()
	lineage.Release()
	anchor.Release()
}
