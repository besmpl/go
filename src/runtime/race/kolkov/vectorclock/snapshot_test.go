package vectorclock

import (
	"math/rand"
	"reflect"
	"testing"
)

type testClockImage struct {
	finite  []FiniteRange
	retired []RetiredRange
}

func imageOf(vc *VectorClock) testClockImage {
	var image testClockImage
	vc.RangeRuns(func(first, last, clock uint32) bool {
		image.finite = append(image.finite, FiniteRange{First: first, Last: last, Clock: clock})
		return true
	})
	vc.RangeRetired(func(first, last uint32) bool {
		image.retired = append(image.retired, RetiredRange{First: first, Last: last})
		return true
	})
	return image
}

func requireSameClock(t *testing.T, want, got *VectorClock) {
	t.Helper()
	if a, b := imageOf(want), imageOf(got); !reflect.DeepEqual(a, b) {
		t.Fatalf("clock mismatch\nwant: %#v\n got: %#v", a, b)
	}
	if want.LessOrEqual(got) != got.LessOrEqual(want) || !want.LessOrEqual(got) {
		t.Fatal("equal images did not compare equal")
	}
}

func validateSnapshotTree(t *testing.T, root *snapshotNode, retired bool) {
	t.Helper()
	var previous *snapshotNode
	var walk func(*snapshotNode) (uint32, uint32)
	walk = func(node *snapshotNode) (uint32, uint32) {
		if node == nil {
			return 0, 0
		}
		if node.first > node.last || node.value == 0 || retired && node.value != 1 {
			t.Fatalf("invalid snapshot node: %+v", node)
		}
		if node.left != nil && node.left.priority < node.priority ||
			node.right != nil && node.right.priority < node.priority {
			t.Fatalf("snapshot heap invariant violated at [%d,%d]", node.first, node.last)
		}
		minFirst, maxLast := node.first, node.last
		if node.left != nil {
			leftMin, leftMax := walk(node.left)
			if leftMax >= node.first {
				t.Fatalf("snapshot left subtree overlaps [%d,%d]", node.first, node.last)
			}
			minFirst = leftMin
		}
		if previous != nil {
			if previous.last >= node.first {
				t.Fatalf("snapshot intervals overlap: [%d,%d] and [%d,%d]", previous.first, previous.last, node.first, node.last)
			}
			if previous.last != ^uint32(0) && previous.last+1 == node.first && previous.value == node.value {
				t.Fatalf("snapshot intervals were not coalesced: [%d,%d] and [%d,%d]", previous.first, previous.last, node.first, node.last)
			}
		}
		previous = node
		if node.right != nil {
			_, rightMax := walk(node.right)
			maxLast = rightMax
		}
		if node.minFirst != minFirst || node.maxLast != maxLast {
			t.Fatalf("snapshot bounds at [%d,%d] = [%d,%d], want [%d,%d]", node.first, node.last, node.minFirst, node.maxLast, minFirst, maxLast)
		}
		return minFirst, maxLast
	}
	walk(root)
}

func validateSnapshot(t *testing.T, snapshot *ClockSnapshot) {
	t.Helper()
	if snapshot == nil {
		return
	}
	validateSnapshotTree(t, snapshot.finite, false)
	validateSnapshotTree(t, snapshot.retired, true)
}

func TestClockSnapshotPointMaxStructuralSharing(t *testing.T) {
	vc := New()
	vc.JoinRange(10, 20, 3)
	vc.JoinRange(1000, 2000, 7)
	s := vc.Freeze()
	if got := s.PointMax(15, 2); got != s {
		t.Fatal("dominated point update changed the root")
	}
	next := s.PointMax(15, 9)
	validateSnapshot(t, s)
	validateSnapshot(t, next)
	if next == s || snapshotGet(next.finite, 15) != 9 || snapshotGet(s.finite, 15) != 3 {
		t.Fatal("point update mutated or failed to replace the immutable root")
	}
	joined := New()
	joined.JoinSnapshot(next)
	if got := joined.Get(15); got != 9 {
		t.Fatalf("joined point = %d, want 9", got)
	}
	joined.JoinSnapshot(next)
	if joined.base != next {
		t.Fatal("same-root join did not preserve the current root")
	}
}

func TestClockSnapshotBaseOverlayFallbacks(t *testing.T) {
	legacy, shared := New(), New()
	for _, pair := range [][2]uint32{{1, 2}, {1023, 4}, {1024, 5}, {4000, 6}, {^uint32(0), 8}} {
		legacy.Set(pair[0], pair[1])
		shared.Set(pair[0], pair[1])
	}
	shared.Freeze()
	shared.Set(4000, 9)
	legacy.Set(4000, 9)
	shared.Set(1024, 1) // lowering materializes the base exactly
	legacy.Set(1024, 1)
	shared.RetireRange(3999, 4001)
	legacy.RetireRange(3999, 4001)
	requireSameClock(t, legacy, shared)

	clone := shared.Clone()
	clone.Set(7, 11)
	if shared.Get(7) != 0 {
		t.Fatal("clone overlay aliased its parent")
	}
	clone.Release()
	shared.Reset()
	if shared.base != nil || len(imageOf(shared).finite) != 0 || len(imageOf(shared).retired) != 0 {
		t.Fatal("Reset retained snapshot state")
	}
}

func TestClockSnapshotDifferentialOperations(t *testing.T) {
	rng := rand.New(rand.NewSource(0x5eed))
	legacy, shared := New(), New()
	tids := []uint32{1, 2, 17, 1023, 1024, 1025, 4096, 65535, ^uint32(0) - 1, ^uint32(0)}
	for step := 0; step < 600; step++ {
		tid := tids[rng.Intn(len(tids))]
		switch rng.Intn(8) {
		case 0, 1:
			clock := uint32(rng.Intn(12))
			legacy.Set(tid, clock)
			shared.Set(tid, clock)
		case 2:
			if !legacy.IsRetired(tid) && legacy.Get(tid) != ^uint32(0) {
				legacy.Increment(tid)
				shared.Increment(tid)
			}
		case 3:
			other := New()
			for i := 0; i < 5; i++ {
				other.Set(tids[rng.Intn(len(tids))], uint32(rng.Intn(12)+1))
			}
			legacy.Join(other)
			shared.JoinSnapshot(other.Freeze())
		case 4:
			first := tids[1+rng.Intn(len(tids)-2)]
			if first != 0 && first != ^uint32(0) {
				legacy.RetireRange(first, first)
				shared.RetireRange(first, first)
			}
		case 5:
			observed := New()
			for i := 0; i < 4; i++ {
				observed.Set(tids[rng.Intn(len(tids))], uint32(rng.Intn(12)+1))
			}
			if step&1 != 0 {
				observed.Freeze()
			}
			legacy.PruneLessOrEqual(observed)
			shared.PruneLessOrEqual(observed)
		case 6:
			validateSnapshot(t, shared.Freeze())
		case 7:
			copy := shared.Clone()
			shared.CopyFrom(copy)
			copy.Release()
		}
		requireSameClock(t, legacy, shared)
	}
}

func TestClockSnapshotLogicalIterationDoesNotAllocate(t *testing.T) {
	vc := New()
	vc.JoinRange(0, 1023, 3)
	vc.JoinRange(50_000, 60_000, 7)
	vc.RetireRange(55_000, 55_100)
	vc.Freeze()
	vc.Set(17, 9)
	vc.Set(59_999, 11)
	vc.RetireRange(50_010, 50_020)

	finiteAllocs := testing.AllocsPerRun(100, func() {
		vc.RangeRuns(func(_, _, _ uint32) bool { return true })
	})
	retiredAllocs := testing.AllocsPerRun(100, func() {
		vc.RangeRetired(func(_, _ uint32) bool { return true })
	})
	if finiteAllocs != 0 || retiredAllocs != 0 {
		t.Fatalf("logical iteration allocations = finite %.1f retired %.1f, want zero", finiteAllocs, retiredAllocs)
	}
}

func TestClockSnapshotTryOperationsAreAtomicAndAllocationFree(t *testing.T) {
	leftClock := New()
	leftClock.Set(1, 3)
	left := leftClock.Freeze()
	rightClock := New()
	rightClock.Set(2, 5)
	right := rightClock.Freeze()

	dst := New()
	dst.JoinSnapshot(left)
	before := imageOf(dst)
	if dst.TryJoinSnapshot(right) {
		t.Fatal("incomparable roots unexpectedly joined without allocation")
	}
	if got := imageOf(dst); !reflect.DeepEqual(got, before) || dst.base != left {
		t.Fatalf("failed TryJoinSnapshot mutated destination: got %#v want %#v", got, before)
	}

	supersetClock := New()
	supersetClock.JoinSnapshot(left)
	supersetClock.Set(2, 5)
	superset := supersetClock.Freeze()
	if allocs := testing.AllocsPerRun(100, func() {
		dst.base = left
		if !dst.TryJoinSnapshot(superset) {
			panic("dominating root was not adopted")
		}
	}); allocs != 0 {
		t.Fatalf("TryJoinSnapshot allocations = %.1f, want zero", allocs)
	}
	if dst.base != superset {
		t.Fatal("TryJoinSnapshot did not adopt the dominating root")
	}

	source := New()
	source.Set(3, 9)
	inline := New()
	if allocs := testing.AllocsPerRun(100, func() {
		inline.Reset()
		if !inline.TryJoin(source) {
			panic("inline TryJoin failed")
		}
	}); allocs != 0 {
		t.Fatalf("inline TryJoin allocations = %.1f, want zero", allocs)
	}
	if got := inline.Get(3); got != 9 {
		t.Fatalf("inline TryJoin clock = %d, want 9", got)
	}
	sparseSource := New()
	sparseSource.Set(100_000, 12)
	sparseDst := sparseSource.Clone()
	sparseDst.Set(100_000, 11)
	if allocs := testing.AllocsPerRun(100, func() {
		sparseDst.Set(100_000, 11)
		if !sparseDst.TryJoin(sparseSource) {
			panic("same-layout sparse TryJoin failed")
		}
	}); allocs != 0 {
		t.Fatalf("same-layout sparse TryJoin allocations = %.1f, want zero", allocs)
	}
	if got := sparseDst.Get(100_000); got != 12 {
		t.Fatalf("same-layout sparse TryJoin clock = %d, want 12", got)
	}

	copySource := New()
	copySource.JoinRange(10_000, 10_100, 4)
	copySource.RetireRange(20_000, 20_010)
	copyDst := New()
	copyDst.sparseRuns = make([]finiteRun, 0, len(copySource.sparseRuns))
	copyDst.retired = make([]RetiredRange, 0, len(copySource.retired))
	if allocs := testing.AllocsPerRun(100, func() {
		if !copyDst.TryCopyFrom(copySource) {
			panic("capacity-qualified TryCopyFrom failed")
		}
	}); allocs != 0 {
		t.Fatalf("TryCopyFrom allocations = %.1f, want zero", allocs)
	}
	requireSameClock(t, copySource, copyDst)

	tooSmall := New()
	tooSmall.Set(7, 1)
	tooSmallBefore := imageOf(tooSmall)
	if tooSmall.TryCopyFrom(copySource) {
		t.Fatal("undersized TryCopyFrom unexpectedly succeeded")
	}
	if got := imageOf(tooSmall); !reflect.DeepEqual(got, tooSmallBefore) {
		t.Fatalf("failed TryCopyFrom mutated destination: got %#v want %#v", got, tooSmallBefore)
	}
}

func TestClockSnapshotSameLineageDominanceWithCausalRoots(t *testing.T) {
	baseClock := New()
	baseClock.Set(7, 3)
	baseClock.RetireRange(40, 45)
	old := baseClock.Freeze()
	newer := old.PointMax(7, 9)

	foreignAnchor := New()
	foreignAnchor.Set(100_000, 11)
	foreignLineage := NewClockLineage(foreignAnchor)
	foreignAnchor.Release()
	foreign := foreignLineage.Pin()
	defer foreign.Release()
	defer foreignLineage.Release()

	dst := New()
	defer dst.Release()
	if !dst.TryJoinSnapshot(newer) || !dst.TryJoinCausal(foreign) {
		t.Fatal("failed to construct snapshot plus causal-root destination")
	}
	want := imageOf(dst)
	baseBefore := dst.base
	causalBefore := dst.causal.roots[0]
	refsBefore := causalBefore.segment.refs.Load()
	if allocs := testing.AllocsPerRun(1000, func() {
		if !dst.TryJoinSnapshot(old) {
			panic("older same-lineage snapshot was not recognized as dominated")
		}
	}); allocs != 0 {
		t.Fatalf("dominated same-lineage join allocated %.1f times", allocs)
	}
	if got := imageOf(dst); !reflect.DeepEqual(got, want) {
		t.Fatalf("dominated join changed logical clock: got %#v want %#v", got, want)
	}
	if dst.base != baseBefore || dst.causal.roots[0].segment != causalBefore.segment ||
		dst.causal.roots[0].version != causalBefore.version || causalBefore.segment.refs.Load() != refsBefore {
		t.Fatal("dominated join changed snapshot or causal-root ownership")
	}

	// The converse remains exact: a newer base is adopted without dropping the
	// unrelated root or the destination-owned overlay.
	dst.base = old
	dst.Set(13, 5)
	if !dst.TryJoinSnapshot(newer) || dst.base != newer || dst.Get(13) != 5 || dst.Get(100_000) != 11 {
		t.Fatal("newer same-lineage adoption lost an owned or causal component")
	}

	incomparableClock := New()
	incomparableClock.Set(8, 12)
	incomparable := incomparableClock.Freeze()
	before := imageOf(dst)
	if dst.TryJoinSnapshot(incomparable) {
		t.Fatal("incomparable snapshot unexpectedly joined with causal roots")
	}
	if got := imageOf(dst); !reflect.DeepEqual(got, before) {
		t.Fatalf("failed incomparable join changed logical clock: got %#v want %#v", got, before)
	}
}

func TestClockSnapshotRetiredUnionAndExtremeRanges(t *testing.T) {
	legacy, shared := New(), New()
	for _, r := range []FiniteRange{
		{First: 0, Last: 0, Clock: 2},
		{First: 1023, Last: 1025, Clock: 3},
		{First: ^uint32(0) - 20, Last: ^uint32(0), Clock: 7},
	} {
		legacy.JoinRange(r.First, r.Last, r.Clock)
		shared.JoinRange(r.First, r.Last, r.Clock)
	}
	shared.Freeze()
	for _, r := range []RetiredRange{
		{First: 1024, Last: 1024},
		{First: ^uint32(0) - 10, Last: ^uint32(0) - 5},
	} {
		legacy.RetireRange(r.First, r.Last)
		shared.RetireRange(r.First, r.Last)
	}
	shared.Freeze()
	legacy.RetireRange(1025, 1030)
	shared.RetireRange(1025, 1030)
	legacy.Set(^uint32(0), 11)
	shared.Set(^uint32(0), 11)
	requireSameClock(t, legacy, shared)
}

func BenchmarkClockSnapshot(b *testing.B) {
	b.Run("NilBaseGet", func(b *testing.B) {
		vc := New()
		vc.Set(7, 1)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = vc.Get(7)
		}
	})
	b.Run("SameRootJoin", func(b *testing.B) {
		vc := New()
		for i := uint32(1024); i < 11264; i += 2 {
			vc.Set(i, i)
		}
		s := vc.Freeze()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vc.JoinSnapshot(s)
		}
	})
	b.Run("DivergentPointJoin", func(b *testing.B) {
		vc := New()
		for i := uint32(1024); i < 11264; i += 2 {
			vc.Set(i, i)
		}
		s := vc.Freeze()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vc.JoinSnapshot(s.PointMax(2048, uint32(i+20000)))
		}
	})
}
