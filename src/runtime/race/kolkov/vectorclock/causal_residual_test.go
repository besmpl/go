package vectorclock

import "testing"

func TestResidualLessOrEqualCausalSameFamilyAndResiduals(t *testing.T) {
	anchor := New()
	anchor.Set(1, 4)
	anchor.Set(2, 5)
	anchor.Set(40, 7)
	lineage := NewClockLineage(anchor)
	old := lineage.Pin()
	newer, _ := lineage.AppendPinned(3, 6)
	anchor.Release()
	defer old.Release()
	defer newer.Release()
	defer lineage.Release()

	vc := New()
	defer vc.Release()
	vc.JoinCausal(old)
	vc.Set(1, 100) // publisher-owned coordinate is intentionally excluded
	base := New()
	base.Set(2, 5)  // duplicate type metadata already in the release lineage
	base.Set(40, 6) // lower residual is also dominated
	vc.JoinSnapshot(base.Freeze())
	base.Release()
	if !vc.ResidualLessOrEqualCausal(newer, 1) {
		t.Fatal("dominated same-family residual was rejected")
	}

	vc.Set(2, 6)
	if vc.ResidualLessOrEqualCausal(newer, 1) {
		t.Fatal("foreign owned coordinate newer than release was accepted")
	}
}

func TestResidualLessOrEqualCausalRejectsFalseStrongProofs(t *testing.T) {
	anchor := New()
	anchor.Set(4, 8)
	anchor.RetireRange(50, 52)
	targetLineage := NewClockLineage(anchor)
	target := targetLineage.Pin()
	anchor.Release()
	defer target.Release()
	defer targetLineage.Release()

	tests := []struct {
		name  string
		build func() *VectorClock
	}{
		{
			name: "owned finite",
			build: func() *VectorClock {
				vc := New()
				vc.Set(7, 1)
				return vc
			},
		},
		{
			name: "snapshot finite",
			build: func() *VectorClock {
				base := New()
				base.Set(8, ^uint32(0))
				vc := New()
				vc.JoinSnapshot(base.Freeze())
				base.Release()
				return vc
			},
		},
		{
			name: "owned retirement",
			build: func() *VectorClock {
				vc := New()
				vc.RetireRange(60, 60)
				return vc
			},
		},
		{
			name: "snapshot retirement",
			build: func() *VectorClock {
				base := New()
				base.RetireRange(61, 61)
				vc := New()
				vc.JoinSnapshot(base.Freeze())
				base.Release()
				return vc
			},
		},
		{
			name: "unrelated causal",
			build: func() *VectorClock {
				foreignAnchor := New()
				foreignAnchor.Set(9, 2)
				foreign := NewClockLineage(foreignAnchor)
				view := foreign.Pin()
				foreignAnchor.Release()
				vc := New()
				vc.JoinCausal(view)
				view.Release()
				foreign.Release()
				return vc
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vc := test.build()
			defer vc.Release()
			if vc.ResidualLessOrEqualCausal(target, 1) {
				t.Fatal("false strong proof accepted")
			}
		})
	}
}

func TestResidualLessOrEqualCausalOwnCoordinateIsFullyExcluded(t *testing.T) {
	anchor := New()
	lineage := NewClockLineage(anchor)
	target := lineage.Pin()
	anchor.Release()
	defer target.Release()
	defer lineage.Release()

	vc := New()
	defer vc.Release()
	vc.Set(11, ^uint32(0))
	if !vc.ResidualLessOrEqualCausal(target, 11) {
		t.Fatal("finite own coordinate was not excluded")
	}
	vc.Set(11, 0)
	vc.RetireRange(11, 11)
	if !vc.ResidualLessOrEqualCausal(target, 11) {
		t.Fatal("retired own coordinate was not excluded")
	}
}

func TestResidualLessOrEqualCausalUnrelatedExactDominance(t *testing.T) {
	leftAnchor := New()
	leftAnchor.Set(70, 3)
	leftLineage := NewClockLineage(leftAnchor)
	left, _ := leftLineage.AppendPinned(71, 4)
	leftAnchor.Release()
	defer left.Release()
	defer leftLineage.Release()

	rightAnchor := New()
	rightAnchor.Set(70, 3)
	rightAnchor.Set(71, 5)
	rightLineage := NewClockLineage(rightAnchor)
	right := rightLineage.Pin()
	rightAnchor.Release()
	defer right.Release()
	defer rightLineage.Release()

	vc := New()
	defer vc.Release()
	vc.JoinCausal(left)
	if !vc.ResidualLessOrEqualCausal(right, 1) {
		t.Fatal("logically dominating unrelated causal root was rejected")
	}
}

func TestResidualLessOrEqualCausalShiftedDenseHeadCannotProduceFalseStrong(t *testing.T) {
	const highTID = uint32(1_000_000_000)
	anchor := New()
	anchor.Set(highTID, 1)
	lineage := NewClockLineage(anchor)
	old := lineage.Pin()
	newer, _ := lineage.AppendPinned(highTID, 2)
	anchor.Release()
	defer old.Release()
	defer newer.Release()
	defer lineage.Release()

	vc := New()
	defer vc.Release()
	vc.JoinCausal(newer)
	if vc.ResidualLessOrEqualCausal(old, 1) {
		t.Fatal("shifted dense-head update was checked at its slice index instead of logical TID")
	}
}

func TestResidualLessOrEqualCausalHugeRunsAreBoundedlyRejected(t *testing.T) {
	anchor := New()
	lineage := NewClockLineage(anchor)
	target := lineage.Pin()
	anchor.Release()
	defer target.Release()
	defer lineage.Release()

	finite := New()
	finite.JoinRange(2, ^uint32(0), 1)
	finite.Freeze()
	if finite.ResidualLessOrEqualCausal(target, 1) {
		t.Fatal("huge finite run unexpectedly produced a bounded proof")
	}
	finite.Release()

	retired := New()
	retired.RetireRange(2, ^uint32(0))
	if retired.ResidualLessOrEqualCausal(target, 1) {
		t.Fatal("huge retired run unexpectedly produced a bounded proof")
	}
	retired.Release()
}

func TestResidualLessOrEqualCausalAllocatesNothing(t *testing.T) {
	vc, view, cleanup := residualBenchmarkClock(t)
	defer cleanup()
	allocs := testing.AllocsPerRun(1000, func() {
		if !vc.ResidualLessOrEqualCausal(view, 1) {
			panic("residual proof failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("residual proof allocated: %v allocs/run", allocs)
	}
}

func BenchmarkResidualLessOrEqualCausalSameFamily(b *testing.B) {
	vc, view, cleanup := residualBenchmarkClock(b)
	b.Cleanup(cleanup)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vc.ResidualLessOrEqualCausal(view, 1)
	}
}

func BenchmarkResidualLessOrEqualCausalHugeRunRejected(b *testing.B) {
	anchor := New()
	lineage := NewClockLineage(anchor)
	target := lineage.Pin()
	anchor.Release()
	vc := New()
	vc.JoinRange(2, ^uint32(0), 1)
	vc.Freeze()
	b.Cleanup(func() {
		vc.Release()
		target.Release()
		lineage.Release()
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vc.ResidualLessOrEqualCausal(target, 1)
	}
}

type testingFataler interface {
	Helper()
	Fatalf(string, ...any)
}

func residualBenchmarkClock(tb testingFataler) (*VectorClock, CausalView, func()) {
	tb.Helper()
	anchor := New()
	for tid := uint32(1); tid <= 10_000; tid++ {
		anchor.Set(tid, tid+10)
	}
	lineage := NewClockLineage(anchor)
	old := lineage.Pin()
	newer, _ := lineage.AppendPinned(10_001, 1)
	anchor.Release()

	vc := New()
	vc.JoinCausal(old)
	old.Release()
	vc.Set(1, 1_000_000)
	base := New()
	base.Set(2, 11) // tiny repeated type metadata, already dominated by anchor
	vc.JoinSnapshot(base.Freeze())
	base.Release()
	if !vc.ResidualLessOrEqualCausal(newer, 1) {
		vc.Release()
		newer.Release()
		lineage.Release()
		tb.Fatalf("benchmark residual proof rejected setup")
	}
	return vc, newer, func() {
		vc.Release()
		newer.Release()
		lineage.Release()
	}
}
