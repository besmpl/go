package vectorclock

import "testing"

func denseTailClockForSharingTest() *VectorClock {
	clock := New()
	for tid := uint32(DenseThreads); tid < DenseThreads+128; tid++ {
		clock.Set(tid, tid%7+1)
	}
	return clock
}

func TestAdaptiveDenseTailCloneCopyOnWrite(t *testing.T) {
	source := denseTailClockForSharingTest()
	clone := source.Clone()
	defer clone.Release()
	if len(source.denseTail) == 0 || &source.denseTail[0] != &clone.denseTail[0] ||
		!source.denseTailShared || !clone.denseTailShared {
		t.Fatal("Clone copied rather than shared the immutable dense tail")
	}

	tid := uint32(DenseThreads + 17)
	sourceValue := source.Get(tid)
	clone.Set(tid, sourceValue+100)
	if source.Get(tid) != sourceValue || clone.Get(tid) != sourceValue+100 ||
		&source.denseTail[0] == &clone.denseTail[0] {
		t.Fatal("clone mutation did not detach from the source")
	}

	cloneValue := clone.Get(tid + 1)
	source.Increment(tid + 1)
	if clone.Get(tid+1) != cloneValue || source.Get(tid+1) != cloneValue+1 {
		t.Fatal("source mutation changed an already detached clone")
	}
}

func TestAdaptiveDenseTailDetachedCopyKeepsSourceMutable(t *testing.T) {
	source := denseTailClockForSharingTest()
	clone := source.CloneDetached()
	defer clone.Release()
	if len(source.denseTail) == 0 || &source.denseTail[0] == &clone.denseTail[0] ||
		source.denseTailShared || clone.denseTailShared {
		t.Fatal("CloneDetached did not retain private dense-tail ownership")
	}

	tid := uint32(DenseThreads + 17)
	before := source.Get(tid)
	source.SetKnownMonotonicAlive(tid, before+1)
	if clone.Get(tid) != before {
		t.Fatal("source mutation changed detached clone")
	}

	clone.Reset()
	if !clone.TryCopyFromDetached(source) {
		t.Fatal("warmed detached copy unexpectedly missed")
	}
	if clone.Get(tid) != before+1 || source.denseTailShared || clone.denseTailShared ||
		&source.denseTail[0] == &clone.denseTail[0] {
		t.Fatal("TryCopyFromDetached shared or lost dense-tail state")
	}
}

func TestAdaptiveDenseTailLogicalReadersDoNotChangeSharing(t *testing.T) {
	clock := denseTailClockForSharingTest()
	anchor := New()
	anchor.Set(DenseThreads+1000, 7)
	lineage := NewClockLineage(anchor)
	anchor.Release()
	defer lineage.Release()
	view := lineage.Pin()
	defer view.Release()
	clock.JoinCausal(view)

	assertPrivate := func(operation string) {
		t.Helper()
		if clock.denseTailShared {
			t.Fatalf("%s changed a read-only dense tail to shared", operation)
		}
	}
	clock.RangeRuns(func(_, _, _ uint32) bool { return true })
	assertPrivate("RangeRuns")
	clock.RangeRetired(func(_, _ uint32) bool { return true })
	assertPrivate("RangeRetired")
	_ = clock.GetMaxTID()
	assertPrivate("GetMaxTID")
	other := clock.CloneDetached()
	defer other.Release()
	if !clock.LessOrEqual(other) {
		t.Fatal("equal detached clock was not dominated")
	}
	assertPrivate("LessOrEqual")
}

func TestAdaptiveDenseTailSharedTryOperationsAreAtomic(t *testing.T) {
	source := denseTailClockForSharingTest()
	target := source.Clone()
	defer target.Release()

	tid := uint32(DenseThreads + 9)
	incoming := source.Clone()
	defer incoming.Release()
	incoming.Set(tid, incoming.Get(tid)+100)
	before := target.Get(tid)
	if target.TryJoin(incoming) || target.Get(tid) != before ||
		&source.denseTail[0] != &target.denseTail[0] {
		t.Fatal("TryJoin mutated a shared dense tail instead of falling back")
	}
	if target.CanSetKnownMonotonicAlive(tid) {
		t.Fatal("shared dense-tail coordinate was reported allocation-free")
	}
	target.PrepareKnownMonotonicSet(tid)
	if !target.CanSetKnownMonotonicAlive(tid) {
		t.Fatal("preparation did not detach the dense-tail coordinate")
	}
	target.SetKnownMonotonicAlive(tid, before+200)
	if target.Get(tid) != before+200 || source.Get(tid) != before {
		t.Fatal("prepared dense-tail commit was not privately owned")
	}
}

func TestAdaptiveDenseTail(t *testing.T) {
	const last = DenseThreads + 10_000 - 1
	vc := New()
	for tid := uint32(DenseThreads); tid <= last; tid++ {
		vc.Set(tid, tid%7+1)
	}
	if got, want := len(vc.denseTail), 10_000; got != want || len(vc.sparseRuns) != 0 {
		t.Fatalf("dense tail len=%d sparse runs=%d, want %d/0", got, len(vc.sparseRuns), want)
	}
	for tid := uint32(DenseThreads); tid <= last; tid++ {
		if got, want := vc.Get(tid), tid%7+1; got != want {
			t.Fatalf("Get(%d)=%d, want %d", tid, got, want)
		}
	}

	joined := New()
	joined.Join(vc)
	if !vc.LessOrEqual(joined) || !joined.LessOrEqual(vc) {
		t.Fatal("dense Join did not preserve equality")
	}
	joined.RetireRange(DenseThreads+123, DenseThreads+456)
	for tid := uint32(DenseThreads + 123); tid <= DenseThreads+456; tid++ {
		if !joined.IsRetired(tid) || joined.Get(tid) != ^uint32(0) {
			t.Fatalf("retired dense TID %d was not +infinity", tid)
		}
	}
	vc.PruneLessOrEqual(joined)
	finite := 0
	vc.Range(func(_, _ uint32) bool { finite++; return true })
	if finite != 0 {
		t.Fatalf("dense prune retained %d finite entries", finite)
	}

	compressed := New()
	compressed.JoinRange(DenseThreads, DenseThreads+100_000, 9)
	if len(compressed.denseTail) != 0 || len(compressed.sparseRuns) != 1 {
		t.Fatalf("compressed range densified: tail=%d runs=%d", len(compressed.denseTail), len(compressed.sparseRuns))
	}
	isolated := New()
	isolated.Set(DenseThreads+1_000_000, 1)
	if len(isolated.denseTail) != 0 || len(isolated.sparseRuns) != 1 {
		t.Fatalf("isolated coordinate densified: tail=%d runs=%d", len(isolated.denseTail), len(isolated.sparseRuns))
	}
}

func BenchmarkAdaptiveDenseTail(b *testing.B) {
	const entries = 10_000
	source := New()
	for tid := uint32(DenseThreads); tid < DenseThreads+entries; tid++ {
		source.Set(tid, tid%7+1)
	}
	b.Run("Join10k", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			dst := New()
			dst.Join(source)
		}
	})
	b.Run("Get10k", func(b *testing.B) {
		b.ReportAllocs()
		var sum uint32
		for i := 0; i < b.N; i++ {
			for tid := uint32(DenseThreads); tid < DenseThreads+entries; tid++ {
				sum += source.Get(tid)
			}
		}
		_ = sum
	})
	b.Run("Clone10k", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			clone := source.Clone()
			clone.Release()
		}
	})
}

func TestAdaptiveDenseTailMixedRepresentations(t *testing.T) {
	dense := New()
	for tid := uint32(DenseThreads); tid < DenseThreads+1000; tid++ {
		dense.Set(tid, tid%3+1)
	}

	compressed := New()
	compressed.JoinRange(DenseThreads, DenseThreads+999, 4)
	if !dense.LessOrEqual(compressed) || compressed.LessOrEqual(dense) {
		t.Fatal("mixed dense/compressed comparison is incorrect")
	}
	compressed.PruneLessOrEqual(dense)
	for tid := uint32(DenseThreads); tid < DenseThreads+1000; tid++ {
		got := compressed.Get(tid)
		want := uint32(4)
		if dense.Get(tid) == 4 {
			want = 0
		}
		if got != want {
			t.Fatalf("mixed prune Get(%d)=%d, want %d", tid, got, want)
		}
	}

	receiver := New()
	receiver.Set(DenseThreads+1500, 9)
	receiver.Join(dense)
	if receiver.Get(DenseThreads+1500) != 9 || !dense.LessOrEqual(receiver) {
		t.Fatal("dense join lost a pre-existing sparse coordinate")
	}

	var ranges []FiniteRange
	dense.RangeRuns(func(first, last, clock uint32) bool {
		ranges = append(ranges, FiniteRange{First: first, Last: last, Clock: clock})
		return true
	})
	fromRanges := New()
	fromRanges.JoinCanonicalRanges(ranges)
	if !dense.LessOrEqual(fromRanges) || !fromRanges.LessOrEqual(dense) {
		t.Fatal("JoinCanonicalRanges changed dense contents")
	}
}

func TestAdaptiveDenseTailJoinSparseSourceOverlap(t *testing.T) {
	const (
		denseFirst = uint32(DenseThreads)
		denseLast  = denseFirst + 200
		joinFirst  = denseFirst + 50
		joinLast   = denseFirst + 150
	)

	dst := New()
	for tid := denseFirst; tid < denseLast; tid++ {
		dst.Set(tid, tid%7+1)
	}
	if got, want := len(dst.denseTail), int(denseLast-denseFirst); got != want {
		t.Fatalf("destination dense tail len=%d, want %d", got, want)
	}

	src := New()
	src.JoinRange(joinFirst, joinLast, 9)
	if len(src.denseTail) != 0 || len(src.sparseRuns) != 1 {
		t.Fatalf("source representation tail=%d runs=%d, want 0/1", len(src.denseTail), len(src.sparseRuns))
	}

	dst.Join(src)
	for tid := joinFirst; tid <= joinLast; tid++ {
		if got := dst.Get(tid); got != 9 {
			t.Fatalf("joined clock[%d]=%d, want 9", tid, got)
		}
	}
}

func TestAdaptiveDenseTailPromotionReleasesSparseBacking(t *testing.T) {
	vc := New()
	for i := uint32(0); i < minDenseTailRuns; i++ {
		vc.Set(DenseThreads+i, i+1)
	}
	if got, want := len(vc.denseTail), minDenseTailRuns; got != want {
		t.Fatalf("dense tail len=%d, want %d", got, want)
	}
	if len(vc.sparseRuns) != 0 || cap(vc.sparseRuns) != 0 {
		t.Fatalf("promoted sparse storage len=%d cap=%d, want 0/0", len(vc.sparseRuns), cap(vc.sparseRuns))
	}
}

func TestAdaptiveDenseTailFrozenBaseOverlayParity(t *testing.T) {
	want, got := New(), New()
	for tid := uint32(DenseThreads); tid < DenseThreads+512; tid++ {
		clock := tid%7 + 1
		want.Set(tid, clock)
		got.Set(tid, clock)
	}
	want.RetireRange(DenseThreads+100, DenseThreads+140)
	got.RetireRange(DenseThreads+100, DenseThreads+140)
	got.Freeze()
	for tid := uint32(DenseThreads + 3); tid < DenseThreads+512; tid += 31 {
		want.Set(tid, 20)
		got.Set(tid, 20)
	}
	if !want.LessOrEqual(got) || !got.LessOrEqual(want) {
		t.Fatal("frozen dense-tail base and mutable representation diverged")
	}
	var wantRuns, gotRuns []FiniteRange
	want.RangeRuns(func(first, last, clock uint32) bool {
		wantRuns = append(wantRuns, FiniteRange{First: first, Last: last, Clock: clock})
		return true
	})
	got.RangeRuns(func(first, last, clock uint32) bool {
		gotRuns = append(gotRuns, FiniteRange{First: first, Last: last, Clock: clock})
		return true
	})
	if len(wantRuns) != len(gotRuns) {
		t.Fatalf("frozen dense-tail emitted %d runs, want %d", len(gotRuns), len(wantRuns))
	}
	for i := range wantRuns {
		if wantRuns[i] != gotRuns[i] {
			t.Fatalf("frozen dense-tail run %d = %+v, want %+v", i, gotRuns[i], wantRuns[i])
		}
	}
}
