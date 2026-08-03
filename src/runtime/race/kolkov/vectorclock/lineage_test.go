package vectorclock

import (
	"math/rand"
	"reflect"
	"sync"
	"testing"
)

func TestOwnerClockLineageCompactPinnedVersions(t *testing.T) {
	lineage, first := NewOwnerClockLineage(1<<20, 7)
	defer lineage.Release()
	defer first.Release()
	if !first.segment.ownerOnly || len(first.segment.cells) != 0 || len(first.segment.denseHeads) != 0 {
		t.Fatal("owner lineage allocated the generic coordinate index")
	}
	owned, ok := first.Duplicate()
	if !ok {
		t.Fatal("could not duplicate owner view")
	}
	if _, ok := lineage.AppendOwned(&owned, 1<<20, 8); !ok {
		t.Fatal("owner append did not publish")
	}
	defer owned.Release()
	if got := first.Get(1 << 20); got != 7 {
		t.Fatalf("old pin observed later append: got %d want 7", got)
	}
	if got := owned.Get(1 << 20); got != 8 {
		t.Fatalf("owned pin = %d, want 8", got)
	}
	var finite []FiniteRange
	var retired []RetiredRange
	first.AppendReleaseComponents(&finite, &retired)
	finite, retired = CanonicalizeReleaseRanges(finite, retired)
	clock := New()
	clock.JoinCanonicalRanges(finite)
	clock.RetireRanges(retired)
	defer clock.Release()
	if got := clock.Get(1 << 20); got != 7 {
		t.Fatalf("direct pinned enumeration = %d, want 7", got)
	}
}

func TestOwnerClockLineagePromotesOnForeignAppend(t *testing.T) {
	lineage, view := NewOwnerClockLineage(77, 2)
	defer lineage.Release()
	defer view.Release()
	if _, ok := lineage.Append(88, 5); !ok {
		t.Fatal("foreign append did not rotate to generic index")
	}
	pinned := lineage.Pin()
	defer pinned.Release()
	if pinned.segment.ownerOnly || pinned.Get(77) != 2 || pinned.Get(88) != 5 {
		t.Fatal("foreign append changed the exact owner-lineage image")
	}
}

func TestClockLineagePinnedVersionsDifferential(t *testing.T) {
	anchor := New()
	anchor.Set(1, 7)
	anchor.Set(10000, 3)
	anchor.RetireRange(90, 92)
	lineage := NewClockLineage(anchor)
	defer lineage.Release()

	want := map[uint32]uint32{1: 7, 10000: 3, 90: ^uint32(0), 91: ^uint32(0), 92: ^uint32(0)}
	type pinned struct {
		view CausalView
		want map[uint32]uint32
	}
	var pins []pinned
	rng := rand.New(rand.NewSource(4))
	for step := 0; step < 12000; step++ {
		tid := uint32(rng.Intn(160))
		clock := uint32(rng.Intn(300) + 1)
		old := want[tid]
		view, appended := lineage.AppendPinned(tid, clock)
		if !view.Valid() {
			t.Fatal("append did not return a view")
		}
		if old != ^uint32(0) && clock > old {
			want[tid] = clock
			if !appended {
				t.Fatalf("step %d: monotone update was not appended", step)
			}
		} else if appended {
			t.Fatalf("step %d: dominated/retired update was appended", step)
		}
		if step%503 == 0 {
			copyWant := make(map[uint32]uint32, len(want))
			for k, v := range want {
				copyWant[k] = v
			}
			pins = append(pins, pinned{view: view, want: copyWant})
		} else {
			view.Release()
		}
		if step == 5000 || step == 9000 {
			if !lineage.Rotate() {
				t.Fatal("manual rotation failed")
			}
		}
	}
	for _, pin := range pins {
		for tid := uint32(0); tid < 180; tid++ {
			if got, expected := pin.view.Get(tid), pin.want[tid]; got != expected {
				t.Fatalf("version %d tid %d = %d, want %d", pin.view.Version(), tid, got, expected)
			}
		}
		materialized := New()
		materialized.JoinSnapshot(pin.view.Snapshot())
		for tid, expected := range pin.want {
			if got := materialized.Get(tid); got != expected {
				t.Fatalf("materialized version %d tid %d = %d, want %d", pin.view.Version(), tid, got, expected)
			}
		}
		pin.view.Release()
	}
}

func TestClockLineageWideMaterializationMatchesPointOracle(t *testing.T) {
	anchor := New()
	anchor.Set(7, 11)
	anchor.RetireRange(80_000, 80_010)
	lineage := NewClockLineage(anchor)
	defer lineage.Release()
	anchor.Release()

	want := map[uint32]uint32{7: 11}
	for tid := uint32(10_000); tid < 20_000; tid++ {
		clock := tid%17 + 1
		if _, appended := lineage.Append(tid, clock); !appended {
			t.Fatalf("initial append for tid %d was rejected", tid)
		}
		want[tid] = clock
	}
	historical := lineage.Pin()
	defer historical.Release()
	for tid := uint32(10_000); tid < 20_000; tid += 3 {
		if _, appended := lineage.Append(tid, want[tid]+100); !appended {
			t.Fatalf("newer append for tid %d was rejected", tid)
		}
	}

	materialized := historical.materialize()
	defer materialized.Release()
	for tid, clock := range want {
		if got := materialized.Get(tid); got != clock {
			t.Fatalf("materialized clock[%d] = %d, want %d", tid, got, clock)
		}
	}
	for tid := uint32(80_000); tid <= 80_010; tid++ {
		if !materialized.IsRetired(tid) {
			t.Fatalf("materialized clock lost retirement for tid %d", tid)
		}
	}
}

func BenchmarkClockLineageWideMaterialization(b *testing.B) {
	lineage := NewClockLineage(nil)
	defer lineage.Release()
	for tid := uint32(10_000); tid < 20_000; tid++ {
		lineage.Append(tid, tid%17+1)
	}
	view := lineage.Pin()
	defer view.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clock := view.materialize()
		clock.Release()
	}
}

func TestClockLineageHistoricalGetAndBranch(t *testing.T) {
	lineage := NewClockLineage(nil)
	defer lineage.Release()
	var at50 CausalView
	for clock := uint32(1); clock <= 1000; clock++ {
		view, appended := lineage.AppendPinned(7, clock)
		if !appended {
			t.Fatalf("clock %d was not appended", clock)
		}
		if clock == 50 {
			at50 = view
		} else {
			view.Release()
		}
	}
	if got := at50.Get(7); got != 50 {
		t.Fatalf("historical get = %d, want 50", got)
	}
	branch := at50.Branch()
	if _, ok := branch.Append(8, 9); !ok {
		t.Fatal("branch append failed")
	}
	branchView := branch.Pin()
	if got := branchView.Get(7); got != 50 {
		t.Fatalf("branch inherited %d, want 50", got)
	}
	if got := branchView.Get(8); got != 9 {
		t.Fatalf("branch update = %d, want 9", got)
	}
	parentView := lineage.Pin()
	if got := parentView.Get(8); got != 0 {
		t.Fatalf("parent observed branch update %d", got)
	}
	if parentView.SameFamily(branchView) {
		t.Fatal("branch unexpectedly retained parent family identity")
	}
	parentView.Release()
	branchView.Release()
	branch.Release()
	at50.Release()
}

func TestClockLineageFamilyDominanceAndMonotoneReanchor(t *testing.T) {
	lineage := NewClockLineage(nil)
	defer lineage.Release()
	older, _ := lineage.AppendPinned(1, 2)
	if !lineage.Rotate() {
		t.Fatal("rotate failed")
	}
	newer, _ := lineage.AppendPinned(2, 3)
	if !newer.Dominates(older) || older.Dominates(newer) {
		t.Fatal("same-family dominance did not survive rotation")
	}
	copyView, ok := older.Duplicate()
	if !ok || copyView.Get(1) != 2 {
		t.Fatal("duplicate pin failed")
	}
	copyView.Release()

	dominating := newer.materialize()
	dominating.Set(100, 9)
	if !lineage.Reanchor(dominating) {
		t.Fatal("monotone reanchor rejected")
	}
	reanchored := lineage.Pin()
	if !reanchored.Dominates(newer) || newer.Dominates(reanchored) ||
		reanchored.Version() != newer.Version()+1 || reanchored.Get(100) != 9 {
		t.Fatal("reanchor lost family/version/image")
	}
	nondominating := New()
	if lineage.Reanchor(nondominating) {
		t.Fatal("non-monotone reanchor accepted")
	}
	reanchored.Release()
	newer.Release()
	older.Release()
}

func TestClockLineageRotateSealAndReferences(t *testing.T) {
	lineage := NewClockLineage(nil)
	old, _ := lineage.AppendPinned(1, 2)
	oldSegment := old.segment
	if refs := oldSegment.refs.Load(); refs != 2 {
		t.Fatalf("open segment refs = %d, want 2", refs)
	}
	if !lineage.Rotate() || !oldSegment.sealed.Load() {
		t.Fatal("rotation did not seal old segment")
	}
	if refs := oldSegment.refs.Load(); refs != 1 {
		t.Fatalf("rotated segment refs = %d, want pinned view only", refs)
	}
	if got := old.Get(1); got != 2 {
		t.Fatalf("old view after rotate = %d, want 2", got)
	}
	final := lineage.Seal()
	if !final.Valid() {
		t.Fatal("seal did not return final view")
	}
	if _, ok := lineage.Append(2, 3); ok {
		t.Fatal("sealed lineage accepted append")
	}
	lineage.Release()
	if got := final.Get(1); got != 2 {
		t.Fatalf("final view after lineage release = %d, want 2", got)
	}
	old.Release()
	if refs := oldSegment.refs.Load(); refs != 0 {
		t.Fatalf("released old segment refs = %d, want 0", refs)
	}
	old.Release() // pointer release is idempotent and clears the handle
	if old.Valid() || old.Get(1) != 0 {
		t.Fatal("released view remained usable")
	}
	final.Release()
}

func TestClockLineageConcurrentReaders(t *testing.T) {
	lineage := NewClockLineage(nil)
	defer lineage.Release()
	const updates = 3000
	views := make(chan CausalView, 32)
	var readers sync.WaitGroup
	for reader := 0; reader < 4; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for view := range views {
				version := uint32(view.Version())
				if got := view.Get(1); got != version {
					t.Errorf("version %d read %d", version, got)
				}
				view.Release()
			}
		}()
	}
	for clock := uint32(1); clock <= updates; clock++ {
		view, ok := lineage.AppendPinned(1, clock)
		if !ok {
			t.Fatalf("append %d failed", clock)
		}
		views <- view
	}
	close(views)
	readers.Wait()
}

func TestClockLineageAutomaticRotationAndReservedAppend(t *testing.T) {
	lineage := NewClockLineage(nil)
	defer lineage.Release()
	if !lineage.Reserve(lineageMinRotation) {
		t.Fatal("reserve failed")
	}
	lastReservedOverflowBlock := uint32((lineageMinRotation - 1) / lineageBlockEntries)
	if lineage.current.overflowBlockAt(lastReservedOverflowBlock) == nil {
		t.Fatal("reserve did not preallocate worst-case exact-checkpoint storage")
	}
	var old CausalView
	clock := uint32(0)
	allocs := testing.AllocsPerRun(100, func() {
		clock++
		if _, ok := lineage.Append(1, clock); !ok {
			t.Fatalf("reserved append %d failed", clock)
		}
	})
	if allocs != 0 {
		t.Fatalf("reserved append allocated: %v", allocs)
	}
	for clock++; clock <= lineageMinRotation; clock++ {
		view, ok := lineage.AppendPinned(1, clock)
		if !ok {
			t.Fatalf("append %d failed", clock)
		}
		if clock == lineageMinRotation {
			old = view
		} else {
			view.Release()
		}
	}
	// The next append rotates before publishing its update.
	newer, ok := lineage.AppendPinned(1, lineageMinRotation+1)
	if !ok || newer.segment == old.segment || !newer.Dominates(old) {
		t.Fatal("automatic rotation did not preserve family ordering")
	}
	if old.Get(1) != lineageMinRotation || newer.Get(1) != lineageMinRotation+1 {
		t.Fatal("rotation changed a pinned image")
	}
	if newer.segment.denseBase != 1 || len(newer.segment.denseHeads) != 1 {
		t.Fatalf("dense head interval = [%d,+%d), want [1,2)", newer.segment.denseBase, len(newer.segment.denseHeads))
	}
	newer.Release()
	old.Release()
}

func TestClockLineageRotationThreshold(t *testing.T) {
	tests := []struct {
		name        string
		coordinates uint64
		want        uint64
	}{
		{name: "empty", coordinates: 0, want: lineageMinRotation},
		{name: "small", coordinates: 1, want: lineageMinRotation},
		{name: "adaptive", coordinates: 100, want: 100 * lineageRotationEntriesPerCoordinate},
		{name: "at-cap", coordinates: lineageMaxRotation / lineageRotationEntriesPerCoordinate, want: lineageMaxRotation},
		{name: "above-cap", coordinates: lineageMaxRotation/lineageRotationEntriesPerCoordinate + 1, want: lineageMaxRotation},
		{name: "overflow-safe", coordinates: ^uint64(0), want: lineageMaxRotation},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := lineageRotationThreshold(test.coordinates); got != test.want {
				t.Fatalf("threshold(%d) = %d, want %d", test.coordinates, got, test.want)
			}
		})
	}
}

func TestClockLineageAutomaticRotationReclaimsUnpinnedSegments(t *testing.T) {
	lineage := NewClockLineage(nil)
	defer lineage.Release()
	const rotations = 4
	var sealed []*lineageSegment
	var want [4]uint32
	clock := uint32(0)
	for rotation := 0; rotation < rotations; rotation++ {
		// Keep this lifecycle test cheap while exercising the production
		// automatic-rotation path rather than the manual Rotate helper.
		lineage.current.rotateAt = 8
		segment := lineage.current
		for uint64(segment.entries) < segment.rotateAt {
			clock++
			if _, appended := lineage.Append(uint32(clock&3), clock); !appended {
				t.Fatalf("rotation %d clock %d was not appended", rotation, clock)
			}
			want[clock&3] = clock
		}
		clock++
		if _, appended := lineage.Append(uint32(clock&3), clock); !appended {
			t.Fatalf("rotating clock %d was not appended", clock)
		}
		want[clock&3] = clock
		if lineage.current == segment || !segment.sealed.Load() {
			t.Fatalf("rotation %d did not replace and seal its segment", rotation)
		}
		if refs := segment.refs.Load(); refs != 0 {
			t.Fatalf("rotation %d left %d references on an unpinned segment", rotation, refs)
		}
		sealed = append(sealed, segment)
	}
	view := lineage.Pin()
	defer view.Release()
	for tid := uint32(0); tid < 4; tid++ {
		if got := view.Get(tid); got != want[tid] {
			t.Fatalf("final image tid %d = %d, want %d after %d rotations", tid, got, want[tid], len(sealed))
		}
	}
}

func TestClockLineageCheckpointAndCompactEntry(t *testing.T) {
	if lineageMaxRotation > int(lineageEntryPrevMask) {
		t.Fatalf("maximum local index %d exceeds %d-bit previous-index encoding", lineageMaxRotation, lineageEntryPrevBits)
	}
	if lineageEntryCheckpointPeriod-1 > 1<<(32-lineageHeadCompactShift)-1 {
		t.Fatalf("checkpoint period %d exceeds packed head distance", lineageEntryCheckpointPeriod)
	}
	if size := reflect.TypeOf(lineageEntry{}).Size(); size != 4 {
		t.Fatalf("lineage entry size = %d, want 4", size)
	}
	if size := reflect.TypeOf(lineageCell{}).Size(); size != 16 {
		t.Fatalf("sparse head cell size = %d, want 16", size)
	}
	if size := reflect.TypeOf(lineageHead{}).Size(); size != 8 {
		t.Fatalf("dense head size = %d, want 8", size)
	}
	lineage := NewClockLineage(nil)
	defer lineage.Release()
	before, _ := lineage.AppendPinned(3, 5)
	if !lineage.Checkpoint() {
		t.Fatal("checkpoint failed")
	}
	after := lineage.Pin()
	if after.Get(3) != 5 || !after.Dominates(before) || before.Dominates(after) ||
		after.Version() != before.Version()+1 {
		t.Fatal("checkpoint did not publish a distinct equal-image version")
	}
	after.Release()
	before.Release()
}

func TestClockLineageDeltaEntriesPreserveInterleavedVersions(t *testing.T) {
	lineage := NewClockLineage(nil)
	defer lineage.Release()
	const owners, rounds = uint32(100), uint32(100)
	views := make([]CausalView, 0, rounds)
	for round := uint32(1); round <= rounds; round++ {
		for tid := uint32(0); tid < owners; tid++ {
			if _, appended := lineage.Append(tid, round); !appended {
				t.Fatalf("round %d tid %d was not appended", round, tid)
			}
		}
		views = append(views, lineage.Pin())
	}
	maxCheckpoints := owners * (rounds / lineageEntryCheckpointPeriod)
	if got := lineage.current.overflows; got == 0 || got > maxCheckpoints {
		t.Fatalf("unit-delta interleaving used %d exact checkpoints, want 1..%d", got, maxCheckpoints)
	}
	if size := reflect.TypeOf(lineageBlock{}).Size(); size != lineageBlockEntries*4 {
		t.Fatalf("lineage block size = %d, want %d", size, lineageBlockEntries*4)
	}
	for round, view := range views {
		want := uint32(round + 1)
		for tid := uint32(0); tid < owners; tid++ {
			if got := view.Get(tid); got != want {
				t.Fatalf("version %d tid %d = %d, want %d", view.Version(), tid, got, want)
			}
		}
		view.Release()
	}
}

func TestClockLineageExactDeltaOverflowAndPreparedAbort(t *testing.T) {
	lineage := NewClockLineage(nil)
	defer lineage.Release()
	first, _ := lineage.AppendPinned(7, 1)
	lineage.Append(9, 11)
	head, found, _ := lineage.current.head(7)
	if index, compact := head.load(); !found || index != 1 || head.clock != 1 || compact != 1 {
		t.Fatalf("initial head found=%v index=%d clock=%d compact=%d", found, index, head.clock, compact)
	}
	prepared, needed := lineage.PrepareAppend(7, 5000)
	reservationOK := needed && prepared.Valid() && prepared.previous == 1 && lineage.current.firstOverflow.Load() != nil && lineage.current.overflows == 0
	prepared.Abort()
	if !reservationOK {
		t.Fatal("overflow preparation did not reserve unpublished exact storage")
	}
	aborted := lineage.Pin()
	if lineage.current.overflows != 0 || aborted.Get(7) != 1 {
		aborted.Release()
		t.Fatal("aborted overflow preparation changed the lineage")
	}
	aborted.Release()
	prepared, needed = lineage.PrepareAppend(7, 5000)
	if !needed || prepared.previous != 1 {
		prepared.Abort()
		t.Fatalf("overflow retry needed=%v previous=%d", needed, prepared.previous)
	}
	after, appended := prepared.CommitPinned()
	if afterClock, firstClock := after.Get(7), first.Get(7); !appended || lineage.current.overflows != 1 || afterClock != 5000 || firstClock != 1 {
		overflow := lineage.current.overflowAt(1)
		t.Fatalf("exact overflow appended=%v overflows=%d after=%d first=%d entry1=%#x overflow=%+v", appended, lineage.current.overflows, afterClock, firstClock, lineage.current.entryAt(1).word, overflow)
	}
	if _, appended := lineage.Append(7, 5001); !appended || lineage.current.overflows != 1 {
		t.Fatal("unit delta after overflow did not return to compact encoding")
	}
	after.Release()
	first.Release()
}

func TestClockLineageDeltaEncodingBoundaries(t *testing.T) {
	lineage := NewClockLineage(nil)
	defer lineage.Release()
	type pinnedClock struct {
		view CausalView
		want uint32
	}
	clocks := []uint32{1, 1 + lineageEntryDeltaMask, 2 + 2*lineageEntryDeltaMask, ^uint32(0) - 1, ^uint32(0)}
	pins := make([]pinnedClock, 0, len(clocks))
	for _, clock := range clocks {
		view, appended := lineage.AppendPinned(17, clock)
		if !appended {
			t.Fatalf("clock %d was not appended", clock)
		}
		pins = append(pins, pinnedClock{view: view, want: clock})
	}
	if word := lineage.current.entryAt(2).word; word&lineageEntryOverflowFlag != 0 || word>>lineageEntryPrevBits != lineageEntryDeltaMask {
		t.Fatalf("maximum compact delta encoded as %#x", word)
	}
	if word := lineage.current.entryAt(3).word; word&lineageEntryOverflowFlag == 0 {
		t.Fatalf("delta above compact maximum encoded as %#x", word)
	}
	if got := lineage.current.overflows; got != 2 {
		t.Fatalf("exact checkpoint count = %d, want 2", got)
	}
	for _, pin := range pins {
		if got := pin.view.Get(17); got != pin.want {
			t.Fatalf("version %d clock = %d, want %d", pin.view.Version(), got, pin.want)
		}
		pin.view.Release()
	}
}

func TestClockLineagePrepareAppendBoundaries(t *testing.T) {
	t.Run("rotation", func(t *testing.T) {
		lineage := NewClockLineage(nil)
		defer lineage.Release()
		if !lineage.Reserve(lineageMinRotation) {
			t.Fatal("reserve failed")
		}
		for clock := uint32(1); clock <= lineageMinRotation; clock++ {
			if _, ok := lineage.Append(1, clock); !ok {
				t.Fatalf("append %d failed", clock)
			}
		}
		oldSegment := lineage.current
		prepared, needed := lineage.PrepareAppend(1, lineageMinRotation+1)
		if !prepared.Valid() || !needed || lineage.current == oldSegment {
			t.Fatal("prepare did not rotate before hardware boundary")
		}
		index := lineage.current.entries + 1
		if lineage.current.blockAt((index-1)/lineageBlockEntries) == nil {
			t.Fatal("prepare did not reserve the commit block")
		}
		if version, appended := prepared.Commit(); !appended || version != lineageMinRotation+1 {
			t.Fatal("prepared commit failed")
		}
		if !lineage.Reserve(101) {
			t.Fatal("post-rotation reserve failed")
		}
		clock := uint32(lineageMinRotation + 1)
		allocs := testing.AllocsPerRun(100, func() {
			clock++
			prepared, needed := lineage.PrepareAppend(1, clock)
			if !needed {
				t.Fatal("prepared monotone append classified as dominated")
			}
			if _, appended := prepared.Commit(); !appended {
				t.Fatal("prepared append did not commit")
			}
		})
		if allocs != 0 {
			t.Fatalf("prepared append allocated after preparation: %v", allocs)
		}
	})

	t.Run("sparse-capacity-boundary", func(t *testing.T) {
		lineage := NewClockLineage(nil)
		defer lineage.Release()
		limit := uint32(len(lineage.current.cells) / 2)
		for tid := uint32(0); tid < limit; tid++ {
			if _, ok := lineage.Append(tid, 1); !ok {
				t.Fatalf("append tid %d failed", tid)
			}
		}
		if lineage.current.sparseHeads*2 != uint64(len(lineage.current.cells)) {
			t.Fatal("test did not reach sparse half load")
		}
		oldSegment := lineage.current
		prepared, needed := lineage.PrepareAppend(limit, 1)
		if !needed || lineage.current == oldSegment {
			t.Fatal("prepare did not rotate at the shared event/index boundary")
		}
		if _, appended := prepared.Commit(); !appended {
			t.Fatal("prepared sparse commit failed")
		}
	})

	t.Run("dominated-and-abort", func(t *testing.T) {
		lineage := NewClockLineage(nil)
		defer lineage.Release()
		lineage.Append(1, 4)
		prepared, needed := lineage.PrepareAppend(1, 3)
		if !prepared.Valid() || needed || prepared.Needed() {
			t.Fatal("dominated preparation classification is wrong")
		}
		if _, appended := prepared.Commit(); appended {
			t.Fatal("dominated commit appended")
		}
		prepared, _ = lineage.PrepareAppend(1, 5)
		prepared.Abort()
		if got := lineage.Pin(); got.Get(1) != 4 {
			got.Release()
			t.Fatalf("abort published clock %d", got.Get(1))
		} else {
			got.Release()
		}
	})
}

func TestClockLineageBoundedHugeRangeConstruction(t *testing.T) {
	anchor := New()
	anchor.JoinRange(0, 1<<30, 7)
	anchor.RetireRange(1<<31, 1<<31+1000)
	lineage := NewClockLineage(anchor)
	defer lineage.Release()
	segment := lineage.current
	if segment.rotateAt > lineageMaxRotation || len(segment.blockPages) != lineageBlockPageCount {
		t.Fatalf("unbounded rotation storage: rotate=%d pages=%d", segment.rotateAt, len(segment.blockPages))
	}
	if len(segment.denseHeads) > lineageMaxDenseHeads || len(segment.cells) > 32768 {
		t.Fatalf("unbounded head storage: dense=%d sparse=%d", len(segment.denseHeads), len(segment.cells))
	}
	view := lineage.Pin()
	defer view.Release()
	if view.Get(1<<30) != 7 || !view.IsRetired(1<<31) {
		t.Fatal("bounded construction changed the huge-range image")
	}
}

func TestClockLineageCompressedRunManyUniqueHeads(t *testing.T) {
	lineage := NewClockLineage(nil)
	defer lineage.Release()
	rotations := 0
	var previousRotationEntries uint32
	for tid := uint32(0); tid < 10000; tid++ {
		previous := lineage.current
		if _, appended := lineage.Append(tid, 1); !appended {
			t.Fatalf("unique owner %d was not appended", tid)
		}
		if lineage.current != previous {
			rotations++
			if previousRotationEntries != 0 && previous.entries < previousRotationEntries*2 {
				t.Fatalf("sparse index did not adapt geometrically: %d after %d events", previous.entries, previousRotationEntries)
			}
			previousRotationEntries = previous.entries
		}
	}
	if rotations > 7 {
		t.Fatalf("10000 consecutive owners caused %d rotations", rotations)
	}
	if rotations == 0 || len(lineage.current.cells) > 32768 {
		t.Fatalf("unique-owner adaptation rotations=%d sparse=%d", rotations, len(lineage.current.cells))
	}

	warm := NewClockLineage(nil)
	defer warm.Release()
	if !warm.Reserve(101) {
		t.Fatal("warm reserve failed")
	}
	clock := uint32(0)
	allocs := testing.AllocsPerRun(100, func() {
		clock++
		if _, appended := warm.Append(7, clock); !appended {
			t.Fatal("warm repeated-owner append failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("warm repeated-owner append allocated: %v", allocs)
	}
}

func TestClockLineageShiftedDenseSegmentBacking(t *testing.T) {
	anchor := New()
	anchor.JoinRange(100000, 109999, 1)
	lineage := NewClockLineage(anchor)
	defer lineage.Release()
	segment := lineage.current
	if len(segment.denseHeads) != 0 {
		t.Fatalf("immutable 10k-coordinate anchor allocated %d mutable dense heads", len(segment.denseHeads))
	}
	if segment.rotateAt != lineageMaxRotation || len(segment.cells) > 16 {
		t.Fatalf("steady sizing rotate=%d sparse=%d, want %d/<=16", segment.rotateAt, len(segment.cells), lineageMaxRotation)
	}
	for page := range segment.blockPages {
		if segment.blockPages[page].Load() != nil {
			t.Fatalf("immutable anchor allocated block page %d", page)
		}
	}
	if _, appended := lineage.Append(105000, 2); !appended {
		t.Fatal("shifted dense owner append failed")
	}
	view := lineage.Pin()
	defer view.Release()
	if view.Get(100000) != 1 || view.Get(105000) != 2 || view.Get(109999) != 1 {
		t.Fatal("small mutable index changed wide-anchor semantics")
	}
}

func TestCausalViewAdvanceTo(t *testing.T) {
	lineage := NewClockLineage(nil)
	defer lineage.Release()
	first, _ := lineage.AppendPinned(1, 1)
	second, _ := lineage.AppendPinned(1, 2)
	segment := first.segment
	refs := segment.refs.Load()
	if !first.AdvanceTo(second) || first.Version() != second.Version() || segment.refs.Load() != refs {
		t.Fatal("same-segment advance changed references or version incorrectly")
	}
	second.Release()
	if !lineage.Rotate() {
		t.Fatal("rotate failed")
	}
	third, _ := lineage.AppendPinned(2, 1)
	oldSegment, newSegment := first.segment, third.segment
	oldRefs, newRefs := oldSegment.refs.Load(), newSegment.refs.Load()
	if !first.AdvanceTo(third) || oldSegment.refs.Load() != oldRefs-1 || newSegment.refs.Load() != newRefs+1 {
		t.Fatal("cross-segment advance did not retain-before-release")
	}
	third.Release()
	first.Release()
}

func TestClockLineageAppendOwnedReferences(t *testing.T) {
	lineage := NewClockLineage(nil)
	defer lineage.Release()
	var owned CausalView
	if _, appended := lineage.AppendOwned(&owned, 1, 1); !appended || !owned.Valid() {
		t.Fatal("empty owner did not adopt appended publication")
	}
	segment := owned.segment
	refs := segment.refs.Load()
	if _, appended := lineage.AppendOwned(&owned, 1, 2); !appended ||
		owned.segment != segment || segment.refs.Load() != refs {
		t.Fatal("same-segment owned append changed references")
	}
	version := owned.Version()
	if _, appended := lineage.AppendOwned(&owned, 1, 2); appended ||
		owned.Version() != version || segment.refs.Load() != refs {
		t.Fatal("dominated owned append changed publication")
	}

	if !lineage.Reserve(lineage.current.rotateAt - uint64(lineage.current.entries)) {
		t.Fatal("reserve to rotation boundary failed")
	}
	for uint64(lineage.current.entries) < lineage.current.rotateAt {
		clock := uint32(lineage.Version() + 1)
		if _, appended := lineage.Append(1, clock); !appended {
			t.Fatal("failed to fill rotation segment")
		}
	}
	oldSegment := owned.segment
	oldRefs := oldSegment.refs.Load()
	if _, appended := lineage.AppendOwned(&owned, 1, uint32(lineage.Version()+1)); !appended {
		t.Fatal("rotating owned append failed")
	}
	newSegment := owned.segment
	if newSegment == oldSegment || oldSegment.refs.Load() != oldRefs-2 || newSegment.refs.Load() != 2 {
		t.Fatalf("owned rotation refs old=%d new=%d", oldSegment.refs.Load(), newSegment.refs.Load())
	}
	owned.Release()
}

func TestClockLineageReanchorOwnedWeakVersion(t *testing.T) {
	lineage := NewClockLineage(nil)
	defer lineage.Release()
	var owned CausalView
	lineage.AppendOwned(&owned, 4, 7)
	anchor := owned.materialize()
	oldSegment, oldVersion := owned.segment, owned.Version()
	oldRefs := oldSegment.refs.Load()
	if !lineage.ReanchorOwned(&owned, anchor, true) {
		t.Fatal("forced equal reanchor failed")
	}
	if owned.segment == oldSegment || owned.Version() != oldVersion+1 || owned.Get(4) != 7 {
		t.Fatal("forced equal reanchor lost image or weak version")
	}
	if oldSegment.refs.Load() != oldRefs-2 || owned.segment.refs.Load() != 2 {
		t.Fatal("forced reanchor did not transfer owned reference")
	}
	currentSegment := owned.segment
	refs := currentSegment.refs.Load()
	if !lineage.ReanchorOwned(&owned, anchor, false) || owned.segment != currentSegment ||
		owned.Version() != oldVersion+1 || currentSegment.refs.Load() != refs {
		t.Fatal("equal non-forced reanchor changed publication")
	}

	other := NewClockLineage(nil)
	defer other.Release()
	foreign := other.Pin()
	if _, appended := lineage.AppendOwned(&foreign, 4, 8); appended {
		t.Fatal("foreign owner was accepted")
	}
	foreign.Release()
	owned.Release()
}

func TestPreparedCausalAppendCommitOwned(t *testing.T) {
	lineage := NewClockLineage(nil)
	defer lineage.Release()
	var owned CausalView
	lineage.AppendOwned(&owned, 1, 1)
	segment := owned.segment
	refs := segment.refs.Load()
	prepared, needed := lineage.PrepareAppend(1, 2)
	if !needed {
		t.Fatal("monotone prepare was dominated")
	}
	if version, appended := prepared.CommitOwned(&owned); !appended || version != 2 ||
		owned.Get(1) != 2 || owned.segment != segment || segment.refs.Load() != refs {
		t.Fatal("same-segment CommitOwned changed refs or publication incorrectly")
	}

	if !lineage.Reserve(101) {
		t.Fatal("reserve failed")
	}
	clock := uint32(2)
	allocs := testing.AllocsPerRun(100, func() {
		clock++
		prepared, needed := lineage.PrepareAppend(1, clock)
		if !needed {
			t.Fatal("prepared owned append was dominated")
		}
		if _, appended := prepared.CommitOwned(&owned); !appended {
			t.Fatal("prepared owned append did not commit")
		}
	})
	if allocs != 0 {
		t.Fatalf("PrepareAppend+CommitOwned allocated: %v", allocs)
	}

	if !lineage.Reserve(lineage.current.rotateAt - uint64(lineage.current.entries)) {
		t.Fatal("reserve to boundary failed")
	}
	for uint64(lineage.current.entries) < lineage.current.rotateAt {
		clock++
		if _, appended := lineage.Append(1, clock); !appended {
			t.Fatal("failed to fill segment")
		}
	}
	oldSegment := owned.segment
	prepared, needed = lineage.PrepareAppend(1, clock+1)
	if !needed || lineage.current == oldSegment || oldSegment.refs.Load() != 1 {
		t.Fatal("prepare did not rotate while preserving old owner")
	}
	newSegment := lineage.current
	if _, appended := prepared.CommitOwned(&owned); !appended || owned.segment != newSegment ||
		oldSegment.refs.Load() != 0 || newSegment.refs.Load() != 2 {
		t.Fatal("rotating CommitOwned did not transfer references exactly")
	}
	owned.Release()
}

func BenchmarkClockLineage(b *testing.B) {
	b.Run("Append", func(b *testing.B) {
		lineage := NewClockLineage(nil)
		defer lineage.Release()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			lineage.Append(1, uint32(i+1))
		}
	})
	b.Run("Owners1024", func(b *testing.B) {
		lineage := NewClockLineage(nil)
		defer lineage.Release()
		lineage.Reserve(lineage.current.rotateAt)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tid := uint32(i & 1023)
			clock := uint32(i/1024 + 1)
			lineage.Append(tid, clock)
		}
	})
	b.Run("Owners10000", func(b *testing.B) {
		lineage := NewClockLineage(nil)
		defer lineage.Release()
		lineage.Reserve(lineage.current.rotateAt)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tid := uint32(i % 10000)
			clock := uint32(i/10000 + 1)
			lineage.Append(tid, clock)
		}
		b.ReportMetric(float64(lineageMaxRotation)/10000, "updates/owner/segment")
	})
	b.Run("CurrentPinnedGet", func(b *testing.B) {
		lineage := NewClockLineage(nil)
		defer lineage.Release()
		for i := uint32(1); i <= 3000; i++ {
			lineage.Append(1, i)
		}
		current := lineage.Pin()
		defer current.Release()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = current.Get(1)
		}
	})
	b.Run("HistoricalPinnedGet", func(b *testing.B) {
		lineage := NewClockLineage(nil)
		defer lineage.Release()
		var historical CausalView
		for i := uint32(1); i <= 3000; i++ {
			view, _ := lineage.AppendPinned(1, i)
			if i == 500 {
				historical = view
			} else {
				view.Release()
			}
		}
		defer historical.Release()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = historical.Get(1)
		}
	})
}
