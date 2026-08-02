package detector

import (
	"strconv"
	"testing"

	"runtime/race/kolkov/goroutine"
)

func TestAtomicHistoryIndexTenThousandConcurrentWitnesses(t *testing.T) {
	a := newAtomicHistoryArena()
	history := atomicHistory{arena: a}
	const (
		first = uint32(300_000)
		count = uint32(10_000)
	)
	for i := uint32(0); i < count; i++ {
		ctx := goroutine.Alloc(first + i)
		recordAtomicAccess(&history, ctx, uintptr(0x6000+i), 1, false)
		ctx.C.Release()
	}

	if got := atomicHistoryCardinality(history); got != int(count) {
		t.Fatalf("concurrent frontier cardinality = %d, want %d", got, count)
	}
	for _, tid := range []uint32{first, first + 3, first + 4, first + 255, first + 256, first + count - 1} {
		entry, ok := history.user.find(tid)
		if !ok || entry.tid != tid || entry.access.clocks[0] != 1 {
			t.Fatalf("indexed witness %d = %+v, present=%v", tid, entry, ok)
		}
	}
	if got := a.Stats().History; got != uint64(count-atomicInlineFrontier) {
		t.Fatalf("overflow history nodes = %d, want %d", got, count-atomicInlineFrontier)
	}
}

func TestAtomicHistoryLazyStaleEntriesAreCheckedAtQuery(t *testing.T) {
	a := newAtomicHistoryArena()
	history := atomicHistory{arena: a}
	observer := goroutine.Alloc(410_000)
	defer observer.C.Release()

	const first = uint32(400_000)
	for i := uint32(0); i < 64; i++ {
		ctx := goroutine.Alloc(first + i)
		recordAtomicAccess(&history, ctx, uintptr(0x7000+i), 1, false)
		observer.C.Set(ctx.TID, 1)
		ctx.C.Release()
	}
	if got := atomicHistoryCardinality(history); got != 64 {
		t.Fatalf("lazy frontier cardinality = %d, want 64 stale witnesses", got)
	}
	if prev, pc, lane, found := firstConcurrentAtomic(history, observer, 1); found {
		t.Fatalf("HB-dominated lazy witness reported as concurrent: epoch=%v pc=%#x lane=%d", prev, pc, lane)
	}

	later := goroutine.Alloc(420_000)
	defer later.C.Release()
	recordAtomicAccess(&history, later, 0x7fff, 1, false)
	prev, pc, lane, found := firstConcurrentAtomic(history, observer, 1)
	prevTID, _ := prev.Decode()
	if !found || prevTID != later.TID || pc != 0x7fff || lane != 0 {
		t.Fatalf("later concurrent witness = (%v, %#x, %d, %v), want TID %d", prev, pc, lane, found, later.TID)
	}
}

func TestAtomicHistorySameTIDIsUniqueLatestAndAllocationFree(t *testing.T) {
	a := newAtomicHistoryArena()
	history := atomicHistory{arena: a}
	ctx := goroutine.Alloc(430_000)
	defer ctx.C.Release()

	recordAtomicAccess(&history, ctx, 0x8000, 1, false)
	ctx.IncrementClock()
	recordAtomicAccess(&history, ctx, 0x8001, 1, false)
	if got := atomicHistoryCardinality(history); got != 1 {
		t.Fatalf("same-TID frontier cardinality = %d, want 1", got)
	}
	entry, ok := history.user.find(ctx.TID)
	if !ok || entry.access.clocks[0] != uint32(ctx.GetEpoch()) || entry.access.pcs[0] != 0x8001 {
		t.Fatalf("same-TID latest witness = %+v, present=%v", entry, ok)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		recordAtomicAccess(&history, ctx, 0x8002, 1, false)
	}); allocs != 0 {
		t.Fatalf("steady same-TID update allocated %.2f objects", allocs)
	}
}

func TestAtomicHistoryIndexPartialLaneRemovalDeletesExactOverflow(t *testing.T) {
	a := newAtomicHistoryArena()
	history := atomicHistory{arena: a}
	for tid := uint32(1); tid <= atomicInlineFrontier; tid++ {
		history.user.insert(a, tid).access.clocks[2] = 1
	}
	const overflowTID = uint32(0x10203)
	overflow := history.user.insert(a, overflowTID)
	overflow.access.clocks[0] = 7
	overflow.access.clocks[7] = 9
	if history.user.index.find(overflowTID) == nil {
		t.Fatal("overflow witness was not indexed")
	}

	clearAtomicHistoryMask(&history, 1)
	entry, ok := history.user.find(overflowTID)
	if !ok || entry.access.clocks[0] != 0 || entry.access.clocks[7] != 9 {
		t.Fatalf("partial lane clear witness = %+v, present=%v", entry, ok)
	}
	clearAtomicHistoryMask(&history, 1<<7)
	if _, ok := history.user.find(overflowTID); ok {
		t.Fatal("empty overflow witness remained findable")
	}
	if history.user.index.root != nil {
		t.Fatal("last overflow deletion retained empty radix pages")
	}
	if got := a.Stats().History; got != 0 {
		t.Fatalf("live overflow history after deletion = %d, want 0", got)
	}
}

func TestAtomicHistoryIndexReuseClearAndArenaResetAreABAFree(t *testing.T) {
	a := newAtomicHistoryArena()
	var old atomicHistoryClass
	for tid := uint32(1); tid <= atomicInlineFrontier; tid++ {
		old.insert(a, tid).access.clocks[0] = 1
	}
	const oldTID = uint32(0x10005)
	old.insert(a, oldTID).access.clocks[0] = 1
	oldNode := old.index.find(oldTID)
	old.clear(a)
	if old.index.root != nil || old.overflow != nil {
		t.Fatal("cleared history retained overflow index state")
	}

	var rebound atomicHistoryClass
	for tid := uint32(11); tid <= 10+atomicInlineFrontier; tid++ {
		rebound.insert(a, tid).access.clocks[0] = 1
	}
	const reboundTID = uint32(0x20005)
	rebound.insert(a, reboundTID).access.clocks[0] = 1
	if rebound.index.find(reboundTID) != oldNode {
		t.Fatal("test did not exercise overflow-node address reuse")
	}
	if _, ok := old.find(reboundTID); ok {
		t.Fatal("cleared history found a witness through a recycled node")
	}

	state := a.newState(0x9000)
	for tid := uint32(21); tid <= 20+atomicInlineFrontier; tid++ {
		state.writes.user.insert(a, tid).access.clocks[0] = 1
	}
	const resetOldTID = uint32(0x30005)
	state.writes.user.insert(a, resetOldTID).access.clocks[0] = 1
	a.reset()
	fresh := a.newState(0xa000)
	if _, ok := fresh.writes.user.find(resetOldTID); ok || fresh.writes.user.index.root != nil {
		t.Fatal("fresh arena state inherited the reset generation's index")
	}
	for tid := uint32(31); tid <= 30+atomicInlineFrontier; tid++ {
		fresh.writes.user.insert(a, tid).access.clocks[0] = 1
	}
	const resetFreshTID = uint32(0x40005)
	fresh.writes.user.insert(a, resetFreshTID).access.clocks[0] = 1
	if _, ok := fresh.writes.user.find(resetOldTID); ok {
		t.Fatal("reused arena node revived a reset TID")
	}
	if _, ok := fresh.writes.user.find(resetFreshTID); !ok {
		t.Fatal("fresh arena generation lost its indexed TID")
	}
}

func TestAtomicHistoryUserPriorityIncludesIndexedWitness(t *testing.T) {
	a := newAtomicHistoryArena()
	history := atomicHistory{arena: a}
	query := goroutine.Alloc(500_000)
	defer query.C.Release()

	for i := uint32(0); i < atomicInlineFrontier; i++ {
		ctx := goroutine.Alloc(500_100 + i)
		recordAtomicAccess(&history, ctx, uintptr(0x9000+i), 1, false)
		query.C.Set(ctx.TID, 1)
		ctx.C.Release()
	}
	user := goroutine.Alloc(500_200)
	defer user.C.Release()
	recordAtomicAccess(&history, user, 0x9fff, 1, false)
	if history.user.index.find(user.TID) == nil {
		t.Fatal("priority witness did not use the overflow index")
	}
	internal := goroutine.Alloc(500_300)
	defer internal.C.Release()
	recordAtomicAccess(&history, internal, 0xa000, 1, true)

	prev, pc, _, found := firstConcurrentAtomic(history, query, 1)
	prevTID, _ := prev.Decode()
	if !found || prevTID != user.TID || pc != 0x9fff {
		t.Fatalf("selected witness = (%v, %#x, %v), want indexed user TID %d", prev, pc, found, user.TID)
	}
}

func BenchmarkAtomicHistoryUniqueInsertion(b *testing.B) {
	for _, count := range []int{1_000, 10_000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			contexts := make([]*goroutine.RaceContext, count)
			for i := range contexts {
				contexts[i] = goroutine.Alloc(600_000 + uint32(i))
			}
			defer func() {
				for _, ctx := range contexts {
					ctx.C.Release()
				}
			}()

			b.ReportAllocs()
			b.ReportMetric(float64(count), "witnesses/op")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				a := newAtomicHistoryArena()
				history := atomicHistory{arena: a}
				for _, ctx := range contexts {
					recordAtomicAccess(&history, ctx, 0xb000, 1, false)
				}
			}
		})
	}
}

func BenchmarkAtomicHistorySameTIDUpdate(b *testing.B) {
	a := newAtomicHistoryArena()
	history := atomicHistory{arena: a}
	ctx := goroutine.Alloc(700_000)
	defer ctx.C.Release()
	recordAtomicAccess(&history, ctx, 0xc000, 1, false)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		recordAtomicAccess(&history, ctx, 0xc001, 1, false)
	}
}
