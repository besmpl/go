package syncshadow

import (
	"runtime/race/kolkov/vectorclock"
	"sync"
	"testing"
)

func TestDeferredReleaseMergeUsesCompactInitialGeneration(t *testing.T) {
	var sv SyncVar
	for i := uint32(0); i < releaseMergeInitialCapacity; i++ {
		clock := vectorclock.New()
		clock.Set(100+i, 10+i)
		sv.PublishReleaseMergeForContext(clock, 100+i, 0)
		clock.Release()
	}
	generation := sv.pending.Load()
	if generation == nil {
		t.Fatal("four releases did not publish a compact generation")
	}
	if generation.initialClaimed.Load() != releaseMergeInitialCapacity {
		t.Fatalf("compact generation=%p claimed=%d, want %d inline releases", generation, generation.initialClaimed.Load(), releaseMergeInitialCapacity)
	}
	for lane := range generation.lanes {
		if batch := generation.lanes[lane].Load(); batch != nil {
			t.Fatalf("four-release generation allocated full batch %p in lane %d", batch, lane)
		}
	}

	overflow := vectorclock.New()
	overflow.Set(200, 20)
	sv.PublishReleaseMergeForContext(overflow, 200, 0)
	overflow.Release()
	laneIndex := mergeLane(200)
	lane := generation.lanes[laneIndex].Load()
	if lane == nil {
		t.Fatal("fifth release did not allocate a compact lane")
	}
	if lane.claimed.Load() != 1 {
		t.Fatalf("fifth release lane=%p claimed=%d, want one exact overflow", lane, lane.claimed.Load())
	}
	if batch := lane.batches.Load(); batch != nil {
		t.Fatalf("fifth release allocated full batch %p", batch)
	}

	sameLane := []uint32{200}
	for tid := uint32(201); len(sameLane) <= releaseMergeLaneCapacity; tid++ {
		if mergeLane(tid) == laneIndex {
			sameLane = append(sameLane, tid)
		}
	}
	for _, tid := range sameLane[1:releaseMergeLaneCapacity] {
		clock := vectorclock.New()
		clock.Set(tid, tid+1)
		sv.PublishReleaseMergeForContext(clock, tid, 0)
		clock.Release()
	}
	if claimed := lane.claimed.Load(); claimed != releaseMergeLaneCapacity {
		t.Fatalf("compact lane claimed=%d, want %d", claimed, releaseMergeLaneCapacity)
	}
	if batch := lane.batches.Load(); batch != nil {
		t.Fatalf("full compact lane allocated batch %p before overflow", batch)
	}

	largeTID := sameLane[releaseMergeLaneCapacity]
	large := vectorclock.New()
	large.Set(largeTID, largeTID+1)
	sv.PublishReleaseMergeForContext(large, largeTID, 0)
	large.Release()
	batch := lane.batches.Load()
	if batch == nil || batch.claimed.Load() != 1 {
		t.Fatalf("large overflow batch=%p, want one committed slot", batch)
	}

	got := sv.GetReleaseClock()
	for i := uint32(0); i < releaseMergeInitialCapacity; i++ {
		if value := got.Get(100 + i); value != 10+i {
			t.Fatalf("inline tid %d=%d, want %d", 100+i, value, 10+i)
		}
	}
	if value := got.Get(200); value != 20 {
		t.Fatalf("overflow tid 200=%d, want 20", value)
	}
	for _, tid := range sameLane[1:releaseMergeLaneCapacity] {
		if value := got.Get(tid); value != tid+1 {
			t.Fatalf("compact lane tid %d=%d, want %d", tid, value, tid+1)
		}
	}
	if value := got.Get(largeTID); value != largeTID+1 {
		t.Fatalf("large batch tid %d=%d, want %d", largeTID, value, largeTID+1)
	}
}

func TestDeferredReleaseMergeConcurrentTenThousandUniqueTIDs(t *testing.T) {
	var sv SyncVar
	base := vectorclock.New()
	base.Set(1, 1)
	sv.SetReleaseClock(base)

	const (
		publications = 10_000
		workers      = 64
		firstTID     = uint32(1 << 20)
	)
	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer wg.Done()
			for i := worker; i < publications; i += workers {
				clock := vectorclock.New()
				tid := firstTID + uint32(i)*2
				clock.Set(tid, uint32(i+2))
				sv.PublishReleaseMergeForContext(clock, tid, 0)
				clock.Release()
			}
		}(worker)
	}
	wg.Wait()

	got := sv.GetReleaseClock()
	if got.Get(1) != 1 {
		t.Fatalf("base coordinate = %d, want 1", got.Get(1))
	}
	for i := 0; i < publications; i++ {
		tid := firstTID + uint32(i)*2
		if clock := got.Get(tid); clock != uint32(i+2) {
			t.Fatalf("tid %d = %d, want %d", tid, clock, i+2)
		}
	}
	if sv.pending.Load() != nil {
		t.Fatal("acquire did not retire the folded generation")
	}
}

func TestDeferredReleaseMergeTenThousandSameFamilyVersions(t *testing.T) {
	var sv SyncVar
	anchor := vectorclock.New()
	lineage := vectorclock.NewClockLineage(anchor)
	const (
		versions = 10_000
		tid      = uint32(1 << 20)
	)
	for i := uint32(1); i <= versions; i++ {
		view, ok := lineage.AppendPinned(tid, i)
		if !ok {
			t.Fatalf("lineage append %d failed", i)
		}
		clock := vectorclock.New()
		if !clock.TryJoinCausal(view) {
			t.Fatalf("lineage view %d did not fit empty clock", i)
		}
		sv.PublishReleaseMergeForContext(clock, tid, 0)
		clock.Release()
		view.Release()
	}
	got := sv.GetReleaseClock()
	if clock := got.Get(tid); clock != versions {
		t.Fatalf("same-family folded clock = %d, want %d", clock, versions)
	}
	lineage.Release()
	anchor.Release()
}

func TestDeferredReleaseMergeSetDiscardsEarlierGeneration(t *testing.T) {
	var sv SyncVar
	merged := vectorclock.New()
	merged.Set(11, 5)
	sv.PublishReleaseMergeForContext(merged, 11, 0)
	replacement := vectorclock.New()
	replacement.Set(22, 9)
	sv.SetReleaseClock(replacement)
	got := vectorclock.New()
	if !sv.JoinReleaseClock(got) {
		t.Fatal("replacement release disappeared")
	}
	if got.Get(11) != 0 || got.Get(22) != 9 {
		t.Fatalf("release after Set = {%d,%d}, want {0,9}", got.Get(11), got.Get(22))
	}
}

func TestDeferredReleaseMergeRetireDiscardsFinalGeneration(t *testing.T) {
	shadow := NewSyncShadow()
	const addr = uintptr(0x5eed00)
	stale := shadow.GetOrCreate(addr)
	clock := vectorclock.New()
	clock.Set(33, 17)
	stale.PublishReleaseMergeForContext(clock, 33, 0)
	shadow.ClearRange(addr, 1)
	if stale.retired.Load() == 0 {
		t.Fatal("owner was not retired")
	}
	if got := stale.GetReleaseClock(); got != nil {
		t.Fatal("retired owner exposed an old release")
	}
	if stale.pending.Load() != nil {
		t.Fatal("retire retained a pending generation")
	}
}

func TestDeferredReleaseMergeRetireDoesNotAllocate(t *testing.T) {
	const runs = 100
	states := make([]*SyncVar, runs+1) // AllocsPerRun performs one warmup call.
	for i := range states {
		states[i] = new(SyncVar)
		clock := vectorclock.New()
		clock.Set(uint32(i+1), uint32(i+2))
		states[i].PublishReleaseMergeForContext(clock, uint32(i+1), 0)
		clock.Release()
	}

	next := 0
	if allocs := testing.AllocsPerRun(runs, func() {
		states[next].retire()
		next++
	}); allocs != 0 {
		t.Fatalf("retire allocated %.2f times", allocs)
	}
	for i, state := range states {
		if state.pending.Load() != nil || state.GetReleaseClock() != nil {
			t.Fatalf("retired state %d retained synchronization state", i)
		}
	}
}

func BenchmarkSyncVarDeferredReleaseMergeParallel(b *testing.B) {
	var sv SyncVar
	warm := vectorclock.New()
	warm.Set(1, 1)
	sv.PublishReleaseMergeForContext(warm, 1, 0)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		clock := vectorclock.New()
		clock.Set(2, 2)
		for pb.Next() {
			if !sv.TryPublishReleaseMergeForContext(clock, 2, 0) {
				sv.PublishReleaseMergeForContext(clock, 2, 0)
			}
		}
	})
}

func BenchmarkSyncVarDeferredReleaseMergeSameFamilyFold(b *testing.B) {
	for range b.N {
		var sv SyncVar
		anchor := vectorclock.New()
		lineage := vectorclock.NewClockLineage(anchor)
		for i := uint32(1); i <= 10_000; i++ {
			view, _ := lineage.AppendPinned(101, i)
			clock := vectorclock.New()
			clock.TryJoinCausal(view)
			sv.PublishReleaseMergeForContext(clock, 101, 0)
			clock.Release()
			view.Release()
		}
		_ = sv.GetReleaseClock()
		lineage.Release()
		anchor.Release()
	}
}
