package vectorclock

import (
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

var poolingClockSink atomic.Pointer[VectorClock]

func resetClockPoolForIntegrationTest(t *testing.T) {
	t.Helper()
	clearPool := func() {
		for i := range poolShards {
			shard := &poolShards[i]
			if !shard.lock.CompareAndSwap(0, 1) {
				t.Fatalf("pool shard %d remained locked", i)
			}
			for j := range shard.slots {
				shard.slots[j] = nil
			}
			shard.count = 0
			shard.lock.Store(0)
		}
		poolCursor.Store(0)
		poolingClockSink.Store(nil)
	}
	clearPool()
	t.Cleanup(clearPool)
}

func poolSizeForIntegrationTest(t *testing.T) int {
	t.Helper()
	total := 0
	for i := range poolShards {
		shard := &poolShards[i]
		if !shard.lock.CompareAndSwap(0, 1) {
			t.Fatalf("pool shard %d is locked", i)
		}
		if int(shard.count) > len(shard.slots) {
			t.Fatalf("pool shard %d count = %d, capacity = %d", i, shard.count, len(shard.slots))
		}
		total += int(shard.count)
		shard.lock.Store(0)
	}
	return total
}

func fillClockPoolForIntegrationTest(t *testing.T) {
	t.Helper()
	for i := range poolShards {
		shard := &poolShards[i]
		if !shard.lock.CompareAndSwap(0, 1) {
			t.Fatalf("pool shard %d is locked", i)
		}
		for j := range shard.slots {
			shard.slots[j] = &VectorClock{poolShard: uint8(i)}
		}
		shard.count = poolShardCapacity
		shard.lock.Store(0)
	}
	if got := poolSizeForIntegrationTest(t); got != poolCapacity {
		t.Fatalf("filled pool size = %d, want %d", got, poolCapacity)
	}
}

func TestVectorClockPooling_Integration(t *testing.T) {
	t.Run("fixed shard and aggregate bounds", func(t *testing.T) {
		resetClockPoolForIntegrationTest(t)
		if poolShardCount != 16 || poolShardCapacity != 16 || poolCapacity != 256 {
			t.Fatalf("pool layout = %d shards x %d slots = %d, want 16 x 16 = 256",
				poolShardCount, poolShardCapacity, poolCapacity)
		}

		fillClockPoolForIntegrationTest(t)
		seen := make(map[*VectorClock]struct{}, poolCapacity)
		for i := range poolShards {
			shard := &poolShards[i]
			if got := int(shard.count); got != poolShardCapacity {
				t.Fatalf("pool shard %d count = %d, want %d", i, got, poolShardCapacity)
			}
			for j, clock := range shard.slots {
				if clock == nil {
					t.Fatalf("pool shard %d slot %d is nil below count", i, j)
				}
				if _, duplicate := seen[clock]; duplicate {
					t.Fatalf("clock %p was published more than once", clock)
				}
				seen[clock] = struct{}{}
			}

			extra := &VectorClock{poolShard: uint8(i)}
			extra.Release()
			if got := int(shard.count); got != poolShardCapacity {
				t.Fatalf("full pool shard %d count changed to %d", i, got)
			}
		}
		if got := len(seen); got != poolCapacity {
			t.Fatalf("pool retained %d unique clocks, want %d", got, poolCapacity)
		}
	})

	t.Run("reuse is empty and pop clears slot", func(t *testing.T) {
		resetClockPoolForIntegrationTest(t)
		const selectedShard = 5
		poolCursor.Store(selectedShard)
		clock := NewFromPool()
		clock.Set(0, 11)
		clock.Set(DenseThreads+10_000, 12)
		clock.RetireRange(9, 10)
		clock.Release()

		poolCursor.Store(selectedShard)
		reused := NewFromPool()
		if reused != clock {
			t.Fatalf("pool returned %p, want released clock %p", reused, clock)
		}
		if reused.poolShard != selectedShard {
			t.Fatalf("checked-out shard tag = %d, want %d", reused.poolShard, selectedShard)
		}
		if reused.maxDense != 0 || reused.GetMaxTID() != 0 || reused.Get(0) != 0 ||
			len(reused.denseTail) != 0 || len(reused.sparseRuns) != 0 || len(reused.retired) != 0 {
			t.Fatalf("reused clock was not empty: maxDense=%d maxTID=%d tail/runs/retired=%d/%d/%d",
				reused.maxDense, reused.GetMaxTID(), len(reused.denseTail), len(reused.sparseRuns), len(reused.retired))
		}
		shard := &poolShards[selectedShard]
		if shard.count != 0 || shard.slots[0] != nil {
			t.Fatalf("pop left shard count/slot = %d/%p, want 0/nil", shard.count, shard.slots[0])
		}
		reused.Release()
	})

	t.Run("release trims metadata above 64 KiB", func(t *testing.T) {
		resetClockPoolForIntegrationTest(t)
		const (
			oversizedShard = 6
			limitShard     = 7
		)
		uint32Size := int(unsafe.Sizeof(uint32(0)))

		oversized := &VectorClock{poolShard: oversizedShard}
		oversized.Set(1, 9)
		oversized.denseTail = make([]uint32, 1, int(maxPooledMetadataBytes)/uint32Size+1)
		oversized.sparseRuns = make([]finiteRun, 1, 8)
		oversized.retired = make([]RetiredRange, 1, 8)
		oversized.Release()
		if oversized.maxDense != 0 || len(oversized.denseTail) != 0 ||
			cap(oversized.denseTail) != 0 || cap(oversized.sparseRuns) != 0 || cap(oversized.retired) != 0 {
			t.Fatalf("oversized release did not reset and trim: maxDense=%d capacities=%d/%d/%d",
				oversized.maxDense, cap(oversized.denseTail), cap(oversized.sparseRuns), cap(oversized.retired))
		}
		poolCursor.Store(oversizedShard)
		if got := NewFromPool(); got != oversized {
			t.Fatalf("trimmed clock was not reusable: got %p, want %p", got, oversized)
		}

		atLimit := &VectorClock{poolShard: limitShard}
		atLimit.denseTail = make([]uint32, 1, int(maxPooledMetadataBytes)/uint32Size)
		atLimit.Release()
		if got := uintptr(cap(atLimit.denseTail)) * unsafe.Sizeof(uint32(0)); got != maxPooledMetadataBytes {
			t.Fatalf("metadata at retention limit was discarded: retained %d bytes, want %d", got, maxPooledMetadataBytes)
		}
	})

	t.Run("get and put fail open on a locked shard", func(t *testing.T) {
		resetClockPoolForIntegrationTest(t)

		const getShard = 3
		stored := &VectorClock{poolShard: getShard}
		get := &poolShards[getShard]
		get.slots[0] = stored
		get.count = 1
		get.lock.Store(1)
		defer get.lock.Store(0)
		poolCursor.Store(getShard)
		fresh := NewFromPool()
		if fresh == stored || fresh.poolShard != getShard {
			t.Fatalf("contended get returned %p with tag %d; stored=%p want tag=%d", fresh, fresh.poolShard, stored, getShard)
		}
		if get.count != 1 || get.slots[0] != stored {
			t.Fatal("contended get modified its locked shard")
		}
		get.lock.Store(0)
		fresh.Release()

		const putShard = 8
		put := &poolShards[putShard]
		put.lock.Store(1)
		defer put.lock.Store(0)
		dropped := &VectorClock{poolShard: putShard}
		dropped.Set(2, 17)
		dropped.denseTail = make([]uint32, 1, int(maxPooledMetadataBytes/unsafe.Sizeof(uint32(0)))+1)
		dropped.Release()
		if dropped.Get(2) != 0 || dropped.maxDense != 0 || cap(dropped.denseTail) != 0 {
			t.Fatalf("contended put did not reset and trim dropped clock")
		}
		if put.count != 0 {
			t.Fatalf("contended put changed locked shard count to %d", put.count)
		}
		for i, clock := range put.slots {
			if clock != nil {
				t.Fatalf("contended put published clock in slot %d", i)
			}
		}
		put.lock.Store(0)
	})

	t.Run("concurrent checkouts are unique and bounded", func(t *testing.T) {
		resetClockPoolForIntegrationTest(t)
		fillClockPoolForIntegrationTest(t)

		const workers = poolCapacity * 2
		start := make(chan struct{})
		release := make(chan struct{})
		checkedOut := make(chan *VectorClock, workers)
		var wg sync.WaitGroup
		wg.Add(workers)
		for i := 0; i < workers; i++ {
			go func() {
				defer wg.Done()
				<-start
				clock := NewFromPool()
				checkedOut <- clock
				<-release
				clock.Release()
			}()
		}
		close(start)

		seen := make(map[*VectorClock]struct{}, workers)
		var duplicate *VectorClock
		for i := 0; i < workers; i++ {
			clock := <-checkedOut
			if _, exists := seen[clock]; exists {
				duplicate = clock
			}
			seen[clock] = struct{}{}
		}
		close(release)
		wg.Wait()
		if duplicate != nil {
			t.Fatalf("clock %p was checked out concurrently more than once", duplicate)
		}

		if total := poolSizeForIntegrationTest(t); total > poolCapacity {
			t.Fatalf("pool retained %d clocks after concurrent release, capacity %d", total, poolCapacity)
		}
		published := make(map[*VectorClock]struct{}, poolCapacity)
		for i := range poolShards {
			shard := &poolShards[i]
			if int(shard.count) > poolShardCapacity {
				t.Fatalf("pool shard %d retained %d clocks, capacity %d", i, shard.count, poolShardCapacity)
			}
			for j := 0; j < int(shard.count); j++ {
				clock := shard.slots[j]
				if _, duplicate := published[clock]; duplicate {
					t.Fatalf("clock %p was published into multiple pool slots", clock)
				}
				published[clock] = struct{}{}
			}
		}
	})

	t.Run("pool allocation reduction is bounded", func(t *testing.T) {
		resetClockPoolForIntegrationTest(t)
		for i := 0; i < poolShardCount; i++ {
			clock := NewFromPool()
			clock.Release()
		}

		const runs = 10_000
		pooledPerRun := testing.AllocsPerRun(runs, func() {
			clock := NewFromPool()
			clock.Set(0, 1)
			poolingClockSink.Store(clock)
			clock.Release()
		})
		directPerRun := testing.AllocsPerRun(runs, func() {
			clock := New()
			clock.Set(0, 1)
			poolingClockSink.Store(clock)
		})
		pooledAllocs := pooledPerRun * runs
		directAllocs := directPerRun * runs
		if pooledAllocs > 100 {
			t.Fatalf("pooled allocations = %.0f for %d runs, want <= 100", pooledAllocs, runs)
		}
		if directAllocs == 0 {
			t.Fatal("direct allocation baseline was optimized away")
		}
		reduction := (directAllocs - pooledAllocs) / directAllocs * 100
		if reduction < 90 {
			t.Fatalf("pooled allocation reduction = %.2f%%, want >= 90%% (direct %.0f, pooled %.0f)",
				reduction, directAllocs, pooledAllocs)
		}
		t.Logf("direct allocations: %.0f; pooled allocations: %.0f; reduction: %.2f%%",
			directAllocs, pooledAllocs, reduction)
	})
}

func BenchmarkVectorClockPooling_Integration(b *testing.B) {
	b.Run("Concurrent_NewFromPool", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			var last *VectorClock
			for pb.Next() {
				clock := NewFromPool()
				clock.Set(0, 1)
				clock.Increment(0)
				_ = clock.Get(0)
				last = clock
				clock.Release()
			}
			poolingClockSink.Store(last)
		})
		poolingClockSink.Store(nil)
	})

	b.Run("Concurrent_New", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			var last *VectorClock
			for pb.Next() {
				clock := New()
				clock.Set(0, 1)
				clock.Increment(0)
				_ = clock.Get(0)
				last = clock
			}
			poolingClockSink.Store(last)
		})
		poolingClockSink.Store(nil)
	})
}
