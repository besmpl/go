//go:build amd64 || arm64

package shadowmem

import (
	"runtime"
	"testing"
	"time"

	"runtime/race/kolkov/epoch"
)

const optimisticClearBase = uintptr(1)<<40 + 0x90000

func optimisticClearTable(t testing.TB) (*PageTableShadow, blockView) {
	t.Helper()
	pt := NewPageTableShadow()
	seed := optimisticClearBase + 17
	current := epoch.NewEpoch(81, 3)
	if !pt.TryCompactWrite(seed, current, compactClearClock(current), 0x8101) {
		t.Fatal("compact seed failed")
	}
	view, ok := pt.blockFor(seed, false)
	if !ok || view.history.compact.Load() == nil {
		t.Fatal("compact seed did not publish its block")
	}
	return pt, view
}

func TestPageTableOptimisticClearBypassesHeldBlockLock(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldProcs)

	pt, view := optimisticClearTable(t)
	target := optimisticClearBase + 256
	view.history.mu.lock()
	done := make(chan struct{})
	go func() {
		pt.ClearRange(target, 16)
		close(done)
	}()
	select {
	case <-done:
		view.history.mu.unlock()
	case <-time.After(time.Second):
		view.history.mu.unlock()
		<-done
		t.Fatal("unrepresented clear waited for the held block lock")
	}
	if got := pt.Get(target); got != nil {
		t.Fatalf("optimistic clear published history %p", got)
	}
}

func TestClearRangeUnrepresentedFallbacks(t *testing.T) {
	t.Run("compact membership", func(t *testing.T) {
		pt, view := optimisticClearTable(t)
		if clearRangeUnrepresented(view, optimisticClearBase+17, 1) {
			t.Fatal("represented compact lane bypassed the locked clear")
		}
		_ = pt
	})

	t.Run("block default", func(t *testing.T) {
		pt, view := optimisticClearTable(t)
		pt.AccessRange(optimisticClearBase, rangeBlockSize, func(_ uintptr, _ uint8, _ *VarState) {})
		if clearRangeUnrepresented(view, optimisticClearBase+256, 1) {
			t.Fatal("default-backed lane bypassed the locked clear")
		}
	})

	t.Run("tombstone", func(t *testing.T) {
		_, view := optimisticClearTable(t)
		compact := view.history.compact.Load()
		view.history.mu.lock()
		compact.provisionTombstones()
		compact.publishTombstoneRange(optimisticClearBase+256, 1)
		view.history.mu.unlock()
		if clearRangeUnrepresented(view, optimisticClearBase+256, 1) {
			t.Fatal("tombstoned lane bypassed the locked clear")
		}
	})

	t.Run("dense palette owner", func(t *testing.T) {
		_, view := optimisticClearTable(t)
		compact := view.history.compact.Load()
		palette := new(compactPalette)
		target := optimisticClearBase + 256
		view.history.mu.lock()
		compact.beginMutation()
		palette.setOwner(target, compactPaletteTombstone)
		compact.palette.Store(palette)
		compact.endMutation()
		view.history.mu.unlock()
		if clearRangeUnrepresented(view, target, 1) {
			t.Fatal("dense palette owner bypassed the locked clear")
		}
	})

	t.Run("materialized", func(t *testing.T) {
		pt, view := optimisticClearTable(t)
		target := optimisticClearBase + 256
		pt.GetOrCreate(target)
		if clearRangeUnrepresented(view, target, 1) {
			t.Fatal("materialized history bypassed the locked clear")
		}
	})

	t.Run("nil materialized lane", func(t *testing.T) {
		pt, view := optimisticClearTable(t)
		word := optimisticClearBase + 256
		pt.GetOrCreate(word + 1)
		if !clearRangeUnrepresented(view, word, 1) {
			t.Fatal("exact-zero lane in a materialized word did not use the no-op clear")
		}
	})
}

func TestPageTableOptimisticClearConcurrentPublication(t *testing.T) {
	pt, _ := optimisticClearTable(t)
	for i := uintptr(0); i < 256; i++ {
		target := optimisticClearBase + 512 + i
		start := make(chan struct{})
		done := make(chan struct{}, 2)
		go func() {
			<-start
			current := epoch.NewEpoch(82, uint64(i+1))
			pt.TryCompactWrite(target, current, compactClearClock(current), 0x8201)
			done <- struct{}{}
		}()
		go func() {
			<-start
			pt.ClearRange(target, 1)
			done <- struct{}{}
		}()
		close(start)
		<-done
		<-done

		// Regardless of which operation won the race, a subsequent clear must
		// observe and retire every completed publication.
		pt.ClearRange(target, 1)
		if got := pt.Get(target); got != nil {
			t.Fatalf("iteration %d retained concurrently published history %p", i, got)
		}
	}
}

func TestPageTableOptimisticClearAllocatesNothing(t *testing.T) {
	pt, _ := optimisticClearTable(t)
	target := optimisticClearBase + 256
	if allocs := testing.AllocsPerRun(1000, func() {
		pt.ClearRange(target, 16)
	}); allocs != 0 {
		t.Fatalf("optimistic unrepresented clear allocated %.2f objects/op", allocs)
	}
}

func BenchmarkPageTableOptimisticClearSameBlockParallel(b *testing.B) {
	pt, _ := optimisticClearTable(b)
	target := optimisticClearBase + 256
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pt.ClearRange(target, 16)
		}
	})
}
