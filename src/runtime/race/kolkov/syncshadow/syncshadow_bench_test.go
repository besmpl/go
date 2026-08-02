package syncshadow

import (
	"internal/runtime/atomic"
	"testing"

	"runtime/race/kolkov/vectorclock"
)

func BenchmarkSyncShadowCached(b *testing.B) {
	shadow := NewSyncShadow()
	addr := uintptr(0x1234)
	want := shadow.GetOrCreate(addr)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if got := shadow.GetOrCreate(addr); got != want {
			b.Fatal("cached lookup changed SyncVar identity")
		}
	}
}

func BenchmarkSyncShadowSameBucketHits(b *testing.B) {
	shadow := NewSyncShadow()
	addrs := collidingAddresses(b, 64)
	want := make([]*SyncVar, len(addrs))
	for i, addr := range addrs {
		want[i] = shadow.GetOrCreate(addr)
	}

	b.ReportAllocs()
	b.ResetTimer()
	var wrong atomic.Uint32
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			index := i & (len(addrs) - 1)
			if got := shadow.GetOrCreate(addrs[index]); got != want[index] {
				wrong.Store(1)
			}
			i++
		}
	})
	if wrong.Load() != 0 {
		b.Fatal("same-bucket lookup changed SyncVar identity")
	}
}

func BenchmarkSyncShadowSpinLock(b *testing.B) {
	var lock spinlock
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		lock.lock()
		lock.unlock()
	}
}

func BenchmarkSyncShadowSpinContention(b *testing.B) {
	var lock spinlock
	var completed uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lock.lock()
			completed++
			lock.unlock()
		}
	})
	if completed != uint64(b.N) {
		b.Fatalf("completed operations = %d, want %d", completed, b.N)
	}
}

// BenchmarkSyncShadowSameBucketContention measures the writer path when
// allocator lifecycle changes repeatedly target pages in one top-level shard.
// Unlike the hit benchmarks, first-use publication necessarily allocates a new
// immutable address owner after each clear.
func BenchmarkSyncShadowSameBucketContention(b *testing.B) {
	shadow := NewSyncShadow()
	addrs := collidingAddresses(b, 64)
	for _, addr := range addrs {
		shadow.GetOrCreate(addr)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			addr := addrs[i&(len(addrs)-1)]
			shadow.ClearRange(addr, 1)
			shadow.GetOrCreate(addr)
			i++
		}
	})
}

func BenchmarkSyncShadowGet(b *testing.B) {
	shadow := NewSyncShadow()
	addr := uintptr(0x1234)
	want := shadow.GetOrCreate(addr)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if got := shadow.Get(addr); got != want {
			b.Fatal("existing-owner lookup changed identity")
		}
	}
}

func BenchmarkSyncVarTryRelease(b *testing.B) {
	var sv SyncVar
	left := vectorclock.New()
	left.Set(1, 10)
	left.Set(2, 20)
	right := left.Clone()
	right.Set(1, 11)
	sv.SetReleaseClock(left)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src := left
		if i&1 != 0 {
			src = right
		}
		if !sv.TrySetReleaseClock(src) {
			b.Fatal("warmed release fell back")
		}
	}
}

func BenchmarkSyncVarTryAcquire(b *testing.B) {
	var sv SyncVar
	release := vectorclock.New()
	release.Set(1, 10)
	release.Set(2, 20)
	sv.SetReleaseClock(release)
	dst := release.Clone()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if joined, ok := sv.TryJoinReleaseClock(dst); !joined || !ok {
			b.Fatal("warmed acquire fell back")
		}
	}
}

func BenchmarkSyncVarTryReleaseMerge(b *testing.B) {
	var sv SyncVar
	release := vectorclock.New()
	release.Set(1, 10)
	release.Set(2, 20)
	merged := release.Clone()
	merged.Set(2, 30)
	sv.SetReleaseClock(release)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if !sv.TryMergeReleaseClock(merged) {
			sv.MergeReleaseClock(merged)
		}
	}
}

func BenchmarkSyncVarTryAcquireReleaseContention(b *testing.B) {
	var sv SyncVar
	left := vectorclock.New()
	left.Set(1, 10)
	left.Set(2, 20)
	right := left.Clone()
	right.Set(1, 11)
	sv.SetReleaseClock(left)
	var fallbacks atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		dst := left.Clone()
		for pb.Next() {
			if joined, ok := sv.TryJoinReleaseClock(dst); !ok {
				fallbacks.Add(1)
				sv.JoinReleaseClock(dst)
			} else if !joined {
				b.Error("fast acquire observed no release")
			}
			if !sv.TrySetReleaseClock(right) {
				fallbacks.Add(1)
				sv.SetReleaseClock(right)
			}
		}
	})
	b.ReportMetric(float64(fallbacks.Load())/float64(b.N), "fallback/op")
}
