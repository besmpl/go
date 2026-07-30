package syncshadow

import (
	"internal/runtime/atomic"
	"testing"
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
