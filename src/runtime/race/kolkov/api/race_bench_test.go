package api

import (
	"sync"
	"testing"
)

// BenchmarkRaceRead measures raceread performance.
//
// Target: <30ns per operation (includes OnRead ~21ns + overhead).
func BenchmarkRaceRead(b *testing.B) {
	Reset()
	Enable()

	// Pre-allocate context for current goroutine (amortize allocation cost).
	getCurrentContext()

	addr := uintptr(0x1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		raceread(addr)
	}
}

// BenchmarkRaceWrite measures racewrite performance.
//
// Target: <25ns per operation (includes OnWrite ~17ns + overhead).
func BenchmarkRaceWrite(b *testing.B) {
	Reset()
	Enable()

	// Pre-allocate context.
	getCurrentContext()

	addr := uintptr(0x2000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		racewrite(addr)
	}
}

// BenchmarkRaceReadWrite_Interleaved measures alternating read/write performance.
func BenchmarkRaceReadWrite_Interleaved(b *testing.B) {
	Reset()
	Enable()

	getCurrentContext()

	addr := uintptr(0x3000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			racewrite(addr)
		} else {
			raceread(addr)
		}
	}
}

// BenchmarkGetCurrentContext_FirstCall measures context allocation cost.
//
// Target: <100ns (includes GID extraction ~500ns amortized over goroutine lifetime).
func BenchmarkGetCurrentContext_FirstCall(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Reset to force allocation.
		Reset()

		b.StartTimer()
		_ = getCurrentContext()
		b.StopTimer()
	}
}

// BenchmarkGetCurrentContext_Cached measures cached context lookup.
//
// Target: <5ns per operation.
func BenchmarkGetCurrentContext_Cached(b *testing.B) {
	Reset()
	Enable()

	// Pre-allocate context.
	getCurrentContext()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = getCurrentContext()
	}
}

// BenchmarkGetGoroutineID measures goroutine ID extraction performance.
//
// MVP Target: ~500ns (runtime.Stack parsing).
// Phase 2 Target: ~1ns (assembly getg() stub).
func BenchmarkGetGoroutineID(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = getGoroutineID()
	}
}

// BenchmarkParseGID measures GID parsing performance.
func BenchmarkParseGID(b *testing.B) {
	input := []byte("goroutine 12345 [running]:\n")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = parseGID(input)
	}
}

// BenchmarkGetCallerPC measures PC extraction performance.
func BenchmarkGetCallerPC(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = getcallerpc()
	}
}

// BenchmarkEnableDisable measures enable/disable toggle performance.
func BenchmarkEnableDisable(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			Enable()
		} else {
			Disable()
		}
	}
}

// BenchmarkRacesDetected measures race counter access performance.
func BenchmarkRacesDetected(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = RacesDetected()
	}
}

// BenchmarkRaceRead_Disabled measures overhead when detector is disabled.
//
// Target: <1ns (should be just atomic load + return).
func BenchmarkRaceRead_Disabled(b *testing.B) {
	Reset()
	Disable()

	addr := uintptr(0x4000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		raceread(addr)
	}
}

// BenchmarkRaceWrite_Disabled measures overhead when detector is disabled.
func BenchmarkRaceWrite_Disabled(b *testing.B) {
	Reset()
	Disable()

	addr := uintptr(0x5000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		racewrite(addr)
	}
}

// BenchmarkRaceRead_MultipleAddresses measures read performance across different addresses.
func BenchmarkRaceRead_MultipleAddresses(b *testing.B) {
	Reset()
	Enable()

	getCurrentContext()

	// Access pattern: 100 different addresses.
	const numAddresses = 100
	addresses := make([]uintptr, numAddresses)
	for i := 0; i < numAddresses; i++ {
		addresses[i] = uintptr(0x10000 + i*8)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		addr := addresses[i%numAddresses]
		raceread(addr)
	}
}

// BenchmarkRaceWrite_MultipleAddresses measures write performance across different addresses.
func BenchmarkRaceWrite_MultipleAddresses(b *testing.B) {
	Reset()
	Enable()

	getCurrentContext()

	const numAddresses = 100
	addresses := make([]uintptr, numAddresses)
	for i := 0; i < numAddresses; i++ {
		addresses[i] = uintptr(0x20000 + i*8)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		addr := addresses[i%numAddresses]
		racewrite(addr)
	}
}

// BenchmarkConcurrentRaceRead measures read performance with concurrent goroutines.
func BenchmarkConcurrentRaceRead(b *testing.B) {
	Reset()
	Enable()

	addr := uintptr(0x6000)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			raceread(addr)
		}
	})
}

// BenchmarkConcurrentRaceWrite measures write performance with concurrent goroutines.
func BenchmarkConcurrentRaceWrite(b *testing.B) {
	Reset()
	Enable()

	addr := uintptr(0x7000)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			racewrite(addr)
		}
	})
}

// BenchmarkContextAllocation_Concurrent measures concurrent context allocation.
//
// Note: We cannot call Reset() concurrently, so this benchmark measures
// context lookups from multiple goroutines with pre-allocated contexts.
func BenchmarkContextAllocation_Concurrent(b *testing.B) {
	Reset()

	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Each goroutine allocates its context once, then caches.
			_ = getCurrentContext()
		}
	})
}

// BenchmarkRaceRead_SameEpoch measures same-epoch fast path performance.
//
// This is the critical optimization - when reading same address in same epoch.
// Should be fastest path through OnRead.
func BenchmarkRaceRead_SameEpoch(b *testing.B) {
	Reset()
	Enable()

	getCurrentContext()

	addr := uintptr(0x8000)

	// Do first read to establish epoch in shadow memory.
	raceread(addr)

	b.ResetTimer()
	b.ReportAllocs()

	// All subsequent reads should hit same-epoch fast path.
	for i := 0; i < b.N; i++ {
		raceread(addr)
	}
}

// BenchmarkRaceWrite_SameEpoch measures same-epoch fast path for writes.
func BenchmarkRaceWrite_SameEpoch(b *testing.B) {
	Reset()
	Enable()

	getCurrentContext()

	addr := uintptr(0x9000)

	// Do first write to establish epoch.
	racewrite(addr)

	b.ResetTimer()
	b.ReportAllocs()

	// All subsequent writes should hit same-epoch fast path.
	for i := 0; i < b.N; i++ {
		racewrite(addr)
	}
}

// BenchmarkGetCurrentContext_MultipleGoroutines measures context lookup across goroutines.
func BenchmarkGetCurrentContext_MultipleGoroutines(b *testing.B) {
	Reset()

	// Pre-allocate contexts for 100 goroutines.
	const numGoroutines = 100
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			getCurrentContext()
		}()
	}
	wg.Wait()

	b.ResetTimer()
	b.ReportAllocs()

	// Now measure cached lookups across multiple goroutines.
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = getCurrentContext()
		}
	})
}

// BenchmarkReset measures Reset performance.
func BenchmarkReset(b *testing.B) {
	// Pre-create some state.
	for i := 0; i < 100; i++ {
		racewrite(uintptr(0x10000 + i*8))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		Reset()
	}
}

// BenchmarkRaceRead_DifferentAddresses measures read performance with different addresses each time.
//
// This stresses shadow memory allocation and avoids same-epoch optimization.
func BenchmarkRaceRead_DifferentAddresses(b *testing.B) {
	Reset()
	Enable()

	getCurrentContext()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Each iteration uses different address.
		addr := uintptr(0x100000 + i*8)
		raceread(addr)
	}
}

// BenchmarkRaceWrite_DifferentAddresses measures write performance with different addresses.
func BenchmarkRaceWrite_DifferentAddresses(b *testing.B) {
	Reset()
	Enable()

	getCurrentContext()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		addr := uintptr(0x200000 + i*8)
		racewrite(addr)
	}
}

// BenchmarkGetGoroutineID_Concurrent measures GID extraction under contention.
func BenchmarkGetGoroutineID_Concurrent(b *testing.B) {
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = getGoroutineID()
		}
	})
}

// BenchmarkRaceReadWrite_TypicalWorkload simulates typical application workload.
//
// Mix of:
//   - 70% reads, 30% writes (typical ratio)
//   - 80% same-epoch hits (hot variables)
//   - 20% different addresses (new allocations)
func BenchmarkRaceReadWrite_TypicalWorkload(b *testing.B) {
	Reset()
	Enable()

	getCurrentContext()

	// Hot addresses (80% of accesses).
	hotAddrs := []uintptr{0x1000, 0x1008, 0x1010, 0x1018, 0x1020}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var addr uintptr

		// 80% hot addresses, 20% cold (new).
		if i%5 == 0 {
			addr = uintptr(0x100000 + i*8) // Cold
		} else {
			addr = hotAddrs[i%len(hotAddrs)] // Hot
		}

		// 70% reads, 30% writes.
		if i%10 < 7 {
			raceread(addr)
		} else {
			racewrite(addr)
		}
	}
}

// BenchmarkFullStack_RaceReadWithPC measures complete overhead including PC extraction.
func BenchmarkFullStack_RaceReadWithPC(b *testing.B) {
	Reset()
	Enable()

	getCurrentContext()

	addr := uintptr(0xA000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Inline what raceread does to measure full cost.
		if enabled.Load() {
			ctx := getCurrentContext()
			_ = getcallerpc()
			det.OnRead(addr, ctx)
		}
	}
}

// BenchmarkFullStack_RaceWriteWithPC measures complete overhead including PC extraction.
func BenchmarkFullStack_RaceWriteWithPC(b *testing.B) {
	Reset()
	Enable()

	getCurrentContext()

	addr := uintptr(0xB000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if enabled.Load() {
			ctx := getCurrentContext()
			_ = getcallerpc()
			det.OnWrite(addr, ctx)
		}
	}
}
