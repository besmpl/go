// Copyright 2025 The racedetector Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api

import (
	"runtime"
	"testing"
)

// BenchmarkGetGoroutineID_Fast benchmarks the optimized fast path.
//
// Target: <1ns per operation on amd64 (assembly).
// Expected: ~4.7µs per operation on other architectures (fallback).
func BenchmarkGetGoroutineID_Fast(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = getGoroutineIDFast()
	}
}

// BenchmarkGetGoroutineID_Slow benchmarks the slow path (runtime.Stack parsing).
//
// Expected: ~4.7µs per operation (baseline before optimization).
func BenchmarkGetGoroutineID_Slow(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = getGoroutineIDSlow()
	}
}

// BenchmarkGetGoroutineID benchmarks the current implementation.
//
// After Phase 2 optimization, this should use the fast path.
func BenchmarkGetGoroutineID_Current(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = getGoroutineID()
	}
}

// BenchmarkGetGoroutineID_Comparison runs fast and slow side-by-side.
//
// This clearly shows the speedup from assembly optimization.
func BenchmarkGetGoroutineID_Comparison(b *testing.B) {
	b.Run("Fast", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = getGoroutineIDFast()
		}
	})

	b.Run("Slow", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = getGoroutineIDSlow()
		}
	})
}

// BenchmarkGetGoroutineID_Parallel benchmarks concurrent GID extraction.
//
// This tests scalability under parallel load.
func BenchmarkGetGoroutineID_Parallel(b *testing.B) {
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = getGoroutineIDFast()
		}
	})
}

// BenchmarkGetGoroutineID_FastVsSlow_Concurrent compares performance under load.
func BenchmarkGetGoroutineID_FastVsSlow_Concurrent(b *testing.B) {
	b.Run("Fast", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = getGoroutineIDFast()
			}
		})
	})

	b.Run("Slow", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = getGoroutineIDSlow()
			}
		})
	})
}

// BenchmarkGetCurrentContext_WithFastGID benchmarks context lookup with fast GID.
//
// This shows the end-to-end impact on getCurrentContext() performance.
func BenchmarkGetCurrentContext_WithFastGID(b *testing.B) {
	Reset()
	Enable()

	// Pre-allocate context to measure cached lookup only.
	getCurrentContext()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = getCurrentContext()
	}
}

// BenchmarkGetCurrentContext_FirstCall_WithFastGID measures initial allocation cost.
//
// Phase 2 Target: <100ns (was ~3.5µs with slow GID in MVP).
func BenchmarkGetCurrentContext_FirstCall_WithFastGID(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Reset to force fresh allocation.
		Reset()

		b.StartTimer()
		_ = getCurrentContext()
		b.StopTimer()
	}
}

// BenchmarkRaceRead_WithFastGID measures raceread with optimized GID extraction.
//
// This shows the impact on the critical hot path.
func BenchmarkRaceRead_WithFastGID(b *testing.B) {
	Reset()
	Enable()

	// Pre-allocate context.
	getCurrentContext()

	addr := uintptr(0x1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		raceread(addr)
	}
}

// BenchmarkRaceWrite_WithFastGID measures racewrite with optimized GID extraction.
func BenchmarkRaceWrite_WithFastGID(b *testing.B) {
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

// BenchmarkParseGID_Optimized benchmarks the string parsing logic.
//
// This isolates the parsing overhead in the slow path.
func BenchmarkParseGID_Optimized(b *testing.B) {
	input := []byte("goroutine 12345 [running]:\n")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = parseGID(input)
	}
}

// BenchmarkParseGID_LargeID benchmarks parsing with large goroutine IDs.
func BenchmarkParseGID_LargeID(b *testing.B) {
	input := []byte("goroutine 999999999 [running]:\n")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = parseGID(input)
	}
}

// BenchmarkGetGoroutineID_CacheMisses measures performance on first context allocation.
//
// This simulates the worst case: many goroutines created, each needing GID extraction.
func BenchmarkGetGoroutineID_CacheMisses(b *testing.B) {
	b.ReportAllocs()

	// Pre-create goroutines to simulate realistic load.
	const numGoroutines = 100

	for i := 0; i < numGoroutines; i++ {
		go func() {
			// Each goroutine just extracts its GID once.
			_ = getGoroutineIDFast()
		}()
	}

	runtime.Gosched() // Let goroutines run.

	b.ResetTimer()

	// Now benchmark GID extraction in the benchmark goroutine.
	for i := 0; i < b.N; i++ {
		_ = getGoroutineIDFast()
	}
}

// BenchmarkGetGoroutineID_Assembly benchmarks just the assembly stub (amd64 only).
//
// This isolates the raw TLS access performance.
// NOTE: Disabled for v0.1.0 - assembly implementation is disabled for stability.
func BenchmarkGetGoroutineID_Assembly(b *testing.B) {
	b.Skip("Assembly implementation disabled - will be re-enabled in v0.4.0")

	// Kept for future when assembly is re-enabled:
	// if runtime.GOARCH != "amd64" {
	// 	b.Skip("Assembly benchmark only relevant on amd64")
	// }
	// b.ReportAllocs()
	// for i := 0; i < b.N; i++ {
	// 	_ = getg()
	// }
}

// BenchmarkGetGoroutineID_FieldAccess benchmarks the goid field dereference (amd64 only).
// NOTE: Disabled for v0.1.0 - assembly implementation is disabled for stability.
func BenchmarkGetGoroutineID_FieldAccess(b *testing.B) {
	b.Skip("Assembly implementation disabled - will be re-enabled in v0.4.0")

	// Kept for future when assembly is re-enabled:
	// if runtime.GOARCH != "amd64" {
	// 	b.Skip("Field access benchmark only relevant on amd64")
	// }
	// b.ReportAllocs()
	// g := getg()
	// if g == nil {
	// 	b.Fatal("getg() returned nil")
	// }
	// b.ResetTimer()
	// for i := 0; i < b.N; i++ {
	// 	// Benchmark just the field access (no TLS lookup).
	// 	goidPtr := (*int64)(unsafe.Pointer(uintptr(g) + goidOffset))
	// 	_ = *goidPtr
	// }
}

// BenchmarkGetGoroutineID_MultipleGoroutines benchmarks across many goroutines.
func BenchmarkGetGoroutineID_MultipleGoroutines(b *testing.B) {
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = getGoroutineIDFast()
		}
	})
}

// BenchmarkGetGoroutineID_UnderLoad simulates realistic workload.
//
// Mix of:
//   - 90% cached context lookups (hot path)
//   - 10% new goroutines needing GID extraction
func BenchmarkGetGoroutineID_UnderLoad(b *testing.B) {
	Reset()
	Enable()

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%10 == 0 {
			// 10% of time: simulate new goroutine (uncached).
			Reset()
		}

		// Get context (may allocate on cache miss).
		_ = getCurrentContext()
	}
}

// BenchmarkGetGoroutineID_WorstCase measures worst-case performance.
//
// This is the scenario where getGoroutineIDSlow() would hurt most:
// Many goroutines, each created and immediately needing GID.
func BenchmarkGetGoroutineID_WorstCase(b *testing.B) {
	Reset()

	b.ReportAllocs()
	b.ResetTimer()

	// Spawn many goroutines, each extracting GID once.
	for i := 0; i < b.N; i++ {
		done := make(chan bool)
		go func() {
			_ = getGoroutineIDFast()
			done <- true
		}()
		<-done
	}
}

// BenchmarkGetGoroutineID_BestCase measures best-case performance.
//
// Best case: same goroutine, repeated GID extraction (fully cached).
func BenchmarkGetGoroutineID_BestCase(b *testing.B) {
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
