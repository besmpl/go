// Copyright 2025 The racedetector Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api

import (
	"runtime"
	"testing"
)

// BenchmarkGetGoroutineID_Fast benchmarks the runtime bridge path.
//
// Uses getg().goid via linkname — expected ~0ns per operation.
func BenchmarkGetGoroutineID_Fast(b *testing.B) {
	if !goroutineIDUsesRuntimeBridge {
		b.Skip("runtime bridge is race-only")
	}
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = getGoroutineIDFast()
	}
}

// BenchmarkGetGoroutineID_Slow benchmarks the slow path (runtime.Stack parsing).
//
// Expected: ~1500ns per operation (baseline for comparison).
func BenchmarkGetGoroutineID_Slow(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = getGoroutineIDSlow()
	}
}

// BenchmarkGetGoroutineID benchmarks the current implementation.
func BenchmarkGetGoroutineID_Current(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = getGoroutineID()
	}
}

// BenchmarkGetGoroutineID_Comparison runs fast and slow side-by-side.
//
// Shows the speedup from runtime bridge vs runtime.Stack parsing.
func BenchmarkGetGoroutineID_Comparison(b *testing.B) {
	b.Run("Fast", func(b *testing.B) {
		if !goroutineIDUsesRuntimeBridge {
			b.Skip("runtime bridge is race-only")
		}
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
		if !goroutineIDUsesRuntimeBridge {
			b.Skip("runtime bridge is race-only")
		}
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
func BenchmarkGetCurrentContext_WithFastGID(b *testing.B) {
	if !goroutineIDUsesRuntimeBridge {
		b.Skip("runtime bridge is race-only")
	}
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
func BenchmarkGetCurrentContext_FirstCall_WithFastGID(b *testing.B) {
	if !goroutineIDUsesRuntimeBridge {
		b.Skip("runtime bridge is race-only")
	}
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		Reset()

		b.StartTimer()
		_ = getCurrentContext()
		b.StopTimer()
	}
}

// BenchmarkRaceRead_WithFastGID measures raceread with runtime bridge GID.
func BenchmarkRaceRead_WithFastGID(b *testing.B) {
	if !goroutineIDUsesRuntimeBridge {
		b.Skip("runtime bridge is race-only")
	}
	Reset()
	Enable()

	getCurrentContext()

	addr := uintptr(0x1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		raceread(addr, 0)
	}
}

// BenchmarkRaceWrite_WithFastGID measures racewrite with runtime bridge GID.
func BenchmarkRaceWrite_WithFastGID(b *testing.B) {
	if !goroutineIDUsesRuntimeBridge {
		b.Skip("runtime bridge is race-only")
	}
	Reset()
	Enable()

	getCurrentContext()

	addr := uintptr(0x2000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		racewrite(addr, 0)
	}
}

// BenchmarkParseGID_Optimized benchmarks the string parsing logic.
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

// BenchmarkGetGoroutineID_CacheMisses measures performance with many goroutines.
func BenchmarkGetGoroutineID_CacheMisses(b *testing.B) {
	b.ReportAllocs()

	const numGoroutines = 100

	for i := 0; i < numGoroutines; i++ {
		go func() {
			_ = getGoroutineIDFast()
		}()
	}

	runtime.Gosched()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = getGoroutineIDFast()
	}
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
func BenchmarkGetGoroutineID_UnderLoad(b *testing.B) {
	Reset()
	Enable()

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%10 == 0 {
			Reset()
		}
		_ = getCurrentContext()
	}
}

// BenchmarkGetGoroutineID_WorstCase measures worst-case performance.
func BenchmarkGetGoroutineID_WorstCase(b *testing.B) {
	Reset()

	b.ReportAllocs()
	b.ResetTimer()

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
func BenchmarkGetGoroutineID_BestCase(b *testing.B) {
	Reset()
	Enable()

	getCurrentContext()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = getCurrentContext()
	}
}
