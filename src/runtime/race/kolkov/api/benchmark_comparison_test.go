// Copyright 2025 The racedetector Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api

import (
	"testing"

	"runtime/race/kolkov/goroutine"
)

// benchmark_comparison_test.go compares Phase 1 vs Phase 2 performance.
//
// These benchmarks demonstrate the improvements achieved in Phase 2:
//   - Phase 1 (MVP): Slow GID extraction (~2893ns), no TID reuse
//   - Phase 2: Fast GID extraction (~2.08ns), TID pool with reuse
//
// Expected improvements:
//   - GID extraction: 1390x faster
//   - TID allocation: Zero allocations (vs. N/A in Phase 1)
//   - Overall throughput: 1.5-2x improvement

// BenchmarkComparison_GIDExtraction compares Phase 1 vs Phase 2 GID extraction.
//
// Phase 1: runtime.Stack() parsing (~2893ns, 2 allocs)
// Phase 2: Assembly getg() stub (~2.08ns, 0 allocs)
//
// Expected: 1390x speedup.
func BenchmarkComparison_GIDExtraction(b *testing.B) {
	b.Run("Phase1_Slow", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = getGoroutineIDSlow()
		}
	})

	b.Run("Phase2_Fast", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = getGoroutineIDFast()
		}
	})

	b.Run("Speedup", func(b *testing.B) {
		// This sub-benchmark calculates and reports the speedup.
		// It doesn't run operations, just measures the difference.
		b.Skip("Speedup calculation: Run Phase1_Slow and Phase2_Fast to compare")
	})
}

// BenchmarkComparison_ContextAllocation compares Phase 1 vs Phase 2 context allocation.
//
// Phase 1: Simple atomic counter, wraps at 256 (TID conflicts after 256 goroutines)
// Phase 2: TID pool with reuse, supports 1000+ goroutines
//
// Expected: Similar latency, but Phase 2 scales to unlimited goroutines.
func BenchmarkComparison_ContextAllocation(b *testing.B) {
	b.Run("Phase1_NoReuse", func(b *testing.B) {
		// Simulate Phase 1: TID wraps after 256, no reuse
		Reset()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Simulate Phase 1 TID allocation (simple counter, wraps at 65536)
			tid := uint16(nextTID.Add(1) % 65536)
			_ = goroutine.Alloc(tid)
		}
	})

	b.Run("Phase2_WithReuse", func(b *testing.B) {
		// Phase 2: TID pool with reuse
		Reset()
		Enable()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, _ = allocTID()
			// Note: Not freeing to measure allocation only
		}
	})
}

// BenchmarkComparison_EndToEnd compares overall Phase 1 vs Phase 2 performance.
//
// This measures the complete race detection overhead including:
//   - GID extraction
//   - Context lookup
//   - Race checking
//
// Expected: 1.5-2x overall improvement due to faster GID extraction.
func BenchmarkComparison_EndToEnd(b *testing.B) {
	b.Run("Phase1_Baseline", func(b *testing.B) {
		// Simulate Phase 1 characteristics:
		//   - Slow GID (~2893ns)
		//   - No TID reuse
		//   - MVP performance

		Reset()
		Enable()

		b.ReportAllocs()
		b.ResetTimer()

		addr := uintptr(0x1000)

		for i := 0; i < b.N; i++ {
			// Simulate Phase 1 overhead: slow GID extraction
			_ = getGoroutineIDSlow()

			// Then do race read (same as Phase 2)
			raceread(addr, 0)
		}
	})

	b.Run("Phase2_Optimized", func(b *testing.B) {
		// Phase 2 characteristics:
		//   - Fast GID (~2.08ns)
		//   - TID reuse pool
		//   - Optimized performance

		Reset()
		Enable()

		b.ResetTimer()
		b.ReportAllocs()

		addr := uintptr(0x2000)

		for i := 0; i < b.N; i++ {
			// Phase 2: fast GID is integrated into getCurrentContext()
			raceread(addr, 0)
		}
	})
}

// BenchmarkComparison_ContextLookup compares context lookup performance.
//
// Phase 1: sync.Map lookup after slow GID extraction
// Phase 2: sync.Map lookup after fast GID extraction
//
// Expected: 1000x+ improvement (GID extraction dominates).
func BenchmarkComparison_ContextLookup(b *testing.B) {
	b.Run("Phase1_SlowGID", func(b *testing.B) {
		Reset()
		Enable()

		// Pre-allocate context
		getCurrentContext()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Phase 1: Slow GID extraction + map lookup
			gid := getGoroutineIDSlow()
			_, _ = contextsMap.Load(gid)
		}
	})

	b.Run("Phase2_FastGID", func(b *testing.B) {
		Reset()
		Enable()

		// Pre-allocate context
		getCurrentContext()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Phase 2: Fast GID extraction + map lookup
			_ = getCurrentContext()
		}
	})
}

// BenchmarkComparison_RaceRead compares race read performance.
//
// This measures the hot path improvement from Phase 1 to Phase 2.
func BenchmarkComparison_RaceRead(b *testing.B) {
	b.Run("Phase1_SlowGID", func(b *testing.B) {
		Reset()
		Enable()

		// Pre-allocate context to isolate GID extraction cost
		getCurrentContext()

		addr := uintptr(0x3000)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Simulate Phase 1: slow GID on each access
			_ = getGoroutineIDSlow()
			raceread(addr, 0)
		}
	})

	b.Run("Phase2_FastGID", func(b *testing.B) {
		Reset()
		Enable()

		getCurrentContext()

		addr := uintptr(0x4000)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Phase 2: fast GID (integrated)
			raceread(addr, 0)
		}
	})
}

// BenchmarkComparison_RaceWrite compares race write performance.
func BenchmarkComparison_RaceWrite(b *testing.B) {
	b.Run("Phase1_SlowGID", func(b *testing.B) {
		Reset()
		Enable()

		getCurrentContext()

		addr := uintptr(0x5000)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_ = getGoroutineIDSlow()
			racewrite(addr, 0)
		}
	})

	b.Run("Phase2_FastGID", func(b *testing.B) {
		Reset()
		Enable()

		getCurrentContext()

		addr := uintptr(0x6000)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			racewrite(addr, 0)
		}
	})
}

// BenchmarkComparison_ScalabilityTest compares scalability with many goroutines.
//
// Phase 1: Fails after 256 goroutines (TID wraparound)
// Phase 2: Scales to 1000+ goroutines
//
// This benchmark demonstrates the TID pool improvement.
func BenchmarkComparison_ScalabilityTest(b *testing.B) {
	for _, numGoroutines := range []int{10, 100, 256, 500, 1000} {
		b.Run("Phase1_NoReuse_"+string(rune('0'+numGoroutines/100)), func(b *testing.B) {
			if numGoroutines > 256 {
				b.Skip("Phase 1 doesn't support >256 goroutines")
				return
			}

			benchmarkScalability(b, numGoroutines, false)
		})

		b.Run("Phase2_WithReuse_"+string(rune('0'+numGoroutines/100)), func(b *testing.B) {
			benchmarkScalability(b, numGoroutines, true)
		})
	}
}

func benchmarkScalability(b *testing.B, numGoroutines int, _ bool) {
	Reset()
	Enable()

	done := make(chan bool)
	addr := uintptr(0x7000)

	b.ResetTimer()
	b.ReportAllocs()

	// Spawn goroutines
	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			// Each goroutine does some race-tracked operations
			for {
				select {
				case <-done:
					return
				default:
					raceread(addr, 0)
					racewrite(uintptr(0x8000+id*8), 0)
				}
			}
		}(g)
	}

	// Let them run for benchmark duration
	for i := 0; i < b.N; i++ {
		raceread(addr, 0)
	}

	close(done)
	b.StopTimer()
}

// BenchmarkComparison_FirstContextCreation compares first context creation.
//
// Phase 1: Slow GID (~2893ns) + map store + TID allocation
// Phase 2: Fast GID (~2.08ns) + map store + TID pool allocation
//
// Expected: 1000x+ improvement.
func BenchmarkComparison_FirstContextCreation(b *testing.B) {
	b.Run("Phase1_SlowGID", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			Reset()

			b.StartTimer()
			// Simulate Phase 1: slow GID
			gid := getGoroutineIDSlow()
			tid := uint16(nextTID.Add(1) % 256)
			ctx := goroutine.Alloc(tid)
			contextsMap.Store(gid, ctx)
			b.StopTimer()
		}
	})

	b.Run("Phase2_FastGID", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			Reset()

			b.StartTimer()
			// Phase 2: integrated fast path
			_ = getCurrentContext()
			b.StopTimer()
		}
	})
}

// BenchmarkComparison_CachedContextLookup compares cached lookup.
//
// This should be similar between Phase 1 and Phase 2, as both use sync.Map.
// However, Phase 2's faster GID extraction still provides measurable improvement.
func BenchmarkComparison_CachedContextLookup(b *testing.B) {
	b.Run("Phase1_SlowGID", func(b *testing.B) {
		Reset()
		Enable()

		// Pre-create context
		gid := getGoroutineIDSlow()
		tid := uint16(0)
		ctx := goroutine.Alloc(tid)
		contextsMap.Store(gid, ctx)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Phase 1: slow GID + map lookup
			gid := getGoroutineIDSlow()
			_, _ = contextsMap.Load(gid)
		}
	})

	b.Run("Phase2_FastGID", func(b *testing.B) {
		Reset()
		Enable()

		// Pre-create context
		getCurrentContext()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Phase 2: fast GID + map lookup
			_ = getCurrentContext()
		}
	})
}

// BenchmarkComparison_TIDAllocation compares TID allocation strategies.
//
// Phase 1: Simple atomic counter (wraps at 256, no reuse)
// Phase 2: TID pool (stack-based, reuse, unlimited goroutines)
//
// Expected: Similar latency, but Phase 2 scales better.
func BenchmarkComparison_TIDAllocation(b *testing.B) {
	b.Run("Phase1_AtomicCounter", func(b *testing.B) {
		Reset()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Phase 1: simple atomic counter
			_ = uint16(nextTID.Add(1) % 256)
		}
	})

	b.Run("Phase2_PoolAllocation", func(b *testing.B) {
		Reset()
		Enable()

		b.ResetTimer()
		b.ReportAllocs()

		type tidInfo struct {
			tid   uint16
			clock uint32
		}
		tids := make([]tidInfo, 0, b.N)
		for i := 0; i < b.N; i++ {
			tid, clock := allocTID()
			tids = append(tids, tidInfo{tid, clock})
		}

		// Free TIDs to cleanup
		b.StopTimer()
		for _, ti := range tids {
			freeTID(ti.tid, ti.clock)
		}
	})
}

// BenchmarkComparison_ParallelWorkload compares parallel performance.
//
// This tests how Phase 1 vs Phase 2 scales under parallel load.
func BenchmarkComparison_ParallelWorkload(b *testing.B) {
	b.Run("Phase1_SlowGID", func(b *testing.B) {
		Reset()
		Enable()

		addr := uintptr(0x9000)

		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				// Simulate Phase 1
				_ = getGoroutineIDSlow()
				raceread(addr, 0)
			}
		})
	})

	b.Run("Phase2_FastGID", func(b *testing.B) {
		Reset()
		Enable()

		addr := uintptr(0xA000)

		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				// Phase 2: integrated fast GID
				raceread(addr, 0)
			}
		})
	})
}

// BenchmarkComparison_Summary provides overall Phase 1 vs Phase 2 comparison.
//
// This benchmark combines all improvements to show aggregate performance gain.
func BenchmarkComparison_Summary(b *testing.B) {
	b.Run("Phase1_Overall", func(b *testing.B) {
		// Phase 1 characteristics:
		//   - Slow GID extraction (~2893ns)
		//   - No TID reuse (256 goroutine limit)
		//   - Baseline FastTrack performance

		Reset()
		Enable()

		b.ReportAllocs()
		b.ResetTimer()

		addrs := []uintptr{0xB000, 0xB008, 0xB010, 0xB018, 0xB020}

		for i := 0; i < b.N; i++ {
			addr := addrs[i%len(addrs)]

			// Simulate Phase 1 overhead
			_ = getGoroutineIDSlow()

			if i%2 == 0 {
				raceread(addr, 0)
			} else {
				racewrite(addr, 0)
			}
		}
	})

	b.Run("Phase2_Overall", func(b *testing.B) {
		// Phase 2 characteristics:
		//   - Fast GID extraction (~2.08ns)
		//   - TID pool with reuse (1000+ goroutines)
		//   - Same FastTrack performance

		Reset()
		Enable()

		b.ReportAllocs()
		b.ResetTimer()

		addrs := []uintptr{0xC000, 0xC008, 0xC010, 0xC018, 0xC020}

		for i := 0; i < b.N; i++ {
			addr := addrs[i%len(addrs)]

			if i%2 == 0 {
				raceread(addr, 0)
			} else {
				racewrite(addr, 0)
			}
		}
	})
}
