// Copyright 2025 The racedetector Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api

import (
	"sync"
	"sync/atomic"
	"testing"
)

// benchmark_overhead_test.go measures race detector overhead.
//
// These benchmarks quantify the performance cost of race detection by comparing
// identical workloads WITH and WITHOUT the detector enabled. This provides
// concrete overhead percentages for different operation types.
//
// Overhead calculation: (WithDetector - WithoutDetector) / WithoutDetector * 100%
//
// Target overhead: <5x for typical workloads, <10x for worst case.

// BenchmarkOverhead_Read measures overhead of raceread().
//
// This isolates the cost of read instrumentation vs. baseline read.
func BenchmarkOverhead_Read(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		Reset()
		Enable()

		// Pre-allocate context
		getCurrentContext()

		addr := uintptr(0x1000)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			raceread(addr)
		}
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		Disable()

		addr := uintptr(0x1000)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			raceread(addr) // No-op when disabled
		}
	})
}

// BenchmarkOverhead_Write measures overhead of racewrite().
func BenchmarkOverhead_Write(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		Reset()
		Enable()

		getCurrentContext()

		addr := uintptr(0x2000)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			racewrite(addr)
		}
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		Disable()

		addr := uintptr(0x2000)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			racewrite(addr) // No-op when disabled
		}
	})
}

// BenchmarkOverhead_MixedReadWrite measures overhead of interleaved operations.
func BenchmarkOverhead_MixedReadWrite(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
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
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		Disable()

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
	})
}

// BenchmarkOverhead_ContextLookup measures overhead of getCurrentContext().
func BenchmarkOverhead_ContextLookup(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		Reset()
		Enable()

		// Pre-allocate context
		getCurrentContext()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_ = getCurrentContext()
		}
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		Disable()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Without detector, context lookup is skipped
			// This measures baseline cost (minimal)
		}
	})
}

// BenchmarkOverhead_TypicalWorkload measures end-to-end overhead.
//
// This simulates a realistic application workload:
//   - Mix of reads (70%), writes (30%)
//   - Mix of hot (80%) and cold (20%) addresses
//   - Atomic operations (10%)
func BenchmarkOverhead_TypicalWorkload(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		benchmarkTypicalWorkload(b, true)
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		benchmarkTypicalWorkload(b, false)
	})
}

func benchmarkTypicalWorkload(b *testing.B, withDetector bool) {
	if withDetector {
		Reset()
		Enable()
	} else {
		Disable()
	}

	var (
		hotData  [10]int
		coldData [100]int
		counter  atomic.Int64
	)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 80% hot, 20% cold
		var data []int
		var idx int
		if i%5 == 0 {
			data = coldData[:]
			idx = i % len(coldData)
		} else {
			data = hotData[:]
			idx = i % len(hotData)
		}

		// Operation mix
		opType := i % 10

		//nolint:gocritic // if-else chain is clearest for operation distribution
		if opType < 7 {
			// 70% reads
			_ = data[idx]
		} else if opType < 9 {
			// 20% writes
			data[idx] = i
		} else {
			// 10% atomics
			counter.Add(1)
		}
	}
}

// BenchmarkOverhead_ConcurrentAccess measures overhead under concurrency.
func BenchmarkOverhead_ConcurrentAccess(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		benchmarkConcurrentAccess(b, true)
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		benchmarkConcurrentAccess(b, false)
	})
}

func benchmarkConcurrentAccess(b *testing.B, withDetector bool) {
	if withDetector {
		Reset()
		Enable()
	} else {
		Disable()
	}

	var data [100]int
	var mu sync.RWMutex

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// 80% reads, 20% writes
			if b.N%5 == 0 {
				mu.Lock()
				data[b.N%100] = b.N
				mu.Unlock()
			} else {
				mu.RLock()
				_ = data[b.N%100]
				mu.RUnlock()
			}
		}
	})
}

// BenchmarkOverhead_MultipleGoroutines measures overhead with many goroutines.
//
// This tests how detector overhead scales with goroutine count.
func BenchmarkOverhead_MultipleGoroutines(b *testing.B) {
	for _, numGoroutines := range []int{1, 10, 100, 1000} {
		b.Run("Goroutines"+string(rune('0'+numGoroutines/100)), func(b *testing.B) {
			b.Run("WithDetector", func(b *testing.B) {
				benchmarkMultipleGoroutines(b, numGoroutines, true)
			})

			b.Run("WithoutDetector", func(b *testing.B) {
				benchmarkMultipleGoroutines(b, numGoroutines, false)
			})
		})
	}
}

func benchmarkMultipleGoroutines(b *testing.B, numGoroutines int, withDetector bool) {
	if withDetector {
		Reset()
		Enable()
	} else {
		Disable()
	}

	var (
		data    [100]int
		mu      sync.RWMutex
		counter atomic.Int64
	)

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(_ int) {
			defer wg.Done()

			iterations := b.N / numGoroutines
			for i := 0; i < iterations; i++ {
				// Mix of operations
				if i%2 == 0 {
					mu.RLock()
					_ = data[i%100]
					mu.RUnlock()
				} else {
					counter.Add(1)
				}
			}
		}(g)
	}

	wg.Wait()
}

// BenchmarkOverhead_MemoryAllocation measures overhead of allocation tracking.
//
// This tests detector overhead when tracking heap allocations.
func BenchmarkOverhead_MemoryAllocation(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		benchmarkMemoryAllocation(b, true)
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		benchmarkMemoryAllocation(b, false)
	})
}

func benchmarkMemoryAllocation(b *testing.B, withDetector bool) {
	if withDetector {
		Reset()
		Enable()
	} else {
		Disable()
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Allocate and access memory
		data := make([]int, 10)
		data[0] = i
		_ = data[0]
	}
}

// BenchmarkOverhead_ChannelOperations measures overhead of channel sync.
//
// Channels have implicit synchronization, so detector overhead should be minimal.
func BenchmarkOverhead_ChannelOperations(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		benchmarkChannelOperations(b, true)
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		benchmarkChannelOperations(b, false)
	})
}

func benchmarkChannelOperations(b *testing.B, withDetector bool) {
	if withDetector {
		Reset()
		Enable()
	} else {
		Disable()
	}

	ch := make(chan int, 100)

	// Producer
	go func() {
		for i := 0; i < b.N; i++ {
			ch <- i
		}
		close(ch)
	}()

	b.ResetTimer()
	b.ReportAllocs()

	// Consumer - draining channel for benchmark
	for range ch {
		// Drain channel
	}
}

// BenchmarkOverhead_AtomicOperations measures overhead of atomic ops.
//
// Atomics are inherently race-free, so detector overhead should be minimal.
func BenchmarkOverhead_AtomicOperations(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		benchmarkAtomicOperations(b, true)
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		benchmarkAtomicOperations(b, false)
	})
}

func benchmarkAtomicOperations(b *testing.B, withDetector bool) {
	if withDetector {
		Reset()
		Enable()
	} else {
		Disable()
	}

	var counter atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			counter.Add(1)
		}
	})
}

// BenchmarkOverhead_LockContention measures overhead under high contention.
//
// This tests worst-case scenario where many goroutines access same variable.
func BenchmarkOverhead_LockContention(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		benchmarkLockContention(b, true)
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		benchmarkLockContention(b, false)
	})
}

func benchmarkLockContention(b *testing.B, withDetector bool) {
	if withDetector {
		Reset()
		Enable()
	} else {
		Disable()
	}

	var (
		counter int
		mu      sync.Mutex
	)

	const numGoroutines = 100

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			iterations := b.N / numGoroutines
			for i := 0; i < iterations; i++ {
				mu.Lock()
				counter++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
}

// BenchmarkOverhead_RealWorldRatio measures overhead with realistic read/write ratio.
//
// Most applications have 80-90% reads, 10-20% writes.
func BenchmarkOverhead_RealWorldRatio(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		benchmarkRealWorldRatio(b, true)
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		benchmarkRealWorldRatio(b, false)
	})
}

func benchmarkRealWorldRatio(b *testing.B, withDetector bool) {
	if withDetector {
		Reset()
		Enable()
	} else {
		Disable()
	}

	var (
		data [100]int
		mu   sync.RWMutex
	)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 90% reads, 10% writes (realistic ratio)
		if i%10 == 0 {
			mu.Lock()
			data[i%100] = i
			mu.Unlock()
		} else {
			mu.RLock()
			_ = data[i%100]
			mu.RUnlock()
		}
	}
}

// BenchmarkOverhead_Summary provides a single comprehensive overhead measurement.
//
// This benchmark combines all common patterns into one realistic workload.
func BenchmarkOverhead_Summary(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		benchmarkOverheadSummary(b, true)
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		benchmarkOverheadSummary(b, false)
	})
}

//nolint:gocognit // Benchmark function complexity is acceptable
func benchmarkOverheadSummary(b *testing.B, withDetector bool) {
	if withDetector {
		Reset()
		Enable()
	} else {
		Disable()
	}

	var (
		hotData     [10]int
		coldData    [100]int
		atomicCount atomic.Int64
		mutexCount  int
		mu          sync.Mutex
	)

	const numGoroutines = 10

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(_ int) {
			defer wg.Done()

			iterations := b.N / numGoroutines
			for i := 0; i < iterations; i++ {
				// 70% hot, 30% cold
				var data []int
				if i%10 < 7 {
					data = hotData[:]
				} else {
					data = coldData[:]
				}

				// 60% reads, 20% writes, 20% atomics
				opType := i % 10
				//nolint:gocritic,nestif // if-else chain is clearest for operation distribution
				if opType < 6 {
					_ = data[i%len(data)]
				} else if opType < 8 {
					data[i%len(data)] = i
				} else {
					if i%2 == 0 {
						atomicCount.Add(1)
					} else {
						mu.Lock()
						mutexCount++
						mu.Unlock()
					}
				}
			}
		}(g)
	}

	wg.Wait()
}
