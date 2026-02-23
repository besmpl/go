// Package benchmarks provides toolchain-agnostic benchmarks for comparing
// race detector implementations. These benchmarks use only standard library
// constructs so they can be compiled with any Go toolchain using -race.
//
// Usage:
//
//	go test -race -bench=. -benchmem -benchtime=1s -count=10
package benchmarks

import (
	"runtime"
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// Micro-benchmarks: per-operation latency
// ---------------------------------------------------------------------------

// BenchmarkRaceRead measures the cost of instrumented reads.
func BenchmarkRaceRead(b *testing.B) {
	var x int
	for b.Loop() {
		_ = x
	}
}

// BenchmarkRaceWrite measures the cost of instrumented writes.
func BenchmarkRaceWrite(b *testing.B) {
	var x int
	for b.Loop() {
		x = 1
	}
	_ = x
}

// BenchmarkRaceReadWrite measures alternating read then write.
func BenchmarkRaceReadWrite(b *testing.B) {
	var x int
	for b.Loop() {
		_ = x
		x = 1
	}
}

// BenchmarkMutexLockUnlock measures mutex acquire/release overhead under
// race instrumentation.
func BenchmarkMutexLockUnlock(b *testing.B) {
	var mu sync.Mutex
	for b.Loop() {
		mu.Lock()
		mu.Unlock()
	}
}

// BenchmarkRWMutexReadLock measures RWMutex reader lock overhead.
func BenchmarkRWMutexReadLock(b *testing.B) {
	var mu sync.RWMutex
	for b.Loop() {
		mu.RLock()
		mu.RUnlock()
	}
}

// BenchmarkGoroutineStartStop measures goroutine creation + channel sync.
func BenchmarkGoroutineStartStop(b *testing.B) {
	for b.Loop() {
		ch := make(chan struct{})
		go func() { close(ch) }()
		<-ch
	}
}

// ---------------------------------------------------------------------------
// Sync-heavy workloads
// ---------------------------------------------------------------------------

// BenchmarkMutexContention measures N goroutines contending on a single mutex.
func BenchmarkMutexContention(b *testing.B) {
	for _, n := range []int{1, 4, 16, 64} {
		b.Run(goroutineLabel(n), func(b *testing.B) {
			var mu sync.Mutex
			var counter int
			b.SetParallelism(n)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					mu.Lock()
					counter++
					mu.Unlock()
				}
			})
			mu.Lock()
			_ = counter
			mu.Unlock()
		})
	}
}

// BenchmarkChannelPingPong measures channel send/recv pair latency.
func BenchmarkChannelPingPong(b *testing.B) {
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	go func() {
		for range ch1 {
			ch2 <- struct{}{}
		}
	}()
	for b.Loop() {
		ch1 <- struct{}{}
		<-ch2
	}
	close(ch1)
}

// BenchmarkWaitGroupFanOut fans out N goroutines with WaitGroup synchronization.
func BenchmarkWaitGroupFanOut(b *testing.B) {
	for _, n := range []int{1, 4, 16, 64} {
		b.Run(goroutineLabel(n), func(b *testing.B) {
			for b.Loop() {
				var wg sync.WaitGroup
				wg.Add(n)
				for range n {
					go func() {
						runtime.Gosched()
						wg.Done()
					}()
				}
				wg.Wait()
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Real-world patterns
// ---------------------------------------------------------------------------

// BenchmarkMapReadWrite simulates concurrent map access protected by RWMutex.
func BenchmarkMapReadWrite(b *testing.B) {
	for _, n := range []int{1, 4, 16, 64} {
		b.Run(goroutineLabel(n), func(b *testing.B) {
			var mu sync.RWMutex
			m := make(map[int]int)
			// Pre-fill map.
			for i := range 1024 {
				m[i] = i
			}
			b.SetParallelism(n)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					if i%10 == 0 {
						// 10% writes
						mu.Lock()
						m[i%1024] = i
						mu.Unlock()
					} else {
						// 90% reads
						mu.RLock()
						_ = m[i%1024]
						mu.RUnlock()
					}
					i++
				}
			})
		})
	}
}

// BenchmarkProducerConsumer simulates a buffered channel pipeline.
func BenchmarkProducerConsumer(b *testing.B) {
	for _, bufSize := range []int{1, 16, 64} {
		b.Run(bufLabel(bufSize), func(b *testing.B) {
			ch := make(chan int, bufSize)
			var wg sync.WaitGroup
			// Consumer.
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range ch {
				}
			}()
			for b.Loop() {
				ch <- 1
			}
			close(ch)
			wg.Wait()
		})
	}
}

// BenchmarkWorkerPool simulates a pool of N workers processing tasks.
func BenchmarkWorkerPool(b *testing.B) {
	for _, n := range []int{1, 4, 16, 64} {
		b.Run(goroutineLabel(n), func(b *testing.B) {
			tasks := make(chan int, n)
			var wg sync.WaitGroup
			// Start workers.
			for range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					sum := 0
					for v := range tasks {
						// Simulate light work.
						sum += v
					}
					_ = sum
				}()
			}
			for b.Loop() {
				tasks <- 1
			}
			close(tasks)
			wg.Wait()
		})
	}
}

// ---------------------------------------------------------------------------
// Memory-intensive benchmarks (for RSS measurement)
// ---------------------------------------------------------------------------

// BenchmarkMemoryAllocation exercises many small allocations under race
// instrumentation to stress shadow memory.
func BenchmarkMemoryAllocation(b *testing.B) {
	for b.Loop() {
		s := make([]byte, 256)
		s[0] = 1
		s[255] = 2
		_ = s
	}
}

// BenchmarkMemoryConcurrent exercises concurrent allocations across goroutines.
func BenchmarkMemoryConcurrent(b *testing.B) {
	for _, n := range []int{4, 16} {
		b.Run(goroutineLabel(n), func(b *testing.B) {
			iters := b.N
			perG := iters / n
			if perG < 1 {
				perG = 1
			}
			var wg sync.WaitGroup
			wg.Add(n)
			b.ResetTimer()
			for range n {
				go func() {
					defer wg.Done()
					for range perG {
						s := make([]byte, 256)
						s[0] = 1
						_ = s
					}
				}()
			}
			wg.Wait()
		})
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func goroutineLabel(n int) string {
	switch n {
	case 1:
		return "g1"
	case 4:
		return "g4"
	case 16:
		return "g16"
	case 64:
		return "g64"
	default:
		return "g?"
	}
}

func bufLabel(n int) string {
	switch n {
	case 1:
		return "buf1"
	case 16:
		return "buf16"
	case 64:
		return "buf64"
	default:
		return "buf?"
	}
}
