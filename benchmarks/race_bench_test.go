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

var (
	raceReadSource      uint64 = 1
	raceReadAlternating        = [2]uint64{1, 2}
	raceReadCollision          = [5]uint64{1, 0, 0, 0, 2}
	raceWriteTarget     uint64
	raceReadWriteTarget uint64 = 1
)

// BenchmarkRaceRead measures a steady-state instrumented read of one exact
// address. Implementations may apply redundant-read elimination after the
// first iteration.
func BenchmarkRaceRead(b *testing.B) {
	n := b.N
	var sum uint64

	b.ResetTimer()
	for i := 0; i < n; i++ {
		sum += raceReadSource
	}
	b.StopTimer()

	runtime.KeepAlive(sum)
}

// BenchmarkRaceReadAlternating alternates between two global addresses that
// occupy distinct entries in the four-slot cache, measuring two-address hits.
func BenchmarkRaceReadAlternating(b *testing.B) {
	n := b.N
	var sum uint64

	b.ResetTimer()
	for i := 0; i < n; i++ {
		sum += raceReadAlternating[i&1]
	}
	b.StopTimer()

	runtime.KeepAlive(sum)
}

// BenchmarkRaceReadCollision alternates exact addresses 32 bytes apart. They
// deterministically map to the same four-slot cache entry, forcing its miss
// path without relying on linker placement of independent globals.
func BenchmarkRaceReadCollision(b *testing.B) {
	n := b.N
	var sum uint64

	b.ResetTimer()
	for i := 0; i < n; i++ {
		sum += raceReadCollision[(i&1)*4]
	}
	b.StopTimer()

	runtime.KeepAlive(sum)
}

// BenchmarkRaceWrite measures one retained compiler-instrumented global write.
func BenchmarkRaceWrite(b *testing.B) {
	n := b.N
	b.ResetTimer()
	for i := 0; i < n; i++ {
		raceWriteTarget = uint64(i)
	}
	b.StopTimer()
	runtime.KeepAlive(&raceWriteTarget)
}

// BenchmarkRaceReadWrite measures one retained global read followed by one
// global write to the same address. The write invalidates redundant-read state.
func BenchmarkRaceReadWrite(b *testing.B) {
	n := b.N
	var sum uint64
	b.ResetTimer()
	for i := 0; i < n; i++ {
		sum += raceReadWriteTarget
		raceReadWriteTarget = uint64(i)
	}
	b.StopTimer()
	runtime.KeepAlive(sum)
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
// Allocation-latency benchmarks
// ---------------------------------------------------------------------------

var (
	memoryAllocationSink []byte
	memoryConcurrentSink [][]byte
)

// BenchmarkMemoryAllocation measures small allocations under race
// instrumentation. Only the final allocation is retained.
func BenchmarkMemoryAllocation(b *testing.B) {
	var last []byte
	for b.Loop() {
		s := make([]byte, 256)
		s[0] = 1
		s[255] = 2
		last = s
	}
	memoryAllocationSink = last
	runtime.KeepAlive(memoryAllocationSink)
}

// BenchmarkMemoryConcurrent measures concurrent allocation latency. Each worker
// retains only its final allocation; process RSS is measured externally by
// running a prebuilt benchmark binary.
func BenchmarkMemoryConcurrent(b *testing.B) {
	for _, n := range []int{4, 16} {
		b.Run(goroutineLabel(n), func(b *testing.B) {
			var wg sync.WaitGroup
			last := make([][]byte, n)
			wg.Add(n)
			b.ResetTimer()
			for worker := range n {
				iters := b.N / n
				if worker < b.N%n {
					iters++
				}
				go func(worker, iters int) {
					defer wg.Done()
					for range iters {
						s := make([]byte, 256)
						s[0] = 1
						s[255] = 2
						last[worker] = s
					}
				}(worker, iters)
			}
			wg.Wait()
			b.StopTimer()
			memoryConcurrentSink = last
			runtime.KeepAlive(memoryConcurrentSink)
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
