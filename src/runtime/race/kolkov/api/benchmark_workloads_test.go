// Copyright 2025 The racedetector Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// benchmark_workloads_test.go implements real-world workload simulations.
//
// These benchmarks measure race detector overhead in realistic scenarios:
//   - Web Server: concurrent request handling with shared cache
//   - Worker Pool: producer-consumer pattern with shared queue
//   - Data Pipeline: multi-stage processing with channels
//   - Mixed Workload: combination of reads/writes/atomics
//
// Each benchmark runs both WITH and WITHOUT the race detector to measure
// overhead percentage. Target overhead: 2-5x for typical workloads.

// BenchmarkWorkload_WebServer simulates a typical web server.
//
// Characteristics:
//   - 100 concurrent goroutines (simulating requests)
//   - 70% reads, 30% writes (typical HTTP pattern)
//   - Shared cache map (simulating session store)
//   - Metrics counter (simulating request counting)
//
// Expected overhead: 2-5x (acceptable for race detection).
func BenchmarkWorkload_WebServer(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		benchmarkWebServer(b, true)
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		benchmarkWebServer(b, false)
	})
}

func benchmarkWebServer(b *testing.B, withDetector bool) {
	if withDetector {
		Reset()
		Enable()
	} else {
		Disable()
	}

	// Shared cache (simulating session store)
	type Cache struct {
		mu   sync.RWMutex
		data map[string]int
	}
	cache := &Cache{data: make(map[string]int, 1000)}

	// Initialize cache with some data
	for i := 0; i < 100; i++ {
		cache.data[string(rune('a'+i%26))] = i
	}

	// Metrics counter (simulating request counting)
	var requests atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()

	// Simulate concurrent requests
	const concurrency = 100
	var wg sync.WaitGroup

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(reqID int) {
			defer wg.Done()

			// 70% reads, 30% writes (typical HTTP pattern)
			if reqID%10 < 7 {
				// Read from cache (shared read)
				cache.mu.RLock()
				_ = cache.data["key"]
				cache.mu.RUnlock()
			} else {
				// Write to cache (exclusive write)
				cache.mu.Lock()
				cache.data["key"] = reqID
				cache.mu.Unlock()
			}

			// Update metrics (atomic increment)
			requests.Add(1)
		}(i)

		// Limit concurrency to 100
		if i%concurrency == concurrency-1 {
			wg.Wait()
		}
	}

	wg.Wait()
	b.StopTimer()

	if withDetector {
		// Verify detector caught any races (should be none)
		if RacesDetected() > 0 {
			b.Fatalf("Unexpected races detected in synchronized workload: %d", RacesDetected())
		}
	}
}

// BenchmarkWorkload_WorkerPool simulates a worker pool pattern.
//
// Characteristics:
//   - 10 worker goroutines
//   - Shared task queue (buffered channel)
//   - Task processing with shared state updates
//   - Result aggregation
//
// Expected overhead: 1-3x (channel overhead dominates).
func BenchmarkWorkload_WorkerPool(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		benchmarkWorkerPool(b, true)
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		benchmarkWorkerPool(b, false)
	})
}

func benchmarkWorkerPool(b *testing.B, withDetector bool) {
	if withDetector {
		Reset()
		Enable()
	} else {
		Disable()
	}

	const numWorkers = 10
	tasks := make(chan int, 100)
	var results atomic.Int64

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				// Process task (simulate work)
				_ = task * task

				// Update results (atomic)
				results.Add(1)
			}
		}()
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Send tasks
	for i := 0; i < b.N; i++ {
		tasks <- i
	}

	close(tasks)
	wg.Wait()
	b.StopTimer()

	if withDetector {
		if RacesDetected() > 0 {
			b.Fatalf("Unexpected races detected in worker pool: %d", RacesDetected())
		}
	}
}

// BenchmarkWorkload_DataPipeline simulates a multi-stage data pipeline.
//
// Characteristics:
//   - 3 stages: Input → Transform → Output
//   - Channel communication between stages
//   - Typical streaming pattern
//
// Expected overhead: 1-2x (channel-heavy workload).
func BenchmarkWorkload_DataPipeline(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		benchmarkDataPipeline(b, true)
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		benchmarkDataPipeline(b, false)
	})
}

func benchmarkDataPipeline(b *testing.B, withDetector bool) {
	if withDetector {
		Reset()
		Enable()
	} else {
		Disable()
	}

	input := make(chan int, 100)
	transform := make(chan int, 100)
	output := make(chan int, 100)

	// Stage 1: Input
	go func() {
		for i := 0; i < b.N; i++ {
			input <- i
		}
		close(input)
	}()

	// Stage 2: Transform
	go func() {
		for val := range input {
			transform <- val * 2
		}
		close(transform)
	}()

	// Stage 3: Output
	go func() {
		for val := range transform {
			output <- val + 1
		}
		close(output)
	}()

	b.ResetTimer()
	b.ReportAllocs()

	// Consume results
	count := 0
	for range output {
		count++
	}

	b.StopTimer()

	if count != b.N {
		b.Fatalf("Expected %d results, got %d", b.N, count)
	}

	if withDetector {
		if RacesDetected() > 0 {
			b.Fatalf("Unexpected races detected in pipeline: %d", RacesDetected())
		}
	}
}

// BenchmarkWorkload_MixedOperations simulates typical mixed application workload.
//
// Characteristics:
//   - Mix of reads (50%), writes (30%), atomics (20%)
//   - Some hot variables (80% of accesses)
//   - Some cold variables (20% of accesses)
//   - Concurrent goroutines accessing shared state
//
// Expected overhead: 2-4x.
func BenchmarkWorkload_MixedOperations(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		benchmarkMixedOperations(b, true)
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		benchmarkMixedOperations(b, false)
	})
}

func benchmarkMixedOperations(b *testing.B, withDetector bool) {
	if withDetector {
		Reset()
		Enable()
	} else {
		Disable()
	}

	// Shared state
	var (
		hotData  [10]int      // Hot variables (frequently accessed)
		coldData [100]int     // Cold variables (rarely accessed)
		counter  atomic.Int64 // Atomic counter
		mu       sync.RWMutex
	)

	b.ResetTimer()
	b.ReportAllocs()

	// Run concurrent goroutines
	const concurrency = 10
	var wg sync.WaitGroup

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(iter int) {
			defer wg.Done()

			// 80% hot, 20% cold
			var data []int
			var idx int
			if iter%5 == 0 {
				// Cold access
				data = coldData[:]
				idx = iter % len(coldData)
			} else {
				// Hot access
				data = hotData[:]
				idx = iter % len(hotData)
			}

			// Operation mix: 50% reads, 30% writes, 20% atomics
			opType := iter % 10

			//nolint:gocritic // if-else chain is clearest for operation distribution
			if opType < 5 {
				// 50% reads
				mu.RLock()
				_ = data[idx]
				mu.RUnlock()
			} else if opType < 8 {
				// 30% writes
				mu.Lock()
				data[idx] = iter
				mu.Unlock()
			} else {
				// 20% atomics
				counter.Add(1)
			}
		}(i)

		// Limit concurrency
		if i%concurrency == concurrency-1 {
			wg.Wait()
		}
	}

	wg.Wait()
	b.StopTimer()

	if withDetector {
		if RacesDetected() > 0 {
			b.Fatalf("Unexpected races detected in mixed workload: %d", RacesDetected())
		}
	}
}

// BenchmarkWorkload_ProducerConsumer simulates producer-consumer pattern.
//
// Characteristics:
//   - Multiple producers (5)
//   - Multiple consumers (5)
//   - Buffered channel communication
//   - No shared state (channel-only)
//
// Expected overhead: <2x (channel overhead dominates, minimal race checking).
func BenchmarkWorkload_ProducerConsumer(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		benchmarkProducerConsumer(b, true)
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		benchmarkProducerConsumer(b, false)
	})
}

func benchmarkProducerConsumer(b *testing.B, withDetector bool) {
	if withDetector {
		Reset()
		Enable()
	} else {
		Disable()
	}

	const (
		numProducers = 5
		numConsumers = 5
		bufferSize   = 100
	)

	queue := make(chan int, bufferSize)
	var produced, consumed atomic.Int64

	// Start producers
	var producerWg sync.WaitGroup
	itemsPerProducer := b.N / numProducers
	for p := 0; p < numProducers; p++ {
		producerWg.Add(1)
		go func(id int) {
			defer producerWg.Done()
			for i := 0; i < itemsPerProducer; i++ {
				queue <- id*1000 + i
				produced.Add(1)
			}
		}(p)
	}

	// Start consumers
	var consumerWg sync.WaitGroup
	for c := 0; c < numConsumers; c++ {
		consumerWg.Add(1)
		go func() {
			defer consumerWg.Done()
			for range queue {
				consumed.Add(1)
			}
		}()
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Wait for production to complete
	producerWg.Wait()
	close(queue)

	// Wait for consumption to complete
	consumerWg.Wait()

	b.StopTimer()

	// Verify counts
	if produced.Load() != consumed.Load() {
		b.Fatalf("Produced %d but consumed %d", produced.Load(), consumed.Load())
	}

	if withDetector {
		if RacesDetected() > 0 {
			b.Fatalf("Unexpected races detected in producer-consumer: %d", RacesDetected())
		}
	}
}

// BenchmarkWorkload_HighContentionCounter simulates high contention scenario.
//
// Characteristics:
//   - 100 goroutines all incrementing same counter
//   - Mix of mutex-protected and atomic operations
//   - Tests detector under high contention
//
// Expected overhead: 3-6x (many goroutines, same memory location).
func BenchmarkWorkload_HighContentionCounter(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		benchmarkHighContentionCounter(b, true)
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		benchmarkHighContentionCounter(b, false)
	})
}

func benchmarkHighContentionCounter(b *testing.B, withDetector bool) {
	if withDetector {
		Reset()
		Enable()
	} else {
		Disable()
	}

	var (
		mutexCounter  int
		atomicCounter atomic.Int64
		mu            sync.Mutex
	)

	const numGoroutines = 100

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(_ int) {
			defer wg.Done()

			iterations := b.N / numGoroutines
			for i := 0; i < iterations; i++ {
				// Alternate between mutex and atomic
				if i%2 == 0 {
					mu.Lock()
					mutexCounter++
					mu.Unlock()
				} else {
					atomicCounter.Add(1)
				}
			}
		}(g)
	}

	wg.Wait()
	b.StopTimer()

	if withDetector {
		if RacesDetected() > 0 {
			b.Fatalf("Unexpected races detected in high contention: %d", RacesDetected())
		}
	}
}

// BenchmarkWorkload_RealWorldHTTPServer simulates realistic HTTP server pattern.
//
// Characteristics:
//   - Request handler goroutines
//   - Shared rate limiter
//   - Shared statistics
//   - Session cache
//   - Realistic read/write ratio
//
// Expected overhead: 2-4x.
func BenchmarkWorkload_RealWorldHTTPServer(b *testing.B) {
	b.Run("WithDetector", func(b *testing.B) {
		benchmarkRealWorldHTTPServer(b, true)
	})

	b.Run("WithoutDetector", func(b *testing.B) {
		benchmarkRealWorldHTTPServer(b, false)
	})
}

//nolint:gocognit // Benchmark function complexity is acceptable
func benchmarkRealWorldHTTPServer(b *testing.B, withDetector bool) {
	if withDetector {
		Reset()
		Enable()
	} else {
		Disable()
	}

	// Server state
	type ServerState struct {
		mu           sync.RWMutex
		sessions     map[string]int
		requestCount atomic.Int64
		errorCount   atomic.Int64
		rateLimiter  atomic.Int64 // Tokens available
	}

	state := &ServerState{
		sessions: make(map[string]int, 1000),
	}

	// Initialize some sessions
	for i := 0; i < 100; i++ {
		state.sessions[string(rune('a'+i%26))] = i
	}

	// Rate limiter refill goroutine
	done := make(chan bool)
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Refill rate limiter tokens
				current := state.rateLimiter.Load()
				if current < 1000 {
					state.rateLimiter.Store(1000)
				}
			case <-done:
				return
			}
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()

	// Simulate concurrent requests
	const concurrency = 50
	var wg sync.WaitGroup

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(reqID int) {
			defer wg.Done()

			// Check rate limiter (atomic)
			tokens := state.rateLimiter.Add(-1)
			if tokens < 0 {
				state.errorCount.Add(1)
				state.rateLimiter.Add(1) // Return token
				return
			}

			// Session lookup (90% reads, 10% writes)
			sessionKey := "session_" + string(rune('a'+reqID%26))
			if reqID%10 < 9 {
				// Read session
				state.mu.RLock()
				_ = state.sessions[sessionKey]
				state.mu.RUnlock()
			} else {
				// Update session
				state.mu.Lock()
				state.sessions[sessionKey] = reqID
				state.mu.Unlock()
			}

			// Update metrics
			state.requestCount.Add(1)
		}(i)

		// Limit concurrency
		if i%concurrency == concurrency-1 {
			wg.Wait()
		}
	}

	wg.Wait()
	b.StopTimer()

	// Stop rate limiter
	close(done)
	time.Sleep(10 * time.Millisecond) // Let goroutine finish

	if withDetector {
		if RacesDetected() > 0 {
			b.Fatalf("Unexpected races detected in HTTP server: %d", RacesDetected())
		}
	}
}
