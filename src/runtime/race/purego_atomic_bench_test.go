// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

package race_test

import (
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const publicAtomicBenchmarkOperations = 100_000

var publicAtomicBenchmarkValue uint64

var (
	publicAtomicValueBenchmark         atomic.Value
	publicAtomicValueBenchmarkPayloads [2]uint64
)

func reportPublicAtomicLatency(b *testing.B) {
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*publicAtomicBenchmarkOperations), "ns/public-op")
}

func BenchmarkPureGoAtomicLoadFixedN(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for range publicAtomicBenchmarkOperations {
			_ = atomic.LoadUint64(&publicAtomicBenchmarkValue)
		}
	}
	b.StopTimer()
	reportPublicAtomicLatency(b)
	runtime.KeepAlive(&publicAtomicBenchmarkValue)
}

func BenchmarkPureGoAtomicStoreFixedN(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for range publicAtomicBenchmarkOperations {
			atomic.StoreUint64(&publicAtomicBenchmarkValue, 1)
		}
	}
	b.StopTimer()
	reportPublicAtomicLatency(b)
	runtime.KeepAlive(&publicAtomicBenchmarkValue)
}

func BenchmarkPureGoAtomicAddFixedN(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for range publicAtomicBenchmarkOperations {
			atomic.AddUint64(&publicAtomicBenchmarkValue, 1)
		}
	}
	b.StopTimer()
	reportPublicAtomicLatency(b)
	runtime.KeepAlive(&publicAtomicBenchmarkValue)
}

func BenchmarkPureGoAtomicCASFixedN(b *testing.B) {
	b.ReportAllocs()
	var want uint64
	atomic.StoreUint64(&publicAtomicBenchmarkValue, want)
	b.ResetTimer()
	for range b.N {
		for range publicAtomicBenchmarkOperations {
			if !atomic.CompareAndSwapUint64(&publicAtomicBenchmarkValue, want, want+1) {
				b.Fatal("uncontended CompareAndSwapUint64 failed")
			}
			want++
		}
	}
	b.StopTimer()
	reportPublicAtomicLatency(b)
	runtime.KeepAlive(want)
}

func BenchmarkPureGoAtomicValueSwapFixedN(b *testing.B) {
	values := [2]*uint64{
		&publicAtomicValueBenchmarkPayloads[0],
		&publicAtomicValueBenchmarkPayloads[1],
	}
	current := 0
	publicAtomicValueBenchmark.Store(values[current])
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for range publicAtomicBenchmarkOperations {
			next := current ^ 1
			if old := publicAtomicValueBenchmark.Swap(values[next]); old != values[current] {
				b.Fatalf("atomic.Value.Swap returned %p, want %p", old, values[current])
			}
			current = next
		}
	}
	b.StopTimer()
	reportPublicAtomicLatency(b)
	runtime.KeepAlive(values)
}

func TestPureGoAtomicUintptrOperations(t *testing.T) {
	var value uintptr
	atomic.StoreUintptr(&value, 7)
	if got := atomic.LoadUintptr(&value); got != 7 {
		t.Fatalf("LoadUintptr after StoreUintptr = %d, want 7", got)
	}
	if got := atomic.AddUintptr(&value, 5); got != 12 {
		t.Fatalf("AddUintptr = %d, want 12", got)
	}
	if atomic.CompareAndSwapUintptr(&value, 11, 13) {
		t.Fatal("failed CompareAndSwapUintptr reported success")
	}
	if got := atomic.LoadUintptr(&value); got != 12 {
		t.Fatalf("failed CompareAndSwapUintptr changed value to %d, want 12", got)
	}
	if !atomic.CompareAndSwapUintptr(&value, 12, 13) {
		t.Fatal("matching CompareAndSwapUintptr failed")
	}
	if got := atomic.LoadUintptr(&value); got != 13 {
		t.Fatalf("LoadUintptr after CompareAndSwapUintptr = %d, want 13", got)
	}
}

func TestPureGoAtomicValueSwapContendedProgress(t *testing.T) {
	const (
		workers    = 16
		operations = 250
	)
	var value atomic.Value
	value.Store(uint64(0))
	start := make(chan struct{})
	done := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for operation := 0; operation < operations; operation++ {
				value.Swap(uint64(worker*operations + operation + 1))
			}
		}(worker)
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	close(start)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("contended atomic.Value swaps did not make scheduler progress")
	}
	if got, ok := value.Load().(uint64); !ok || got == 0 {
		t.Fatalf("final atomic.Value = %#v, want a swapped uint64", value.Load())
	}
}

func TestPureGoAtomicValueLoadStoreContendedProgress(t *testing.T) {
	const (
		workers    = 16
		operations = 250
	)
	var value atomic.Value
	value.Store(uint64(1))
	start := make(chan struct{})
	done := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for operation := 0; operation < operations; operation++ {
				value.Store(uint64(worker*operations + operation + 1))
				if _, ok := value.Load().(uint64); !ok {
					panic("atomic.Value returned a non-uint64 value")
				}
			}
		}(worker)
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	close(start)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("contended atomic.Value loads and stores did not make scheduler progress")
	}
}

func TestPureGoAtomicValueSnapshotScale(t *testing.T) {
	if os.Getenv("GO_RACE_SNAPSHOT_SCALE") != "1" {
		t.Skip("set GO_RACE_SNAPSHOT_SCALE=1 to run the 10-million-swap scale gate")
	}
	const (
		phases          = 10
		workers         = 10_000
		swapsPerWorker  = 100
		operationsPhase = workers * swapsPerWorker
	)
	var value atomic.Value
	value.Store(uint64(0))
	var returnedTotal uint64
	postGCInuse := make([]uint64, 0, phases)
	started := time.Now()
	for phase := 0; phase < phases; phase++ {
		start := make(chan struct{})
		returned := make([]uint64, workers)
		phaseBase := uint64(phase * operationsPhase)
		var wg sync.WaitGroup
		for worker := 0; worker < workers; worker++ {
			wg.Add(1)
			go func(worker int, phaseBase uint64) {
				defer wg.Done()
				<-start
				var sum uint64
				base := phaseBase + uint64(worker*swapsPerWorker)
				for operation := 1; operation <= swapsPerWorker; operation++ {
					sum += value.Swap(base + uint64(operation)).(uint64)
				}
				returned[worker] = sum
			}(worker, phaseBase)
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		close(start)
		select {
		case <-done:
		case <-time.After(5 * time.Minute):
			t.Fatalf("snapshot scale phase %d stalled", phase+1)
		}
		for _, sum := range returned {
			returnedTotal += sum
		}
		runtime.GC()
		runtime.GC()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		postGCInuse = append(postGCInuse, mem.HeapInuse)
		t.Logf("phase=%d elapsed=%s TotalAlloc=%d HeapAlloc=%d HeapInuse=%d NumGC=%d",
			phase+1, time.Since(started), mem.TotalAlloc, mem.HeapAlloc, mem.HeapInuse, mem.NumGC)
	}
	final := value.Load().(uint64)
	operations := uint64(phases * operationsPhase)
	want := operations * (operations + 1) / 2
	if returnedTotal+final != want {
		t.Fatalf("swap permutation sum = %d, want %d", returnedTotal+final, want)
	}
	min, max := postGCInuse[phases/2], postGCInuse[phases/2]
	for _, inuse := range postGCInuse[phases/2:] {
		if inuse < min {
			min = inuse
		}
		if inuse > max {
			max = inuse
		}
	}
	if max > min+min/10 {
		t.Fatalf("last-half post-GC HeapInuse did not plateau: min=%d max=%d", min, max)
	}
}
