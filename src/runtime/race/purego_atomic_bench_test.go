// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

package race_test

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const publicAtomicBenchmarkOperations = 100_000

var publicAtomicBenchmarkValue uint64

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
