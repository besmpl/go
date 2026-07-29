// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

package atomic_test

import (
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

type pointerGCObject struct {
	id        uint64
	finalized atomic.Uint32
}

func newPointerGCObject(id uint64, finalizerRuns *atomic.Uint32) *pointerGCObject {
	object := &pointerGCObject{id: id}
	runtime.SetFinalizer(object, func(object *pointerGCObject) {
		object.finalized.Store(1)
		finalizerRuns.Add(1)
	})
	return object
}

func checkPointerGCRoot(failures *atomic.Uint32, pointer unsafe.Pointer) {
	if pointer == nil {
		return
	}
	object := (*pointerGCObject)(pointer)
	if object.finalized.Load() != 0 {
		failures.Add(1)
	}
	// Keep the loaded or returned pointer live through the finalized check.
	runtime.KeepAlive(object)
}

// Stress the low-level pointer entry points while concurrent collections and
// finalizers exercise both insertion and deletion barriers. In particular, a
// Swap result must become a typed root before detector End can allocate, and a
// pointer published by Store, Swap, or successful CAS must remain live through
// a GC phase transition.
func TestPointerOperationsKeepGCVisibleRoots(t *testing.T) {
	oldGCPercent := debug.SetGCPercent(1)
	defer debug.SetGCPercent(oldGCPercent)

	const (
		workers    = 4
		iterations = 250
	)
	var slot unsafe.Pointer
	var failures atomic.Uint32
	var finalizerRuns atomic.Uint32
	stopGC := make(chan struct{})
	gcDone := make(chan struct{})
	go func() {
		defer close(gcDone)
		for {
			select {
			case <-stopGC:
				return
			default:
				runtime.GC()
				runtime.Gosched()
			}
		}
	}()

	var workersDone sync.WaitGroup
	workersDone.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer workersDone.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				id := uint64(worker*iterations + iteration + 1)
				candidate := unsafe.Pointer(newPointerGCObject(id, &finalizerRuns))
				switch iteration % 3 {
				case 0:
					atomic.StorePointer(&slot, candidate)
				case 1:
					old := atomic.SwapPointer(&slot, candidate)
					checkPointerGCRoot(&failures, old)
				case 2:
					old := atomic.LoadPointer(&slot)
					if !atomic.CompareAndSwapPointer(&slot, old, candidate) {
						// Keep contention high without assuming that this
						// attempt won the modification-order race.
						runtime.Gosched()
					}
				}
				checkPointerGCRoot(&failures, atomic.LoadPointer(&slot))
			}
		}(worker)
	}
	workersDone.Wait()
	close(stopGC)
	<-gcDone
	checkPointerGCRoot(&failures, atomic.LoadPointer(&slot))
	if count := failures.Load(); count != 0 {
		t.Fatalf("observed %d finalized objects through live atomic pointer roots", count)
	}
	atomic.StorePointer(&slot, nil)
	for attempt := 0; attempt < 10 && finalizerRuns.Load() == 0; attempt++ {
		runtime.GC()
		runtime.Gosched()
	}
	if finalizerRuns.Load() == 0 {
		t.Fatal("GC stress completed without running a finalizer")
	}
}
