// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

package runtime_test

import (
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestKolkovAtomicRMWRetryCleanMissBackoff(t *testing.T) {
	delay := uint32(1)
	for _, want := range []uint32{2, 4, 8} {
		if runtime.RaceKolkovAtomicRMWRetry(&delay, nil, nil, false, false) {
			t.Fatal("clean queue-registration miss requested a retained resume")
		}
		if delay != want {
			t.Fatalf("clean queue-registration miss delay = %d, want %d", delay, want)
		}
	}
}

func TestKolkovAtomicRMWRetryRetainedSpinnerResetsBackoff(t *testing.T) {
	doorbell := uint32(1)
	delay := uint32(8)
	if !runtime.RaceKolkovAtomicRMWRetry(&delay, &doorbell, nil, true, false) {
		t.Fatal("retained spinner did not request resume")
	}
	if delay != 1 {
		t.Fatalf("retained spinner delay = %d, want 1", delay)
	}
}

func TestKolkovAtomicRMWRetryPatientSpinnerResetsBackoff(t *testing.T) {
	doorbell := uint32(0)
	patientMarker := uint32(1)
	released := make(chan struct{})
	go func() {
		time.Sleep(time.Millisecond)
		atomic.StoreUint32(&doorbell, 1)
		close(released)
	}()

	delay := uint32(8)
	if !runtime.RaceKolkovAtomicRMWRetry(&delay, &doorbell, unsafe.Pointer(&patientMarker), true, true) {
		t.Fatal("patient retained spinner did not request resume")
	}
	if delay != 1 {
		t.Fatalf("patient retained spinner delay = %d, want 1", delay)
	}
	<-released
}

func TestKolkovAtomicRMWRetryDoesNotParkWhileProcPinned(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldProcs)

	sema := uint32(0)
	released := make(chan struct{})
	go func() {
		time.Sleep(time.Millisecond)
		runtime.RaceKolkovAtomicRMWWake(&sema)
		close(released)
	}()

	delay := uint32(8)
	runtime.RaceKolkovProcPin()
	resumed := runtime.RaceKolkovAtomicRMWRetry(&delay, &sema, nil, false, false)
	runtime.RaceKolkovProcUnpin()
	if !resumed {
		t.Fatal("retained pinned waiter did not request resume")
	}
	if delay != 1 {
		t.Fatalf("retained pinned waiter delay = %d, want 1", delay)
	}
	<-released
}

func TestKolkovAtomicSyncFrameCacheMatchesExactClassification(t *testing.T) {
	syncPC := reflect.ValueOf((*atomic.Value).Swap).Pointer() + 1
	userPC := reflect.ValueOf(TestKolkovAtomicSyncFrameCacheMatchesExactClassification).Pointer() + 1
	for _, pc := range []uintptr{syncPC, userPC} {
		want := runtime.RaceKolkovAtomicSyncFrameUncached(pc)
		for i := 0; i < 100; i++ {
			if got := runtime.RaceKolkovAtomicSyncFrame(pc); got != want {
				t.Fatalf("cached classification for %#x = %v, want %v", pc, got, want)
			}
		}
	}
	if !runtime.RaceKolkovAtomicSyncFrame(syncPC) {
		t.Fatal("sync/atomic Value method was not classified as a wrapper")
	}
	if runtime.RaceKolkovAtomicSyncFrame(userPC) {
		t.Fatal("runtime test method was classified as a sync/atomic wrapper")
	}
}
