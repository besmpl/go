// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api

import (
	"runtime"
	"testing"
	"unsafe"

	"runtime/race/kolkov/detector"
	"runtime/race/kolkov/goroutine"
	"runtime/race/kolkov/shadowmem"
)

func installFastBridgeDetector(t *testing.T) *detector.Detector {
	t.Helper()
	oldDetector := det
	oldShadow := shadow
	oldEnabled := enabled.Load()
	oldInit := apiInitCalled.Load()
	oldLifecycle := lifecycleState.Load()

	fresh := detector.NewDetector()
	det = fresh
	shadow = fresh.GetShadow().(*shadowmem.PageTableShadow)
	apiInitCalled.Store(2)
	lifecycleState.Store(uint32(lifecycleEnabled))
	enabled.Store(1)
	t.Cleanup(func() {
		enabled.Store(0)
		det = oldDetector
		shadow = oldShadow
		apiInitCalled.Store(oldInit)
		lifecycleState.Store(oldLifecycle)
		enabled.Store(oldEnabled)
	})
	return fresh
}

func materializeFastBridgeScalar(d *detector.Detector, addr, size uintptr) {
	mask := uint8(((uint16(1) << size) - 1) << (addr & 7))
	d.GetShadow().(*shadowmem.PageTableShadow).GetOrCreateSlot(addr).AccessGroups(mask, func(_ uint8, _ *shadowmem.VarState) {})
}

func TestOrdinaryFastBridgeStatusABIAndLifecycleMisses(t *testing.T) {
	d := installFastBridgeDetector(t)
	ctx := goroutine.Alloc(501)
	racectx := uintptr(unsafe.Pointer(ctx))
	const (
		addr = uintptr(0x71000)
		size = uintptr(8)
	)

	if state, status := raceTryReadFast(addr, size, 0x7101, 0); state != nil || status != uint8(shadowmem.OrdinaryFastMiss) {
		t.Fatalf("nil-context ABI = (%p,%d), want (nil,%d)", state, status, shadowmem.OrdinaryFastMiss)
	}
	if state, status := raceTryReadFast(addr, size, 0x7101, racectx); state != nil || status != uint8(shadowmem.OrdinaryFastMiss) {
		t.Fatalf("cold ABI = (%p,%d), want (nil,%d)", state, status, shadowmem.OrdinaryFastMiss)
	}

	materializeFastBridgeScalar(d, addr, size)
	d.OnWriteSized(addr, size, ctx, 0x7102)
	materializeFastBridgeScalar(d, addr, size)
	state, status := raceTryReadFast(addr, size, 0x7103, racectx)
	if status != uint8(shadowmem.OrdinaryFastHandledCacheable) || state == nil {
		t.Fatalf("warmed read ABI = (%p,%d), want cacheable status %d", state, status, shadowmem.OrdinaryFastHandledCacheable)
	}
	if !raceTryWriteFast(addr, size, 0x7104, racectx) {
		t.Fatal("warmed write bridge missed")
	}

	enabled.Store(0)
	if state, status := raceTryReadFast(addr, size, 0x7105, racectx); state != nil || status != uint8(shadowmem.OrdinaryFastMiss) {
		t.Fatalf("disabled ABI = (%p,%d), want miss", state, status)
	}
	if raceTryWriteFast(addr, size, 0x7105, racectx) {
		t.Fatal("disabled write bridge completed")
	}
	enabled.Store(1)

	stale := goroutine.Alloc(502)
	stale.C.Release()
	stale.C = nil
	if state, status := raceTryReadFast(addr, size, 0x7106, uintptr(unsafe.Pointer(stale))); state != nil || status != uint8(shadowmem.OrdinaryFastMiss) {
		t.Fatalf("stale-context ABI = (%p,%d), want miss", state, status)
	}
}

func TestOrdinaryFastBridgeRejectsClearedGeneration(t *testing.T) {
	d := installFastBridgeDetector(t)
	ctx := goroutine.Alloc(511)
	racectx := uintptr(unsafe.Pointer(ctx))
	const (
		addr = uintptr(0x72000)
		size = uintptr(8)
	)
	materializeFastBridgeScalar(d, addr, size)
	d.OnWriteSized(addr, size, ctx, 0x7201)
	materializeFastBridgeScalar(d, addr, size)
	old, status := raceTryReadFast(addr, size, 0x7202, racectx)
	if status != uint8(shadowmem.OrdinaryFastHandledCacheable) || old == nil {
		t.Fatalf("initial read = (%p,%d), want cacheable", old, status)
	}

	d.ClearShadowRange(addr, size)
	runtime.GC()
	state, status := raceTryReadFast(addr, size, 0x7203, racectx)
	if status == uint8(shadowmem.OrdinaryFastHandledCacheable) && state == old {
		t.Fatalf("post-clear bridge revived stale generation %p", old)
	}
}

func TestSynchronizationFastBridgeHitAndMiss(t *testing.T) {
	d := installFastBridgeDetector(t)
	ctx := goroutine.Alloc(521)
	racectx := uintptr(unsafe.Pointer(ctx))
	const addr = uintptr(0x73000)
	if raceTryAcquireFast(addr, racectx) || raceTryReleaseFast(addr, racectx) || raceTryReleaseMergeFast(addr, racectx) {
		t.Fatal("cold synchronization bridge created an owner")
	}
	d.OnRelease(addr, ctx)
	if !raceTryAcquireFast(addr, racectx) {
		t.Fatal("warmed acquire bridge missed")
	}
	if !raceTryReleaseFast(addr, racectx) {
		t.Fatal("warmed release bridge missed")
	}
	if raceTryReleaseMergeFast(addr, racectx) {
		t.Fatal("first release-merge bridge allocated a pending generation")
	}
	d.OnReleaseMerge(addr, ctx)
	if !raceTryReleaseMergeFast(addr, racectx) {
		t.Fatal("canonically provisioned release-merge bridge missed")
	}
}
