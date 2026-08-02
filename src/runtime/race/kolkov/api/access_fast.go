// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api

import (
	"unsafe"

	"runtime/race/kolkov/goroutine"
	"runtime/race/kolkov/shadowmem"
)

// fastContext resolves the runtime-owned context only while the detector is in
// its stable enabled lifecycle. Unlike the canonical bridges, fast probes must
// never initialize the API or recover a missing/stale context.
//
//go:nosplit
func fastContext(racectx uintptr) *goroutine.RaceContext {
	if racectx <= 1 || apiInitCalled.Load() != 2 || enabled.Load() == 0 ||
		detectorLifecycle(lifecycleState.Load()) != lifecycleEnabled || det == nil {
		return nil
	}
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	if ctx.C == nil {
		return nil
	}
	return ctx
}

// raceTryReadFast is the allocation-free ordinary-access bridge used directly
// from the user goroutine after the runtime's Tier-0 cache misses. A miss leaves
// all detector state unchanged so the caller can execute one canonical event.
//
//go:linkname raceTryReadFast
//go:nosplit
func raceTryReadFast(addr, size, pc, racectx uintptr) (state unsafe.Pointer, status uint8) {
	ctx := fastContext(racectx)
	if ctx == nil {
		return nil, uint8(shadowmem.OrdinaryFastMiss)
	}
	result, authoritative := det.TryOrdinaryRead(addr, size, ctx, pc)
	return unsafe.Pointer(authoritative), uint8(result)
}

// raceTryWriteFast is the write counterpart of raceTryReadFast. The runtime
// invalidates its overlapping read-cache entries before entering this bridge.
//
//go:linkname raceTryWriteFast
//go:nosplit
func raceTryWriteFast(addr, size, pc, racectx uintptr) bool {
	ctx := fastContext(racectx)
	return ctx != nil && det.TryOrdinaryWrite(addr, size, ctx, pc)
}

// raceMaterializeOrdinaryScalar performs the one-time representation move used
// by the runtime's retained heap/stack/static scalar certificate. Callers run
// it on the system stack because first materialization may allocate.
//
//go:linkname raceMaterializeOrdinaryScalar
func raceMaterializeOrdinaryScalar(addr, size, racectx uintptr) bool {
	return fastContext(racectx) != nil && det.MaterializeOrdinaryScalar(addr, size)
}

//go:linkname raceTryAcquireFast
//go:nosplit
func raceTryAcquireFast(addr, racectx uintptr) bool {
	ctx := fastContext(racectx)
	return ctx != nil && det.TryAcquire(addr, ctx)
}

//go:linkname raceTryReleaseFast
//go:nosplit
func raceTryReleaseFast(addr, racectx uintptr) bool {
	ctx := fastContext(racectx)
	return ctx != nil && det.TryRelease(addr, ctx)
}

//go:linkname raceTryReleaseMergeFast
//go:nosplit
func raceTryReleaseMergeFast(addr, racectx uintptr) bool {
	ctx := fastContext(racectx)
	return ctx != nil && det.TryReleaseMerge(addr, ctx)
}
