// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

package runtime

import (
	"internal/runtime/atomic"
	_ "unsafe" // for go:linkname
)

// Kolkov detector state.
// These are used for local tracking. The full implementation
// is in runtime/race/kolkov/api/ and connected via linkname.

var (
	kolkovEnabled atomic.Uint32
	kolkovInited  atomic.Uint32
	kolkovErrors  atomic.Uint64
)

// kolkovDetectorInit initializes the Kolkov race detector.
//
//go:nosplit
func kolkovDetectorInit() {
	if kolkovInited.CompareAndSwap(0, 1) {
		kolkovEnabled.Store(1)
		// Note: Kolkov API initializes via init()
	}
}

// kolkovDetectorFini finalizes the Kolkov race detector.
func kolkovDetectorFini() {
	kolkovEnabled.Store(0)
	// Call API Fini to print full report
	kolkovApiFini()
}

// kolkovRaceErrors returns the number of races detected.
//
//go:nosplit
func kolkovRaceErrors() int {
	return int(kolkovErrors.Load())
}

// kolkovIncrementErrors is called by Kolkov API when a race is detected.
//
//go:linkname kolkovIncrementErrors
//go:nosplit
func kolkovIncrementErrors() {
	kolkovErrors.Add(1)
}

// kolkovOnRead handles a memory read access.
// Delegates to the Kolkov API implementation, passing through the PC
// captured by sys.GetCallerPC() at the runtime entry point.
//
//go:nosplit
func kolkovOnRead(addr, pc uintptr) {
	kolkovApiOnRead(addr, pc)
}

// kolkovOnWrite handles a memory write access.
// Delegates to the Kolkov API implementation, passing through the PC
// captured by sys.GetCallerPC() at the runtime entry point.
//
//go:nosplit
func kolkovOnWrite(addr, pc uintptr) {
	kolkovApiOnWrite(addr, pc)
}

// kolkovOnAcquire handles a synchronization acquire operation.
// This is called when a mutex is locked or a channel receive completes.
//
//go:nosplit
func kolkovOnAcquire(addr uintptr) {
	kolkovApiOnAcquire(addr)
}

// kolkovOnRelease handles a synchronization release operation.
// This is called when a mutex is unlocked or a channel send completes.
//
//go:nosplit
func kolkovOnRelease(addr uintptr) {
	kolkovApiOnRelease(addr)
}

// kolkovOnReleaseMerge handles a release-merge synchronization operation.
// This is like release but also merges with prior releases on the same address.
//
//go:nosplit
func kolkovOnReleaseMerge(addr uintptr) {
	kolkovApiOnReleaseMerge(addr)
}

// kolkovGetGoid returns the current user goroutine's ID.
// Called from runtime/race/kolkov/api via linkname.
//
// Uses getg() compiler intrinsic — zero overhead, version-independent.
// Handles g0/gsignal by checking m.curg for the actual user goroutine.
//
//go:linkname kolkovGetGoid
//go:nosplit
func kolkovGetGoid() int64 {
	gp := getg()
	if gp.m != nil && gp.m.curg != nil {
		return int64(gp.m.curg.goid)
	}
	return int64(gp.goid)
}

// Linkname imports from runtime/race/kolkov/api package.
// These functions are implemented in the Kolkov API and exported to runtime.

//go:linkname kolkovApiOnRead runtime/race/kolkov/api.raceread
func kolkovApiOnRead(addr, pc uintptr)

//go:linkname kolkovApiOnWrite runtime/race/kolkov/api.racewrite
func kolkovApiOnWrite(addr, pc uintptr)

//go:linkname kolkovApiOnAcquire runtime/race/kolkov/api.raceacquire
func kolkovApiOnAcquire(addr uintptr)

//go:linkname kolkovApiOnRelease runtime/race/kolkov/api.racerelease
func kolkovApiOnRelease(addr uintptr)

//go:linkname kolkovApiOnReleaseMerge runtime/race/kolkov/api.racereleasemerge
func kolkovApiOnReleaseMerge(addr uintptr)

//go:linkname kolkovApiOnAcquireForGoroutine runtime/race/kolkov/api.raceAcquireForGoroutine
func kolkovApiOnAcquireForGoroutine(addr uintptr, goid int64)

//go:linkname kolkovApiOnReleaseForGoroutine runtime/race/kolkov/api.raceReleaseForGoroutine
func kolkovApiOnReleaseForGoroutine(addr uintptr, goid int64)

//go:linkname kolkovApiOnReleaseMergeForGoroutine runtime/race/kolkov/api.raceReleaseMergeForGoroutine
func kolkovApiOnReleaseMergeForGoroutine(addr uintptr, goid int64)

//go:linkname kolkovApiGoSetChildID runtime/race/kolkov/api.raceGoSetChildID
func kolkovApiGoSetChildID(childGoid int64)

// T13: Eager context creation during goroutine spawn.
//go:linkname kolkovApiGoSetChildIDWithCtx runtime/race/kolkov/api.raceGoSetChildIDWithCtx
func kolkovApiGoSetChildIDWithCtx(childGoid int64) uintptr

//go:linkname kolkovApiClearShadow runtime/race/kolkov/api.raceClearShadow
func kolkovApiClearShadow(addr, size uintptr)

//go:linkname kolkovApiFini runtime/race/kolkov/api.Fini
func kolkovApiFini()

//go:linkname kolkovApiOnGoStart runtime/race/kolkov/api.raceGoStartFromRuntime
func kolkovApiOnGoStart(pc uintptr, parentGoid int64)

//go:linkname kolkovApiOnGoEnd runtime/race/kolkov/api.raceGoEndFromRuntime
func kolkovApiOnGoEnd(goid int64)

// === g.racectx Fast/Slow Path Bridges (T9 optimization) ===
// Fast path: context pointer passed directly as uintptr (skips contextsMap lookup).

//go:linkname kolkovOnReadCtx runtime/race/kolkov/api.racereadCtx
func kolkovOnReadCtx(addr, pc, racectx uintptr)

//go:linkname kolkovOnWriteCtx runtime/race/kolkov/api.racewriteCtx
func kolkovOnWriteCtx(addr, pc, racectx uintptr)

//go:linkname kolkovOnAcquireCtx runtime/race/kolkov/api.raceacquireCtx
func kolkovOnAcquireCtx(addr, racectx uintptr)

//go:linkname kolkovOnReleaseCtx runtime/race/kolkov/api.racereleaseCtx
func kolkovOnReleaseCtx(addr, racectx uintptr)

//go:linkname kolkovOnReleaseMergeCtx runtime/race/kolkov/api.racereleasemergeCtx
func kolkovOnReleaseMergeCtx(addr, racectx uintptr)

// Slow path: creates context, performs operation, returns pointer for caching in g.racectx.

//go:linkname kolkovOnReadSlow runtime/race/kolkov/api.racereadSlow
func kolkovOnReadSlow(addr, pc uintptr) uintptr

//go:linkname kolkovOnWriteSlow runtime/race/kolkov/api.racewriteSlow
func kolkovOnWriteSlow(addr, pc uintptr) uintptr

//go:linkname kolkovOnAcquireSlow runtime/race/kolkov/api.raceacquireSlow
func kolkovOnAcquireSlow(addr uintptr) uintptr
