package api

import (
	"unsafe"

	"runtime/race/kolkov/detector"
	"runtime/race/kolkov/goroutine"
)

// raceAtomicBegin is the runtime bridge for one sync/atomic operation. It
// fills a bounded lane token in the suspended, GC-scanned user wrapper frame and
// returns the RaceContext used by the transaction. The systemstack closure must
// capture that token rather than declare it on g0. Returning the context lets the
// runtime populate g.racectx when an atomic operation is a goroutine's first
// instrumented event.
//
// The hardware operation must execute after this call and before raceAtomicEnd,
// on the same systemstack callback. A non-empty token is an opaque single-use
// capability: raceAtomicEnd must be called exactly once with the same token,
// address, size, and returned context. Tokens from raceAtomicBeginPlain and
// raceAtomicBeginRMW must be completed with synchronize=true. This is a trusted
// runtime ABI; mismatches can retain detector locks.
//
//go:linkname raceAtomicBegin
//go:nocheckptr
func raceAtomicBegin(addr, size, racectx uintptr, acquire bool, token *[8]unsafe.Pointer) (context uintptr) {
	return atomicBegin(addr, size, racectx, acquire, atomicBeginGeneral, token)
}

// raceAtomicBeginPlain is the separate, ABI-stable entry point used only by
// enabled aligned sync/atomic Load and Store operations. It may both reuse and
// enroll an exact capability.
//
//go:linkname raceAtomicBeginPlain
//go:nocheckptr
func raceAtomicBeginPlain(addr, size, racectx uintptr, acquire bool, token *[8]unsafe.Pointer) (context uintptr) {
	return atomicBegin(addr, size, racectx, acquire, atomicBeginPlain, token)
}

//go:linkname raceAtomicBeginLoad
//go:nocheckptr
func raceAtomicBeginLoad(addr, size, racectx uintptr, token *[8]unsafe.Pointer) (context uintptr) {
	if apiInitCalled.Load() == 0 {
		ensureInitialized()
	}
	if enabled.Load() == 0 {
		return 0
	}
	var ctx *goroutine.RaceContext
	if racectx > 1 {
		ctx = (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	} else {
		ctx = getCurrentContext()
	}
	if token != nil {
		det.AtomicBeginLoad(addr, size, ctx, (*detector.AtomicToken)(token))
	}
	return uintptr(unsafe.Pointer(ctx))
}

//go:linkname raceAtomicBeginLoadCooperative
//go:nocheckptr
func raceAtomicBeginLoadCooperative(addr, size, racectx uintptr, token *[8]unsafe.Pointer) (context uintptr, retry bool) {
	if apiInitCalled.Load() == 0 {
		ensureInitialized()
	}
	if enabled.Load() == 0 {
		return 0, false
	}
	var ctx *goroutine.RaceContext
	if racectx > 1 {
		ctx = (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	} else {
		ctx = getCurrentContext()
	}
	if token != nil {
		retry = det.AtomicBeginLoadCooperative(addr, size, ctx, (*detector.AtomicToken)(token))
	}
	return uintptr(unsafe.Pointer(ctx)), retry
}

//go:linkname raceAtomicBeginStoreCooperative
//go:nocheckptr
func raceAtomicBeginStoreCooperative(addr, size, racectx uintptr, token *[8]unsafe.Pointer) (context uintptr, retry bool) {
	if apiInitCalled.Load() == 0 {
		ensureInitialized()
	}
	if enabled.Load() == 0 {
		return 0, false
	}
	var ctx *goroutine.RaceContext
	if racectx > 1 {
		ctx = (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	} else {
		ctx = getCurrentContext()
	}
	if token != nil {
		retry = det.AtomicBeginStoreCooperative(addr, size, ctx, (*detector.AtomicToken)(token))
	}
	return uintptr(unsafe.Pointer(ctx)), retry
}

// raceAtomicBeginRMW is used only by enabled read-modify-write and
// compare-and-swap operations. An aligned exact-mask operation may reuse an
// existing capability or enroll one through canonical setup.
// Ignored operations continue to use raceAtomicBegin.
//
//go:linkname raceAtomicBeginRMW
//go:nocheckptr
func raceAtomicBeginRMW(addr, size, racectx uintptr, acquire bool, token *[8]unsafe.Pointer) (context uintptr) {
	return atomicBegin(addr, size, racectx, acquire, atomicBeginRMW, token)
}

// raceAtomicBeginRMWCooperative is the public-runtime RMW entry point. A true
// retry with spin or a non-nil park retains the exact capability in token[0];
// the caller waits on the spinner doorbell on its user G or acquires park
// before Resume. A
// true retry with neither remains a clean lock/queue miss with an empty token.
// Canonical enrollment and general misses remain blocking.
//
//go:linkname raceAtomicBeginRMWCooperative
//go:nocheckptr
func raceAtomicBeginRMWCooperative(addr, size, racectx uintptr, acquire bool, token *[8]unsafe.Pointer) (context uintptr, retry, spin, polite bool, park *uint32) {
	if apiInitCalled.Load() == 0 {
		ensureInitialized()
	}
	if enabled.Load() == 0 {
		return 0, false, false, false, nil
	}
	var ctx *goroutine.RaceContext
	if racectx > 1 {
		ctx = (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	} else {
		ctx = getCurrentContext()
	}
	if token != nil {
		retry, spin, polite, park = det.AtomicBeginRMWCooperative(addr, size, ctx, acquire, (*detector.AtomicToken)(token))
	}
	return uintptr(unsafe.Pointer(ctx)), retry, spin, polite, park
}

//go:linkname raceAtomicResumeRMW
//go:nocheckptr
func raceAtomicResumeRMW(addr, size, racectx uintptr, acquire bool, token *[8]unsafe.Pointer) (context uintptr, retry, spin, polite bool, park *uint32) {
	if token == nil || racectx <= 1 {
		return 0, false, false, false, nil
	}
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	retry, spin, polite, park = det.AtomicResumeRMW(addr, size, ctx, acquire, (*detector.AtomicToken)(token))
	return racectx, retry, spin, polite, park
}

//go:linkname raceAtomicInternalRMWPC
func raceAtomicInternalRMWPC(pc uintptr) bool {
	return detector.AtomicInternalRMWPC(pc)
}

//go:linkname raceAtomicBeginInternalRMWCooperative
//go:nocheckptr
func raceAtomicBeginInternalRMWCooperative(addr, size, pc, racectx uintptr, synchronize bool, token *[8]unsafe.Pointer) (context uintptr, retry, direct, spin, polite bool, park *uint32) {
	if apiInitCalled.Load() == 0 {
		ensureInitialized()
	}
	if enabled.Load() == 0 {
		return 0, false, false, false, false, nil
	}
	var ctx *goroutine.RaceContext
	if racectx > 1 {
		ctx = (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	} else {
		ctx = getCurrentContext()
	}
	if token != nil {
		retry, direct, spin, polite, park = det.AtomicBeginInternalRMWCooperative(addr, size, ctx, pc, synchronize, (*detector.AtomicToken)(token))
	}
	return uintptr(unsafe.Pointer(ctx)), retry, direct, spin, polite, park
}

type atomicBeginMode uint8

const (
	atomicBeginGeneral atomicBeginMode = iota
	atomicBeginPlain
	atomicBeginRMW
)

//go:nocheckptr
func atomicBegin(addr, size, racectx uintptr, acquire bool, mode atomicBeginMode, token *[8]unsafe.Pointer) (context uintptr) {
	if apiInitCalled.Load() == 0 {
		ensureInitialized()
	}
	// The package-global flag is a terminal/quiescent test switch. Production
	// runtime.RaceDisable reaches this bridge with synchronize=false instead.
	if enabled.Load() == 0 {
		return 0
	}

	var ctx *goroutine.RaceContext
	if racectx > 1 {
		ctx = (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	} else {
		ctx = getCurrentContext()
	}
	if token != nil {
		switch mode {
		case atomicBeginPlain:
			det.AtomicBeginPlain(addr, size, ctx, acquire, true, (*detector.AtomicToken)(token))
		case atomicBeginRMW:
			det.AtomicBeginRMW(addr, size, ctx, acquire, (*detector.AtomicToken)(token))
		default:
			det.AtomicBegin(addr, size, ctx, acquire, (*detector.AtomicToken)(token))
		}
	}
	return uintptr(unsafe.Pointer(ctx))
}

// raceAtomicEnd publishes the result of the hardware operation and releases
// the per-address detector transaction lock. write is false for loads and
// failed compare-and-swaps. synchronize controls release publication and the
// logical clock advance; memory history is recorded in either mode.
//
//go:linkname raceAtomicEnd
//go:nocheckptr
func raceAtomicEnd(addr, size, pc, racectx uintptr, token *[8]unsafe.Pointer, write, synchronize bool) {
	if token == nil || racectx <= 1 {
		return
	}
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	det.AtomicEndMode(addr, size, ctx, (*detector.AtomicToken)(token), pc, write, synchronize)
}

//go:linkname raceAtomicEndInternalRMW
//go:nocheckptr
func raceAtomicEndInternalRMW(addr, size, pc, racectx uintptr, token *[8]unsafe.Pointer, write, synchronize, direct bool) *uint32 {
	if token == nil || racectx <= 1 {
		return nil
	}
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	return det.AtomicEndInternalRMW(addr, size, ctx, (*detector.AtomicToken)(token), pc, write, synchronize, direct)
}

//go:linkname raceAtomicLoadFastBegin
//go:nocheckptr
func raceAtomicLoadFastBegin(addr, size, pc, racectx uintptr, token *[8]unsafe.Pointer, revision, generation *uint64) bool {
	if enabled.Load() == 0 || racectx <= 1 || token == nil || revision == nil || generation == nil {
		return false
	}
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	gotRevision, gotGeneration, ok := det.AtomicBeginLoadFast(addr, size, ctx, pc, (*detector.AtomicToken)(token))
	if ok {
		*revision = gotRevision
		*generation = gotGeneration
	}
	return ok
}

//go:linkname raceAtomicLoadFastEnd
//go:nocheckptr
func raceAtomicLoadFastEnd(racectx uintptr, token *[8]unsafe.Pointer, revision, generation uint64) bool {
	if racectx <= 1 || token == nil {
		return false
	}
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	return det.AtomicEndLoadFast(ctx, (*detector.AtomicToken)(token), revision, generation)
}

//go:linkname raceAtomicLoadEnd
//go:nocheckptr
func raceAtomicLoadEnd(addr, size, pc, racectx uintptr, token *[8]unsafe.Pointer) {
	if token == nil || racectx <= 1 {
		return
	}
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	det.AtomicEndLoad(addr, size, ctx, (*detector.AtomicToken)(token), pc)
}
