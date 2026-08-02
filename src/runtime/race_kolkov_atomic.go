// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

package runtime

import (
	"internal/goarch"
	"internal/runtime/atomic"
	"internal/runtime/sys"
	"unsafe"
)

// The begin/end bridge deliberately surrounds the hardware operation. This
// makes the per-address release clock describe the exact modification observed
// by a load or RMW, and prevents detector bookkeeping from drifting away from
// the atomic object's modification order. Each typed bridge enrolls only on
// the current user goroutine, then runs begin, hardware operation, and end in
// one system-stack callback. This keeps a stack-local atomic object's address
// fixed for the whole transaction. The token contains GC-visible pointers, so
// it is declared in the suspended goroutine's scanned frame rather than on g0.

//go:linkname kolkovApiAtomicBegin runtime/race/kolkov/api.raceAtomicBegin
//go:noescape
func kolkovApiAtomicBegin(addr, size, racectx uintptr, acquire bool, token *[8]unsafe.Pointer) (context uintptr)

//go:linkname kolkovApiAtomicBeginPlain runtime/race/kolkov/api.raceAtomicBeginPlain
//go:noescape
func kolkovApiAtomicBeginPlain(addr, size, racectx uintptr, acquire bool, token *[8]unsafe.Pointer) (context uintptr)

//go:linkname kolkovApiAtomicBeginLoad runtime/race/kolkov/api.raceAtomicBeginLoad
//go:noescape
func kolkovApiAtomicBeginLoad(addr, size, racectx uintptr, token *[8]unsafe.Pointer) (context uintptr)

//go:linkname kolkovApiAtomicBeginLoadCooperative runtime/race/kolkov/api.raceAtomicBeginLoadCooperative
//go:noescape
func kolkovApiAtomicBeginLoadCooperative(addr, size, racectx uintptr, token *[8]unsafe.Pointer) (context uintptr, retry bool)

//go:linkname kolkovApiAtomicBeginStoreCooperative runtime/race/kolkov/api.raceAtomicBeginStoreCooperative
//go:noescape
func kolkovApiAtomicBeginStoreCooperative(addr, size, racectx uintptr, token *[8]unsafe.Pointer) (context uintptr, retry bool)

//go:linkname kolkovApiAtomicBeginRMW runtime/race/kolkov/api.raceAtomicBeginRMW
//go:noescape
func kolkovApiAtomicBeginRMW(addr, size, racectx uintptr, acquire bool, token *[8]unsafe.Pointer) (context uintptr)

//go:linkname kolkovApiAtomicBeginRMWCooperative runtime/race/kolkov/api.raceAtomicBeginRMWCooperative
//go:noescape
func kolkovApiAtomicBeginRMWCooperative(addr, size, racectx uintptr, acquire bool, token *[8]unsafe.Pointer) (context uintptr, retry, spin, polite bool, park *uint32)

//go:linkname kolkovApiAtomicResumeRMW runtime/race/kolkov/api.raceAtomicResumeRMW
//go:noescape
func kolkovApiAtomicResumeRMW(addr, size, racectx uintptr, acquire bool, token *[8]unsafe.Pointer) (context uintptr, retry, spin, polite bool, park *uint32)

//go:linkname kolkovApiAtomicInternalRMWPC runtime/race/kolkov/api.raceAtomicInternalRMWPC
func kolkovApiAtomicInternalRMWPC(pc uintptr) bool

//go:linkname kolkovApiAtomicBeginInternalRMWCooperative runtime/race/kolkov/api.raceAtomicBeginInternalRMWCooperative
//go:noescape
func kolkovApiAtomicBeginInternalRMWCooperative(addr, size, pc, racectx uintptr, synchronize bool, token *[8]unsafe.Pointer) (context uintptr, retry, direct, spin, polite bool, park *uint32)

//go:linkname kolkovApiAtomicEnd runtime/race/kolkov/api.raceAtomicEnd
//go:noescape
func kolkovApiAtomicEnd(addr, size, pc, racectx uintptr, token *[8]unsafe.Pointer, write, synchronize bool)

//go:linkname kolkovApiAtomicEndInternalRMW runtime/race/kolkov/api.raceAtomicEndInternalRMW
//go:noescape
func kolkovApiAtomicEndInternalRMW(addr, size, pc, racectx uintptr, token *[8]unsafe.Pointer, write, synchronize, direct bool) *uint32

//go:linkname kolkovApiAtomicLoadFastBegin runtime/race/kolkov/api.raceAtomicLoadFastBegin
//go:noescape
func kolkovApiAtomicLoadFastBegin(addr, size, pc, racectx uintptr, token *[8]unsafe.Pointer, revision, generation *uint64) bool

//go:linkname kolkovApiAtomicLoadFastEnd runtime/race/kolkov/api.raceAtomicLoadFastEnd
//go:noescape
func kolkovApiAtomicLoadFastEnd(racectx uintptr, token *[8]unsafe.Pointer, revision, generation uint64) bool

//go:linkname kolkovApiAtomicLoadEnd runtime/race/kolkov/api.raceAtomicLoadEnd
//go:noescape
func kolkovApiAtomicLoadEnd(addr, size, pc, racectx uintptr, token *[8]unsafe.Pointer)

// sync/atomic declares the numeric operations as noescape assembly functions.
// Defining their symbols here keeps that public compiler contract while making
// the implementation ABIInternal: there is no ABI0 trampoline between the
// caller-PC capture and the typed detector/hardware handler.

//go:nosplit
//go:linkname kolkovSyncAtomicLoadInt32 sync/atomic.LoadInt32
//go:nocheckptr
func kolkovSyncAtomicLoadInt32(addr *int32) int32 {
	pc := sys.GetCallerPC()
	return int32(kolkovAtomicLoad32((*uint32)(unsafe.Pointer(addr)), pc))
}

//go:nosplit
//go:linkname kolkovSyncAtomicLoadUint32 sync/atomic.LoadUint32
func kolkovSyncAtomicLoadUint32(addr *uint32) uint32 {
	pc := sys.GetCallerPC()
	return kolkovAtomicLoad32(addr, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicLoadInt64 sync/atomic.LoadInt64
//go:nocheckptr
func kolkovSyncAtomicLoadInt64(addr *int64) int64 {
	pc := sys.GetCallerPC()
	return int64(kolkovAtomicLoad64((*uint64)(unsafe.Pointer(addr)), pc))
}

//go:nosplit
//go:linkname kolkovSyncAtomicLoadUint64 sync/atomic.LoadUint64
func kolkovSyncAtomicLoadUint64(addr *uint64) uint64 {
	pc := sys.GetCallerPC()
	return kolkovAtomicLoad64(addr, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicLoadUintptr sync/atomic.LoadUintptr
func kolkovSyncAtomicLoadUintptr(addr *uintptr) uintptr {
	pc := sys.GetCallerPC()
	return kolkovAtomicLoadUintptr(addr, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicLoadPointer sync/atomic.LoadPointer
func kolkovSyncAtomicLoadPointer(addr *unsafe.Pointer) unsafe.Pointer {
	pc := sys.GetCallerPC()
	pc = kolkovAtomicUserPC(pc, getfp())
	return kolkovAtomicLoadPointer(addr, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicStoreInt32 sync/atomic.StoreInt32
//go:nocheckptr
func kolkovSyncAtomicStoreInt32(addr *int32, value int32) {
	pc := sys.GetCallerPC()
	kolkovAtomicStore32((*uint32)(unsafe.Pointer(addr)), uint32(value), pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicStoreUint32 sync/atomic.StoreUint32
func kolkovSyncAtomicStoreUint32(addr *uint32, value uint32) {
	pc := sys.GetCallerPC()
	kolkovAtomicStore32(addr, value, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicStoreInt64 sync/atomic.StoreInt64
//go:nocheckptr
func kolkovSyncAtomicStoreInt64(addr *int64, value int64) {
	pc := sys.GetCallerPC()
	kolkovAtomicStore64((*uint64)(unsafe.Pointer(addr)), uint64(value), pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicStoreUint64 sync/atomic.StoreUint64
func kolkovSyncAtomicStoreUint64(addr *uint64, value uint64) {
	pc := sys.GetCallerPC()
	kolkovAtomicStore64(addr, value, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicStoreUintptr sync/atomic.StoreUintptr
func kolkovSyncAtomicStoreUintptr(addr *uintptr, value uintptr) {
	pc := sys.GetCallerPC()
	kolkovAtomicStoreUintptr(addr, value, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicSwapInt32 sync/atomic.SwapInt32
//go:nocheckptr
func kolkovSyncAtomicSwapInt32(addr *int32, new int32) int32 {
	pc := sys.GetCallerPC()
	return int32(kolkovAtomicSwap32((*uint32)(unsafe.Pointer(addr)), uint32(new), pc))
}

//go:nosplit
//go:linkname kolkovSyncAtomicSwapUint32 sync/atomic.SwapUint32
func kolkovSyncAtomicSwapUint32(addr *uint32, new uint32) uint32 {
	pc := sys.GetCallerPC()
	return kolkovAtomicSwap32(addr, new, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicSwapInt64 sync/atomic.SwapInt64
//go:nocheckptr
func kolkovSyncAtomicSwapInt64(addr *int64, new int64) int64 {
	pc := sys.GetCallerPC()
	return int64(kolkovAtomicSwap64((*uint64)(unsafe.Pointer(addr)), uint64(new), pc))
}

//go:nosplit
//go:linkname kolkovSyncAtomicSwapUint64 sync/atomic.SwapUint64
func kolkovSyncAtomicSwapUint64(addr *uint64, new uint64) uint64 {
	pc := sys.GetCallerPC()
	return kolkovAtomicSwap64(addr, new, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicSwapUintptr sync/atomic.SwapUintptr
func kolkovSyncAtomicSwapUintptr(addr *uintptr, new uintptr) uintptr {
	pc := sys.GetCallerPC()
	return kolkovAtomicSwapUintptr(addr, new, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicCompareAndSwapInt32 sync/atomic.CompareAndSwapInt32
//go:nocheckptr
func kolkovSyncAtomicCompareAndSwapInt32(addr *int32, old, new int32) bool {
	pc := sys.GetCallerPC()
	return kolkovAtomicCAS32((*uint32)(unsafe.Pointer(addr)), uint32(old), uint32(new), pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicCompareAndSwapUint32 sync/atomic.CompareAndSwapUint32
func kolkovSyncAtomicCompareAndSwapUint32(addr *uint32, old, new uint32) bool {
	pc := sys.GetCallerPC()
	return kolkovAtomicCAS32(addr, old, new, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicCompareAndSwapInt64 sync/atomic.CompareAndSwapInt64
//go:nocheckptr
func kolkovSyncAtomicCompareAndSwapInt64(addr *int64, old, new int64) bool {
	pc := sys.GetCallerPC()
	return kolkovAtomicCAS64((*uint64)(unsafe.Pointer(addr)), uint64(old), uint64(new), pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicCompareAndSwapUint64 sync/atomic.CompareAndSwapUint64
func kolkovSyncAtomicCompareAndSwapUint64(addr *uint64, old, new uint64) bool {
	pc := sys.GetCallerPC()
	return kolkovAtomicCAS64(addr, old, new, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicCompareAndSwapUintptr sync/atomic.CompareAndSwapUintptr
func kolkovSyncAtomicCompareAndSwapUintptr(addr *uintptr, old, new uintptr) bool {
	pc := sys.GetCallerPC()
	return kolkovAtomicCASUintptr(addr, old, new, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicAddInt32 sync/atomic.AddInt32
//go:nocheckptr
func kolkovSyncAtomicAddInt32(addr *int32, delta int32) int32 {
	pc := sys.GetCallerPC()
	return int32(kolkovAtomicAdd32((*uint32)(unsafe.Pointer(addr)), uint32(delta), pc))
}

//go:nosplit
//go:linkname kolkovSyncAtomicAddUint32 sync/atomic.AddUint32
func kolkovSyncAtomicAddUint32(addr *uint32, delta uint32) uint32 {
	pc := sys.GetCallerPC()
	return kolkovAtomicAdd32(addr, delta, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicAddInt64 sync/atomic.AddInt64
//go:nocheckptr
func kolkovSyncAtomicAddInt64(addr *int64, delta int64) int64 {
	pc := sys.GetCallerPC()
	return int64(kolkovAtomicAdd64((*uint64)(unsafe.Pointer(addr)), uint64(delta), pc))
}

//go:nosplit
//go:linkname kolkovSyncAtomicAddUint64 sync/atomic.AddUint64
func kolkovSyncAtomicAddUint64(addr *uint64, delta uint64) uint64 {
	pc := sys.GetCallerPC()
	return kolkovAtomicAdd64(addr, delta, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicAddUintptr sync/atomic.AddUintptr
func kolkovSyncAtomicAddUintptr(addr *uintptr, delta uintptr) uintptr {
	pc := sys.GetCallerPC()
	return kolkovAtomicAddUintptr(addr, delta, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicAndInt32 sync/atomic.AndInt32
//go:nocheckptr
func kolkovSyncAtomicAndInt32(addr *int32, mask int32) int32 {
	pc := sys.GetCallerPC()
	return int32(kolkovAtomicAnd32((*uint32)(unsafe.Pointer(addr)), uint32(mask), pc))
}

//go:nosplit
//go:linkname kolkovSyncAtomicAndUint32 sync/atomic.AndUint32
func kolkovSyncAtomicAndUint32(addr *uint32, mask uint32) uint32 {
	pc := sys.GetCallerPC()
	return kolkovAtomicAnd32(addr, mask, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicAndInt64 sync/atomic.AndInt64
//go:nocheckptr
func kolkovSyncAtomicAndInt64(addr *int64, mask int64) int64 {
	pc := sys.GetCallerPC()
	return int64(kolkovAtomicAnd64((*uint64)(unsafe.Pointer(addr)), uint64(mask), pc))
}

//go:nosplit
//go:linkname kolkovSyncAtomicAndUint64 sync/atomic.AndUint64
func kolkovSyncAtomicAndUint64(addr *uint64, mask uint64) uint64 {
	pc := sys.GetCallerPC()
	return kolkovAtomicAnd64(addr, mask, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicAndUintptr sync/atomic.AndUintptr
func kolkovSyncAtomicAndUintptr(addr *uintptr, mask uintptr) uintptr {
	pc := sys.GetCallerPC()
	return kolkovAtomicAndUintptr(addr, mask, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicOrInt32 sync/atomic.OrInt32
//go:nocheckptr
func kolkovSyncAtomicOrInt32(addr *int32, mask int32) int32 {
	pc := sys.GetCallerPC()
	return int32(kolkovAtomicOr32((*uint32)(unsafe.Pointer(addr)), uint32(mask), pc))
}

//go:nosplit
//go:linkname kolkovSyncAtomicOrUint32 sync/atomic.OrUint32
func kolkovSyncAtomicOrUint32(addr *uint32, mask uint32) uint32 {
	pc := sys.GetCallerPC()
	return kolkovAtomicOr32(addr, mask, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicOrInt64 sync/atomic.OrInt64
//go:nocheckptr
func kolkovSyncAtomicOrInt64(addr *int64, mask int64) int64 {
	pc := sys.GetCallerPC()
	return int64(kolkovAtomicOr64((*uint64)(unsafe.Pointer(addr)), uint64(mask), pc))
}

//go:nosplit
//go:linkname kolkovSyncAtomicOrUint64 sync/atomic.OrUint64
func kolkovSyncAtomicOrUint64(addr *uint64, mask uint64) uint64 {
	pc := sys.GetCallerPC()
	return kolkovAtomicOr64(addr, mask, pc)
}

//go:nosplit
//go:linkname kolkovSyncAtomicOrUintptr sync/atomic.OrUintptr
func kolkovSyncAtomicOrUintptr(addr *uintptr, mask uintptr) uintptr {
	pc := sys.GetCallerPC()
	return kolkovAtomicOrUintptr(addr, mask, pc)
}

// kolkovAtomicUserPC removes sync/atomic method frames between a pointer
// primitive and its user. In particular, atomic.Value implements its operations
// with pointer primitives, and recording the Value method itself would make the
// saved access site useless. Frame-pointer unwinding keeps this off the general
// runtime.Callers path on amd64 and arm64; other architectures use the runtime
// unwinder only when a sync/atomic wrapper is actually present.
//
// fp is the public pointer provider's frame pointer. pc is its return PC.
//
//go:nosplit
func kolkovAtomicUserPC(pc, fp uintptr) uintptr {
	if !kolkovAtomicSyncFrame(pc) {
		return pc
	}
	if goarch.IsAmd64 != 0 || goarch.IsArm64 != 0 {
		for i := 0; i < 4 && kolkovAtomicSyncFrame(pc); i++ {
			if fp == 0 {
				return pc
			}
			fp = *(*uintptr)(unsafe.Pointer(fp))
			if fp == 0 {
				return pc
			}
			pc = *(*uintptr)(unsafe.Pointer(fp + goarch.PtrSize))
		}
		return pc
	}
	return kolkovAtomicUserPCSlow(pc)
}

//go:nosplit
func kolkovAtomicSyncFrame(pc uintptr) bool {
	if pc == 0 {
		return false
	}
	entry := &kolkovAtomicSyncFrameCache[(pc^(pc>>11))&uintptr(len(kolkovAtomicSyncFrameCache)-1)]
	if entry.state.LoadAcquire() == kolkovAtomicSyncFrameCacheReady && entry.pc == pc {
		return entry.sync
	}
	sync := kolkovAtomicSyncFrameUncached(pc)
	if entry.state.CompareAndSwap(kolkovAtomicSyncFrameCacheEmpty, kolkovAtomicSyncFrameCacheWriting) {
		entry.pc = pc
		entry.sync = sync
		entry.state.StoreRelease(kolkovAtomicSyncFrameCacheReady)
	}
	return sync
}

const (
	kolkovAtomicSyncFrameCacheEmpty = iota
	kolkovAtomicSyncFrameCacheWriting
	kolkovAtomicSyncFrameCacheReady
	kolkovAtomicSyncFrameCacheSize = 1024
)

// Each exact-PC cache slot is written at most once. Immutable publication
// avoids torn key/value pairs without a lock or generation-wrap argument;
// collisions remain ordinary uncached lookups and can never guess. The table
// is intentionally bounded because code PCs and their classification are
// process-lifetime constants.
type kolkovAtomicSyncFrameCacheEntry struct {
	state atomic.Uint32
	pc    uintptr
	sync  bool
}

var kolkovAtomicSyncFrameCache [kolkovAtomicSyncFrameCacheSize]kolkovAtomicSyncFrameCacheEntry

//go:nosplit
func kolkovAtomicSyncFrameUncached(pc uintptr) bool {
	name := funcname(findfunc(pc - 1))
	const prefix = "sync/atomic."
	return len(name) >= len(prefix) && name[:len(prefix)] == prefix
}

//go:noinline
func kolkovAtomicUserPCSlow(fallback uintptr) uintptr {
	var pcs [8]uintptr
	// callers, this helper, and the public pointer provider precede the
	// sync/atomic method that we need to skip.
	n := callers(3, pcs[:])
	for _, pc := range pcs[:n] {
		if !kolkovAtomicSyncFrame(pc) {
			return pc
		}
	}
	return fallback
}

// kolkovAtomic64MustPanic identifies the recoverable alignment failure enforced
// by the standard 32-bit atomic implementations. Callers execute the hardware
// primitive directly in this case, before AtomicBegin can retain detector locks;
// the primitive remains responsible for producing the standard panic value.
//
//go:nosplit
func kolkovAtomic64MustPanic(addr *uint64) bool {
	return goarch.PtrSize == 4 && uintptr(unsafe.Pointer(addr))&7 != 0
}

// kolkovAtomicPlainBegin keeps the existing general bridge ABI intact while
// dispatching only enabled, aligned Load/Store call sites to the detector's
// atomic-only enrollment path. Ignored operations deliberately use the general
// path so they retire releases and permanently escape an enrolled generation.
//
//go:nosplit
func kolkovAtomicPlainBegin(addr, size, racectx uintptr, acquire, synchronize bool, token *[8]unsafe.Pointer) uintptr {
	if synchronize {
		return kolkovApiAtomicBeginPlain(addr, size, racectx, acquire, token)
	}
	return kolkovApiAtomicBegin(addr, size, racectx, false, token)
}

//go:nosplit
func kolkovAtomicLoadBegin(addr, size, racectx uintptr, synchronize bool, token *[8]unsafe.Pointer) (context uintptr, retry bool) {
	if synchronize {
		return kolkovApiAtomicBeginLoadCooperative(addr, size, racectx, token)
	}
	return kolkovApiAtomicBegin(addr, size, racectx, false, token), false
}

//go:nosplit
func kolkovAtomicStoreBegin(addr, size, racectx uintptr, synchronize bool, token *[8]unsafe.Pointer) (context uintptr, retry bool) {
	if synchronize {
		return kolkovApiAtomicBeginStoreCooperative(addr, size, racectx, token)
	}
	return kolkovApiAtomicBegin(addr, size, racectx, false, token), false
}

// kolkovAtomicRMWBegin lets an enabled aligned RMW reuse or canonically enroll
// an exact capability. Ignored and incompatible-shape operations retain the
// general path and its full ordinary-state transaction.
//
//go:nosplit
func kolkovAtomicRMWBegin(addr, size, pc, racectx uintptr, synchronize bool, token *[8]unsafe.Pointer) (context uintptr, retry, direct, spin, polite bool, park *uint32) {
	return kolkovApiAtomicBeginInternalRMWCooperative(addr, size, pc, racectx, synchronize, token)
}

// kolkovAtomicRMWSynchronize excludes only internal/sync Mutex atomics. Mutex
// publishes the precise Go memory-model edge with explicit race annotations;
// the detector still records its atomic/plain access history. Public atomics
// retain full synchronization.
//
//go:nosplit
func kolkovAtomicRMWSynchronize(gp *g, pc uintptr) bool {
	return gp.raceignore == 0 && !kolkovApiAtomicInternalRMWPC(pc)
}

// procyield is a pause-count on most targets, but an approximate nanosecond
// delay on arm64. Keep the portable count deliberately small while giving
// arm64 enough elapsed time for the short detector critical section to finish.
const kolkovAtomicRetryMaxDelay = uint32(32 + goarch.IsArm64*(1<<20-32))

const (
	kolkovAtomicPoliteDelay  = uint32(64 + goarch.IsArm64*(10000-64))
	kolkovAtomicPatientDelay = uint32(256 + goarch.IsArm64*(40000-256))
)

// kolkovAtomicRetry backs off on the user goroutine after a clean exact-lock
// miss. Short ownership intervals normally resolve during bounded processor
// pauses; after saturation, scheduling another goroutine prevents a descheduled
// owner from being starved. Detector capabilities and tokens have already been
// released before this function is called.
//
//go:nosplit
func kolkovAtomicRetry(delay *uint32) {
	if *delay == 0 {
		*delay = 1
	}
	if *delay <= kolkovAtomicRetryMaxDelay {
		procyield(*delay)
		*delay <<= 1
		return
	}
	*delay = 1
	goschedguarded()
}

// kolkovAtomicRMWRetry waits for a retained spinner's one-bit ownership-epoch
// doorbell, parks a later retained waiter, or backs off after a clean queue
// miss. A retained wait is already a bounded delay, so it restarts the clean
// miss backoff for any later enrollment attempt.
//
//go:nosplit
func kolkovAtomicRMWRetry(delay *uint32, park *uint32, patient unsafe.Pointer, spin, polite bool) (resume bool) {
	if spin {
		if park == nil {
			throw("race detector missing atomic RMW spinner doorbell")
		}
		*delay = 1
		for attempts := uint32(0); atomic.Load(park) == 0; attempts++ {
			if polite {
				politeDelay := kolkovAtomicPoliteDelay
				if patient != nil {
					politeDelay = kolkovAtomicPatientDelay
				}
				procyield(politeDelay)
			} else {
				procyield(32)
			}
			if attempts&63 == 63 {
				goschedguarded()
			}
		}
		return true
	}
	if park != nil {
		*delay = 1
		// Normal handoff remains LIFO to preserve exact-address/TID locality. A
		// global FIFO queue rotates ownership across the whole waiter population
		// and makes exact causal projection dominate the hardware operation.
		// Queue progress is provided by the detector's finite spinner/cohort
		// epochs rather than by changing the semaphore's admission order.
		// Some atomic implementations (notably atomic.Value's first store) pin
		// the current P across their hardware operation. semacquire1 must not
		// park such a goroutine: procPin is represented by m.locks, and park_m
		// would reach the scheduler while still holding that lock. The detector
		// has already reserved this waiter a logical handoff, so it must still
		// consume exactly one semaphore permit before resuming. Detector owners
		// never block while holding the corresponding state lock, making a
		// bounded processor wait the scheduler-safe form of that handoff.
		if canPreemptM(getg().m) {
			semacquire1(park, true, 0, 0, waitReasonSemacquire)
		} else {
			for !cansemacquire(park) {
				procyield(uint32(64 + goarch.IsArm64*(10000-64)))
			}
		}
		return true
	}
	kolkovAtomicRetry(delay)
	return false
}

//go:nosplit
func kolkovAtomicRMWWake(addr *uint32) {
	if addr != nil {
		semrelease1Direct(addr, true, 0)
	}
}

func kolkovAtomicLoad32(addr *uint32, pc uintptr) (value uint32) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.Load(addr)
	}
	racectx := gp.racectx
	synchronize := gp.raceignore == 0
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	fastAttempt := synchronize && racectx > 1
	retryDelay := uint32(1)
	for {
		var retry bool
		systemstack(func() {
			if fastAttempt {
				fastAttempt = false
				var revision, generation uint64
				if kolkovApiAtomicLoadFastBegin(uintptr(unsafe.Pointer(addr)), 4, pc, racectx, &token, &revision, &generation) {
					value = atomic.Load(addr)
					if kolkovApiAtomicLoadFastEnd(racectx, &token, revision, generation) {
						context = racectx
						return
					}
					value = 0
				}
			}
			context, retry = kolkovAtomicLoadBegin(uintptr(unsafe.Pointer(addr)), 4, racectx, synchronize, &token)
			if retry {
				return
			}
			value = atomic.Load(addr)
			if synchronize {
				kolkovApiAtomicLoadEnd(uintptr(unsafe.Pointer(addr)), 4, pc, context, &token)
			} else {
				kolkovApiAtomicEnd(uintptr(unsafe.Pointer(addr)), 4, pc, context, &token, false, false)
			}
		})
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		kolkovAtomicRetry(&retryDelay)
	}
	gp.raceguard--
	return value
}

func kolkovAtomicLoad64(addr *uint64, pc uintptr) (value uint64) {
	if kolkovAtomic64MustPanic(addr) {
		return atomic.Load64(addr)
	}
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.Load64(addr)
	}
	racectx := gp.racectx
	synchronize := gp.raceignore == 0
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	fastAttempt := synchronize && racectx > 1
	retryDelay := uint32(1)
	for {
		var retry bool
		systemstack(func() {
			if fastAttempt {
				fastAttempt = false
				var revision, generation uint64
				if kolkovApiAtomicLoadFastBegin(uintptr(unsafe.Pointer(addr)), 8, pc, racectx, &token, &revision, &generation) {
					value = atomic.Load64(addr)
					if kolkovApiAtomicLoadFastEnd(racectx, &token, revision, generation) {
						context = racectx
						return
					}
					value = 0
				}
			}
			context, retry = kolkovAtomicLoadBegin(uintptr(unsafe.Pointer(addr)), 8, racectx, synchronize, &token)
			if retry {
				return
			}
			value = atomic.Load64(addr)
			if synchronize {
				kolkovApiAtomicLoadEnd(uintptr(unsafe.Pointer(addr)), 8, pc, context, &token)
			} else {
				kolkovApiAtomicEnd(uintptr(unsafe.Pointer(addr)), 8, pc, context, &token, false, false)
			}
		})
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		kolkovAtomicRetry(&retryDelay)
	}
	gp.raceguard--
	return value
}

func kolkovAtomicLoadUintptr(addr *uintptr, pc uintptr) (value uintptr) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.Loaduintptr(addr)
	}
	racectx := gp.racectx
	synchronize := gp.raceignore == 0
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	fastAttempt := synchronize && racectx > 1
	retryDelay := uint32(1)
	for {
		var retry bool
		systemstack(func() {
			if fastAttempt {
				fastAttempt = false
				var revision, generation uint64
				if kolkovApiAtomicLoadFastBegin(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, racectx, &token, &revision, &generation) {
					value = atomic.Loaduintptr(addr)
					if kolkovApiAtomicLoadFastEnd(racectx, &token, revision, generation) {
						context = racectx
						return
					}
					value = 0
				}
			}
			// A second hardware load is reachable only after fast validation has
			// rejected the speculative value. It must be repeated inside the locked
			// Begin/End transaction so the acquired release matches that value's
			// modification order.
			context, retry = kolkovAtomicLoadBegin(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, racectx, synchronize, &token)
			if retry {
				return
			}
			value = atomic.Loaduintptr(addr)
			if synchronize {
				kolkovApiAtomicLoadEnd(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, context, &token)
			} else {
				kolkovApiAtomicEnd(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, context, &token, false, false)
			}
		})
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		kolkovAtomicRetry(&retryDelay)
	}
	gp.raceguard--
	return value
}

func kolkovAtomicLoadPointer(addr *unsafe.Pointer, pc uintptr) (value unsafe.Pointer) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.Loadp(unsafe.Pointer(addr))
	}
	racectx := gp.racectx
	synchronize := gp.raceignore == 0
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	fastAttempt := synchronize && racectx > 1
	retryDelay := uint32(1)
	for {
		var retry bool
		systemstack(func() {
			if fastAttempt {
				fastAttempt = false
				var revision, generation uint64
				if kolkovApiAtomicLoadFastBegin(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, racectx, &token, &revision, &generation) {
					value = atomic.Loadp(unsafe.Pointer(addr))
					if kolkovApiAtomicLoadFastEnd(racectx, &token, revision, generation) {
						context = racectx
						return
					}
					value = nil
				}
			}
			context, retry = kolkovAtomicLoadBegin(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, racectx, synchronize, &token)
			if retry {
				return
			}
			value = atomic.Loadp(unsafe.Pointer(addr))
			if synchronize {
				kolkovApiAtomicLoadEnd(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, context, &token)
			} else {
				kolkovApiAtomicEnd(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, context, &token, false, false)
			}
		})
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		kolkovAtomicRetry(&retryDelay)
	}
	gp.raceguard--
	return value
}

func kolkovAtomicStore32(addr *uint32, value uint32, pc uintptr) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		atomic.Store(addr, value)
		return
	}
	racectx := gp.racectx
	synchronize := gp.raceignore == 0
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	retryDelay := uint32(1)
	for {
		var retry bool
		systemstack(func() {
			context, retry = kolkovAtomicStoreBegin(uintptr(unsafe.Pointer(addr)), 4, racectx, synchronize, &token)
			if retry {
				return
			}
			atomic.Store(addr, value)
			kolkovApiAtomicEnd(uintptr(unsafe.Pointer(addr)), 4, pc, context, &token, true, synchronize)
		})
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		kolkovAtomicRetry(&retryDelay)
	}
	gp.raceguard--
}

func kolkovAtomicStore64(addr *uint64, value uint64, pc uintptr) {
	if kolkovAtomic64MustPanic(addr) {
		atomic.Store64(addr, value)
		return
	}
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		atomic.Store64(addr, value)
		return
	}
	racectx := gp.racectx
	synchronize := gp.raceignore == 0
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	retryDelay := uint32(1)
	for {
		var retry bool
		systemstack(func() {
			context, retry = kolkovAtomicStoreBegin(uintptr(unsafe.Pointer(addr)), 8, racectx, synchronize, &token)
			if retry {
				return
			}
			atomic.Store64(addr, value)
			kolkovApiAtomicEnd(uintptr(unsafe.Pointer(addr)), 8, pc, context, &token, true, synchronize)
		})
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		kolkovAtomicRetry(&retryDelay)
	}
	gp.raceguard--
}

func kolkovAtomicStoreUintptr(addr *uintptr, value uintptr, pc uintptr) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		atomic.Storeuintptr(addr, value)
		return
	}
	racectx := gp.racectx
	synchronize := gp.raceignore == 0
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	retryDelay := uint32(1)
	for {
		var retry bool
		systemstack(func() {
			context, retry = kolkovAtomicStoreBegin(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, racectx, synchronize, &token)
			if retry {
				return
			}
			atomic.Storeuintptr(addr, value)
			kolkovApiAtomicEnd(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, context, &token, true, synchronize)
		})
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		kolkovAtomicRetry(&retryDelay)
	}
	gp.raceguard--
}

// These small no-split primitives keep the write barrier and hardware update
// adjacent. This is the ordering required by mwbbuf: no preemption point may
// permit a GC phase transition between buffering the barrier and publishing
// the pointer.
//
//go:nosplit
func kolkovAtomicStorePointerHardware(addr *unsafe.Pointer, new unsafe.Pointer) {
	raw := noescape(unsafe.Pointer(addr))
	if writeBarrier.enabled {
		atomicwb(addr, new)
	}
	atomic.StorepNoWB(raw, new)
}

//go:nosplit
func kolkovAtomicSwapPointerHardware(addr *unsafe.Pointer, new unsafe.Pointer) (old unsafe.Pointer) {
	word := (*uintptr)(noescape(unsafe.Pointer(addr)))
	if writeBarrier.enabled {
		atomicwb(addr, new)
	}
	old = unsafe.Pointer(atomic.Xchguintptr(word, uintptr(new)))
	KeepAlive(addr)
	KeepAlive(new)
	KeepAlive(old)
	return old
}

//go:nosplit
func kolkovAtomicCASPointerHardware(addr *unsafe.Pointer, old, new unsafe.Pointer) (swapped bool) {
	if writeBarrier.enabled {
		atomicwb(addr, new)
	}
	swapped = atomic.Casp1(addr, old, new)
	KeepAlive(addr)
	KeepAlive(old)
	KeepAlive(new)
	return swapped
}

// The detector-facing pointer variants intentionally do not reuse the uintptr
// handlers. Their typed arguments and results remain GC roots in the suspended
// user-G frame while detector work runs. Detector Begin precedes the barrier
// test so a GC phase change during Begin cannot cause a missed write barrier.
func kolkovAtomicStorePointer(addr *unsafe.Pointer, new unsafe.Pointer, pc uintptr) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		kolkovAtomicStorePointerHardware(addr, new)
		KeepAlive(addr)
		KeepAlive(new)
		return
	}
	racectx := gp.racectx
	synchronize := gp.raceignore == 0
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	retryDelay := uint32(1)
	for {
		var retry bool
		systemstack(func() {
			context, retry = kolkovAtomicStoreBegin(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, racectx, synchronize, &token)
			if retry {
				return
			}
			kolkovAtomicStorePointerHardware(addr, new)
			kolkovApiAtomicEnd(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, context, &token, true, synchronize)
		})
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		kolkovAtomicRetry(&retryDelay)
	}
	gp.raceguard--
	KeepAlive(addr)
	KeepAlive(new)
}

func kolkovAtomicSwapPointer(addr *unsafe.Pointer, new unsafe.Pointer, pc uintptr) (old unsafe.Pointer) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		old = kolkovAtomicSwapPointerHardware(addr, new)
		KeepAlive(addr)
		KeepAlive(new)
		KeepAlive(old)
		return old
	}
	racectx := gp.racectx
	synchronize := kolkovAtomicRMWSynchronize(gp, pc)
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	var park, wake *uint32
	var resume, spin, polite bool
	retryDelay := uint32(1)
	for {
		var retry bool
		var direct bool
		systemstack(func() {
			if resume {
				resume = false
				context, retry, spin, polite, park = kolkovApiAtomicResumeRMW(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, racectx, synchronize, &token)
				direct = false
			} else {
				context, retry, direct, spin, polite, park = kolkovAtomicRMWBegin(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, racectx, synchronize, &token)
			}
			if retry {
				return
			}
			old = kolkovAtomicSwapPointerHardware(addr, new)
			wake = kolkovApiAtomicEndInternalRMW(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, context, &token, true, synchronize, direct)
		})
		kolkovAtomicRMWWake(wake)
		wake = nil
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		resume = kolkovAtomicRMWRetry(&retryDelay, park, token[2], spin, polite)
		park = nil
	}
	gp.raceguard--
	KeepAlive(addr)
	KeepAlive(new)
	KeepAlive(old)
	return old
}

func kolkovAtomicCASPointer(addr *unsafe.Pointer, old, new unsafe.Pointer, pc uintptr) (swapped bool) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		swapped = kolkovAtomicCASPointerHardware(addr, old, new)
		KeepAlive(addr)
		KeepAlive(old)
		KeepAlive(new)
		return swapped
	}
	racectx := gp.racectx
	synchronize := kolkovAtomicRMWSynchronize(gp, pc)
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	var park, wake *uint32
	var resume, spin, polite bool
	retryDelay := uint32(1)
	for {
		var retry bool
		var direct bool
		systemstack(func() {
			if resume {
				resume = false
				context, retry, spin, polite, park = kolkovApiAtomicResumeRMW(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, racectx, synchronize, &token)
				direct = false
			} else {
				context, retry, direct, spin, polite, park = kolkovAtomicRMWBegin(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, racectx, synchronize, &token)
			}
			if retry {
				return
			}
			swapped = kolkovAtomicCASPointerHardware(addr, old, new)
			wake = kolkovApiAtomicEndInternalRMW(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, context, &token, swapped, synchronize, direct)
		})
		kolkovAtomicRMWWake(wake)
		wake = nil
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		resume = kolkovAtomicRMWRetry(&retryDelay, park, token[2], spin, polite)
		park = nil
	}
	gp.raceguard--
	KeepAlive(addr)
	KeepAlive(old)
	KeepAlive(new)
	return swapped
}

func kolkovAtomicSwap32(addr *uint32, new uint32, pc uintptr) (old uint32) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.Xchg(addr, new)
	}
	racectx := gp.racectx
	synchronize := kolkovAtomicRMWSynchronize(gp, pc)
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	var park, wake *uint32
	var resume, spin, polite bool
	retryDelay := uint32(1)
	for {
		var retry bool
		var direct bool
		systemstack(func() {
			if resume {
				resume = false
				context, retry, spin, polite, park = kolkovApiAtomicResumeRMW(uintptr(unsafe.Pointer(addr)), 4, racectx, synchronize, &token)
				direct = false
			} else {
				context, retry, direct, spin, polite, park = kolkovAtomicRMWBegin(uintptr(unsafe.Pointer(addr)), 4, pc, racectx, synchronize, &token)
			}
			if retry {
				return
			}
			old = atomic.Xchg(addr, new)
			wake = kolkovApiAtomicEndInternalRMW(uintptr(unsafe.Pointer(addr)), 4, pc, context, &token, true, synchronize, direct)
		})
		kolkovAtomicRMWWake(wake)
		wake = nil
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		resume = kolkovAtomicRMWRetry(&retryDelay, park, token[2], spin, polite)
		park = nil
	}
	gp.raceguard--
	return old
}

func kolkovAtomicSwap64(addr *uint64, new uint64, pc uintptr) (old uint64) {
	if kolkovAtomic64MustPanic(addr) {
		return atomic.Xchg64(addr, new)
	}
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.Xchg64(addr, new)
	}
	racectx := gp.racectx
	synchronize := kolkovAtomicRMWSynchronize(gp, pc)
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	var park, wake *uint32
	var resume, spin, polite bool
	retryDelay := uint32(1)
	for {
		var retry bool
		var direct bool
		systemstack(func() {
			if resume {
				resume = false
				context, retry, spin, polite, park = kolkovApiAtomicResumeRMW(uintptr(unsafe.Pointer(addr)), 8, racectx, synchronize, &token)
				direct = false
			} else {
				context, retry, direct, spin, polite, park = kolkovAtomicRMWBegin(uintptr(unsafe.Pointer(addr)), 8, pc, racectx, synchronize, &token)
			}
			if retry {
				return
			}
			old = atomic.Xchg64(addr, new)
			wake = kolkovApiAtomicEndInternalRMW(uintptr(unsafe.Pointer(addr)), 8, pc, context, &token, true, synchronize, direct)
		})
		kolkovAtomicRMWWake(wake)
		wake = nil
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		resume = kolkovAtomicRMWRetry(&retryDelay, park, token[2], spin, polite)
		park = nil
	}
	gp.raceguard--
	return old
}

func kolkovAtomicSwapUintptr(addr *uintptr, new uintptr, pc uintptr) (old uintptr) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.Xchguintptr(addr, new)
	}
	racectx := gp.racectx
	synchronize := kolkovAtomicRMWSynchronize(gp, pc)
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	var park, wake *uint32
	var resume, spin, polite bool
	retryDelay := uint32(1)
	for {
		var retry bool
		var direct bool
		systemstack(func() {
			if resume {
				resume = false
				context, retry, spin, polite, park = kolkovApiAtomicResumeRMW(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, racectx, synchronize, &token)
				direct = false
			} else {
				context, retry, direct, spin, polite, park = kolkovAtomicRMWBegin(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, racectx, synchronize, &token)
			}
			if retry {
				return
			}
			old = atomic.Xchguintptr(addr, new)
			wake = kolkovApiAtomicEndInternalRMW(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, context, &token, true, synchronize, direct)
		})
		kolkovAtomicRMWWake(wake)
		wake = nil
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		resume = kolkovAtomicRMWRetry(&retryDelay, park, token[2], spin, polite)
		park = nil
	}
	gp.raceguard--
	return old
}

func kolkovAtomicCAS32(addr *uint32, old, new uint32, pc uintptr) (swapped bool) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.Cas(addr, old, new)
	}
	racectx := gp.racectx
	synchronize := kolkovAtomicRMWSynchronize(gp, pc)
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	var park, wake *uint32
	var resume, spin, polite bool
	retryDelay := uint32(1)
	for {
		var retry bool
		var direct bool
		systemstack(func() {
			if resume {
				resume = false
				context, retry, spin, polite, park = kolkovApiAtomicResumeRMW(uintptr(unsafe.Pointer(addr)), 4, racectx, synchronize, &token)
				direct = false
			} else {
				context, retry, direct, spin, polite, park = kolkovAtomicRMWBegin(uintptr(unsafe.Pointer(addr)), 4, pc, racectx, synchronize, &token)
			}
			if retry {
				return
			}
			swapped = atomic.Cas(addr, old, new)
			wake = kolkovApiAtomicEndInternalRMW(uintptr(unsafe.Pointer(addr)), 4, pc, context, &token, swapped, synchronize, direct)
		})
		kolkovAtomicRMWWake(wake)
		wake = nil
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		resume = kolkovAtomicRMWRetry(&retryDelay, park, token[2], spin, polite)
		park = nil
	}
	gp.raceguard--
	return swapped
}

func kolkovAtomicCAS64(addr *uint64, old, new uint64, pc uintptr) (swapped bool) {
	if kolkovAtomic64MustPanic(addr) {
		return atomic.Cas64(addr, old, new)
	}
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.Cas64(addr, old, new)
	}
	racectx := gp.racectx
	synchronize := kolkovAtomicRMWSynchronize(gp, pc)
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	var park, wake *uint32
	var resume, spin, polite bool
	retryDelay := uint32(1)
	for {
		var retry bool
		var direct bool
		systemstack(func() {
			if resume {
				resume = false
				context, retry, spin, polite, park = kolkovApiAtomicResumeRMW(uintptr(unsafe.Pointer(addr)), 8, racectx, synchronize, &token)
				direct = false
			} else {
				context, retry, direct, spin, polite, park = kolkovAtomicRMWBegin(uintptr(unsafe.Pointer(addr)), 8, pc, racectx, synchronize, &token)
			}
			if retry {
				return
			}
			swapped = atomic.Cas64(addr, old, new)
			wake = kolkovApiAtomicEndInternalRMW(uintptr(unsafe.Pointer(addr)), 8, pc, context, &token, swapped, synchronize, direct)
		})
		kolkovAtomicRMWWake(wake)
		wake = nil
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		resume = kolkovAtomicRMWRetry(&retryDelay, park, token[2], spin, polite)
		park = nil
	}
	gp.raceguard--
	return swapped
}

func kolkovAtomicCASUintptr(addr *uintptr, old, new uintptr, pc uintptr) (swapped bool) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.Casuintptr(addr, old, new)
	}
	racectx := gp.racectx
	synchronize := kolkovAtomicRMWSynchronize(gp, pc)
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	var park, wake *uint32
	var resume, spin, polite bool
	retryDelay := uint32(1)
	for {
		var retry bool
		var direct bool
		systemstack(func() {
			if resume {
				resume = false
				context, retry, spin, polite, park = kolkovApiAtomicResumeRMW(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, racectx, synchronize, &token)
				direct = false
			} else {
				context, retry, direct, spin, polite, park = kolkovAtomicRMWBegin(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, racectx, synchronize, &token)
			}
			if retry {
				return
			}
			swapped = atomic.Casuintptr(addr, old, new)
			wake = kolkovApiAtomicEndInternalRMW(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, context, &token, swapped, synchronize, direct)
		})
		kolkovAtomicRMWWake(wake)
		wake = nil
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		resume = kolkovAtomicRMWRetry(&retryDelay, park, token[2], spin, polite)
		park = nil
	}
	gp.raceguard--
	return swapped
}

func kolkovAtomicAdd32(addr *uint32, delta uint32, pc uintptr) (value uint32) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.Xadd(addr, int32(delta))
	}
	racectx := gp.racectx
	synchronize := kolkovAtomicRMWSynchronize(gp, pc)
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	var park, wake *uint32
	var resume, spin, polite bool
	retryDelay := uint32(1)
	for {
		var retry bool
		var direct bool
		systemstack(func() {
			if resume {
				resume = false
				context, retry, spin, polite, park = kolkovApiAtomicResumeRMW(uintptr(unsafe.Pointer(addr)), 4, racectx, synchronize, &token)
				direct = false
			} else {
				context, retry, direct, spin, polite, park = kolkovAtomicRMWBegin(uintptr(unsafe.Pointer(addr)), 4, pc, racectx, synchronize, &token)
			}
			if retry {
				return
			}
			value = atomic.Xadd(addr, int32(delta))
			wake = kolkovApiAtomicEndInternalRMW(uintptr(unsafe.Pointer(addr)), 4, pc, context, &token, true, synchronize, direct)
		})
		kolkovAtomicRMWWake(wake)
		wake = nil
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		resume = kolkovAtomicRMWRetry(&retryDelay, park, token[2], spin, polite)
		park = nil
	}
	gp.raceguard--
	return value
}

func kolkovAtomicAdd64(addr *uint64, delta uint64, pc uintptr) (value uint64) {
	if kolkovAtomic64MustPanic(addr) {
		return atomic.Xadd64(addr, int64(delta))
	}
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.Xadd64(addr, int64(delta))
	}
	racectx := gp.racectx
	synchronize := kolkovAtomicRMWSynchronize(gp, pc)
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	var park, wake *uint32
	var resume, spin, polite bool
	retryDelay := uint32(1)
	for {
		var retry bool
		var direct bool
		systemstack(func() {
			if resume {
				resume = false
				context, retry, spin, polite, park = kolkovApiAtomicResumeRMW(uintptr(unsafe.Pointer(addr)), 8, racectx, synchronize, &token)
				direct = false
			} else {
				context, retry, direct, spin, polite, park = kolkovAtomicRMWBegin(uintptr(unsafe.Pointer(addr)), 8, pc, racectx, synchronize, &token)
			}
			if retry {
				return
			}
			value = atomic.Xadd64(addr, int64(delta))
			wake = kolkovApiAtomicEndInternalRMW(uintptr(unsafe.Pointer(addr)), 8, pc, context, &token, true, synchronize, direct)
		})
		kolkovAtomicRMWWake(wake)
		wake = nil
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		resume = kolkovAtomicRMWRetry(&retryDelay, park, token[2], spin, polite)
		park = nil
	}
	gp.raceguard--
	return value
}

func kolkovAtomicAddUintptr(addr *uintptr, delta uintptr, pc uintptr) (value uintptr) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.Xadduintptr(addr, delta)
	}
	racectx := gp.racectx
	synchronize := kolkovAtomicRMWSynchronize(gp, pc)
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	var park, wake *uint32
	var resume, spin, polite bool
	retryDelay := uint32(1)
	for {
		var retry bool
		var direct bool
		systemstack(func() {
			if resume {
				resume = false
				context, retry, spin, polite, park = kolkovApiAtomicResumeRMW(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, racectx, synchronize, &token)
				direct = false
			} else {
				context, retry, direct, spin, polite, park = kolkovAtomicRMWBegin(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, racectx, synchronize, &token)
			}
			if retry {
				return
			}
			value = atomic.Xadduintptr(addr, delta)
			wake = kolkovApiAtomicEndInternalRMW(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, context, &token, true, synchronize, direct)
		})
		kolkovAtomicRMWWake(wake)
		wake = nil
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		resume = kolkovAtomicRMWRetry(&retryDelay, park, token[2], spin, polite)
		park = nil
	}
	gp.raceguard--
	return value
}

func kolkovAtomicAnd32(addr *uint32, mask uint32, pc uintptr) (old uint32) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.And32(addr, mask)
	}
	racectx := gp.racectx
	synchronize := kolkovAtomicRMWSynchronize(gp, pc)
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	var park, wake *uint32
	var resume, spin, polite bool
	retryDelay := uint32(1)
	for {
		var retry bool
		var direct bool
		systemstack(func() {
			if resume {
				resume = false
				context, retry, spin, polite, park = kolkovApiAtomicResumeRMW(uintptr(unsafe.Pointer(addr)), 4, racectx, synchronize, &token)
				direct = false
			} else {
				context, retry, direct, spin, polite, park = kolkovAtomicRMWBegin(uintptr(unsafe.Pointer(addr)), 4, pc, racectx, synchronize, &token)
			}
			if retry {
				return
			}
			old = atomic.And32(addr, mask)
			wake = kolkovApiAtomicEndInternalRMW(uintptr(unsafe.Pointer(addr)), 4, pc, context, &token, true, synchronize, direct)
		})
		kolkovAtomicRMWWake(wake)
		wake = nil
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		resume = kolkovAtomicRMWRetry(&retryDelay, park, token[2], spin, polite)
		park = nil
	}
	gp.raceguard--
	return old
}

func kolkovAtomicAnd64(addr *uint64, mask uint64, pc uintptr) (old uint64) {
	if kolkovAtomic64MustPanic(addr) {
		return atomic.And64(addr, mask)
	}
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.And64(addr, mask)
	}
	racectx := gp.racectx
	synchronize := kolkovAtomicRMWSynchronize(gp, pc)
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	var park, wake *uint32
	var resume, spin, polite bool
	retryDelay := uint32(1)
	for {
		var retry bool
		var direct bool
		systemstack(func() {
			if resume {
				resume = false
				context, retry, spin, polite, park = kolkovApiAtomicResumeRMW(uintptr(unsafe.Pointer(addr)), 8, racectx, synchronize, &token)
				direct = false
			} else {
				context, retry, direct, spin, polite, park = kolkovAtomicRMWBegin(uintptr(unsafe.Pointer(addr)), 8, pc, racectx, synchronize, &token)
			}
			if retry {
				return
			}
			old = atomic.And64(addr, mask)
			wake = kolkovApiAtomicEndInternalRMW(uintptr(unsafe.Pointer(addr)), 8, pc, context, &token, true, synchronize, direct)
		})
		kolkovAtomicRMWWake(wake)
		wake = nil
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		resume = kolkovAtomicRMWRetry(&retryDelay, park, token[2], spin, polite)
		park = nil
	}
	gp.raceguard--
	return old
}

func kolkovAtomicAndUintptr(addr *uintptr, mask uintptr, pc uintptr) (old uintptr) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.Anduintptr(addr, mask)
	}
	racectx := gp.racectx
	synchronize := kolkovAtomicRMWSynchronize(gp, pc)
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	var park, wake *uint32
	var resume, spin, polite bool
	retryDelay := uint32(1)
	for {
		var retry bool
		var direct bool
		systemstack(func() {
			if resume {
				resume = false
				context, retry, spin, polite, park = kolkovApiAtomicResumeRMW(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, racectx, synchronize, &token)
				direct = false
			} else {
				context, retry, direct, spin, polite, park = kolkovAtomicRMWBegin(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, racectx, synchronize, &token)
			}
			if retry {
				return
			}
			old = atomic.Anduintptr(addr, mask)
			wake = kolkovApiAtomicEndInternalRMW(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, context, &token, true, synchronize, direct)
		})
		kolkovAtomicRMWWake(wake)
		wake = nil
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		resume = kolkovAtomicRMWRetry(&retryDelay, park, token[2], spin, polite)
		park = nil
	}
	gp.raceguard--
	return old
}

func kolkovAtomicOr32(addr *uint32, mask uint32, pc uintptr) (old uint32) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.Or32(addr, mask)
	}
	racectx := gp.racectx
	synchronize := kolkovAtomicRMWSynchronize(gp, pc)
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	var park, wake *uint32
	var resume, spin, polite bool
	retryDelay := uint32(1)
	for {
		var retry bool
		var direct bool
		systemstack(func() {
			if resume {
				resume = false
				context, retry, spin, polite, park = kolkovApiAtomicResumeRMW(uintptr(unsafe.Pointer(addr)), 4, racectx, synchronize, &token)
				direct = false
			} else {
				context, retry, direct, spin, polite, park = kolkovAtomicRMWBegin(uintptr(unsafe.Pointer(addr)), 4, pc, racectx, synchronize, &token)
			}
			if retry {
				return
			}
			old = atomic.Or32(addr, mask)
			wake = kolkovApiAtomicEndInternalRMW(uintptr(unsafe.Pointer(addr)), 4, pc, context, &token, true, synchronize, direct)
		})
		kolkovAtomicRMWWake(wake)
		wake = nil
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		resume = kolkovAtomicRMWRetry(&retryDelay, park, token[2], spin, polite)
		park = nil
	}
	gp.raceguard--
	return old
}

func kolkovAtomicOr64(addr *uint64, mask uint64, pc uintptr) (old uint64) {
	if kolkovAtomic64MustPanic(addr) {
		return atomic.Or64(addr, mask)
	}
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.Or64(addr, mask)
	}
	racectx := gp.racectx
	synchronize := kolkovAtomicRMWSynchronize(gp, pc)
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	var park, wake *uint32
	var resume, spin, polite bool
	retryDelay := uint32(1)
	for {
		var retry bool
		var direct bool
		systemstack(func() {
			if resume {
				resume = false
				context, retry, spin, polite, park = kolkovApiAtomicResumeRMW(uintptr(unsafe.Pointer(addr)), 8, racectx, synchronize, &token)
				direct = false
			} else {
				context, retry, direct, spin, polite, park = kolkovAtomicRMWBegin(uintptr(unsafe.Pointer(addr)), 8, pc, racectx, synchronize, &token)
			}
			if retry {
				return
			}
			old = atomic.Or64(addr, mask)
			wake = kolkovApiAtomicEndInternalRMW(uintptr(unsafe.Pointer(addr)), 8, pc, context, &token, true, synchronize, direct)
		})
		kolkovAtomicRMWWake(wake)
		wake = nil
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		resume = kolkovAtomicRMWRetry(&retryDelay, park, token[2], spin, polite)
		park = nil
	}
	gp.raceguard--
	return old
}

func kolkovAtomicOrUintptr(addr *uintptr, mask uintptr, pc uintptr) (old uintptr) {
	gp := getg()
	if addr == nil || gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return atomic.Oruintptr(addr, mask)
	}
	racectx := gp.racectx
	synchronize := kolkovAtomicRMWSynchronize(gp, pc)
	gp.raceguard++
	var token [8]unsafe.Pointer
	var context uintptr
	var park, wake *uint32
	var resume, spin, polite bool
	retryDelay := uint32(1)
	for {
		var retry bool
		var direct bool
		systemstack(func() {
			if resume {
				resume = false
				context, retry, spin, polite, park = kolkovApiAtomicResumeRMW(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, racectx, synchronize, &token)
				direct = false
			} else {
				context, retry, direct, spin, polite, park = kolkovAtomicRMWBegin(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, racectx, synchronize, &token)
			}
			if retry {
				return
			}
			old = atomic.Oruintptr(addr, mask)
			wake = kolkovApiAtomicEndInternalRMW(uintptr(unsafe.Pointer(addr)), goarch.PtrSize, pc, context, &token, true, synchronize, direct)
		})
		kolkovAtomicRMWWake(wake)
		wake = nil
		if racectx <= 1 && context > 1 {
			gp.racectx = context
			kolkovCacheShadowPtr()
			racectx = context
		}
		if !retry {
			break
		}
		resume = kolkovAtomicRMWRetry(&retryDelay, park, token[2], spin, polite)
		park = nil
	}
	gp.raceguard--
	return old
}
