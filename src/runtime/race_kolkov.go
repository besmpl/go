// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

// Pure-Go race detector implementation.
// This file provides the race detection API when building with -race
// but CGO_ENABLED=0. It replaces ThreadSanitizer (C++) with a pure Go
// implementation based on the FastTrack algorithm.

package runtime

import (
	"internal/abi"
	"internal/runtime/sys"
	"unsafe"
)

// Public race detection API, present when built with -race and CGO_ENABLED=0.

// RaceRead records a read of the memory location addr by the current goroutine.
// This function is called by the compiler-inserted instrumentation.
//
//go:nosplit
func RaceRead(addr unsafe.Pointer) {
	raceread(uintptr(addr))
}

//go:linkname race_Read internal/race.Read
//go:nosplit
func race_Read(addr unsafe.Pointer) {
	RaceRead(addr)
}

// RaceWrite records a write to the memory location addr by the current goroutine.
// This function is called by the compiler-inserted instrumentation.
//
//go:nosplit
func RaceWrite(addr unsafe.Pointer) {
	racewrite(uintptr(addr))
}

//go:linkname race_Write internal/race.Write
//go:nosplit
func race_Write(addr unsafe.Pointer) {
	RaceWrite(addr)
}

// RaceReadRange records a read of the memory range [addr, addr+len) by the current goroutine.
// This function is called by the compiler-inserted instrumentation.
//
//go:nosplit
func RaceReadRange(addr unsafe.Pointer, len int) {
	racereadrange(uintptr(addr), uintptr(len))
}

//go:linkname race_ReadRange internal/race.ReadRange
//go:nosplit
func race_ReadRange(addr unsafe.Pointer, len int) {
	RaceReadRange(addr, len)
}

// RaceWriteRange records a write to the memory range [addr, addr+len) by the current goroutine.
// This function is called by the compiler-inserted instrumentation.
//
//go:nosplit
func RaceWriteRange(addr unsafe.Pointer, len int) {
	racewriterange(uintptr(addr), uintptr(len))
}

//go:linkname race_WriteRange internal/race.WriteRange
//go:nosplit
func race_WriteRange(addr unsafe.Pointer, len int) {
	RaceWriteRange(addr, len)
}

// RaceErrors returns the number of races detected by the race detector.
//
//go:nosplit
func RaceErrors() int {
	return kolkovRaceErrors()
}

//go:linkname race_Errors internal/race.Errors
//go:nosplit
func race_Errors() int {
	return RaceErrors()
}

// RaceAcquire establishes a happens-before relation with the preceding
// RaceReleaseMerge on addr up to and including the last RaceRelease on addr.
// In terms of the C memory model (C11 §5.1.2.4, §7.17.3),
// RaceAcquire is equivalent to atomic_load(memory_order_acquire).
//
//go:nosplit
func RaceAcquire(addr unsafe.Pointer) {
	raceacquire(addr)
}

//go:linkname race_Acquire internal/race.Acquire
//go:nosplit
func race_Acquire(addr unsafe.Pointer) {
	RaceAcquire(addr)
}

// RaceRelease performs a release operation on addr that
// can synchronize with a later RaceAcquire on addr.
//
// In terms of the C memory model, RaceRelease is equivalent to
// atomic_store(memory_order_release).
//
//go:nosplit
func RaceRelease(addr unsafe.Pointer) {
	racerelease(addr)
}

//go:linkname race_Release internal/race.Release
//go:nosplit
func race_Release(addr unsafe.Pointer) {
	RaceRelease(addr)
}

// RaceReleaseMerge is like RaceRelease, but also establishes a happens-before
// relation with the preceding RaceRelease or RaceReleaseMerge on addr.
//
// In terms of the C memory model, RaceReleaseMerge is equivalent to
// atomic_exchange(memory_order_release).
//
//go:nosplit
func RaceReleaseMerge(addr unsafe.Pointer) {
	racereleasemerge(addr)
}

//go:linkname race_ReleaseMerge internal/race.ReleaseMerge
//go:nosplit
func race_ReleaseMerge(addr unsafe.Pointer) {
	RaceReleaseMerge(addr)
}

// RaceDisable disables handling of race synchronization events in the current goroutine.
// Handling is re-enabled with RaceEnable. RaceDisable/RaceEnable can be nested.
// Non-synchronization events (memory accesses, function entry/exit) still affect
// the race detector.
//
//go:nosplit
func RaceDisable() {
	gp := getg()
	gp.raceignore++
	// TODO: Notify the detector to ignore synchronization events
}

//go:linkname race_Disable internal/race.Disable
//go:nosplit
func race_Disable() {
	RaceDisable()
}

// RaceEnable re-enables handling of race events in the current goroutine.
//
//go:nosplit
func RaceEnable() {
	gp := getg()
	gp.raceignore--
	// TODO: Notify the detector to resume synchronization tracking
}

//go:linkname race_Enable internal/race.Enable
//go:nosplit
func race_Enable() {
	RaceEnable()
}

// Private interface for the runtime.

const raceenabled = true

// raceReadObjectPC records a read of an object by the current goroutine.
// For composite objects (array, struct), it reads the entire object.
// For non-composite objects, it reads just the first byte.
func raceReadObjectPC(t *_type, addr unsafe.Pointer, callerpc, pc uintptr) {
	kind := t.Kind()
	if kind == abi.Array || kind == abi.Struct {
		// for composite objects we have to read every address
		// because a write might happen to any subobject.
		racereadrangepc(addr, t.Size_, callerpc, pc)
	} else {
		// for non-composite objects we can read just the start
		// address, as any write must write the first byte.
		racereadpc(addr, callerpc, pc)
	}
}

//go:linkname race_ReadObjectPC internal/race.ReadObjectPC
func race_ReadObjectPC(t *abi.Type, addr unsafe.Pointer, callerpc, pc uintptr) {
	raceReadObjectPC(t, addr, callerpc, pc)
}

// raceWriteObjectPC records a write of an object by the current goroutine.
// For composite objects (array, struct), it writes the entire object.
// For non-composite objects, it writes just the first byte.
func raceWriteObjectPC(t *_type, addr unsafe.Pointer, callerpc, pc uintptr) {
	kind := t.Kind()
	if kind == abi.Array || kind == abi.Struct {
		// for composite objects we have to write every address
		// because a write might happen to any subobject.
		racewriterangepc(addr, t.Size_, callerpc, pc)
	} else {
		// for non-composite objects we can write just the start
		// address, as any write must write the first byte.
		racewritepc(addr, callerpc, pc)
	}
}

//go:linkname race_WriteObjectPC internal/race.WriteObjectPC
func race_WriteObjectPC(t *abi.Type, addr unsafe.Pointer, callerpc, pc uintptr) {
	raceWriteObjectPC(t, addr, callerpc, pc)
}

// racereadpc records a read of the memory location addr with explicit PC values.
//
//go:nosplit
func racereadpc(addr unsafe.Pointer, callpc, pc uintptr) {
	gp := getg()
	if gp == nil || gp.m == nil || gp.m.curg == nil {
		return
	}
	if gp.raceignore != 0 {
		return
	}
	gp.raceignore++
	systemstack(func() {
		kolkovOnRead(uintptr(addr), pc)
	})
	gp.raceignore--
}

// racewritepc records a write of the memory location addr with explicit PC values.
//
//go:nosplit
func racewritepc(addr unsafe.Pointer, callpc, pc uintptr) {
	gp := getg()
	if gp == nil || gp.m == nil || gp.m.curg == nil {
		return
	}
	if gp.raceignore != 0 {
		return
	}
	gp.raceignore++
	systemstack(func() {
		kolkovOnWrite(uintptr(addr), pc)
	})
	gp.raceignore--
}

//go:linkname race_ReadPC internal/race.ReadPC
func race_ReadPC(addr unsafe.Pointer, callerpc, pc uintptr) {
	racereadpc(addr, callerpc, pc)
}

//go:linkname race_WritePC internal/race.WritePC
func race_WritePC(addr unsafe.Pointer, callerpc, pc uintptr) {
	racewritepc(addr, callerpc, pc)
}

// raceinit initializes the race detector.
// It returns the global context and the main goroutine context.
//
//go:nosplit
func raceinit() (gctx, pctx uintptr) {
	lockInit(&raceFiniLock, lockRankRaceFini)

	// Initialize global detector state
	raceKolkovInit()

	// Return non-zero sentinel values so that runtime stores them in
	// g.racectx and p.racectx. raceacquirectx checks "if racectx == 0 { return }"
	// so returning 0 would cause the main goroutine to be silently skipped.
	gctx = 1
	pctx = 1

	return
}

// racefini finalizes the race detector and prints any detected races.
//
//go:nosplit
func racefini() {
	// racefini() can only be called once to avoid races.
	lock(&raceFiniLock)

	// TODO: Print race report and cleanup
	raceKolkovFini()
}

// raceproccreate creates a new processor context.
//
//go:nosplit
func raceproccreate() uintptr {
	// TODO: Create a new processor context for the pure-Go detector
	return 0
}

// raceprocdestroy destroys a processor context.
//
//go:nosplit
func raceprocdestroy(ctx uintptr) {
	// TODO: Destroy the processor context
}

// racemapshadow maps shadow memory for the given memory range.
//
//go:nosplit
func racemapshadow(addr unsafe.Pointer, size uintptr) {
	// TODO: Map shadow memory for the address range
	// This is called when the heap grows or new data segments are mapped
}

// racemalloc notifies the race detector of a memory allocation.
// Clears shadow memory for the allocated range to prevent false positives
// from stale access history when the allocator reuses addresses.
//
//go:nosplit
func racemalloc(p unsafe.Pointer, sz uintptr) {
	gp := getg()
	if gp.m != nil && gp.m.curg != nil {
		gp = gp.m.curg
	}
	if gp.raceignore != 0 {
		return
	}
	gp.raceignore++
	systemstack(func() {
		kolkovApiClearShadow(uintptr(p), sz)
	})
	gp.raceignore--
}

// racefree notifies the race detector of a memory free.
// Clears shadow memory for the freed range to prevent false positives
// when the allocator reuses the same addresses for new objects.
//
//go:nosplit
func racefree(p unsafe.Pointer, sz uintptr) {
	gp := getg()
	if gp.m != nil && gp.m.curg != nil {
		gp = gp.m.curg
	}
	if gp.raceignore != 0 {
		return
	}
	gp.raceignore++
	systemstack(func() {
		kolkovApiClearShadow(uintptr(p), sz)
	})
	gp.raceignore--
}

// racegostart notifies the race detector that a new goroutine is starting.
// Called by proc.go's newproc1 (on systemstack) when creating a goroutine.
//
// IMPORTANT: This runs on g0 (systemstack), so getg() returns g0.
// We use gp.m.curg to get the parent (spawning) goroutine and pass its
// goid explicitly to the API, since the API cannot extract goid on g0.
//
// The return value is stored in newg.racectx by proc.go.
// For Kolkov detector, racectx is not used (API tracks contexts by goid).
// We return a non-zero value so proc.go doesn't think initialization failed.
//
//go:nosplit
func racegostart(pc uintptr) uintptr {
	gp := getg()
	// Get the parent goroutine (the one that called 'go func()').
	// On systemstack, gp is g0; gp.m.curg is the user goroutine.
	var spawng *g
	if gp.m.curg != nil {
		spawng = gp.m.curg
	} else {
		spawng = gp
	}

	if spawng.raceignore != 0 {
		return 0
	}
	spawng.raceignore++
	systemstack(func() {
		kolkovApiOnGoStart(pc, int64(spawng.goid))
	})
	spawng.raceignore--

	// Return non-zero so proc.go stores it in newg.racectx.
	// The Kolkov API tracks contexts by goid, not racectx.
	return 1
}

// racegosetchildid associates the most recently created spawn context with
// the actual child goroutine goid. Called by proc.go right after racegostart.
//
//go:nosplit
func racegosetchildid(childGoid uint64) {
	gp := getg()
	if gp.m != nil && gp.m.curg != nil {
		gp = gp.m.curg
	}
	if gp.raceignore != 0 {
		return
	}
	gp.raceignore++
	systemstack(func() {
		kolkovApiGoSetChildID(int64(childGoid))
	})
	gp.raceignore--
}

// racegoend notifies the race detector that the current goroutine is ending.
// Called by proc.go when a goroutine exits.
//
// Passes the goroutine's goid explicitly to the API for context cleanup.
//
//go:nosplit
func racegoend() {
	gp := getg()
	if gp.m.curg != nil {
		gp = gp.m.curg
	}
	if gp.raceignore != 0 {
		return
	}
	gp.raceignore++
	systemstack(func() {
		kolkovApiOnGoEnd(int64(gp.goid))
	})
	gp.raceignore--
}

// racectxstart creates a new race context for a goroutine.
//
//go:nosplit
func racectxstart(pc, spawnctx uintptr) uintptr {
	// TODO: Create a new context with explicit spawn context
	return 0
}

// racectxend ends a race context.
//
//go:nosplit
func racectxend(racectx uintptr) {
	// TODO: End the race context
}

// racewriterangepc records a write to the memory range [addr, addr+sz) with explicit PC.
//
//go:nosplit
func racewriterangepc(addr unsafe.Pointer, sz, callpc, pc uintptr) {
	gp := getg()
	if gp != gp.m.curg {
		// The call is coming from manual instrumentation of Go code running on g0/gsignal.
		// Not interesting.
		return
	}
	if callpc != 0 {
		racefuncenter(callpc)
	}
	racewriterangepc1(uintptr(addr), sz, pc)
	if callpc != 0 {
		racefuncexit()
	}
}

// racereadrangepc records a read of the memory range [addr, addr+sz) with explicit PC.
//
//go:nosplit
func racereadrangepc(addr unsafe.Pointer, sz, callpc, pc uintptr) {
	gp := getg()
	if gp != gp.m.curg {
		// The call is coming from manual instrumentation of Go code running on g0/gsignal.
		// Not interesting.
		return
	}
	if callpc != 0 {
		racefuncenter(callpc)
	}
	racereadrangepc1(uintptr(addr), sz, pc)
	if callpc != 0 {
		racefuncexit()
	}
}

// raceacquire records an acquire operation on the given address.
//
//go:nosplit
func raceacquire(addr unsafe.Pointer) {
	raceacquireg(getg(), addr)
}

// raceacquireg records an acquire operation on the given address for a specific goroutine.
// The target goroutine gp may differ from the current goroutine (e.g., in channel
// operations where one goroutine acquires on behalf of its blocked partner).
//
//go:nosplit
func raceacquireg(gp *g, addr unsafe.Pointer) {
	if gp.raceignore != 0 {
		return
	}
	// Protect the current goroutine from re-entrant race detector calls
	// during the API call (which may trigger instrumented memory accesses).
	curg := getg()
	if curg.m != nil && curg.m.curg != nil {
		curg = curg.m.curg
	}
	curg.raceignore++
	systemstack(func() {
		kolkovApiOnAcquireForGoroutine(uintptr(addr), int64(gp.goid))
	})
	curg.raceignore--
}

// raceacquirectx records an acquire operation with an explicit context.
// The racectx parameter is the context of the goroutine to acquire on behalf of.
// For the Kolkov detector, we just perform the acquire on the current goroutine.
//
//go:nosplit
func raceacquirectx(racectx uintptr, addr unsafe.Pointer) {
	if racectx == 0 {
		return
	}
	gp := getg()
	if gp.m != nil && gp.m.curg != nil {
		gp = gp.m.curg
	}
	if gp.raceignore != 0 {
		return
	}
	gp.raceignore++
	systemstack(func() {
		kolkovOnAcquire(uintptr(addr))
	})
	gp.raceignore--
}

// racerelease records a release operation on the given address.
//
//go:nosplit
func racerelease(addr unsafe.Pointer) {
	racereleaseg(getg(), addr)
}

// racereleaseg records a release operation on the given address for a specific goroutine.
// The target goroutine gp may differ from the current goroutine (e.g., in channel
// operations where one goroutine releases on behalf of its blocked partner).
//
//go:nosplit
func racereleaseg(gp *g, addr unsafe.Pointer) {
	if gp.raceignore != 0 {
		return
	}
	curg := getg()
	if curg.m != nil && curg.m.curg != nil {
		curg = curg.m.curg
	}
	curg.raceignore++
	systemstack(func() {
		kolkovApiOnReleaseForGoroutine(uintptr(addr), int64(gp.goid))
	})
	curg.raceignore--
}

// racereleaseacquire records a combined release-acquire operation.
//
//go:nosplit
func racereleaseacquire(addr unsafe.Pointer) {
	racereleaseacquireg(getg(), addr)
}

// racereleaseacquireg records a combined release-acquire operation for a specific goroutine.
// The target goroutine gp may differ from the current goroutine.
//
//go:nosplit
func racereleaseacquireg(gp *g, addr unsafe.Pointer) {
	if gp.raceignore != 0 {
		return
	}
	curg := getg()
	if curg.m != nil && curg.m.curg != nil {
		curg = curg.m.curg
	}
	curg.raceignore++
	systemstack(func() {
		kolkovApiOnReleaseForGoroutine(uintptr(addr), int64(gp.goid))
		kolkovApiOnAcquireForGoroutine(uintptr(addr), int64(gp.goid))
	})
	curg.raceignore--
}

// racereleasemerge records a release-merge operation on the given address.
//
//go:nosplit
func racereleasemerge(addr unsafe.Pointer) {
	racereleasemergeg(getg(), addr)
}

// racereleasemergeg records a release-merge operation for a specific goroutine.
// The target goroutine gp may differ from the current goroutine (e.g., in channel
// operations where one goroutine releases on behalf of its blocked partner).
//
//go:nosplit
func racereleasemergeg(gp *g, addr unsafe.Pointer) {
	if gp.raceignore != 0 {
		return
	}
	curg := getg()
	if curg.m != nil && curg.m.curg != nil {
		curg = curg.m.curg
	}
	curg.raceignore++
	systemstack(func() {
		kolkovApiOnReleaseMergeForGoroutine(uintptr(addr), int64(gp.goid))
	})
	curg.raceignore--
}

// racefingo notifies the race detector that the current goroutine is a finalizer.
//
//go:nosplit
func racefingo() {
	// TODO: Special handling for finalizer goroutines
}

// Hot-path race detection functions.
// These are called directly by compiler-generated instrumentation.
// Previously implemented in assembly (race_kolkov_*.s), now pure Go
// per @randall77 guidance: no assembly needed, use sys.GetCallerPC().

// raceread records a read of the given address.
// Called from compiler-generated instrumentation.
// sys.GetCallerPC() must be the first call to capture instrumented code's PC.
//
//go:nosplit
func raceread(addr uintptr) {
	pc := sys.GetCallerPC()
	gp := getg()
	if gp == nil || gp.m == nil || gp.m.curg == nil {
		return
	}
	if gp.raceignore != 0 {
		return
	}
	gp.raceignore++
	systemstack(func() {
		kolkovOnRead(addr, pc)
	})
	gp.raceignore--
}

// racewrite records a write to the given address.
// Called from compiler-generated instrumentation.
//
//go:nosplit
func racewrite(addr uintptr) {
	pc := sys.GetCallerPC()
	gp := getg()
	if gp == nil || gp.m == nil || gp.m.curg == nil {
		return
	}
	if gp.raceignore != 0 {
		return
	}
	gp.raceignore++
	systemstack(func() {
		kolkovOnWrite(addr, pc)
	})
	gp.raceignore--
}

// racereadrange records a read of the given address range.
// Called from compiler-generated instrumentation.
//
//go:nosplit
func racereadrange(addr, size uintptr) {
	pc := sys.GetCallerPC()
	gp := getg()
	if gp == nil || gp.m == nil || gp.m.curg == nil {
		return
	}
	if gp.raceignore != 0 {
		return
	}
	gp.raceignore++
	systemstack(func() {
		// Track base address for now. Full range tracking is T11.
		kolkovOnRead(addr, pc)
	})
	gp.raceignore--
}

// racewriterange records a write to the given address range.
// Called from compiler-generated instrumentation.
//
//go:nosplit
func racewriterange(addr, size uintptr) {
	pc := sys.GetCallerPC()
	gp := getg()
	if gp == nil || gp.m == nil || gp.m.curg == nil {
		return
	}
	if gp.raceignore != 0 {
		return
	}
	gp.raceignore++
	systemstack(func() {
		// Track base address for now. Full range tracking is T11.
		kolkovOnWrite(addr, pc)
	})
	gp.raceignore--
}

// racereadrangepc1 is the internal implementation for range reads with explicit PC.
//
//go:nosplit
func racereadrangepc1(addr, size, pc uintptr) {
	gp := getg()
	if gp == nil || gp.m == nil || gp.m.curg == nil {
		return
	}
	if gp.raceignore != 0 {
		return
	}
	gp.raceignore++
	systemstack(func() {
		kolkovOnRead(addr, pc)
	})
	gp.raceignore--
}

// racewriterangepc1 is the internal implementation for range writes with explicit PC.
//
//go:nosplit
func racewriterangepc1(addr, size, pc uintptr) {
	gp := getg()
	if gp == nil || gp.m == nil || gp.m.curg == nil {
		return
	}
	if gp.raceignore != 0 {
		return
	}
	gp.raceignore++
	systemstack(func() {
		kolkovOnWrite(addr, pc)
	})
	gp.raceignore--
}

// racefuncenter records function entry.
// Called by compiler at every function entry when race detection is enabled.
// No-op for now — stack traces use sys.GetCallerPC() instead.
//
//go:nosplit
func racefuncenter(callpc uintptr) {
}

// racefuncenterfp records function entry using frame pointer.
//
//go:nosplit
func racefuncenterfp(fp uintptr) {
}

// racefuncexit records function exit.
//
//go:nosplit
func racefuncexit() {
}

// Pure-Go detector implementation stubs
// These will be implemented as the detector is ported to runtime.

// raceKolkovInit initializes the pure-Go race detector.
//
//go:nosplit
func raceKolkovInit() {
	kolkovDetectorInit()
}

// raceKolkovFini finalizes the pure-Go race detector.
//
//go:nosplit
func raceKolkovFini() {
	kolkovDetectorFini()
}

// The declarations below generate ABI wrappers for functions
// implemented in assembly in this package but declared in another
// package.

//go:linkname abigen_sync_atomic_LoadInt32 sync/atomic.LoadInt32
func abigen_sync_atomic_LoadInt32(addr *int32) (val int32)

//go:linkname abigen_sync_atomic_LoadInt64 sync/atomic.LoadInt64
func abigen_sync_atomic_LoadInt64(addr *int64) (val int64)

//go:linkname abigen_sync_atomic_LoadUint32 sync/atomic.LoadUint32
func abigen_sync_atomic_LoadUint32(addr *uint32) (val uint32)

//go:linkname abigen_sync_atomic_LoadUint64 sync/atomic.LoadUint64
func abigen_sync_atomic_LoadUint64(addr *uint64) (val uint64)

//go:linkname abigen_sync_atomic_LoadUintptr sync/atomic.LoadUintptr
func abigen_sync_atomic_LoadUintptr(addr *uintptr) (val uintptr)

//go:linkname abigen_sync_atomic_LoadPointer sync/atomic.LoadPointer
func abigen_sync_atomic_LoadPointer(addr *unsafe.Pointer) (val unsafe.Pointer)

//go:linkname abigen_sync_atomic_StoreInt32 sync/atomic.StoreInt32
func abigen_sync_atomic_StoreInt32(addr *int32, val int32)

//go:linkname abigen_sync_atomic_StoreInt64 sync/atomic.StoreInt64
func abigen_sync_atomic_StoreInt64(addr *int64, val int64)

//go:linkname abigen_sync_atomic_StoreUint32 sync/atomic.StoreUint32
func abigen_sync_atomic_StoreUint32(addr *uint32, val uint32)

//go:linkname abigen_sync_atomic_StoreUint64 sync/atomic.StoreUint64
func abigen_sync_atomic_StoreUint64(addr *uint64, val uint64)

//go:linkname abigen_sync_atomic_SwapInt32 sync/atomic.SwapInt32
func abigen_sync_atomic_SwapInt32(addr *int32, new int32) (old int32)

//go:linkname abigen_sync_atomic_SwapInt64 sync/atomic.SwapInt64
func abigen_sync_atomic_SwapInt64(addr *int64, new int64) (old int64)

//go:linkname abigen_sync_atomic_SwapUint32 sync/atomic.SwapUint32
func abigen_sync_atomic_SwapUint32(addr *uint32, new uint32) (old uint32)

//go:linkname abigen_sync_atomic_SwapUint64 sync/atomic.SwapUint64
func abigen_sync_atomic_SwapUint64(addr *uint64, new uint64) (old uint64)

//go:linkname abigen_sync_atomic_AddInt32 sync/atomic.AddInt32
func abigen_sync_atomic_AddInt32(addr *int32, delta int32) (new int32)

//go:linkname abigen_sync_atomic_AddUint32 sync/atomic.AddUint32
func abigen_sync_atomic_AddUint32(addr *uint32, delta uint32) (new uint32)

//go:linkname abigen_sync_atomic_AddInt64 sync/atomic.AddInt64
func abigen_sync_atomic_AddInt64(addr *int64, delta int64) (new int64)

//go:linkname abigen_sync_atomic_AddUint64 sync/atomic.AddUint64
func abigen_sync_atomic_AddUint64(addr *uint64, delta uint64) (new uint64)

//go:linkname abigen_sync_atomic_AddUintptr sync/atomic.AddUintptr
func abigen_sync_atomic_AddUintptr(addr *uintptr, delta uintptr) (new uintptr)

//go:linkname abigen_sync_atomic_AndInt32 sync/atomic.AndInt32
func abigen_sync_atomic_AndInt32(addr *int32, mask int32) (old int32)

//go:linkname abigen_sync_atomic_AndUint32 sync/atomic.AndUint32
func abigen_sync_atomic_AndUint32(addr *uint32, mask uint32) (old uint32)

//go:linkname abigen_sync_atomic_AndInt64 sync/atomic.AndInt64
func abigen_sync_atomic_AndInt64(addr *int64, mask int64) (old int64)

//go:linkname abigen_sync_atomic_AndUint64 sync/atomic.AndUint64
func abigen_sync_atomic_AndUint64(addr *uint64, mask uint64) (old uint64)

//go:linkname abigen_sync_atomic_AndUintptr sync/atomic.AndUintptr
func abigen_sync_atomic_AndUintptr(addr *uintptr, mask uintptr) (old uintptr)

//go:linkname abigen_sync_atomic_OrInt32 sync/atomic.OrInt32
func abigen_sync_atomic_OrInt32(addr *int32, mask int32) (old int32)

//go:linkname abigen_sync_atomic_OrUint32 sync/atomic.OrUint32
func abigen_sync_atomic_OrUint32(addr *uint32, mask uint32) (old uint32)

//go:linkname abigen_sync_atomic_OrInt64 sync/atomic.OrInt64
func abigen_sync_atomic_OrInt64(addr *int64, mask int64) (old int64)

//go:linkname abigen_sync_atomic_OrUint64 sync/atomic.OrUint64
func abigen_sync_atomic_OrUint64(addr *uint64, mask uint64) (old uint64)

//go:linkname abigen_sync_atomic_OrUintptr sync/atomic.OrUintptr
func abigen_sync_atomic_OrUintptr(addr *uintptr, mask uintptr) (old uintptr)

//go:linkname abigen_sync_atomic_CompareAndSwapInt32 sync/atomic.CompareAndSwapInt32
func abigen_sync_atomic_CompareAndSwapInt32(addr *int32, old, new int32) (swapped bool)

//go:linkname abigen_sync_atomic_CompareAndSwapInt64 sync/atomic.CompareAndSwapInt64
func abigen_sync_atomic_CompareAndSwapInt64(addr *int64, old, new int64) (swapped bool)

//go:linkname abigen_sync_atomic_CompareAndSwapUint32 sync/atomic.CompareAndSwapUint32
func abigen_sync_atomic_CompareAndSwapUint32(addr *uint32, old, new uint32) (swapped bool)

//go:linkname abigen_sync_atomic_CompareAndSwapUint64 sync/atomic.CompareAndSwapUint64
func abigen_sync_atomic_CompareAndSwapUint64(addr *uint64, old, new uint64) (swapped bool)
