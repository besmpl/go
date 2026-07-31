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
	"internal/goarch"
	"internal/runtime/atomic"
	"internal/runtime/sys"
	"unsafe"
)

const raceKolkovDefaultExitCode = int32(66)

var (
	raceKolkovExitCode    = raceKolkovDefaultExitCode
	raceKolkovHaltOnError bool
)

// Runtime init runs after schedinit has captured the process environment and
// before the pure-Go detector package can observe an instrumented user access.
// Parse the process-control subset of GORACE here so the access and reporting
// paths only read immutable scalar state.
func init() {
	raceKolkovExitCode, raceKolkovHaltOnError = raceKolkovProcessOptions(gogetenv("GORACE"))
}

// raceKolkovProcessOptions parses the GORACE options that control process
// termination. The documented format is a whitespace-separated list of
// key=value fields. Unknown and malformed fields are ignored, and later valid
// occurrences override earlier ones, matching the startup flag convention.
func raceKolkovProcessOptions(options string) (exitCode int32, haltOnError bool) {
	exitCode = raceKolkovDefaultExitCode
	for options != "" {
		for len(options) != 0 && raceKolkovOptionSpace(options[0]) {
			options = options[1:]
		}
		if options == "" {
			break
		}

		end := 0
		for end < len(options) && !raceKolkovOptionSpace(options[end]) {
			end++
		}
		field := options[:end]
		options = options[end:]

		eq := 0
		for eq < len(field) && field[eq] != '=' {
			eq++
		}
		if eq == len(field) {
			continue
		}
		value, ok := raceKolkovOptionInt32(field[eq+1:])
		if !ok {
			continue
		}
		switch field[:eq] {
		case "exitcode":
			exitCode = value
		case "halt_on_error":
			haltOnError = value != 0
		}
	}
	return
}

func raceKolkovOptionSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

func raceKolkovOptionInt32(value string) (int32, bool) {
	if value == "" {
		return 0, false
	}
	negative := false
	switch value[0] {
	case '-':
		negative = true
		value = value[1:]
	case '+':
		value = value[1:]
	}
	if value == "" {
		return 0, false
	}

	limit := uint64(1<<31 - 1)
	if negative {
		limit++
	}
	var n uint64
	for i := 0; i < len(value); i++ {
		digit := value[i] - '0'
		if digit > 9 || n > (limit-uint64(digit))/10 {
			return 0, false
		}
		n = n*10 + uint64(digit)
	}
	if negative {
		return int32(-int64(n)), true
	}
	return int32(n), true
}

// T26: Inline same-epoch fast path constants and functions.
// These replace the go:linkname CALL to kolkovSameEpochRead/Write,
// eliminating ~12ns cross-package call overhead per memory access.
//
// PageTableShadow layout (from shadowmem/shadow_pagetable.go):
//
//	offset 0:       base (atomic.Uintptr, 8 bytes)
//	offset 8:       pages[0] (65536 * atomic.Pointer[shadowPage], each 8 bytes)
//	offset 524296:  external[0] (65536 atomic.Pointer[externalBlockCell], each 8 bytes)
//
// shadowPage layout:
//
//	offset 0:     slotTables[0] (512 * atomic.Pointer[blockSlotTable], each 8 bytes)
//	offset 4096:  blocks[0] (512 rangeBlock values, each 24 bytes)
//
// rangeBlock layout:
//
//	offset 0:   mu (spinlock, 4 bytes plus alignment padding)
//	offset 8:   state (atomic.Pointer[VarState], 8 bytes)
//	offset 16:  compact (atomic.Pointer[compactGroups], 8 bytes)
//
// compactGroups runtime header:
//
//	offset 0: active (atomic.Uint32, permanent conservative-lookup gate)
//
// externalBlockCell layout:
//
//	offset 0:    base (uintptr)
//	offset 8:    next (*externalBlockCell)
//	offset 16:   block.slots (*blockSlotTable, 512 slot pointers when present)
//	offset 32:   block.history.state (atomic.Pointer[VarState])
//	offset 40:   block.history.compact (atomic.Pointer[compactGroups])
//
// shadowSlot layout:
//
//	offset 0: states[0] (8 * atomic.Pointer[VarState], each 8 bytes)
//
// VarState layout (from shadowmem/varstate.go):
//
//	offset 0:  W (atomic.Uint64, 8 bytes) — write epoch
//	offset 40: readerState (atomic.Uint32, 4 bytes) — reader count
//
// RaceContext ABI-sensitive prefix on 64-bit targets (from
// goroutine/context.go):
//
//	offset 4:  ReadCacheInvalidatedClock (atomic.Uint32)
//	offset 16: Epoch (uint64)
//	offset 24: ReadCache[0] (4 uintptr entries)
//	offset 56: ReadCacheStates[0] (4 unsafe.Pointer entries)
//	offset 88: ReadCacheWidths[0] (4 uint8 entries)
const (
	ptL1Shift       = 21
	ptL1Size        = 65536
	ptL2Mask        = 0x3FFFF                        // (1<<18) - 1
	ptTotalCoverage = uintptr(ptL1Size) << ptL1Shift // 128 GiB
	ptPagesOffset   = 8                              // offset of pages[0] in PageTableShadow

	// A shadowPage's 512-entry lazy table directory is followed by one 24-byte
	// ordinary-history default for each 4KiB application block. Each published
	// table contains 512 word-slot pointers. Keep these values synchronized with
	// shadowmem.shadowPage, blockSlotTable, and rangeBlock;
	// runtime_fastpath_abi_test.go owns the corresponding layout assertions.
	shadowBlockShift           = 12
	shadowBlocksPerPage        = 1 << (ptL1Shift - shadowBlockShift)
	shadowBlockMask            = shadowBlocksPerPage - 1
	shadowBlockWordMask        = (1 << (shadowBlockShift - 3)) - 1
	shadowPageBlocksOffset     = 4096
	shadowRangeBlockSize       = 24
	shadowRangeBlockStateOff   = 8
	shadowRangeBlockCompactOff = 16
	shadowCompactActiveOff     = 0

	// Sparse out-of-window block directory. The bounded traversal is only
	// an optimization: longer collision chains take the detector slow path.
	ptExternalOffset              = ptPagesOffset + ptL1Size*goarch.PtrSize
	externalBlockBuckets          = 1 << 16
	externalBlockMask             = externalBlockBuckets - 1
	externalBlockShift            = 12
	externalBlockWordMask         = (1 << (externalBlockShift - 3)) - 1
	externalBlockCellNextOffset   = goarch.PtrSize
	externalBlockCellSlotsOffset  = 2 * goarch.PtrSize
	externalBlockHistoryOffset    = externalBlockCellSlotsOffset + goarch.PtrSize
	externalBlockHistoryStateOff  = externalBlockHistoryOffset + shadowRangeBlockStateOff
	externalBlockCompactOffset    = externalBlockHistoryOffset + shadowRangeBlockCompactOff
	externalBlockMaxChainFastPath = 8

	shadowHashMultiplier = uint64(0x9E3779B97F4A7C15)

	vsWOffset                          = 0                  // offset of W in VarState
	ctxReadCacheInvalidatedClockOffset = 4                  // offset of ReadCacheInvalidatedClock in RaceContext
	ctxEpochOffset                     = 8 + goarch.PtrSize // TID + marker + C
	ctxReadCacheOffset                 = ctxEpochOffset + 8 // offset of ReadCache[0] in RaceContext
	ctxReadCacheSlots                  = 4
	ctxReadCacheMask                   = ctxReadCacheSlots - 1
	ctxReadStateOffset                 = ctxReadCacheOffset + ctxReadCacheSlots*goarch.PtrSize
	ctxReadWidthOffset                 = ctxReadStateOffset + ctxReadCacheSlots*goarch.PtrSize
	ctxReadWeakWidth                   = uint8(1 << 7)

	// PageTableShadow has this layout only on amd64 and arm64. Other supported
	// race architectures use a fallback-only stub and must take the slow path.
	raceInlineShadowSupported = goarch.IsAmd64 | goarch.IsArm64
)

// ReadCache is owned by its RaceContext. Ordinary accesses happen only while
// that logical goroutine is running; target-g synchronization may advance the
// context only while the target is parked under the runtime synchronization
// primitive. A one-way external observation instead atomically publishes the
// observed source clock in ReadCacheInvalidatedClock; the owner keeps exclusive
// access to the cache entries. racegoend retires the context, and a newly
// allocated context is zero-initialized, so cached addresses never cross a
// context lifetime.

// raceExternalBlockCell returns the exact sparse directory cell used outside
// the primary window. The bounded walk is inlineable into racereadSlowPath;
// missing or unusually long chains take the sound detector slow path.
//
//go:nosplit
func raceExternalBlockCell(shadowPtr, blockBase uintptr) unsafe.Pointer {
	hash := (uint64(blockBase) * shadowHashMultiplier) >> 48
	bucket := uintptr(hash) & externalBlockMask
	cellPtr := atomic.Loadp(unsafe.Pointer(shadowPtr + ptExternalOffset + bucket*goarch.PtrSize))
	for i := 0; i < externalBlockMaxChainFastPath; i++ {
		if cellPtr == nil || *(*uintptr)(cellPtr) == blockBase {
			return cellPtr
		}
		cellPtr = atomic.Loadp(unsafe.Add(cellPtr, externalBlockCellNextOffset))
	}
	return nil
}

// raceShadowState resolves the authoritative exact-address VarState mirrored
// by PageTableShadow. A materialized word is authoritative even when its lane
// is nil. For an unmaterialized word, an active compact header forces a
// conservative detector miss; a hot word promoted out of compact regains exact
// pointer lookup because slot precedence is checked first. Headers are eagerly
// installed before block defaults so allocator clear never allocates, but their
// active word remains zero until the first membership/tombstone publication;
// such default-only blocks retain this fast path.
//
// The returned pointer is a snapshot. Runtime read shortcuts use the mapping
// load itself as their linearization point: ClearRange drains detector
// publishers before replacing the mapping, and cached pointers remain
// GC-visible in RaceContext, so pointer equality cannot suffer an allocator ABA.
//
//go:nosplit
func raceShadowState(shadowPtr, addr uintptr) unsafe.Pointer {
	if raceInlineShadowSupported == 0 || shadowPtr == 0 {
		return nil
	}
	base := atomic.LoadAcquintptr((*uintptr)(unsafe.Pointer(shadowPtr)))
	if base == 0 {
		return nil
	}
	offset := addr - base
	if offset < ptTotalCoverage {
		pagePtr := atomic.Loadp(unsafe.Pointer(shadowPtr + ptPagesOffset + (offset>>ptL1Shift)*goarch.PtrSize))
		if pagePtr == nil {
			return nil
		}
		blockIdx := (offset >> shadowBlockShift) & shadowBlockMask
		if slotsPtr := atomic.Loadp(unsafe.Add(pagePtr, blockIdx*goarch.PtrSize)); slotsPtr != nil {
			wordIdx := (offset >> 3) & shadowBlockWordMask
			if slotPtr := atomic.Loadp(unsafe.Add(slotsPtr, wordIdx*goarch.PtrSize)); slotPtr != nil {
				return atomic.Loadp(unsafe.Add(slotPtr, (offset&7)*goarch.PtrSize))
			}
		}
		blockPtr := unsafe.Add(pagePtr, shadowPageBlocksOffset+blockIdx*shadowRangeBlockSize)
		if compactPtr := atomic.Loadp(unsafe.Add(blockPtr, shadowRangeBlockCompactOff)); compactPtr != nil {
			if atomic.Load((*uint32)(unsafe.Add(compactPtr, shadowCompactActiveOff))) != 0 {
				return nil
			}
		}
		return atomic.Loadp(unsafe.Add(blockPtr, shadowRangeBlockStateOff))
	}

	blockBase := addr &^ (uintptr(1)<<externalBlockShift - 1)
	cellPtr := raceExternalBlockCell(shadowPtr, blockBase)
	if cellPtr == nil {
		return nil
	}
	wordIdx := (addr >> 3) & externalBlockWordMask
	if slotsPtr := atomic.Loadp(unsafe.Add(cellPtr, externalBlockCellSlotsOffset)); slotsPtr != nil {
		slotPtr := atomic.Loadp(unsafe.Add(slotsPtr, wordIdx*goarch.PtrSize))
		if slotPtr != nil {
			return atomic.Loadp(unsafe.Add(slotPtr, (addr&7)*goarch.PtrSize))
		}
	}
	if compactPtr := atomic.Loadp(unsafe.Add(cellPtr, externalBlockCompactOffset)); compactPtr != nil {
		if atomic.Load((*uint32)(unsafe.Add(compactPtr, shadowCompactActiveOff))) != 0 {
			return nil
		}
	}
	return atomic.Loadp(unsafe.Add(cellPtr, externalBlockHistoryStateOff))
}

// raceFirstModuleStaticData reports whether addr belongs to mutable static
// storage in the executable's first module. These sections live for the
// process lifetime, so an exact redundant-read cache entry cannot be revived
// by allocator reuse. Keep the section checks separate: their order and
// contiguity are not guaranteed, in particular with external linking.
//
// Limiting this shortcut to firstmoduledata deliberately leaves plugin globals
// on the exact shadow-state revalidation path.
//
//go:nosplit
func raceFirstModuleStaticData(addr uintptr) bool {
	datap := &firstmoduledata
	return datap.noptrdata <= addr && addr < datap.enoptrdata ||
		datap.data <= addr && addr < datap.edata ||
		datap.bss <= addr && addr < datap.ebss ||
		datap.noptrbss <= addr && addr < datap.enoptrbss
}

// raceFirstModuleStaticDataRange is the compiler-scalar counterpart of
// raceFirstModuleStaticData. It is reached only after racereadn matches an
// exact cache entry published by the validated detector path, and racereadn
// supplies only the compiler's exact 2-, 4-, or 8-byte scalar widths. The
// range is therefore known to be non-empty and non-wrapping. Both endpoints
// must belong to the same mutable static section; adjacency between linker
// sections is not a lifetime guarantee.
//
//go:nosplit
func raceFirstModuleStaticDataRange(addr, size uintptr) bool {
	last := addr + size - 1
	datap := &firstmoduledata
	return datap.noptrdata <= addr && last < datap.enoptrdata ||
		datap.data <= addr && last < datap.edata ||
		datap.bss <= addr && last < datap.ebss ||
		datap.noptrbss <= addr && last < datap.enoptrbss
}

// raceClearReadCache invalidates per-context redundant-read elimination before
// a write can bypass the detector's full path.
//
//go:nosplit
func raceClearReadCache(racectx uintptr) {
	if racectx > 1 {
		*(*[ctxReadCacheSlots]uintptr)(unsafe.Pointer(racectx + ctxReadCacheOffset)) = [ctxReadCacheSlots]uintptr{}
		for i := uintptr(0); i < ctxReadCacheSlots; i++ {
			atomicstorep(unsafe.Pointer(racectx+ctxReadStateOffset+i*goarch.PtrSize), nil)
		}
		*(*[ctxReadCacheSlots]uint8)(unsafe.Pointer(racectx + ctxReadWidthOffset)) = [ctxReadCacheSlots]uint8{}
	}
}

// raceInvalidateReadCache removes the exact address written by a scalar hook.
// Other slots remain represented and can still eliminate redundant reads.
//
//go:nosplit
func raceInvalidateReadCache(racectx, addr uintptr) {
	raceInvalidateReadCacheRange(racectx, addr, 1)
}

// raceAccessRangeValid validates a half-open range without forming a wrapping
// end address.
//
//go:nosplit
func raceAccessRangeValid(addr, size uintptr) bool {
	return size != 0 && size-1 <= ^uintptr(0)-addr
}

// raceInvalidateReadCacheRange removes only exact cached reads covered by a
// range write. Unrelated cache entries remain valid.
//
//go:nosplit
func raceInvalidateReadCacheRange(racectx, addr, size uintptr) {
	if racectx <= 1 || !raceAccessRangeValid(addr, size) {
		return
	}
	cache := (*[ctxReadCacheSlots]uintptr)(unsafe.Pointer(racectx + ctxReadCacheOffset))
	widths := (*[ctxReadCacheSlots]uint8)(unsafe.Pointer(racectx + ctxReadWidthOffset))
	for i := range cache {
		cached := cache[i]
		width := uintptr(widths[i] &^ ctxReadWeakWidth)
		if cached != 0 && width != 0 && raceRangesOverlap(cached, width, addr, size) {
			cache[i] = 0
			stateSlot := unsafe.Pointer(racectx + ctxReadStateOffset + uintptr(i)*goarch.PtrSize)
			atomicstorep(stateSlot, nil)
			widths[i] = 0
		}
	}
}

// raceRangesOverlap reports whether two validated, non-empty half-open ranges
// overlap without forming either end address.
//
//go:nosplit
func raceRangesOverlap(first, firstSize, second, secondSize uintptr) bool {
	if first <= second {
		return second-first < firstSize
	}
	return first-second < secondSize
}

// kolkovShadowPtr is the runtime-local, GC-visible shadow pointer for T26's
// inline fast paths. The API's runtime instance initializes its shadow once;
// tests that replace the API detector do not run through these hooks.
var kolkovShadowPtr atomic.UnsafePointer

// kolkovCacheShadowPtr caches the shadow pointer if not yet cached.
// Called from slow path exits after detector initialization is complete.
func kolkovCacheShadowPtr() {
	if kolkovShadowPtr.Load() == nil {
		kolkovShadowPtr.Store(unsafe.Pointer(kolkovGetShadowPtr()))
	}
}

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
	pc := sys.GetCallerPC()
	racereadpc(addr, pc, pc)
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
	if len <= 0 {
		return
	}
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
	if len <= 0 {
		return
	}
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
// Atomic-sized scalar objects use their exact width so ObjectPC callers cannot
// hide partial overlaps with atomic operations.
func raceReadObjectPC(t *_type, addr unsafe.Pointer, callerpc, pc uintptr) {
	kind := t.Kind()
	if kind == abi.Array || kind == abi.Struct {
		// for composite objects we have to read every address
		// because a write might happen to any subobject.
		racereadrangepc(addr, t.Size_, callerpc, pc)
	} else if t.Size_ == 2 || t.Size_ == 4 || t.Size_ == 8 {
		// Keep ordinary scalar history anchored at its start address while exposing
		// its physical width to the mixed ordinary/atomic sidecar.
		racereadnpc(addr, t.Size_, pc)
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
// Atomic-sized scalar objects use their exact width.
func raceWriteObjectPC(t *_type, addr unsafe.Pointer, callerpc, pc uintptr) {
	kind := t.Kind()
	if kind == abi.Array || kind == abi.Struct {
		// for composite objects we have to write every address
		// because a write might happen to any subobject.
		racewriterangepc(addr, t.Size_, callerpc, pc)
	} else if t.Size_ == 2 || t.Size_ == 4 || t.Size_ == 8 {
		// Keep ordinary scalar history anchored at its start address while exposing
		// its physical width to the mixed ordinary/atomic sidecar.
		racewritenpc(uintptr(addr), t.Size_, pc)
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
	if gp != gp.m.curg {
		// Running on g0/gsignal — suppress to prevent detector re-entrancy.
		// Without this, detector allocations on g0 trigger racewrite which
		// re-enters the detector, causing cascading false positives.
		return
	}
	if gp.raceguard != 0 {
		return
	}
	racectx := gp.racectx
	gp.raceguard++
	if racectx > 1 {
		systemstack(func() {
			kolkovOnReadCtx(uintptr(addr), pc, racectx)
		})
	} else {
		var newCtx uintptr
		systemstack(func() {
			newCtx = kolkovOnReadSlow(uintptr(addr), pc)
		})
		if newCtx > 1 {
			gp.racectx = newCtx
			kolkovCacheShadowPtr()
		}
	}
	gp.raceguard--
}

// racewritepc records a write of the memory location addr with explicit PC values.
//
//go:nosplit
func racewritepc(addr unsafe.Pointer, callpc, pc uintptr) {
	gp := getg()
	if gp == nil || gp.m == nil || gp.m.curg == nil {
		return
	}
	if gp != gp.m.curg {
		// Running on g0/gsignal — suppress to prevent detector re-entrancy.
		// Without this, detector allocations on g0 trigger racewrite which
		// re-enters the detector, causing cascading false positives.
		return
	}
	if gp.raceguard != 0 {
		return
	}
	racectx := gp.racectx
	raceInvalidateReadCache(racectx, uintptr(addr))
	gp.raceguard++
	if racectx > 1 {
		systemstack(func() {
			kolkovOnWriteCtx(uintptr(addr), pc, racectx)
		})
	} else {
		var newCtx uintptr
		systemstack(func() {
			newCtx = kolkovOnWriteSlow(uintptr(addr), pc)
		})
		if newCtx > 1 {
			gp.racectx = newCtx
			kolkovCacheShadowPtr()
		}
	}
	gp.raceguard--
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

	// Main goroutine context: use sentinel (1) during raceinit.
	// The allocator isn't ready yet during schedinit, so we can't create
	// the RaceContext here. The main goroutine's context will be lazily
	// created on its first raceread/racewrite (slow path).
	// Child goroutines get eager context via racegosetchildid (T13).
	gctx = 1
	pctx = 1

	return
}

// racefini finalizes the race detector. Race reports are printed when detected.
//
//go:nosplit
func racefini() {
	// racefini() can only be called once to avoid races.
	lock(&raceFiniLock)

	// runtime.main and os_beforeExit require racefini not to return. Match the
	// standard race detector's configured exit status when any race was reported.
	raceKolkovFini()
	if kolkovRaceErrors() != 0 {
		exit(raceKolkovExitCode)
	}
	exit(0)
}

// raceproccreate creates a new processor context.
//
//go:nosplit
func raceproccreate() uintptr {
	// FastTrack state belongs to logical goroutine contexts. The pure-Go
	// backend has no per-P detector state to allocate.
	return 0
}

// raceprocdestroy destroys a processor context.
//
//go:nosplit
func raceprocdestroy(ctx uintptr) {
	// raceproccreate returns no backend-owned resource.
}

// racemapshadow maps shadow memory for the given memory range.
//
//go:nosplit
func racemapshadow(addr unsafe.Pointer, size uintptr) {
	// The pure-Go shadow allocates address-indexed pages lazily. Unlike TSAN's
	// fixed shadow mapping, heap growth needs no eager map operation.
}

// racemalloc notifies the race detector of a memory allocation.
// Clears shadow memory for the allocated range to prevent false positives
// from stale access history when the allocator reuses addresses.
//
// Uses raceguard to skip clearing detector-private allocations. Detector work
// runs on g0 while the instrumented user stack is suspended, so a suppressed
// allocation must never become user-visible memory. Every user heap allocation
// and every new or reused goroutine stack still reaches this hook unguarded and
// clears any history from the preceding address lifetime.
//
//go:nosplit
func racemalloc(p unsafe.Pointer, sz uintptr) {
	gp := getg()
	if gp.m != nil && gp.m.curg != nil {
		gp = gp.m.curg
	}
	if gp.raceguard != 0 {
		return
	}
	raceClearReadCache(gp.racectx)
	gp.raceguard++
	systemstack(func() {
		kolkovApiClearShadow(uintptr(p), sz)
	})
	gp.raceguard--
}

// racefree notifies the race detector of an individual object free. PureGo
// defers shadow retirement to racemalloc: every heap, stack, and arena address
// is cleared at its allocation-side boundary before becoming user-visible.
// Keeping the old generation until then avoids duplicate allocator work.
//
//go:nosplit
func racefree(p unsafe.Pointer, sz uintptr) {}

// raceheapspanfree notifies the detector that the allocator is returning a
// whole span to the heap. Unlike individual object frees, this is a quiescent
// lifetime boundary for every address in the span, so it directly clears the
// exact full span and may discard retained full-block compact state.
//
//go:nosplit
func raceheapspanfree(p unsafe.Pointer, size uintptr) {
	gp := getg()
	if gp.m != nil && gp.m.curg != nil {
		gp = gp.m.curg
	}
	if gp.raceguard != 0 {
		return
	}
	raceClearReadCache(gp.racectx)
	gp.raceguard++
	systemstack(func() {
		kolkovApiClearShadow(uintptr(p), size)
	})
	gp.raceguard--
}

// racegostart notifies the race detector that a new goroutine is starting.
// Called by proc.go's newproc1 (on systemstack) when creating a goroutine.
//
// IMPORTANT: This runs on g0 (systemstack), so getg() returns g0.
// We use gp.m.curg to get the parent (spawning) goroutine and pass its
// goid explicitly to the API, since the API cannot extract goid on g0.
//
// The return value is a short-lived spawn token. proc.go keeps it local and
// passes it back with the child's goid; only the eagerly allocated child
// RaceContext pointer is published in g.racectx.
//
//go:nosplit
func racegostart(pc uintptr) uintptr {
	gp := getg()
	if gp == gp.m.g0 && gp.racectx > 1 {
		// Runtime callbacks such as timers execute on g0 with an explicit
		// temporary context. Spawn from that context so the child inherits
		// synchronization acquired on behalf of the callback.
		if gp.raceguard != 0 {
			return 0
		}
		var spawnID uintptr
		gp.raceguard++
		systemstack(func() {
			spawnID = kolkovApiOnGoStartFromContext(pc, gp.racectx)
		})
		gp.raceguard--
		return spawnID
	}

	// Get the parent goroutine (the one that called 'go func()').
	// On systemstack, gp is g0; gp.m.curg is the user goroutine.
	var spawng *g
	if gp.m.curg != nil {
		spawng = gp.m.curg
	} else {
		spawng = gp
	}

	// RaceDisable suppresses the fork edge, but racegosetchildid still creates
	// an independent child context from the zero token below.
	if spawng.raceguard != 0 || spawng.raceignore != 0 {
		return 0
	}
	var spawnID uintptr
	spawng.raceguard++
	systemstack(func() {
		spawnID = kolkovApiOnGoStart(pc, int64(spawng.goid))
	})
	spawng.raceguard--

	// This is a short-lived spawn token. newproc1 keeps it local and passes it
	// unchanged to racegosetchildid, which returns the eagerly-created context.
	return spawnID
}

// racegosetchildid associates the most recently created spawn context with
// the actual child goroutine goid. Called by proc.go right after racegostart.
//
// T13 optimization: Also eagerly creates the child's RaceContext and returns
// its pointer as uintptr for direct caching in newg.racectx. This eliminates
// the first-access slow path (contextsMap lookup) for every new goroutine.
// The context is stored in both contextsMap (for GC safety) and g.racectx
// (as uintptr for fast path access).
//
//go:nosplit
func racegosetchildid(childGoid uint64, spawnID uintptr) uintptr {
	gp := getg()
	if gp.m != nil && gp.m.curg != nil {
		gp = gp.m.curg
	}
	if gp.raceguard != 0 {
		return 0
	}
	var ctx uintptr
	gp.raceguard++
	systemstack(func() {
		ctx = kolkovApiGoSetChildIDWithCtx(int64(childGoid), spawnID)
	})
	gp.raceguard--
	return ctx
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
	if gp.raceguard != 0 {
		return
	}
	gp.raceguard++
	systemstack(func() {
		kolkovApiOnGoEnd(int64(gp.goid))
	})
	gp.raceguard--
	// Clear cached context to prevent dangling pointer after cleanup
	gp.racectx = 0
}

// racectxstart creates a new race context for a goroutine.
//
//go:nosplit
func racectxstart(pc, spawnctx uintptr) uintptr {
	gp := getg()
	if gp.m != nil && gp.m.curg != nil {
		gp = gp.m.curg
	}
	if gp.raceguard != 0 {
		return 0
	}
	if gp.raceignore != 0 {
		// Keep the temporary context lifecycle intact while suppressing the
		// user-disabled fork inheritance and parent clock advance.
		spawnctx = 0
	}
	var ctx uintptr
	gp.raceguard++
	systemstack(func() {
		ctx = kolkovApiContextStart(pc, spawnctx)
	})
	gp.raceguard--
	return ctx
}

// racectxend ends a race context.
//
//go:nosplit
func racectxend(racectx uintptr) {
	gp := getg()
	if gp.m != nil && gp.m.curg != nil {
		gp = gp.m.curg
	}
	if gp.raceguard != 0 {
		return
	}
	gp.raceguard++
	systemstack(func() {
		kolkovApiContextEnd(racectx)
	})
	gp.raceguard--
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
	gp := getg()
	if gp != gp.m.curg {
		return
	}
	if gp.raceguard != 0 || gp.raceignore != 0 {
		return
	}
	racectx := gp.racectx
	gp.raceguard++
	if racectx > 1 {
		systemstack(func() {
			kolkovOnAcquireCtx(uintptr(addr), racectx)
		})
	} else {
		var newCtx uintptr
		systemstack(func() {
			newCtx = kolkovOnAcquireSlow(uintptr(addr))
		})
		if newCtx > 1 {
			gp.racectx = newCtx
			kolkovCacheShadowPtr()
		}
	}
	gp.raceguard--
}

// raceacquireg records an acquire operation on the given address for a specific goroutine.
// The target goroutine gp may differ from the current goroutine (e.g., in channel
// operations where one goroutine acquires on behalf of its blocked partner).
//
//go:nosplit
func raceacquireg(gp *g, addr unsafe.Pointer) {
	// Protect the current goroutine from re-entrant race detector calls
	// during the API call (which may trigger instrumented memory accesses).
	curg := getg()
	if curg.m != nil && curg.m.curg != nil {
		curg = curg.m.curg
	}
	if curg.raceguard != 0 || curg.raceignore != 0 || gp.raceignore != 0 {
		return
	}
	curg.raceguard++
	racectx := gp.racectx
	if racectx > 1 {
		// Fast path: target goroutine has cached context
		systemstack(func() {
			kolkovOnAcquireCtx(uintptr(addr), racectx)
		})
	} else {
		// Slow path: fall back to goid-based lookup
		systemstack(func() {
			kolkovApiOnAcquireForGoroutine(uintptr(addr), int64(gp.goid))
		})
	}
	curg.raceguard--
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
	// Explicit contexts are used by timer callbacks and must not inherit the
	// current goroutine's user synchronization suppression.
	if gp.raceguard != 0 {
		return
	}
	gp.raceguard++
	if racectx > 1 {
		// racectx is a real cached *RaceContext pointer
		systemstack(func() {
			kolkovOnAcquireCtx(uintptr(addr), racectx)
		})
	} else {
		// racectx == 1 (sentinel), fall back to current goroutine context
		systemstack(func() {
			kolkovOnAcquire(uintptr(addr))
		})
	}
	gp.raceguard--
}

// racerelease records a release operation on the given address.
//
//go:nosplit
func racerelease(addr unsafe.Pointer) {
	gp := getg()
	if gp != gp.m.curg {
		return
	}
	if gp.raceguard != 0 || gp.raceignore != 0 {
		return
	}
	racectx := gp.racectx
	gp.raceguard++
	if racectx > 1 {
		systemstack(func() {
			kolkovOnReleaseCtx(uintptr(addr), racectx)
		})
	} else {
		systemstack(func() {
			kolkovOnRelease(uintptr(addr))
		})
	}
	gp.raceguard--
}

// racereleaseg records a release operation on the given address for a specific goroutine.
// The target goroutine gp may differ from the current goroutine (e.g., in channel
// operations where one goroutine releases on behalf of its blocked partner).
//
//go:nosplit
func racereleaseg(gp *g, addr unsafe.Pointer) {
	curg := getg()
	if curg.m != nil && curg.m.curg != nil {
		curg = curg.m.curg
	}
	if curg.raceguard != 0 || curg.raceignore != 0 || gp.raceignore != 0 {
		return
	}
	curg.raceguard++
	racectx := gp.racectx
	if racectx > 1 {
		systemstack(func() {
			kolkovOnReleaseCtx(uintptr(addr), racectx)
		})
	} else {
		systemstack(func() {
			kolkovApiOnReleaseForGoroutine(uintptr(addr), int64(gp.goid))
		})
	}
	curg.raceguard--
}

// racereleaseacquire records a combined release-acquire operation.
//
//go:nosplit
func racereleaseacquire(addr unsafe.Pointer) {
	racereleaseacquireg(getg(), addr)
}

// racereleaseacquireg records a combined release-acquire operation for a specific goroutine.
// The target goroutine gp may differ from the current goroutine. Channel callers serialize
// this exchange under the channel lock, so acquire must consume the prior release before
// release publishes the target context for the next slot user.
//
//go:nosplit
func racereleaseacquireg(gp *g, addr unsafe.Pointer) {
	curg := getg()
	if curg.m != nil && curg.m.curg != nil {
		curg = curg.m.curg
	}
	if curg.raceguard != 0 || curg.raceignore != 0 || gp.raceignore != 0 {
		return
	}
	curg.raceguard++
	racectx := gp.racectx
	if racectx > 1 {
		systemstack(func() {
			kolkovOnAcquireCtx(uintptr(addr), racectx)
			kolkovOnReleaseCtx(uintptr(addr), racectx)
		})
	} else {
		systemstack(func() {
			kolkovApiOnAcquireForGoroutine(uintptr(addr), int64(gp.goid))
			kolkovApiOnReleaseForGoroutine(uintptr(addr), int64(gp.goid))
		})
	}
	curg.raceguard--
}

// racereleasemerge records a release-merge operation on the given address.
//
//go:nosplit
func racereleasemerge(addr unsafe.Pointer) {
	gp := getg()
	if gp != gp.m.curg {
		return
	}
	if gp.raceguard != 0 || gp.raceignore != 0 {
		return
	}
	racectx := gp.racectx
	gp.raceguard++
	if racectx > 1 {
		systemstack(func() {
			kolkovOnReleaseMergeCtx(uintptr(addr), racectx)
		})
	} else {
		systemstack(func() {
			kolkovOnReleaseMerge(uintptr(addr))
		})
	}
	gp.raceguard--
}

// racereleasemergeg records a release-merge operation for a specific goroutine.
// The target goroutine gp may differ from the current goroutine (e.g., in channel
// operations where one goroutine releases on behalf of its blocked partner).
//
//go:nosplit
func racereleasemergeg(gp *g, addr unsafe.Pointer) {
	curg := getg()
	if curg.m != nil && curg.m.curg != nil {
		curg = curg.m.curg
	}
	if curg.raceguard != 0 || curg.raceignore != 0 || gp.raceignore != 0 {
		return
	}
	curg.raceguard++
	racectx := gp.racectx
	if racectx > 1 {
		systemstack(func() {
			kolkovOnReleaseMergeCtx(uintptr(addr), racectx)
		})
	} else {
		systemstack(func() {
			kolkovApiOnReleaseMergeForGoroutine(uintptr(addr), int64(gp.goid))
		})
	}
	curg.raceguard--
}

// racefingo notifies the race detector that the current goroutine is a finalizer.
//
//go:nosplit
func racefingo() {
	gp := getg()
	if gp.m != nil && gp.m.curg != nil {
		gp = gp.m.curg
	}
	if gp.raceguard != 0 || gp.racectx <= 1 {
		return
	}
	racectx := gp.racectx
	gp.raceguard++
	systemstack(func() {
		kolkovApiFinalizerGo(racectx)
	})
	gp.raceguard--
}

// Hot-path race detection functions.
// These are called directly by compiler-generated instrumentation.
// Previously implemented in assembly (race_kolkov_*.s), now pure Go
// per @randall77 guidance: no assembly needed, use sys.GetCallerPC().

// racereadSlowPath contains the closure and systemstack state needed only when
// the inline FastTrack checks miss. Keeping it out of raceread prevents those
// cold-path captures from inflating every compiler-generated read hook's frame.
//
//go:noinline
//go:nosplit
func racereadSlowPath(addr, pc uintptr) {
	gp := getg()
	racectx := gp.racectx
	if racectx > 1 {
		// Cache misses can still avoid systemstack when this goroutine wrote the
		// location in the current FastTrack epoch.
		shadowPtr := uintptr(kolkovShadowPtr.Load())
		if vsPtr := raceShadowState(shadowPtr, addr); vsPtr != nil {
			currentEpoch := *(*uint64)(unsafe.Pointer(racectx + ctxEpochOffset))
			if atomic.Load64((*uint64)(unsafe.Add(vsPtr, vsWOffset))) == currentEpoch &&
				raceShadowState(shadowPtr, addr) == vsPtr {
				return
			}
		}
	}

	gp.raceguard++
	if racectx > 1 {
		systemstack(func() {
			kolkovOnReadCtx(addr, pc, racectx)
		})
		gp.raceguard--
		return
	}

	var newCtx uintptr
	systemstack(func() {
		newCtx = kolkovOnReadSlow(addr, pc)
	})
	if newCtx > 1 {
		gp.racectx = newCtx
		kolkovCacheShadowPtr()
	}
	gp.raceguard--
}

// raceReadCacheIndex mirrors goroutine.ReadCacheIndex without importing the
// detector implementation into the runtime hot path.
//
//go:nosplit
func raceReadCacheIndex(addr uintptr) uintptr {
	word := addr >> 3
	return (word ^ (word >> 2)) & ctxReadCacheMask
}

// raceread records a read of the given address.
// Called from compiler-generated instrumentation.
// sys.GetCallerPC() is evaluated only on a cache miss and before the first
// actual call, so it still captures the instrumented caller's PC.
//
//go:nosplit
func raceread(addr uintptr) {
	gp := getg()
	if gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg {
		// Running on g0/gsignal — suppress to prevent detector re-entrancy.
		// Without this, detector allocations on g0 trigger racewrite which
		// re-enters the detector, causing cascading false positives.
		return
	}
	if gp.raceguard != 0 {
		return
	}
	racectx := gp.racectx
	// FastTrack redundant-read elimination: a completed exact read represents
	// subsequent reads until this context writes that address or advances at
	// synchronization. The parallel GC-visible state pointer prevents an
	// allocator clear from reviving an address-only entry: the mapping load is
	// the hook's linearization point, and a replacement pointer forces the sound
	// slow path. Cache collisions only reduce optimization coverage.
	if racectx > 1 {
		index := raceReadCacheIndex(addr)
		slot := (*uintptr)(unsafe.Pointer(racectx + ctxReadCacheOffset + index*goarch.PtrSize))
		cachedWidth := *(*uint8)(unsafe.Pointer(racectx + ctxReadWidthOffset + index))
		if *slot == addr && cachedWidth == 1 {
			invalidated := atomic.Load((*uint32)(unsafe.Pointer(racectx + ctxReadCacheInvalidatedClockOffset)))
			if invalidated != 0 {
				currentEpoch := atomic.Load64((*uint64)(unsafe.Pointer(racectx + ctxEpochOffset)))
				if uint32(currentEpoch) <= invalidated {
					goto cacheMiss
				}
			}
			// Static storage in the first module cannot be freed or reused. The
			// exact address match is therefore sufficient after the initial read
			// published this entry; heap, stack, and plugin storage still require
			// exact shadow-state identity revalidation below.
			if raceFirstModuleStaticData(addr) {
				return
			}
			cachedState := *(*unsafe.Pointer)(unsafe.Pointer(racectx + ctxReadStateOffset + index*goarch.PtrSize))
			if cachedState != nil && raceShadowState(uintptr(kolkovShadowPtr.Load()), addr) == cachedState {
				return
			}
		}
	}
cacheMiss:
	pc := sys.GetCallerPC()
	racereadSlowPath(addr, pc)
}

// racereadn records an exact 2-, 4-, or 8-byte compiler scalar read. A
// completed read of first-module storage may use an address-only cache entry;
// reclaimable storage also revalidates the authoritative start-address state.
//
//go:nosplit
func racereadn(addr, size uintptr) {
	gp := getg()
	if gp.raceguard != 0 {
		return
	}
	racectx := gp.racectx
	if racectx > 1 {
		index := raceReadCacheIndex(addr)
		cachedAddr := *(*uintptr)(unsafe.Pointer(racectx + ctxReadCacheOffset + index*goarch.PtrSize))
		cachedWidth := *(*uint8)(unsafe.Pointer(racectx + ctxReadWidthOffset + index))
		if cachedAddr == addr && cachedWidth == uint8(size) {
			invalidated := atomic.Load((*uint32)(unsafe.Pointer(racectx + ctxReadCacheInvalidatedClockOffset)))
			if invalidated != 0 {
				currentEpoch := atomic.Load64((*uint64)(unsafe.Pointer(racectx + ctxEpochOffset)))
				if uint32(currentEpoch) <= invalidated {
					goto cacheMiss
				}
			}
			if raceFirstModuleStaticDataRange(addr, size) {
				return
			}
			cachedState := *(*unsafe.Pointer)(unsafe.Pointer(racectx + ctxReadStateOffset + index*goarch.PtrSize))
			if cachedState != nil && raceShadowState(uintptr(kolkovShadowPtr.Load()), addr) == cachedState {
				return
			}
		}
	}
cacheMiss:
	// Compiler hooks normally run on the current user goroutine. Keep the
	// system-stack rejection on cache misses, but off the steady-state hit path;
	// runtime work on g0/gsignal remains suppressed exactly as in raceread.
	if gp.m == nil || gp.m.curg == nil || gp != gp.m.curg {
		return
	}
	pc := sys.GetCallerPC()
	racereadnSlowPath(addr, size, pc)
}

// racereadnpc is the explicit-PC counterpart used by ObjectPC scalar hooks.
// Those calls are not compiler hot-path hooks, so they deliberately enter the
// authoritative sized detector path rather than duplicating the inline cache.
//
//go:nosplit
func racereadnpc(addr unsafe.Pointer, size, pc uintptr) {
	if !raceAccessRangeValid(uintptr(addr), size) {
		return
	}
	gp := getg()
	if gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return
	}
	racereadnSlowPath(uintptr(addr), size, pc)
}

//go:noinline
//go:nosplit
func racereadnSlowPath(addr, size, pc uintptr) {
	gp := getg()
	racectx := gp.racectx
	gp.raceguard++
	if racectx > 1 {
		systemstack(func() {
			kolkovOnReadSizedCtx(addr, size, pc, racectx)
		})
		gp.raceguard--
		return
	}

	var newCtx uintptr
	systemstack(func() {
		newCtx = kolkovOnReadSizedSlow(addr, size, pc)
	})
	if newCtx > 1 {
		gp.racectx = newCtx
		kolkovCacheShadowPtr()
	}
	gp.raceguard--
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
	if gp != gp.m.curg {
		// Running on g0/gsignal — suppress to prevent detector re-entrancy.
		// Without this, detector allocations on g0 trigger racewrite which
		// re-enters the detector, causing cascading false positives.
		return
	}
	if gp.raceguard != 0 {
		return
	}
	racectx := gp.racectx
	raceInvalidateReadCache(racectx, addr)
	gp.raceguard++
	if racectx > 1 {
		systemstack(func() {
			kolkovOnWriteCtx(addr, pc, racectx)
		})
	} else {
		var newCtx uintptr
		systemstack(func() {
			newCtx = kolkovOnWriteSlow(addr, pc)
		})
		if newCtx > 1 {
			gp.racectx = newCtx
			kolkovCacheShadowPtr()
		}
	}
	gp.raceguard--
}

// racewriten records an exact compiler scalar write. Ordinary FastTrack
// history remains anchored at the start address while the atomic sidecar sees
// the physical width.
//
//go:nosplit
func racewriten(addr, size uintptr) {
	if !raceAccessRangeValid(addr, size) {
		return
	}
	pc := sys.GetCallerPC()
	racewritenpc(addr, size, pc)
}

// racewritenpc is the shared explicit-PC sized scalar write path.
//
//go:nosplit
func racewritenpc(addr, size, pc uintptr) {
	if !raceAccessRangeValid(addr, size) {
		return
	}
	gp := getg()
	if gp == nil || gp.m == nil || gp.m.curg == nil || gp != gp.m.curg || gp.raceguard != 0 {
		return
	}
	racectx := gp.racectx
	raceInvalidateReadCacheRange(racectx, addr, size)
	gp.raceguard++
	if racectx > 1 {
		systemstack(func() {
			kolkovOnWriteSizedCtx(addr, size, pc, racectx)
		})
	} else {
		var newCtx uintptr
		systemstack(func() {
			newCtx = kolkovOnWriteSizedSlow(addr, size, pc)
		})
		if newCtx > 1 {
			gp.racectx = newCtx
			kolkovCacheShadowPtr()
		}
	}
	gp.raceguard--
}

// racereadrange records a read of the given address range.
// Called from compiler-generated instrumentation.
//
//go:nosplit
func racereadrange(addr, size uintptr) {
	if !raceAccessRangeValid(addr, size) {
		return
	}
	pc := sys.GetCallerPC()
	gp := getg()
	if gp == nil || gp.m == nil || gp.m.curg == nil {
		return
	}
	if gp != gp.m.curg {
		// Running on g0/gsignal — suppress to prevent detector re-entrancy.
		// Without this, detector allocations on g0 trigger racewrite which
		// re-enters the detector, causing cascading false positives.
		return
	}
	if gp.raceguard != 0 {
		return
	}
	racectx := gp.racectx
	gp.raceguard++
	if racectx > 1 {
		systemstack(func() {
			kolkovOnReadRangeCtx(addr, size, pc, racectx)
		})
	} else {
		var newCtx uintptr
		systemstack(func() {
			newCtx = kolkovOnReadRangeSlow(addr, size, pc)
		})
		if newCtx > 1 {
			gp.racectx = newCtx
			kolkovCacheShadowPtr()
		}
	}
	gp.raceguard--
}

// racewriterange records a write to the given address range.
// Called from compiler-generated instrumentation.
//
//go:nosplit
func racewriterange(addr, size uintptr) {
	if !raceAccessRangeValid(addr, size) {
		return
	}
	pc := sys.GetCallerPC()
	gp := getg()
	if gp == nil || gp.m == nil || gp.m.curg == nil {
		return
	}
	if gp != gp.m.curg {
		// Running on g0/gsignal — suppress to prevent detector re-entrancy.
		// Without this, detector allocations on g0 trigger racewrite which
		// re-enters the detector, causing cascading false positives.
		return
	}
	if gp.raceguard != 0 {
		return
	}
	racectx := gp.racectx
	raceInvalidateReadCacheRange(racectx, addr, size)
	gp.raceguard++
	if racectx > 1 {
		systemstack(func() {
			kolkovOnWriteRangeCtx(addr, size, pc, racectx)
		})
	} else {
		var newCtx uintptr
		systemstack(func() {
			newCtx = kolkovOnWriteRangeSlow(addr, size, pc)
		})
		if newCtx > 1 {
			gp.racectx = newCtx
			kolkovCacheShadowPtr()
		}
	}
	gp.raceguard--
}

// racereadrangepc1 is the internal implementation for range reads with explicit PC.
//
//go:nosplit
func racereadrangepc1(addr, size, pc uintptr) {
	if !raceAccessRangeValid(addr, size) {
		return
	}
	gp := getg()
	if gp == nil || gp.m == nil || gp.m.curg == nil {
		return
	}
	if gp != gp.m.curg {
		// Running on g0/gsignal — suppress to prevent detector re-entrancy.
		// Without this, detector allocations on g0 trigger racewrite which
		// re-enters the detector, causing cascading false positives.
		return
	}
	if gp.raceguard != 0 {
		return
	}
	racectx := gp.racectx
	gp.raceguard++
	if racectx > 1 {
		systemstack(func() {
			kolkovOnReadRangeCtx(addr, size, pc, racectx)
		})
	} else {
		var newCtx uintptr
		systemstack(func() {
			newCtx = kolkovOnReadRangeSlow(addr, size, pc)
		})
		if newCtx > 1 {
			gp.racectx = newCtx
			kolkovCacheShadowPtr()
		}
	}
	gp.raceguard--
}

// racewriterangepc1 is the internal implementation for range writes with explicit PC.
//
//go:nosplit
func racewriterangepc1(addr, size, pc uintptr) {
	if !raceAccessRangeValid(addr, size) {
		return
	}
	gp := getg()
	if gp == nil || gp.m == nil || gp.m.curg == nil {
		return
	}
	if gp != gp.m.curg {
		// Running on g0/gsignal — suppress to prevent detector re-entrancy.
		// Without this, detector allocations on g0 trigger racewrite which
		// re-enters the detector, causing cascading false positives.
		return
	}
	if gp.raceguard != 0 {
		return
	}
	racectx := gp.racectx
	raceInvalidateReadCacheRange(racectx, addr, size)
	gp.raceguard++
	if racectx > 1 {
		systemstack(func() {
			kolkovOnWriteRangeCtx(addr, size, pc, racectx)
		})
	} else {
		var newCtx uintptr
		systemstack(func() {
			newCtx = kolkovOnWriteRangeSlow(addr, size, pc)
		})
		if newCtx > 1 {
			gp.racectx = newCtx
			kolkovCacheShadowPtr()
		}
	}
	gp.raceguard--
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
