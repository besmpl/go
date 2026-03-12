//go:build amd64 || arm64

package shadowmem

import (
	"internal/runtime/atomic"
)

// Page table constants for two-level direct-mapped shadow memory.
//
// Design: Each application address is decomposed into:
//   - L1 index: upper bits -> page directory entry (which 2MB region)
//   - L2 index: lower bits -> slot within page (which 8-byte word)
//
// Coverage: l1Size * 2^l1Shift = 65536 * 2MB = 128GB of application memory.
// This covers typical Go programs. Out-of-range addresses use the fallback.
const (
	// l1Size is the number of L1 page directory entries.
	// 65536 entries * 8 bytes = 512KB fixed overhead.
	l1Size = 65536

	// l2Size is the number of VarState slots per L2 page.
	// Each slot tracks one 8-byte application memory word.
	// 262144 slots * 8 bytes (pointer) = 2MB per page.
	l2Size = 1 << 18 // 262144

	// l1Shift is the number of address bits covered by L2.
	// L2 covers l2Size * 8 bytes = 262144 * 8 = 2097152 = 2^21.
	l1Shift = 21

	// l2Mask masks the L2 index bits after right-shifting by 3.
	// 18 bits -> 262144 entries.
	l2Mask = l2Size - 1 // 0x3FFFF

	// ptTotalCoverage is the total address range covered by the page table.
	// 65536 * 2^21 = 65536 * 2MB = 128GB.
	ptTotalCoverage = uintptr(l1Size) << l1Shift
)

// shadowPage is a Level 2 page containing VarState pointers.
//
// Each page covers 2MB of application memory (262144 eight-byte words).
// Pages are allocated lazily on first access to the 2MB app range.
//
// Size: 262144 * 8 bytes = 2,097,152 bytes = 2MB per page.
type shadowPage struct {
	slots [l2Size]atomic.Pointer[VarState]
}

// PageTableShadow implements two-level direct-mapped shadow memory.
//
// This replaces hash-based lookups (CASBasedShadow) with direct index
// computation, eliminating hashing and linear probing entirely.
//
// Architecture:
//   - L1: Fixed page directory of 65536 atomic pointers (512KB, always allocated)
//   - L2: Shadow pages of 262144 VarState pointers (2MB each, lazily allocated)
//   - Each L1 entry covers 2MB of application memory
//   - Fallback: CASBasedShadow for addresses outside the covered 128GB range
//
// Lookup formula (hot path, after initialization):
//
//	offset  = addr - base
//	pageIdx = offset >> 21              // L1 index
//	wordIdx = (offset >> 3) & 0x3FFFF   // L2 index
//	vs      = pages[pageIdx].slots[wordIdx]
//
// Hot path cost: 2 atomic pointer loads + index computation (~5-8ns)
// vs CASBasedShadow hash + probe (~15-25ns).
//
// The base address is auto-detected on first access and centered to provide
// headroom for addresses below and above the initial access. Once set, base
// is immutable (never changes), enabling a single atomic load on the hot path.
type PageTableShadow struct {
	// base is the start of the covered address range.
	// Set once on first access via atomic CAS, then immutable.
	// Zero means "not yet initialized" (Go heap never maps address 0).
	base atomic.Uintptr

	// pages is the L1 page directory.
	// pages[i] covers app memory [base + i*2MB, base + (i+1)*2MB).
	pages [l1Size]atomic.Pointer[shadowPage]

	// fallback handles addresses outside the covered range.
	// Expected to handle <1% of accesses in typical Go programs.
	fallback CASBasedShadow
}

// NewPageTableShadow creates a new page table shadow memory.
//
// The returned shadow is ready to use. The base address is auto-detected
// on first GetOrCreate call. The L1 directory is zero-initialized (all nil),
// and pages are allocated lazily.
//
// Memory: 512KB (L1 directory) + 2MB per active page.
func NewPageTableShadow() *PageTableShadow {
	pt := &PageTableShadow{}
	pt.fallback.compressAddresses = true
	return pt
}

// initBase sets the base address on first access.
//
// Strategy: align the first address down to 2MB boundary, then subtract
// 1/4 of total coverage to provide headroom for lower addresses.
// This gives a centered window around the first heap access.
//
// The base is guaranteed to be non-zero after this call (minimum value is 1),
// so that base==0 reliably indicates "not yet initialized."
func (pt *PageTableShadow) initBase(addr uintptr) {
	// Align to 2MB boundary (L1 granularity).
	aligned := addr &^ (uintptr(1)<<l1Shift - 1)

	// Subtract 1/4 of coverage for headroom below first access.
	headroom := ptTotalCoverage / 4
	if aligned > headroom {
		aligned -= headroom
	} else {
		// Ensure base is never zero (zero means "not initialized").
		aligned = 1
	}

	// CAS ensures only one goroutine sets the base.
	pt.base.CompareAndSwap(0, aligned)
}

// getOrCreateSlow handles page and VarState allocation on cold path.
// Separated from GetOrCreate to keep the hot path small and inlineable.
func (pt *PageTableShadow) getOrCreateSlow(pageIdx, wordIdx uintptr, page *shadowPage) *VarState {
	if page == nil {
		// Allocate new L2 page.
		newPage := new(shadowPage)
		if !pt.pages[pageIdx].CompareAndSwap(nil, newPage) {
			page = pt.pages[pageIdx].Load()
		} else {
			page = newPage
		}
	}

	// Allocate new VarState.
	newVS := NewVarState()
	if !page.slots[wordIdx].CompareAndSwap(nil, newVS) {
		return page.slots[wordIdx].Load()
	}
	return newVS
}

// GetOrCreate returns the VarState for addr, creating page and VarState if needed.
//
// This is the HOT PATH method -- called on every instrumented memory access.
//
// Warm path (base set, page exists, VarState exists): ~5-8ns
//   - base atomic load: ~3ns
//   - L1 atomic load + L2 atomic load: ~3-5ns
//   - Index computation: ~1ns
//
// Cold path (page or VarState miss): delegates to getOrCreateSlow
// Fallback (out of range): delegates to CASBasedShadow
func (pt *PageTableShadow) GetOrCreate(addr uintptr) *VarState {
	// Load base (single atomic load, replaces separate baseSet check).
	// After first access, base is immutable -- branch predictor handles this well.
	base := pt.base.Load()
	if base == 0 {
		pt.initBase(addr)
		base = pt.base.Load()
	}

	// Compress to 8-byte alignment.
	addr = addr &^ 7

	// Range check and index computation.
	// For addresses outside [base, base+128GB), fall back to CAS hash table.
	if addr < base {
		return pt.fallback.GetOrCreate(addr)
	}
	offset := addr - base
	if offset >= ptTotalCoverage {
		return pt.fallback.GetOrCreate(addr)
	}

	// L1 index: which 2MB page.
	pageIdx := offset >> l1Shift

	// L2 index: which 8-byte word within the page.
	wordIdx := (offset >> 3) & l2Mask

	// Hot path: 2 atomic loads (page + slot).
	page := pt.pages[pageIdx].Load()
	if page != nil {
		vs := page.slots[wordIdx].Load()
		if vs != nil {
			return vs
		}
	}

	// Cold path: allocate page and/or VarState.
	return pt.getOrCreateSlow(pageIdx, wordIdx, page)
}

// Get returns the VarState for addr, or nil if not found.
//
// This does NOT create pages or VarStates -- it only reads existing entries.
// Used for diagnostics and optional lookup paths.
func (pt *PageTableShadow) Get(addr uintptr) *VarState {
	base := pt.base.Load()
	if base == 0 {
		return nil
	}

	addr = addr &^ 7

	if addr < base {
		return pt.fallback.Load(addr)
	}
	offset := addr - base
	if offset >= ptTotalCoverage {
		return pt.fallback.Load(addr)
	}

	pageIdx := offset >> l1Shift
	page := pt.pages[pageIdx].Load()
	if page == nil {
		return nil
	}

	wordIdx := (offset >> 3) & l2Mask
	return page.slots[wordIdx].Load()
}

// ClearRange clears shadow state for all addresses in [addr, addr+size).
//
// This is called on memory allocation/free to prevent stale shadow state
// from causing false positives when addresses are reused.
//
// For addresses within the page table range, clears VarState pointers directly.
// For addresses outside the range, delegates to fallback CASBasedShadow.
func (pt *PageTableShadow) ClearRange(addr, size uintptr) {
	if size == 0 {
		return
	}

	base := pt.base.Load()
	if base == 0 {
		return
	}

	// Align start down, end up to 8-byte boundaries.
	start := addr &^ 7
	end := (addr + size + 7) &^ 7

	for a := start; a < end; a += 8 {
		if a < base || (a-base) >= ptTotalCoverage {
			pt.fallback.clearAddr(a)
			continue
		}

		offset := a - base
		pageIdx := offset >> l1Shift
		page := pt.pages[pageIdx].Load()
		if page == nil {
			continue
		}

		wordIdx := (offset >> 3) & l2Mask
		page.slots[wordIdx].Store(nil)
	}
}

// Reset clears all shadow memory (pages and fallback).
//
// After Reset(), all tracked addresses are forgotten.
// Pages are released for GC. Base is reset for re-detection.
//
// NOT safe for concurrent access during Reset().
func (pt *PageTableShadow) Reset() {
	for i := range pt.pages {
		pt.pages[i].Store(nil)
	}
	pt.fallback.Reset()
	pt.base.Store(0)
}