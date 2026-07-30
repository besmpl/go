//go:build !(amd64 || arm64)

package shadowmem

import (
	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/vectorclock"
)

// PageTableShadow on unsupported platforms delegates to CASBasedShadow.
// The page table requires 64-bit address space (128GB coverage).
type PageTableShadow struct {
	fallback CASBasedShadow
}

// NewPageTableShadow creates a page table shadow (fallback on 32-bit).
func NewPageTableShadow() *PageTableShadow {
	pt := &PageTableShadow{}
	pt.fallback.compressAddresses = false
	return pt
}

// GetOrCreate delegates to CASBasedShadow on unsupported platforms.
func (pt *PageTableShadow) GetOrCreate(addr uintptr) *VarState {
	return pt.fallback.GetOrCreate(addr)
}

// GetOrCreateSlot delegates to the exact CAS fallback on unsupported platforms.
func (pt *PageTableShadow) GetOrCreateSlot(addr uintptr) *ShadowSlot {
	return pt.fallback.GetOrCreateSlot(addr)
}

// Get delegates to CASBasedShadow on unsupported platforms.
func (pt *PageTableShadow) Get(addr uintptr) *VarState {
	return pt.fallback.Load(addr)
}

// GetSlot delegates to the exact CAS fallback on unsupported platforms.
func (pt *PageTableShadow) GetSlot(addr uintptr) *ShadowSlot {
	return pt.fallback.GetSlot(addr)
}

// MaterializeReadHintSlot is unavailable without the direct page-table layout.
func (pt *PageTableShadow) MaterializeReadHintSlot(_ uintptr) bool {
	return false
}

// LockReadHintSlotRange is unavailable without the direct page-table layout.
func (pt *PageTableShadow) LockReadHintSlotRange(_ uintptr, _ uintptr) (*VarState, bool) {
	return nil, false
}

// TryCompactWrite is unavailable without the direct page-table block layout.
func (pt *PageTableShadow) TryCompactWrite(_ uintptr, _ epoch.Epoch, _ *vectorclock.VectorClock, _ uintptr) bool {
	return false
}

// TryCompactRead is unavailable without the direct page-table block layout.
func (pt *PageTableShadow) TryCompactRead(_ uintptr, _ epoch.Epoch, _ *vectorclock.VectorClock, _ uintptr) CompactReadResult {
	return CompactReadMiss
}

// AccessRange uses the exact slot protocol on architectures without the
// direct page-table/block representation.
func (pt *PageTableShadow) AccessRange(addr, size uintptr, visit func(word uintptr, mask uint8, state *VarState)) {
	if size == 0 || size-1 > ^uintptr(0)-addr {
		return
	}
	for current, remaining := addr, size; remaining != 0; {
		lane := current & 7
		count := uintptr(8) - lane
		if count > remaining {
			count = remaining
		}
		mask := uint8(((uint16(1) << count) - 1) << lane)
		word := current &^ uintptr(7)
		pt.fallback.GetOrCreateSlot(current).AccessGroups(mask, func(groupMask uint8, state *VarState) {
			visit(word, groupMask, state)
		})
		current += count
		remaining -= count
	}
}

// ClearRange delegates to CASBasedShadow on unsupported platforms.
func (pt *PageTableShadow) ClearRange(addr, size uintptr) {
	pt.fallback.ClearRange(addr, size)
}

// Reset delegates to CASBasedShadow on unsupported platforms.
func (pt *PageTableShadow) Reset() {
	pt.fallback.Reset()
}
