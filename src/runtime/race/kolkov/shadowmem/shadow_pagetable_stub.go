//go:build !(amd64 || arm64)

package shadowmem

// PageTableShadow on unsupported platforms delegates to CASBasedShadow.
// The page table requires 64-bit address space (128GB coverage).
type PageTableShadow struct {
	fallback CASBasedShadow
}

// NewPageTableShadow creates a page table shadow (fallback on 32-bit).
func NewPageTableShadow() *PageTableShadow {
	pt := &PageTableShadow{}
	pt.fallback.compressAddresses = true
	return pt
}

// GetOrCreate delegates to CASBasedShadow on unsupported platforms.
func (pt *PageTableShadow) GetOrCreate(addr uintptr) *VarState {
	return pt.fallback.GetOrCreate(addr)
}

// Get delegates to CASBasedShadow on unsupported platforms.
func (pt *PageTableShadow) Get(addr uintptr) *VarState {
	return pt.fallback.Load(addr)
}

// ClearRange delegates to CASBasedShadow on unsupported platforms.
func (pt *PageTableShadow) ClearRange(addr, size uintptr) {
	pt.fallback.ClearRange(addr, size)
}

// Reset delegates to CASBasedShadow on unsupported platforms.
func (pt *PageTableShadow) Reset() {
	pt.fallback.Reset()
}
