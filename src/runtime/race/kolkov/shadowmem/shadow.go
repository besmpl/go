package shadowmem

// Shadow is the interface for shadow memory implementations.
//
// All shadow memory backends must implement this interface to be used
// by the Detector. Implementations include:
//   - ShadowMemory: Sharded sync.Map (general-purpose, uses stdlib)
//   - CASBasedShadow: Lock-free CAS array (runtime-compatible)
//   - PageTableShadow: Two-level page table (optimized, direct-mapped)
type Shadow interface {
	// GetOrCreate returns the VarState for addr, creating it if needed.
	// This is the hot path -- called on every instrumented memory access.
	// Must be safe for concurrent calls.
	GetOrCreate(addr uintptr) *VarState

	// Get returns the VarState for addr, or nil if not tracked.
	// Does NOT create new entries.
	Get(addr uintptr) *VarState

	// ClearRange clears shadow state for all addresses in [addr, addr+size).
	// Called on memory allocation/free to prevent false positives from
	// stale shadow state when addresses are reused.
	ClearRange(addr, size uintptr)

	// Reset clears all shadow memory state.
	// NOT safe for concurrent access.
	Reset()
}

// Compile-time interface checks.
var (
	_ Shadow = (*CASBasedShadow)(nil)
	_ Shadow = (*PageTableShadow)(nil)
)

// DefaultShadow returns the recommended shadow memory implementation.
//
// Returns PageTableShadow which uses direct index computation instead of
// hash + linear probing. Hot path cost: 1 base load + 2 pointer loads (~5-8ns)
// vs CASBasedShadow hash + probe (~15-25ns with collisions).
//
// PageTableShadow covers 128GB of address space via a two-level page table
// with lazy L2 page allocation. Addresses outside the range fall back to
// CASBasedShadow automatically.
func DefaultShadow() Shadow {
	return NewPageTableShadow()
}
