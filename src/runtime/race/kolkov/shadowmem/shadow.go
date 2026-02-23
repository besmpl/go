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
// Currently returns CASBasedShadow which performs better for random
// access patterns (1 atomic load on hot path vs 4 for PageTableShadow).
// PageTableShadow is better for sequential patterns but runtime workloads
// are dominated by random accesses across goroutines.
func DefaultShadow() Shadow {
	s := NewCASBasedShadow()
	s.SetAddressCompression(true)
	return s
}
