package shadowmem

// OrdinaryFastResult reports whether an optimistic ordinary-memory
// transaction completed and whether its exact authoritative state may be
// published in the per-context redundant-read cache.
type OrdinaryFastResult uint8

const (
	OrdinaryFastMiss OrdinaryFastResult = iota
	OrdinaryFastHandled
	OrdinaryFastHandledCacheable
)

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

// SlotShadow exposes the word-slot protocol used by range instrumentation and
// by exact accesses that must copy-on-write before mutation. A slot always
// covers the absolute aligned word containing addr.
type SlotShadow interface {
	Shadow
	GetOrCreateSlot(addr uintptr) *ShadowSlot
	GetSlot(addr uintptr) *ShadowSlot
}

// RangeShadow extends the exact word-slot protocol with a bulk traversal. The
// visitor receives each distinct ordinary-history group intersecting the
// range, in address order, while that group's access lock is held. A backend
// may represent identical histories more coarsely than one group per word as
// long as scalar and partial accesses copy-on-write before they diverge.
type RangeShadow interface {
	SlotShadow
	AccessRange(addr, size uintptr, visit func(word uintptr, mask uint8, state *VarState))
}

// Compile-time interface checks.
var (
	_ Shadow        = (*CASBasedShadow)(nil)
	_ Shadow        = (*PageTableShadow)(nil)
	_ SlotShadow    = (*CASBasedShadow)(nil)
	_ SlotShadow    = (*PageTableShadow)(nil)
	_ RangeShadow   = (*PageTableShadow)(nil)
	_ func() Shadow = DefaultShadow
)

// DefaultShadow returns the recommended shadow memory implementation.
//
// Returns PageTableShadow which uses direct index computation instead of
// hash + linear probing. Hot path cost: 1 base load + 2 pointer loads (~5-8ns)
// vs CASBasedShadow hash + probe (~15-25ns with collisions).
//
// PageTableShadow covers a centered 128GiB window with a two-level page table
// and uses a sparse absolute-block directory outside that window. Bulk ranges
// share ordinary history at 4KiB granularity until an exact word diverges.
func DefaultShadow() Shadow {
	return NewPageTableShadow()
}
