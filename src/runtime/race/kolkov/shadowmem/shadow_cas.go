// Package shadowmem implements shadow memory cells for FastTrack race detection.
package shadowmem

import (
	"internal/runtime/atomic"
)

// CASCell represents a single cell in the CAS-based shadow memory array.
//
// Each cell stores:
//   - addr: The memory address this cell tracks (for collision detection)
//   - varState: Pointer to the VarState tracking access epochs
//
// The cell is 24 bytes (8 + 8 + 8 padding) for cache line alignment.
// This provides natural spacing to reduce false sharing between adjacent cells.
//
// Memory layout:
//   - Offset 0-7: addr (uintptr, 8 bytes)
//   - Offset 8-15: varState pointer (8 bytes)
//   - Offset 16-23: padding (8 bytes, reserved for future use)
type CASCell struct {
	addr     uintptr   // Memory address this cell tracks.
	varState *VarState // Access state for this address.
	_        [8]byte   // Padding to 24 bytes for cache alignment.
}

// CASBasedShadow implements shadow memory using CAS (Compare-And-Swap) operations.
//
// This is a high-performance alternative to sync.Map with the following benefits:
//   - 43% faster on read-heavy workloads (9.74ns vs 17.16ns per operation)
//   - Zero allocations on hot path (vs 16-21 B/op with sync.Map)
//   - Predictable memory footprint (512KB fixed array)
//   - Lock-free operations using atomic.Pointer
//
// Architecture:
//   - Fixed-size array of 65536 slots (64K × 8 bytes = 512KB)
//   - FNV-1a hash function for address distribution
//   - Linear probing (max 8 probes) for collision handling
//   - Typical collision rate: <0.1% for workloads with <10K variables
//
// v0.3.0 Address Compression (P1):
//   - Addresses are compressed to 8-byte alignment
//   - Reduces memory usage by up to 8x for sequential accesses
//   - Conservative: treats all bytes in 8-byte block as same (correct but may over-detect)
//   - Configurable via SetAddressCompression()
//
// Performance characteristics:
//   - Load (hit): ~10ns, 0 allocs
//   - Store (new): ~20ns, 2 allocs (CASCell + VarState)
//   - LoadOrStore (hit): ~12ns, 0 allocs
//   - LoadOrStore (miss): ~25ns, 2 allocs
//
// Thread Safety: All operations are lock-free and safe for concurrent access.
//
// See design document: docs/dev/v0.2.0-cas-shadow-memory-design.md.
type CASBasedShadow struct {
	// Fixed-size array of atomic pointers to CASCell.
	// Using atomic.Pointer provides type-safe CAS operations (Go 1.19+).
	//
	// Array size: 65536 (2^16) slots
	// Memory: 65536 × 8 bytes = 524,288 bytes (512KB)
	cells [65536]atomic.Pointer[CASCell]

	// v0.3.0: Address compression flag.
	// When true, addresses are aligned to 8-byte boundaries before hashing.
	// This reduces memory usage but may cause false sharing detection.
	// Default: true (enabled for memory efficiency).
	compressAddresses bool
}

// NewCASBasedShadow creates a new CAS-based shadow memory.
//
// The returned shadow memory is ready to use with zero initialization.
// All array slots are initially nil (no cells allocated).
//
// v0.3.0: Address compression is enabled by default.
// Use SetAddressCompression(false) to disable.
//
// Memory allocation:
//   - Initial: 512KB for array (allocated on heap)
//   - Per-address: 24 bytes (CASCell) + 96 bytes (VarState v0.3.0) = 120 bytes
//   - For 10K variables: 512KB + 10K × 120 bytes ≈ 1.7MB total
//
// Example:
//
//	shadow := NewCASBasedShadow()
//	vs := shadow.LoadOrStore(0x1234, nil) // Get or create shadow cell.
//	vs.W = epoch.NewEpoch(1, 10)          // Record write access.
func NewCASBasedShadow() *CASBasedShadow {
	return &CASBasedShadow{
		compressAddresses: true, // v0.3.0: Enable address compression by default.
	}
}

// SetAddressCompression enables or disables 8-byte address alignment.
//
// v0.3.0: When enabled (default), addresses are aligned to 8-byte boundaries
// before hashing. This provides:
//   - Up to 8x reduction in memory usage for sequential accesses
//   - Better hash distribution (fewer collisions)
//   - Potential for false sharing detection (conservative)
//
// When disabled, exact address tracking is used (original behavior).
//
// This should be called before any Load/Store operations.
// Changing this during operation may cause inconsistent behavior.
//
// Example:
//
//	shadow := NewCASBasedShadow()
//	shadow.SetAddressCompression(false) // Disable for exact tracking.
func (s *CASBasedShadow) SetAddressCompression(enabled bool) {
	s.compressAddresses = enabled
}

// GetAddressCompression returns whether address compression is enabled.
func (s *CASBasedShadow) GetAddressCompression() bool {
	return s.compressAddresses
}

// alignAddr returns the address aligned to 8-byte boundary.
//
// v0.3.0: This is the address compression function.
// Formula: addr & ^uintptr(7) clears the lowest 3 bits.
//
// Examples:
//   - 0x1000 → 0x1000 (already aligned)
//   - 0x1001 → 0x1000 (align down)
//   - 0x1007 → 0x1000 (align down)
//   - 0x1008 → 0x1008 (already aligned)
//
//go:nosplit
func alignAddr(addr uintptr) uintptr {
	return addr &^ uintptr(7) // Clear bottom 3 bits.
}

// fastHash computes a fast hash of an address, masked to 16-bit range.
//
// This uses a simple multiplicative hash with a good mixing constant:
//   - Very fast: 1-2 CPU cycles (multiply + shift)
//   - Good distribution for sequential and random addresses
//   - Avoids FNV-1a's poor behavior on sequential addresses
//
// Algorithm:
//  1. Multiply by golden ratio constant: 0x9E3779B97F4A7C15
//  2. Right shift by 48 bits to get top 16 bits
//  3. Result is in range [0, 65535]
//
// Performance: <1ns per call (inlined by compiler).
//
// Reference: Thomas Wang's integer hash function
//
//go:nosplit
func fastHash(addr uintptr) uint64 {
	// Golden ratio constant for multiplicative hashing.
	// This constant has excellent avalanche properties.
	const goldenRatio = 0x9E3779B97F4A7C15

	hash := uint64(addr) * goldenRatio

	// Take top 16 bits (right shift by 48).
	// This gives us range [0, 65535] without modulo.
	return hash >> 48
}

// Load retrieves the VarState for the given address, or nil if not found.
//
// This is the fast path for checking if a shadow cell exists without creating one.
// It performs zero allocations even on cache miss.
//
// Algorithm:
//  1. Compute FNV-1a hash of address → initial index
//  2. Linear probe up to 8 slots: [hash, hash+1, ..., hash+7]
//  3. For each slot:
//     - If nil → address not found, return nil
//     - If addr matches → found, return VarState
//     - Else → collision, continue probing
//  4. After 8 probes → collision overflow, return nil (rare)
//
// Performance:
//   - Hit (first probe): ~8ns, 0 allocs
//   - Miss (probe exhausted): ~20ns, 0 allocs
//   - Collision rate: <0.1% for typical workloads
//
// Thread Safety: Safe for concurrent calls. Uses atomic.Pointer.Load().
//
// Example:
//
//	vs := shadow.Load(0x1234)
//	if vs == nil {
//	    // Address never accessed, or collision overflow.
//	} else {
//	    // Check write epoch.
//	    lastWrite := vs.W
//	}
//
//go:nosplit
func (s *CASBasedShadow) Load(addr uintptr) *VarState {
	// v0.3.0: Apply address compression if enabled.
	if s.compressAddresses {
		addr = alignAddr(addr)
	}

	hash := fastHash(addr)

	// Linear probing: try up to 8 slots.
	// 8 probes covers 99.99% of cases for load factor <0.8.
	for i := uint64(0); i < 8; i++ {
		idx := (hash + i) & 0xFFFF // Wrap around (equivalent to % 65536).
		cellPtr := s.cells[idx].Load()

		if cellPtr == nil {
			// Empty slot → address not found.
			return nil
		}

		if cellPtr.addr == addr {
			// Found matching address.
			return cellPtr.varState
		}

		// Collision: this slot occupied by different address, try next.
	}

	// Collision overflow after 8 probes (extremely rare, <0.01%).
	// Return nil to indicate "not found".
	return nil
}

// Store stores a VarState for the given address, creating a new CASCell.
//
// This is the slow path for creating a new shadow cell. It allocates a CASCell
// and attempts to CAS it into the array using linear probing.
//
// Algorithm:
//  1. Allocate new CASCell with addr and VarState
//  2. Compute FNV-1a hash of address → initial index
//  3. Linear probe up to 8 slots: [hash, hash+1, ..., hash+7]
//  4. For each slot:
//     - If nil → attempt CAS to store cell, return on success
//     - If addr matches → someone else stored it, return existing
//     - Else → collision, continue probing
//  5. After 8 probes → collision overflow, allocate VarState and return
//
// Performance:
//   - Success (first probe): ~20ns, 1 alloc (CASCell)
//   - Collision (8 probes): ~50ns, 1 alloc
//   - CAS retry: Adds ~5ns per retry
//
// Thread Safety: Safe for concurrent calls. Uses atomic.Pointer.CompareAndSwap().
//
// Parameters:
//   - addr: Memory address to store
//   - vs: VarState to store (must not be nil)
//
// Returns:
//   - *VarState: The stored VarState (either vs, or existing one if race occurred)
//
// Note: This is NOT marked //go:nosplit because it allocates (CASCell creation).
func (s *CASBasedShadow) Store(addr uintptr, vs *VarState) *VarState {
	// v0.3.0: Apply address compression if enabled.
	if s.compressAddresses {
		addr = alignAddr(addr)
	}

	// Allocate new cell (happens outside the CAS loop for efficiency).
	newCell := &CASCell{
		addr:     addr,
		varState: vs,
	}

	hash := fastHash(addr)

	// Linear probing: try up to 8 slots.
	for i := uint64(0); i < 8; i++ {
		idx := (hash + i) & 0xFFFF

		// Load current cell.
		cellPtr := s.cells[idx].Load()

		// If slot is empty, attempt to CAS our cell in.
		if cellPtr == nil {
			if s.cells[idx].CompareAndSwap(nil, newCell) {
				// Successfully stored at this index.
				return vs
			}
			// CAS failed, someone else stored. Reload and check.
			cellPtr = s.cells[idx].Load()
		}

		// Slot is non-empty. Check if it's our address.
		if cellPtr != nil && cellPtr.addr == addr {
			// Address already exists (lost the race), return existing VarState.
			return cellPtr.varState
		}

		// Collision: this slot occupied by different address, try next.
	}

	// Collision overflow after 8 probes.
	// This is extremely rare (<0.01%) but we handle it gracefully:
	// Return the VarState we tried to store (caller can use it locally).
	return vs
}

// LoadOrStore retrieves or creates the VarState for the given address.
//
// This is the primary method for accessing shadow memory in the FastTrack detector.
// It implements "get or create" semantics: returns existing VarState if present,
// otherwise allocates and stores a new one.
//
// Algorithm:
//  1. Try Load() first (fast path, zero allocs if present)
//  2. If not found, allocate new VarState
//  3. Try Store() to insert it (CAS-based)
//  4. Return final VarState (either new or winner of CAS race)
//
// Performance:
//   - Hit (existing cell): ~10ns, 0 allocs (Load fast path)
//   - Miss (new cell): ~25ns, 2 allocs (VarState + CASCell)
//   - Concurrent creation: One allocation wins, others discarded by GC
//
// Thread Safety: Safe for concurrent calls. Multiple goroutines calling
// LoadOrStore for the same address will result in exactly one VarState stored,
// and all callers will receive a pointer to that single instance.
//
// Parameters:
//   - addr: Memory address to look up or create
//
// Returns:
//   - *VarState: Pointer to VarState (never nil)
//   - bool: true if cell was newly created, false if it already existed
//
// Example:
//
//	vs, created := shadow.LoadOrStore(0x1234)
//	if created {
//	    // First access to this address.
//	    vs.W = epoch.NewEpoch(tid, clock)
//	} else {
//	    // Address already tracked, check for race.
//	    if vs.W.HappensBefore(currentEpoch) {
//	        // No race.
//	    }
//	}
//
// Note: This is NOT marked //go:nosplit because it calls Store which allocates.
func (s *CASBasedShadow) LoadOrStore(addr uintptr) (*VarState, bool) {
	// Fast path: Try to load existing cell (zero allocations).
	if vs := s.Load(addr); vs != nil {
		return vs, false // Found existing.
	}

	// Slow path: Cell doesn't exist, allocate new VarState.
	newVS := NewVarState()

	// Store the new VarState (CAS-based insertion).
	// If another goroutine stores the same address concurrently,
	// Store() will return the winner's VarState.
	finalVS := s.Store(addr, newVS)

	// Check if we won the race (our VarState was stored).
	created := (finalVS == newVS)

	return finalVS, created
}

// GetOrCreate retrieves or creates the VarState for the given address.
//
// This is a wrapper around LoadOrStore that discards the "created" boolean.
// Provides API compatibility with ShadowMemory interface.
//
// Parameters:
//   - addr: Memory address to look up or create
//
// Returns:
//   - *VarState: Pointer to VarState (never nil)
func (s *CASBasedShadow) GetOrCreate(addr uintptr) *VarState {
	vs, _ := s.LoadOrStore(addr)
	return vs
}

// Reset clears all shadow memory cells.
//
// This is used for testing and reinitialization. After Reset(), all
// previously tracked addresses are forgotten and the shadow memory is empty.
//
// Implementation: Zero out all atomic pointers in the array.
// The old CASCell and VarState allocations will be garbage collected
// when no references remain.
//
// Performance: O(N) where N = 65536 (array size).
// Typical time: ~50μs (50,000ns) to clear all slots.
//
// Thread Safety: NOT safe for concurrent access during Reset().
// Caller must ensure no other goroutines are accessing the shadow memory.
//
// Note: This is NOT marked //go:nosplit because it's not on hot path.
func (s *CASBasedShadow) Reset() {
	// Clear all slots by storing nil.
	for i := range s.cells {
		s.cells[i].Store(nil)
	}
}

// GetCollisionStats returns statistics about hash collision rate.
//
// This is used for monitoring and debugging to ensure the hash function
// and array size are appropriate for the workload.
//
// Returns:
//   - totalSlots: Total number of slots in array (always 65536)
//   - occupiedSlots: Number of non-nil slots
//   - collisionChains: Number of slots occupied due to collisions
//
// Example:
//
//	total, occupied, collisions := shadow.GetCollisionStats()
//	loadFactor := float64(occupied) / float64(total)
//	collisionRate := float64(collisions) / float64(occupied)
//	fmt.Printf("Load: %.1f%%, Collisions: %.2f%%\n", loadFactor*100, collisionRate*100)
//
// Note: This is for diagnostics only, not used on hot path.
func (s *CASBasedShadow) GetCollisionStats() (totalSlots, occupiedSlots, collisionChains int) {
	totalSlots = len(s.cells)
	occupiedSlots = 0
	collisionChains = 0

	// Track addresses we've seen (to detect collisions).
	seen := make(map[uintptr]bool)

	for i := range s.cells {
		cellPtr := s.cells[i].Load()
		if cellPtr == nil {
			continue
		}

		occupiedSlots++

		// Check if this address hashes to this index naturally.
		expectedIdx := fastHash(cellPtr.addr)
		if uint64(i) != expectedIdx {
			// This cell is here due to collision (linear probing).
			collisionChains++
		}

		seen[cellPtr.addr] = true
	}

	return totalSlots, occupiedSlots, collisionChains
}
