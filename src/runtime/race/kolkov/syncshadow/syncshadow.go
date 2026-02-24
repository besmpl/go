package syncshadow

import (
	"internal/runtime/atomic"
)

// syncCell is a cell in the CAS-based sync shadow map.
type syncCell struct {
	addr    uintptr
	syncVar *SyncVar
}

// syncTableSize is the number of slots in the SyncShadow hash table.
// Must be a power of two. 131072 (128K) supports ~60K concurrent sync
// primitives at <50% load factor, preventing overflow in long-running
// benchmark suites that create many short-lived channels/WaitGroups.
const syncTableSize = 131072

// syncTableMask is syncTableSize - 1, used for fast modular indexing.
const syncTableMask = syncTableSize - 1

// syncMaxProbe is the maximum number of linear probing steps before
// falling back to eviction.
const syncMaxProbe = 16

// SyncShadow manages shadow memory for synchronization primitives.
//
// This maps each sync primitive address (uintptr) to its SyncVar, which
// tracks the release clock for happens-before tracking.
//
// Implementation:
//   - Uses CAS-based fixed-size array for runtime-compatible concurrent access
//   - SyncVar allocated on first access to a mutex address
//   - Never freed (mutexes typically live for program lifetime)
//   - On probe overflow, evicts first collision slot to maintain HB chain
//
// Memory Model:
//   - Key: uintptr (address of sync.Mutex, sync.RWMutex, etc.)
//   - Value: *SyncVar (tracks release clock)
//
// Thread Safety: All methods are safe for concurrent calls.
type SyncShadow struct {
	// cells is a fixed-size array for CAS-based sync var storage.
	// Using atomic.Pointer for lock-free access.
	cells [syncTableSize]atomic.Pointer[syncCell]
}

// NewSyncShadow creates and initializes a new SyncShadow instance.
//
// The shadow memory is initially empty. SyncVar entries are created lazily
// on first access to each mutex address.
func NewSyncShadow() *SyncShadow {
	return &SyncShadow{}
}

// fastHashSync computes a fast hash for sync primitive addresses.
//
//go:nosplit
func fastHashSync(addr uintptr) uint64 {
	const goldenRatio = 0x9E3779B97F4A7C15
	hash := uint64(addr) * goldenRatio
	return hash >> 47 // 17 bits = 131072 slots
}

// GetOrCreate returns the SyncVar for the given address, creating it if needed.
//
// This is the primary entry point for accessing sync variable state.
// On first access to an address, a new SyncVar is allocated and stored.
// Subsequent accesses return the existing SyncVar.
//
// Thread Safety: Safe for concurrent calls via CAS operations.
func (s *SyncShadow) GetOrCreate(addr uintptr) *SyncVar {
	hash := fastHashSync(addr)

	// Linear probing: try up to syncMaxProbe slots.
	for i := uint64(0); i < syncMaxProbe; i++ {
		idx := (hash + i) & syncTableMask

		cellPtr := s.cells[idx].Load()

		// Found existing cell for this address.
		if cellPtr != nil && cellPtr.addr == addr {
			return cellPtr.syncVar
		}

		// Empty slot - try to insert.
		if cellPtr == nil {
			newCell := &syncCell{
				addr:    addr,
				syncVar: &SyncVar{},
			}
			if s.cells[idx].CompareAndSwap(nil, newCell) {
				return newCell.syncVar
			}
			// CAS failed, reload and check.
			cellPtr = s.cells[idx].Load()
			if cellPtr != nil && cellPtr.addr == addr {
				return cellPtr.syncVar
			}
		}
		// Collision, try next slot.
	}

	// Overflow — evict first probe slot to maintain HB chain.
	// The evicted entry likely belongs to a GC'd sync primitive
	// (e.g., short-lived channel from a previous benchmark).
	idx := hash & syncTableMask
	newCell := &syncCell{
		addr:    addr,
		syncVar: &SyncVar{},
	}
	s.cells[idx].Store(newCell)
	return newCell.syncVar
}

// HasEntry checks if a sync variable exists for the given address.
//
// This is used to suppress false positive race reports on addresses that
// are used for synchronization (mutex/rwmutex/channel internal state).
// Go's sync primitives use atomic CAS on their internal fields, which
// triggers raceread/racewrite. Since the actual synchronization is tracked
// via raceacquire/racerelease, races on the primitive's own address are
// false positives from the detector's perspective.
//
// Thread Safety: Safe for concurrent calls (read-only atomic loads).
//
//go:nosplit
func (s *SyncShadow) HasEntry(addr uintptr) bool {
	hash := fastHashSync(addr)
	for i := uint64(0); i < syncMaxProbe; i++ {
		idx := (hash + i) & syncTableMask
		cellPtr := s.cells[idx].Load()
		if cellPtr == nil {
			return false
		}
		if cellPtr.addr == addr {
			return true
		}
	}
	return false
}

// Reset clears all sync variable state.
//
// Thread Safety: NOT safe for concurrent access.
func (s *SyncShadow) Reset() {
	for i := range s.cells {
		s.cells[i].Store(nil)
	}
}
