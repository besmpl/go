package syncshadow

import (
	"internal/runtime/atomic"
)

// syncCell is a cell in the CAS-based sync shadow map.
type syncCell struct {
	addr    uintptr
	syncVar *SyncVar
}

// SyncShadow manages shadow memory for synchronization primitives.
//
// This maps each sync primitive address (uintptr) to its SyncVar, which
// tracks the release clock for happens-before tracking.
//
// Implementation:
//   - Uses CAS-based fixed-size array for runtime-compatible concurrent access
//   - SyncVar allocated on first access to a mutex address
//   - Never freed (mutexes typically live for program lifetime)
//
// Memory Model:
//   - Key: uintptr (address of sync.Mutex, sync.RWMutex, etc.)
//   - Value: *SyncVar (tracks release clock)
//
// Thread Safety: All methods are safe for concurrent calls.
type SyncShadow struct {
	// cells is a fixed-size array for CAS-based sync var storage.
	// Using atomic.Pointer for lock-free access.
	cells [16384]atomic.Pointer[syncCell]
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
	return hash >> 50 // 14 bits = 16384 slots
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

	// Linear probing: try up to 8 slots.
	for i := uint64(0); i < 8; i++ {
		idx := (hash + i) & 0x3FFF // 16384 - 1

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

	// Overflow - allocate standalone SyncVar.
	// This is rare but handles edge cases.
	return &SyncVar{}
}

// Reset clears all sync variable state.
//
// Thread Safety: NOT safe for concurrent access.
func (s *SyncShadow) Reset() {
	for i := range s.cells {
		s.cells[i].Store(nil)
	}
}
