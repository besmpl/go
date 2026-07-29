package syncshadow

import (
	"internal/runtime/atomic"
	"runtime/race/kolkov/vectorclock"
)

// SyncVar tracks happens-before relationships for one runtime-selected
// synchronization address.
//
// The runtime maps synchronization operations to addresses before calling the
// detector. For example, buffered channel elements use per-slot addresses,
// channel close uses the channel address, and WaitGroup Done operations merge
// releases at the WaitGroup address. SyncVar therefore needs only the generic
// release clock; it must not aggregate channel or WaitGroup state itself.
//
// Layout:
//   - releaseMu: serializes release-clock reads and in-place updates
//   - releaseClock: VectorClock captured or merged at Release
//
// Operations:
//   - Acquire: Thread merges releaseClock into its own clock
//   - Release: Thread copies its clock into releaseClock
//   - ReleaseMerge: Thread merges its clock into releaseClock
//
// Lifecycle:
//   - Created on first operation for a synchronization address
//   - Removed from SyncShadow when the allocator clears that address's range
//   - releaseClock allocated lazily on first Release
//
// Example:
//
//	sv := &SyncVar{}
//	// First unlock: sv.releaseClock = nil
//	sv.SetReleaseClock(threadClock)  // Allocates and copies
//	// Next lock: threadClock.Join(sv.releaseClock)
//	sv.SetReleaseClock(threadClock)  // Updates existing clock
type SyncVar struct {
	// releaseMu serializes every access that can observe or mutate the contents
	// of releaseClock. The atomic pointer publishes the initial allocation, but
	// it cannot by itself protect in-place VectorClock updates.
	releaseMu spinlock

	// releaseClock is the vector clock from the last Release operation.
	// nil means no Release has occurred yet (uninitialized mutex).
	//
	// On Acquire (Lock), threads merge this into their own clock to establish
	// happens-before from the previous Unlock.
	//
	// On Release (Unlock), this is updated to the current thread's clock.
	//
	// Thread Safety: releaseMu protects both pointer access and the pointed-to
	// VectorClock. MergeReleaseClock may be called concurrently by multiple
	// goroutines (for example, WaitGroup.Done or RWMutex.RUnlock).
	releaseClock atomic.Pointer[vectorclock.VectorClock]
}

// GetReleaseClock returns the release clock for this sync variable.
//
// Returns nil if no Release has occurred yet (uninitialized mutex).
// The caller should check for nil before using the clock.
//
// GetReleaseClock is intended for single-threaded inspection and tests. The
// returned clock remains owned by SyncVar and may be mutated by a later
// release. Concurrent detector code must use JoinReleaseClock instead.
//
// Example:
//
//	sv := &SyncVar{}
//	clock := sv.GetReleaseClock()  // Returns nil (no releases yet)
//	sv.SetReleaseClock(someClock)
//	clock = sv.GetReleaseClock()   // Returns someClock
func (sv *SyncVar) GetReleaseClock() *vectorclock.VectorClock {
	sv.releaseMu.lock()
	clock := sv.releaseClock.Load()
	sv.releaseMu.unlock()
	return clock
}

// JoinReleaseClock merges the current release clock into dst while holding the
// same lock used by SetReleaseClock and MergeReleaseClock. It reports whether a
// release existed so the owning RaceContext can conservatively invalidate
// exact foreign-projection proofs. This prevents an acquire from observing an
// in-place update halfway through CopyFrom or Join.
func (sv *SyncVar) JoinReleaseClock(dst *vectorclock.VectorClock) bool {
	if dst == nil {
		return false
	}
	sv.releaseMu.lock()
	clock := sv.releaseClock.Load()
	if clock != nil {
		dst.Join(clock)
	}
	sv.releaseMu.unlock()
	return clock != nil
}

// SetReleaseClock sets the release clock for this sync variable.
//
// This is called during Release (Unlock) to capture the current thread's
// vector clock. The clock is copied (not referenced) to avoid aliasing issues.
//
// If releaseClock is nil (first Release), a new VectorClock is allocated.
// Otherwise, the existing clock is updated in place. Widening sparse metadata
// may still grow backing storage.
//
// Parameters:
//   - clock: The vector clock to copy (must not be nil)
//
// Thread Safety: Safe for concurrent release, release-merge, and acquire
// operations. Updates retain the allocated clock after the first release.
//
// Example:
//
//	sv := &SyncVar{}
//	ctx := goroutine.Alloc(0)
//	sv.SetReleaseClock(ctx.C)  // First call: allocates + copies
//	ctx.IncrementClock()
//	sv.SetReleaseClock(ctx.C)  // Second call: updates the retained clock
func (sv *SyncVar) SetReleaseClock(clock *vectorclock.VectorClock) {
	if clock == nil {
		return
	}
	sv.releaseMu.lock()
	old := sv.releaseClock.Load()
	if old == nil {
		// First Release: allocate and store.
		sv.releaseClock.Store(clock.Clone())
	} else {
		// Subsequent Release: update in place under releaseMu.
		old.CopyFrom(clock)
	}
	sv.releaseMu.unlock()
}

// MergeReleaseClock merges a clock into the release clock (for RWMutex).
//
// This is used for RWMutex read unlock (racereleasemerge) where multiple
// readers may have overlapping critical sections. We merge all their clocks
// to capture the union of happens-before relationships.
//
// If releaseClock is nil (first Release), the clock is copied.
// Otherwise, the join operation (element-wise max) is performed in place.
//
// Parameters:
//   - clock: The vector clock to merge (must not be nil)
//
// Thread Safety: Safe for concurrent access from multiple goroutines (for
// example, concurrent RWMutex.RUnlock operations) and concurrent acquires.
//
// Example (RWMutex scenario):
//
//	sv := &SyncVar{}
//	// Reader 1 unlocks
//	sv.MergeReleaseClock(reader1Clock)  // First unlock: copy
//	// Reader 2 unlocks
//	sv.MergeReleaseClock(reader2Clock)  // Second unlock: merge
//	// Writer locks
//	sv.JoinReleaseClock(writerClock)  // Gets union of both readers
func (sv *SyncVar) MergeReleaseClock(clock *vectorclock.VectorClock) {
	if clock == nil {
		return
	}
	sv.releaseMu.lock()
	if old := sv.releaseClock.Load(); old == nil {
		sv.releaseClock.Store(clock.Clone())
	} else {
		old.Join(clock)
	}
	sv.releaseMu.unlock()
}
