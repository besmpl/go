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
//   - releaseMu: serializes canonical release-clock reads and writers
//   - releaseClock: exact canonical value captured or merged at Release
//   - versions: exactly three reusable immutable publication slots
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
//	sv.SetReleaseClock(threadClock)  // Publishes an exact replacement
type SyncVar struct {
	// releaseMu serializes canonical access to releaseClock and every writer.
	// Fast acquires instead pin and revalidate an immutable version slot.
	releaseMu spinlock

	// releaseClock is the vector clock from the last Release operation.
	// nil means no Release has occurred yet (uninitialized mutex).
	//
	// On Acquire (Lock), threads merge this into their own clock to establish
	// happens-before from the previous Unlock.
	//
	// On Release (Unlock), this is updated to the current thread's clock.
	//
	// Thread Safety: releaseMu protects canonical access. MergeReleaseClock may
	// be called concurrently by multiple goroutines (for example, concurrent
	// WaitGroup.Done or RWMutex.RUnlock).
	releaseClock atomic.Pointer[vectorclock.VectorClock]

	// pending is an append-only generation of exact ReleaseMerge projections.
	// Publishers do not read or import this aggregate; Acquire folds it into the
	// canonical release clock once.
	pending atomic.Pointer[mergeGeneration]

	// versions are the only reusable publication slots. current points at an
	// immutable slot. A writer may change only a non-current slot whose pin
	// count is zero, and then publishes it with a fresh non-zero generation.
	versions [3]releaseVersion
	current  atomic.Pointer[releaseVersion]

	// nextGeneration is protected by releaseMu. Once exhausted is set, the
	// canonical path remains available but no slot can ever be republished;
	// this prevents a wrapped generation from reviving an old reader token.
	nextGeneration uint64
	exhausted      bool

	// retired is set only after SyncShadow has removed this identity from its
	// lookup chains. Retired identities remain GC-safe for stale readers, but
	// can never publish another reusable version.
	retired atomic.Uint32

	// sourceProof describes the RaceContext projection captured by the latest
	// Set release. TIDs are process-lifetime identities, so a matching TID and
	// foreign generation proves that the source still dominates this release.
	// folded means releaseClock is an immutable base whose sourceTID coordinate
	// must be replaced by sourceClock. These fields are protected by releaseMu.
	sourceTID               uint32
	sourceClock             uint32
	sourceForeignGeneration uint64
	sourceProofValid        bool
	folded                  bool
}

type releaseVersion struct {
	clock      *vectorclock.VectorClock
	generation atomic.Uint64
	pins       atomic.Uint32
}

func (sv *SyncVar) retire() {
	sv.releaseMu.lock()
	// ClearRange is an allocator-lifecycle callback and may run while the heap
	// is being swept, so retirement must not fold a pending ReleaseMerge: that
	// may materialize a wide projection and allocate. Close publisher admission,
	// invalidate reusable publications, then quiesce and discard the detached
	// generation without materializing it. The address-lifetime cut makes the
	// old identity inert; stale context-cache entries re-resolve the address.
	sv.retired.Store(1)
	sv.current.Store(nil)
	sv.discardPendingLocked()
	sv.releaseMu.unlock()
}

func (sv *SyncVar) sourceDominatedLocked(clock *vectorclock.VectorClock, tid uint32, foreignGeneration uint64) bool {
	return clock != nil && sv.sourceProofValid && sv.sourceTID == tid &&
		sv.sourceForeignGeneration == foreignGeneration && sv.releaseClock.Load() != nil &&
		clock.Get(tid) >= sv.sourceClock
}

func (sv *SyncVar) setSourceProofLocked(tid, clock uint32, foreignGeneration uint64, valid bool) {
	sv.sourceTID = tid
	sv.sourceClock = clock
	sv.sourceForeignGeneration = foreignGeneration
	sv.sourceProofValid = valid
	sv.folded = false
}

// materializeFoldedLocked converts the exact base-plus-owner representation
// into a standalone canonical VectorClock. It is used only by a canonical
// operation, so allocation and shape growth are permitted here.
func (sv *SyncVar) materializeFoldedLocked() *vectorclock.VectorClock {
	base := sv.releaseClock.Load()
	if base == nil || !sv.folded {
		return base
	}
	current := sv.current.Load()
	slot := sv.reusableVersionLocked(current, base, nil, false)
	var result *vectorclock.VectorClock
	if slot == nil {
		result = base.CloneDetached()
	} else if slot.clock == nil {
		slot.clock = base.CloneDetached()
		result = slot.clock
	} else {
		slot.clock.CopyFromDetached(base)
		result = slot.clock
	}
	result.Set(sv.sourceTID, sv.sourceClock)
	sv.folded = false
	sv.publishLocked(slot, result)
	sv.refreshVersionsLocked(result)
	return result
}

func (sv *SyncVar) nextGenerationLocked() (uint64, bool) {
	if sv.exhausted {
		return 0, false
	}
	next := sv.nextGeneration + 1
	if next == 0 {
		sv.exhausted = true
		return 0, false
	}
	sv.nextGeneration = next
	if next == ^uint64(0) {
		sv.exhausted = true
	}
	return next, true
}

// reusableVersionLocked returns a non-current, unpinned slot. exclude is an
// authoritative clock that may still be mutable through the canonical path
// even when current is nil, so its slot must not be selected as scratch.
func (sv *SyncVar) reusableVersionLocked(current *releaseVersion, excludeA, excludeB *vectorclock.VectorClock, requireClock bool) *releaseVersion {
	for i := range sv.versions {
		slot := &sv.versions[i]
		if slot == current || (excludeA != nil && slot.clock == excludeA) ||
			(excludeB != nil && slot.clock == excludeB) || slot.pins.Load() != 0 {
			continue
		}
		if requireClock && slot.clock == nil {
			continue
		}
		return slot
	}
	return nil
}

func (sv *SyncVar) canPublishLocked() bool {
	return !sv.exhausted && sv.nextGeneration != ^uint64(0)
}

func (sv *SyncVar) publishLocked(slot *releaseVersion, clock *vectorclock.VectorClock) bool {
	sv.releaseClock.Store(clock)
	generation, ok := sv.nextGenerationLocked()
	if !ok || slot == nil || sv.retired.Load() != 0 {
		sv.current.Store(nil)
		return false
	}
	slot.generation.Store(generation)
	sv.current.Store(slot)
	return true
}

// refreshVersionsLocked provisions inactive reusable clocks only on the
// canonical path. Existing inactive clocks deliberately remain stale: every
// fast writer overwrites its selected scratch clock with TryCopyFrom before
// publication, so recopying all three slots on every canonical fallback is
// pure O(vector-clock-size) overhead. A first publication still allocates each
// slot once, ensuring later warmed writers have owned storage to overwrite.
func (sv *SyncVar) refreshVersionsLocked(clock *vectorclock.VectorClock) {
	if clock == nil {
		return
	}
	current := sv.current.Load()
	for i := range sv.versions {
		slot := &sv.versions[i]
		if slot == current || slot.pins.Load() != 0 {
			continue
		}
		if slot.clock == nil {
			slot.clock = clock.CloneDetached()
		} else if !slot.clock.CanCopyFromDetached(clock) {
			// Capacity growth is a canonical-path effect. CopyFrom both grows and
			// initializes the slot; warmed same-shape fallbacks skip this work.
			slot.clock.CopyFromDetached(clock)
		}
	}
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
	if sv.retired.Load() != 0 {
		sv.releaseMu.unlock()
		return nil
	}
	clock := sv.foldPendingLocked()
	sv.releaseMu.unlock()
	return clock
}

// TryJoinReleaseClockForContext first checks the exact same-source dominance
// proof under the writer lock. A successful proof makes the join a no-op; no
// vector-clock coordinate can be missing because ForeignGeneration changes on
// every imported projection while own-clock advances leave it unchanged.
func (sv *SyncVar) TryJoinReleaseClockForContext(dst *vectorclock.VectorClock, tid uint32, foreignGeneration uint64) (joined, ok bool) {
	if dst == nil || sv.retired.Load() != 0 || sv.pending.Load() != nil || !sv.releaseMu.tryLock() {
		return false, false
	}
	if sv.retired.Load() != 0 || sv.pending.Load() != nil {
		sv.releaseMu.unlock()
		return false, false
	}
	if sv.pending.Load() == nil && sv.sourceDominatedLocked(dst, tid, foreignGeneration) {
		sv.releaseMu.unlock()
		return false, true
	}
	sv.releaseMu.unlock()
	return sv.TryJoinReleaseClock(dst)
}

// TryJoinReleaseClock imports one exact immutable published version without
// waiting or allocating. joined reports whether a release was imported; ok
// reports whether the fast operation completed. An empty, retired, contended,
// or capacity-insufficient case conservatively returns ok=false.
func (sv *SyncVar) TryJoinReleaseClock(dst *vectorclock.VectorClock) (joined, ok bool) {
	if dst == nil || sv.retired.Load() != 0 || sv.pending.Load() != nil {
		return false, false
	}
	slot := sv.current.Load()
	if slot == nil {
		return false, false
	}
	generation := slot.generation.Load()
	if generation == 0 {
		return false, false
	}
	pins := slot.pins.Load()
	if pins == ^uint32(0) || !slot.pins.CompareAndSwap(pins, pins+1) {
		return false, false
	}
	if sv.retired.Load() != 0 || sv.pending.Load() != nil || sv.current.Load() != slot || slot.generation.Load() != generation {
		slot.pins.Add(-1)
		return false, false
	}
	ok = dst.TryJoin(slot.clock)
	slot.pins.Add(-1)
	if !ok {
		return false, false
	}
	return true, true
}

// TrySetReleaseClock publishes an exact replacement using a pre-provisioned
// inactive slot. Scratch preparation may change only that unpublished slot;
// failure never changes the current or canonical release value.
func (sv *SyncVar) TrySetReleaseClock(src *vectorclock.VectorClock) bool {
	return sv.trySetReleaseClock(src, 0, 0, false)
}

// TrySetReleaseClockForContext publishes an exact replacement while retaining
// a same-source projection proof. Once warmed, repeated releases from a context
// whose foreign projection is unchanged update only its owner coordinate and
// invalidate the immutable generic publication. A foreign acquire falls back
// and materializes the exact base-plus-owner clock.
func (sv *SyncVar) TrySetReleaseClockForContext(src *vectorclock.VectorClock, tid uint32, foreignGeneration uint64) bool {
	return sv.trySetReleaseClock(src, tid, foreignGeneration, true)
}

func (sv *SyncVar) trySetReleaseClock(src *vectorclock.VectorClock, tid uint32, foreignGeneration uint64, proof bool) bool {
	if src == nil || sv.retired.Load() != 0 || sv.pending.Load() != nil || !sv.releaseMu.tryLock() {
		return false
	}
	defer sv.releaseMu.unlock()
	if sv.retired.Load() != 0 || sv.pending.Load() != nil {
		return false
	}
	if proof && sv.sourceDominatedLocked(src, tid, foreignGeneration) {
		// current may still be pinned by a reader which sampled the preceding
		// release. Clearing it makes that reader's generation revalidation fail;
		// the immutable clock itself is not mutated while pinned.
		sv.current.Store(nil)
		sv.sourceClock = src.Get(tid)
		sv.folded = true
		return true
	}
	if !sv.canPublishLocked() {
		return false
	}
	current := sv.current.Load()
	if sv.folded {
		base := sv.releaseClock.Load()
		slot := sv.reusableVersionLocked(current, base, nil, true)
		if slot == nil || !slot.clock.TryCopyFromDetached(src) {
			return false
		}
		if !sv.publishLocked(slot, slot.clock) {
			return false
		}
		sv.setSourceProofLocked(tid, src.Get(tid), foreignGeneration, proof)
		return true
	}
	if current == nil {
		return false
	}
	slot := sv.reusableVersionLocked(current, nil, nil, true)
	if slot == nil || !slot.clock.TryCopyFromDetached(src) {
		return false
	}
	if sv.retired.Load() != 0 {
		return false
	}
	if !sv.publishLocked(slot, slot.clock) {
		return false
	}
	sv.setSourceProofLocked(tid, src.Get(tid), foreignGeneration, proof)
	return true
}

// TryMergeReleaseClock publishes the exact union of the current release and
// src. Both preparation steps are all-or-nothing for their destination; a
// failed second step may leave only the unpublished scratch slot changed.
func (sv *SyncVar) TryMergeReleaseClock(src *vectorclock.VectorClock) bool {
	return sv.TryPublishReleaseMergeForContext(src, 0, 0)
}

// TryMergeReleaseClockForContext folds a same-source merge into the owner
// coordinate when the source's foreign projection is unchanged. Otherwise it
// retains the immutable-version merge path.
func (sv *SyncVar) TryMergeReleaseClockForContext(src *vectorclock.VectorClock, tid uint32, foreignGeneration uint64) bool {
	return sv.TryPublishReleaseMergeForContext(src, tid, foreignGeneration)
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
	if sv.retired.Load() != 0 {
		sv.releaseMu.unlock()
		return false
	}
	clock := sv.foldPendingLocked()
	if clock != nil {
		dst.Join(clock)
	}
	sv.releaseMu.unlock()
	return clock != nil
}

// JoinReleaseClockForContext is the canonical identity-aware acquire. It keeps
// a same-source release folded and dominated; every other source materializes
// and imports the full exact release before returning.
func (sv *SyncVar) JoinReleaseClockForContext(dst *vectorclock.VectorClock, tid uint32, foreignGeneration uint64) bool {
	if dst == nil {
		return false
	}
	sv.releaseMu.lock()
	if sv.retired.Load() != 0 {
		sv.releaseMu.unlock()
		return false
	}
	if sv.pending.Load() == nil && sv.sourceDominatedLocked(dst, tid, foreignGeneration) {
		sv.releaseMu.unlock()
		return false
	}
	clock := sv.foldPendingLocked()
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
// The canonical path may allocate or grow an inactive slot, then publishes it
// immutably. It also refreshes other unpinned slots opportunistically.
//
// Parameters:
//   - clock: The vector clock to copy (must not be nil)
//
// Thread Safety: Safe for concurrent release, release-merge, and acquire
// operations.
//
// Example:
//
//	sv := &SyncVar{}
//	ctx := goroutine.Alloc(0)
//	sv.SetReleaseClock(ctx.C)  // First call: allocates + copies
//	ctx.IncrementClock()
//	sv.SetReleaseClock(ctx.C)  // Second call: updates the retained clock
func (sv *SyncVar) SetReleaseClock(clock *vectorclock.VectorClock) {
	sv.setReleaseClock(clock, 0, 0, false)
}

// SetReleaseClockForContext is the canonical proof-producing Set operation.
func (sv *SyncVar) SetReleaseClockForContext(clock *vectorclock.VectorClock, tid uint32, foreignGeneration uint64) {
	sv.setReleaseClock(clock, tid, foreignGeneration, true)
}

func (sv *SyncVar) setReleaseClock(clock *vectorclock.VectorClock, tid uint32, foreignGeneration uint64, proof bool) {
	if clock == nil {
		return
	}
	sv.releaseMu.lock()
	if sv.retired.Load() != 0 {
		sv.releaseMu.unlock()
		return
	}
	sv.discardPendingLocked()
	current := sv.current.Load()
	sv.current.Store(nil)
	slot := sv.reusableVersionLocked(current, nil, nil, false)
	var result *vectorclock.VectorClock
	if slot == nil {
		result = clock.CloneDetached()
	} else if slot.clock == nil {
		slot.clock = clock.CloneDetached()
		result = slot.clock
	} else {
		slot.clock.CopyFromDetached(clock)
		result = slot.clock
	}
	sv.publishLocked(slot, result)
	sv.setSourceProofLocked(tid, clock.Get(tid), foreignGeneration, proof)
	sv.refreshVersionsLocked(result)
	sv.releaseMu.unlock()
}

// MergeReleaseClock merges a clock into the release clock (for RWMutex).
//
// This is used for RWMutex read unlock (racereleasemerge) where multiple
// readers may have overlapping critical sections. We merge all their clocks
// to capture the union of happens-before relationships.
//
// The canonical path constructs the exact union in an inactive slot or a
// standalone fallback clock, then publishes the completed value.
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
	sv.PublishReleaseMergeForContext(clock, 0, 0)
}
