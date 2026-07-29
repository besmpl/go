// Package shadowmem implements shadow memory cells for FastTrack race detection.
//
// Shadow memory stores the access history for every instrumented memory location.
// VarState is the basic building block - a single cell tracking the last write
// and last read to a variable.
package shadowmem

import (
	"internal/runtime/atomic"
	"unsafe"

	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/vectorclock"
)

//go:linkname runtimeThrow runtime.throw
func runtimeThrow(s string)

//go:linkname runtimeKolkovSpinWait runtime.kolkovSpinWait
func runtimeKolkovSpinWait(cycles uint32, yield bool)

var nextLifecycleID atomic.Uint64

// lifecycleID is a monotonically allocated shadow-object generation. Seven
// bytes are sufficient for every realizable detector run and fit in the
// alignment gap between readerCount and readClock, keeping VarState at its
// 128-byte size class. It is immutable except while an otherwise unreachable
// state is constructed or Reset is called under accessMu.
type lifecycleID [7]byte

const maxLifecycleID = uint64(1)<<56 - 1

func allocateLifecycleID() lifecycleID {
	id := nextLifecycleID.Add(1)
	if id == 0 || id > maxLifecycleID {
		runtimeThrow("race detector exhausted shadow lifecycle IDs")
	}
	return lifecycleID{
		byte(id),
		byte(id >> 8),
		byte(id >> 16),
		byte(id >> 24),
		byte(id >> 32),
		byte(id >> 40),
		byte(id >> 48),
	}
}

// uint64 expands the packed identity on the cold report path.
func (id lifecycleID) uint64() uint64 {
	return uint64(id[0]) |
		uint64(id[1])<<8 |
		uint64(id[2])<<16 |
		uint64(id[3])<<24 |
		uint64(id[4])<<32 |
		uint64(id[5])<<40 |
		uint64(id[6])<<48
}

// spinlock is a simple spinlock for runtime-compatible locking.
// Uses atomic operations only, avoiding sync.Mutex dependency.
type spinlock struct {
	state atomic.Uint32
}

// Keep the maximum pause short enough to react promptly to an unlock while
// reducing cache-line traffic under contention. The runtime helper preserves
// nosplit spinning on g0 and yields schedulable callers only at this cap.
const spinlockMaxBackoff = uint32(64)

//go:nosplit
func (s *spinlock) lock() {
	for delay := uint32(1); ; {
		if s.state.Load() == 0 && s.state.CompareAndSwap(0, 1) {
			return
		}
		runtimeKolkovSpinWait(delay, delay == spinlockMaxBackoff)
		if delay < spinlockMaxBackoff {
			delay <<= 1
		}
	}
}

//go:nosplit
func (s *spinlock) tryLock() bool {
	return s.state.CompareAndSwap(0, 1)
}

//go:nosplit
func (s *spinlock) unlock() {
	s.state.Store(0)
}

const (
	// maxInlineReaders is the number of inline reader slots in VarState.
	// When exceeded, VarState promotes to VectorClock (1KB allocation).
	// 4 slots is optimal: covers 95%+ of read-shared patterns while keeping VarState small.
	// Research: SmartTrack PLDI 2020 + TSAN multi-cell approach.
	maxInlineReaders = 4

	// promotedMarker indicates the VarState has been promoted to VectorClock.
	// readerCount == promotedMarker means readClock is active.
	promotedMarker uint8 = 255

	// promotedMarker32 is the atomic.Uint32 version of promotedMarker for readerState.
	// Using 0xFF to match promotedMarker semantics.
	promotedMarker32 uint32 = 0xFF
)

// VarState stores one exact ordinary-memory history. On 64-bit targets it is
// deliberately 128 bytes, enforced by tests. A write epoch and up to four read
// epochs stay inline; additional concurrent readers promote to a pooled vector
// clock. accessMu linearizes detector transactions, while mu protects changes
// to the adaptive read representation. The optional atomic state is detached
// behind a pointer so ordinary histories retain the fixed size class.
type VarState struct {
	// Lock-free hot-path fields (atomic operations, no mutex needed):
	// These fields are accessed on EVERY memory access, so lock-free is critical.
	W               atomic.Uint64  // Last write epoch (always present). Stores epoch.Epoch as uint64.
	exclusiveWriter atomic.Int64   // TID of sole writer, -1 if shared, 0 if uninitialized.
	writePC         atomic.Uintptr // PC (program counter) of last write caller (8 bytes).
	readPC          atomic.Uintptr // PC (program counter) of last read caller (8 bytes).

	// Atomically published read-tracking discriminator and primary epoch.
	// IsPromoted/GetReadEpoch remain lock-free; mutations are serialized by mu
	// so promotion and demotion cannot lose a completed reader.
	//
	// Synchronization contract:
	//   - readEpoch0 mirrors readEpochs[0] for lock-free reads on the fast path.
	//   - readerState tracks reader count: 0=none, 1-4=inline count, 0xFF=promoted.
	//   - Promotion: publish promoted state under mu after readClock contains
	//     every completed reader, then retire readEpoch0.
	//   - Demotion: retire readEpoch0/state and readClock together under mu.
	//   - This ordering ensures IsPromoted()==true always implies readClock!=nil.
	readEpoch0  atomic.Uint64 // Mirrors readEpochs[0] for lock-free single-reader fast path.
	readerState atomic.Uint32 // 0=no readers, 1-4=inline count, 0xFF=promoted to VectorClock.

	// accessMu linearizes a detector access's validation and publication. This
	// is separate from mu so representation helpers can lock mu while the
	// enclosing read or write transaction remains exclusive.
	accessMu spinlock

	// Spinlock-protected fields (complex operations):
	// These are accessed less frequently or require complex multi-field updates.
	mu spinlock // Protects read fields, readClock, and write counters from concurrent access.

	// Keeping this diagnostic counter next to mu uses padding that would
	// otherwise be lost and leaves room for the lazy atomic state without
	// growing VarState.
	writeCount uint32 // Number of writes (for statistics and debugging).

	// Inline slots retain up to four concurrent readers before vector-clock
	// promotion.
	//
	// States:
	//   - readerCount == 0: No readers
	//   - readerCount == 1-4: Use readEpochs[0..readerCount-1]
	//   - readerCount == 255 (maxInlineReaders+1): Promoted to readClock
	//
	// If readClock != nil → use readClock (5+ readers, promoted state)
	readEpochs  [maxInlineReaders]epoch.Epoch // Inline reader slots (32 bytes = 4 × 8).
	readerCount uint8                         // Number of inline readers (0-4, or 255 if promoted).
	lifecycleID lifecycleID                   // Allocator generation, packed into readerCount's alignment gap.
	readClock   *vectorclock.VectorClock      // Promoted actual-read event frontier (8-byte pointer + clock allocation).

	// Hash references to stack depot for the previous write/read.
	// Enables complete race reports showing both current and previous stacks.
	writeStackHash uint64 // Hash of stack trace for last write (8 bytes).
	readStackHash  uint64 // Hash of stack trace for last read (8 bytes, only set when read-shared).

	// atomicState points to a lazily allocated sidecar which owns the opaque
	// detector history and the exact atomic-only transaction capability. Keeping
	// the sidecar behind this existing GC-visible word avoids growing VarState or
	// ShadowSlot. accessMu serializes attachment/reset and copy-on-write; cloned
	// ordinary groups share the binding, whose detector lock serializes history.
	atomicState atomic.Pointer[AtomicFastPath]
}

// LockAccess starts one linearized detector access to this exact address.
//
//go:nosplit
func (vs *VarState) LockAccess() {
	vs.accessMu.lock()
}

// TryLockAccess starts a linearized detector access without waiting.
//
//go:nosplit
func (vs *VarState) TryLockAccess() bool {
	return vs.accessMu.tryLock()
}

// UnlockAccess completes an access started by LockAccess.
//
//go:nosplit
func (vs *VarState) UnlockAccess() {
	vs.accessMu.unlock()
}

// GetAtomicState returns the opaque detector-owned history in the current
// sidecar. Callers that dereference the result must hold the access lock or a
// retained, revalidated AtomicFastPath transaction.
//
//go:nosplit
func (vs *VarState) GetAtomicState() unsafe.Pointer {
	binding := vs.atomicState.Load()
	if binding == nil {
		return nil
	}
	return binding.overlay
}

// SetAtomicState publishes a new sidecar for opaque detector-owned history.
// The caller must hold the access lock and must have escaped any mapped fast
// capability through its ShadowSlot protocol.
func (vs *VarState) SetAtomicState(state unsafe.Pointer) {
	if binding := vs.atomicState.Load(); binding != nil {
		binding.escapeIncompatible()
	}
	vs.atomicState.Store(&AtomicFastPath{overlay: state})
}

// NewVarState creates a new zero-initialized variable state.
//
// A zero VarState represents a variable that has never been accessed.
// Both W and readEpoch are zero (TID=0, Clock=0), readClock is nil.
//
//go:nosplit
func NewVarState() *VarState {
	return &VarState{lifecycleID: allocateLifecycleID()}
}

// CloneOrdinaryLocked returns a deep copy of all ordinary FastTrack state.
// The caller must hold vs's access lock, which stabilizes both the atomic
// fields and the representation protected by mu.
//
// The detector-owned atomic overlay is shared, not duplicated. Its own lock
// protects width-aware per-lane history, so copy-on-write ordinary groups can
// safely retain the same overlay while their FastTrack histories diverge.
func (vs *VarState) CloneOrdinaryLocked() *VarState {
	clone := &VarState{lifecycleID: vs.lifecycleID}
	clone.W.Store(vs.W.Load())
	clone.exclusiveWriter.Store(vs.exclusiveWriter.Load())
	clone.writePC.Store(vs.writePC.Load())
	clone.readPC.Store(vs.readPC.Load())
	clone.readEpoch0.Store(vs.readEpoch0.Load())
	clone.readerState.Store(vs.readerState.Load())
	clone.atomicState.Store(vs.atomicState.Load())

	vs.mu.lock()
	clone.writeCount = vs.writeCount
	clone.readEpochs = vs.readEpochs
	clone.readerCount = vs.readerCount
	if vs.readClock != nil {
		clone.readClock = vs.readClock.Clone()
	}
	clone.writeStackHash = vs.writeStackHash
	clone.readStackHash = vs.readStackHash
	vs.mu.unlock()

	return clone
}

// GetLifecycleID returns the immutable allocator-lifetime identity represented
// by this state. Callers that snapshot a state for deferred reporting retain it
// alongside the access metadata.
func (vs *VarState) GetLifecycleID() uint64 {
	return vs.lifecycleID.uint64()
}

// Reset resets the variable state to zero.
//
// This is used when a memory location is freed and reused.
// After Reset(), the state represents a fresh, never-accessed variable.
// If promoted, this demotes back to fast path (releases VectorClock to pool).
//
// Reset also clears ownership, stack, PC, inline-reader, and atomic sidecar
// state. A promoted vector clock is returned to its pool.
func (vs *VarState) Reset() {
	vs.LockAccess()
	defer vs.UnlockAccess()
	// A retained atomic capability does not hold accessMu. Close and drain it
	// before changing any ordinary field, detector overlay, or lifecycle byte so
	// fast completion observes either the complete old generation or the
	// complete reset generation, never a mixture of both.
	if binding := vs.atomicState.Load(); binding != nil {
		binding.escapeIncompatible()
	}

	// Reset lock-free fields using atomic stores.
	vs.W.Store(0)
	vs.exclusiveWriter.Store(0)
	vs.writePC.Store(0)
	vs.readPC.Store(0)

	// Reset mutex-protected fields.
	vs.mu.lock()
	vs.readerState.Store(0)
	vs.readEpoch0.Store(0)
	// Release VectorClock back to pool if promoted.
	if vs.readClock != nil {
		vs.readClock.Release()
	}
	// Clear all inline reader slots.
	for i := range vs.readEpochs {
		vs.readEpochs[i] = 0
	}
	vs.readerCount = 0
	vs.readClock = nil
	vs.writeCount = 0
	vs.writeStackHash = 0
	vs.readStackHash = 0
	vs.mu.unlock()

	// The binding was closed and drained before any state mutation above.
	vs.atomicState.Store(nil)
	vs.lifecycleID = allocateLifecycleID()
}

// IsPromoted returns true if VarState uses VectorClock (promoted state).
//
// This is the discriminator for the adaptive representation:
//   - false: Fast path (inline reader slots, up to 4 readers, 32 bytes)
//   - true: Slow path (readClock, 5+ readers, 1KB allocation)
//
// The promotion/demotion ordering contract guarantees that when
// readerState==promotedMarker32, readClock is non-nil.
//
//go:nosplit
func (vs *VarState) IsPromoted() bool {
	return vs.readerState.Load() == promotedMarker32
}

// PromoteToReadClock upgrades from inline reader slots to multi-reader VectorClock.
//
// Promotion happens when all four inline reader slots are full and another
// concurrent reader must be represented.
//
// Steps:
//  1. Allocate VectorClock from pool (one-time cost, reused allocation)
//  2. Copy ALL inline reader epochs into VectorClock
//  3. Discard read events ordered before the new reader
//  4. Record the new reader's epoch
//  5. Set readerCount = promotedMarker (marks as promoted)
//
// After promotion, all subsequent reads use VectorClock path.
//
// The vector clock is obtained from the pool.
//
// Parameters:
//   - current: The epoch of the new concurrent reader
//   - observed: The new reader's causal clock, used only to prune old events
func (vs *VarState) PromoteToReadClock(current epoch.Epoch, observed *vectorclock.VectorClock) {
	vs.mu.lock()
	defer vs.mu.unlock()

	// Another reader may have completed promotion while this caller waited.
	// Record only this completed read in the already-published event set.
	if vs.readerCount == promotedMarker {
		recordPromotedRead(vs.readClock, current, observed)
		return
	}

	// Slot 0 is published atomically for lock-free readers and mirrored under
	// this same lock by SetReadEpoch/AddReader.
	if vs.readerCount > 0 {
		vs.readEpochs[0] = epoch.Epoch(vs.readEpoch0.Load())
	}

	// Allocate VectorClock from pool for promoted read tracking.
	vs.readClock = vectorclock.NewFromPool()

	// Copy ALL inline reader epochs into VectorClock.
	for i := uint8(0); i < vs.readerCount && i < maxInlineReaders; i++ {
		if vs.readEpochs[i] != 0 {
			tid, clock := vs.readEpochs[i].Decode()
			//nolint:gosec // G115: Epoch clock is uint64, but per-thread VectorClock uses uint32 (safe truncation).
			vs.readClock.Set(tid, uint32(clock))
		}
	}

	// readClock is an event set, not a causal VectorClock snapshot. Recording
	// the reader's full causal clock would manufacture reads by every thread
	// that this reader has merely observed.
	pruneObservedReadEvents(vs.readClock, observed)
	setReadClockEpoch(vs.readClock, current)

	// Clear inline slots and mark as promoted.
	for i := range vs.readEpochs {
		vs.readEpochs[i] = 0
	}
	vs.readerCount = promotedMarker

	// Publish the discriminator last, while still holding the writer lock. Any
	// SetReadEpoch racing with this transition rechecks readerCount under the
	// lock and joins readClock instead of reviving the inline representation.
	vs.readerState.Store(promotedMarker32)
	vs.readEpoch0.Store(0)
}

// pruneObservedReadEvents drops actual read events dominated by observed in
// one pass. It never imports observed coordinates into the event set.
func pruneObservedReadEvents(readClock, observed *vectorclock.VectorClock) {
	if readClock == nil || observed == nil {
		return
	}
	readClock.PruneLessOrEqual(observed)
}

func setReadClockEpoch(readClock *vectorclock.VectorClock, read epoch.Epoch) {
	if readClock == nil || read == 0 {
		return
	}
	tid, clock := read.Decode()
	//nolint:gosec // Epoch clocks intentionally use the VectorClock's uint32 representation.
	readClock.Set(tid, uint32(clock))
}

// recordPromotedRead maintains the promoted read-event frontier. If this
// logical thread already has a witness, program order places that witness
// before current. Replacing it is therefore exact: any future write which has
// observed current has also observed the old witness, and any write which has
// not observed current can use current itself as the conflict witness.
//
// Pruning other observed readers is only frontier compaction. Retaining those
// real, HB-dominated reads cannot hide a race, so defer the O(frontier) scan
// until a new logical thread must be inserted.
func recordPromotedRead(readClock *vectorclock.VectorClock, current epoch.Epoch, observed *vectorclock.VectorClock) {
	if readClock == nil || current == 0 {
		return
	}
	tid, clock := current.Decode()
	if readClock.Get(tid) != 0 {
		//nolint:gosec // Epoch clocks intentionally use the VectorClock's uint32 representation.
		readClock.Set(tid, uint32(clock))
		return
	}
	pruneObservedReadEvents(readClock, observed)
	//nolint:gosec // Epoch clocks intentionally use the VectorClock's uint32 representation.
	readClock.Set(tid, uint32(clock))
}

// GetReadEpoch returns the first read epoch (backward compatibility).
//
// The first reader is published through atomic readEpoch0.
// For multiple readers, use GetReadEpochs() to get all inline readers.
//
// PRECONDITION: !IsPromoted() - caller must check this first.
// If promoted, this returns 0 (invalid epoch).
//
// This is used by detector OnRead/OnWrite for fast-path checks.
//
//go:nosplit
func (vs *VarState) GetReadEpoch() epoch.Epoch {
	state := vs.readerState.Load()
	if state > 0 && state != promotedMarker32 {
		return epoch.Epoch(vs.readEpoch0.Load())
	}
	return 0
}

// ReadPCForEpoch returns the PC for e only when the ordinary read history has
// one represented reader. VarState stores one aggregate read PC, so after
// inline sharing or promotion it cannot safely attribute that PC to a specific
// conflicting epoch. Callers must treat ok=false as unknown rather than using
// the aggregate PC for suppression decisions.
func (vs *VarState) ReadPCForEpoch(e epoch.Epoch) (pc uintptr, ok bool) {
	if e == 0 {
		return 0, false
	}
	vs.mu.lock()
	if vs.readerCount == 1 && epoch.Epoch(vs.readEpoch0.Load()) == e {
		pc, ok = vs.readPC.Load(), true
	}
	vs.mu.unlock()
	return pc, ok
}

// GetReadEpochs returns all inline reader epochs.
//
// PRECONDITION: !IsPromoted() - caller must check this first.
// If promoted, this returns nil.
//
// Returns a slice of all active reader epochs (0 to 4 elements).
// The caller should check all epochs for happens-before relationships.
func (vs *VarState) GetReadEpochs() []epoch.Epoch {
	vs.mu.lock()
	defer vs.mu.unlock()

	if vs.readerCount == 0 || vs.readerCount == promotedMarker {
		return nil
	}

	// Return a copy of active reader epochs.
	count := int(vs.readerCount)
	if count > maxInlineReaders {
		count = maxInlineReaders
	}
	result := make([]epoch.Epoch, count)
	copy(result, vs.readEpochs[:count])
	return result
}

// GetReaderCount returns the number of inline readers.
//
// Returns:
//   - 0: No readers
//   - 1-4: Number of inline readers
//   - 255 (promotedMarker): Promoted to VectorClock
//
//go:nosplit
func (vs *VarState) GetReaderCount() uint8 {
	return uint8(vs.readerState.Load())
}

// SetReadEpoch sets the read epoch for single-reader fast path.
//
// Reader publication is serialized with promotion and demotion so a completed
// read cannot be lost while the adaptive representation changes. Repeated reads
// are eliminated by the owning RaceContext before reaching this method.
//
// This runs only when per-context redundant-read elimination misses. If the
// state is already promoted, the reader is recorded directly in readClock.
func (vs *VarState) SetReadEpoch(e epoch.Epoch) {
	vs.mu.lock()
	defer vs.mu.unlock()

	if vs.readerCount == promotedMarker {
		// A reader racing with promotion must join the published read clock,
		// rather than writing the retired inline mirror.
		setReadClockEpoch(vs.readClock, e)
		return
	}

	vs.readEpoch0.Store(uint64(e))
	vs.readEpochs[0] = e
	if vs.readerCount == 0 {
		vs.readerCount = 1
	}
	vs.readerState.Store(uint32(vs.readerCount))
}

// AddReader adds or updates a reader in the inline slots.
//
// Logic:
//  1. If TID already exists in slots → update that slot's epoch
//  2. If there's room (readerCount < 4) → add to next slot
//  3. If slots are full → return false (caller should promote)
//
// Returns:
//   - true: Reader added/updated successfully
//   - false: Slots are full, promotion to VectorClock needed
//
// If already promoted, this is a no-op and returns true.
func (vs *VarState) AddReader(e epoch.Epoch) bool {
	vs.mu.lock()
	defer vs.mu.unlock()

	// Already promoted - let VectorClock handle it.
	if vs.readerCount == promotedMarker {
		return true
	}

	// Sync slot 0 from atomic mirror: SetReadEpoch updates only the atomic
	// readEpoch0 field (lock-free hot path). Sync it here before iterating
	// slots, so that AddReader and PromoteToReadClock see the latest value.
	if vs.readerCount > 0 {
		vs.readEpochs[0] = epoch.Epoch(vs.readEpoch0.Load())
	}

	tid, _ := e.Decode()

	// Check if TID already exists in slots.
	for i := uint8(0); i < vs.readerCount; i++ {
		existingTID, _ := vs.readEpochs[i].Decode()
		if existingTID == tid {
			// Update existing slot.
			vs.readEpochs[i] = e
			// Sync atomic mirror if slot 0 was updated.
			if i == 0 {
				vs.readEpoch0.Store(uint64(e))
			}
			return true
		}
	}

	// Check if there's room for new reader.
	if vs.readerCount < maxInlineReaders {
		vs.readEpochs[vs.readerCount] = e
		// Sync atomic mirror if this is the first reader (slot 0).
		if vs.readerCount == 0 {
			vs.readEpoch0.Store(uint64(e))
		}
		vs.readerCount++
		vs.readerState.Store(uint32(vs.readerCount))
		return true
	}

	// Slots are full - caller should promote.
	return false
}

// HasInlineSlot returns true if there's room for another inline reader.
//
//go:nosplit
func (vs *VarState) HasInlineSlot() bool {
	state := vs.readerState.Load()
	return state < maxInlineReaders && state != promotedMarker32
}

// GetReadClock returns the read VectorClock (slow path only).
//
// PRECONDITION: IsPromoted() - caller must check this first.
// If not promoted, this returns nil.
//
// Note: Removed //go:nosplit because sync.Mutex.Lock() requires stack space.
func (vs *VarState) GetReadClock() *vectorclock.VectorClock {
	vs.mu.lock()
	rc := vs.readClock
	vs.mu.unlock()
	return rc
}

// JoinReadClock records a reader after a caller observed promoted state. In
// promoted state it prunes actual read events ordered before observed, but
// never imports observed's non-reader coordinates. If a writer demoted the
// state before the lock was acquired, it republishes the reader in the current
// representation rather than silently dropping it.
func (vs *VarState) JoinReadClock(e epoch.Epoch, observed *vectorclock.VectorClock) {
	vs.mu.lock()
	defer vs.mu.unlock()

	if vs.readerCount == promotedMarker && vs.readClock != nil {
		recordPromotedRead(vs.readClock, e, observed)
		return
	}

	// A stale promoted decision raced with demotion. Preserve this completed
	// reader in the now-current inline state when possible.
	if vs.readerCount == 0 {
		vs.readEpochs[0] = e
		vs.readerCount = 1
		vs.readEpoch0.Store(uint64(e))
		vs.readerState.Store(1)
		return
	}

	tid, _ := e.Decode()
	for i := uint8(0); i < vs.readerCount && i < maxInlineReaders; i++ {
		existingTID, _ := vs.readEpochs[i].Decode()
		if existingTID == tid {
			vs.readEpochs[i] = e
			if i == 0 {
				vs.readEpoch0.Store(uint64(e))
			}
			return
		}
	}

	// Another reader won the demotion race. Retain both completed reads inline
	// while capacity remains.
	if vs.readerCount < maxInlineReaders {
		vs.readEpochs[vs.readerCount] = e
		vs.readerCount++
		vs.readerState.Store(uint32(vs.readerCount))
		return
	}

	// All inline slots were filled after the stale promoted observation.
	// Promote the actual inline read events and this completed read.
	vs.readClock = vectorclock.NewFromPool()
	for i := uint8(0); i < vs.readerCount && i < maxInlineReaders; i++ {
		if vs.readEpochs[i] != 0 {
			readerTID, clock := vs.readEpochs[i].Decode()
			//nolint:gosec // Epoch clocks intentionally use VectorClock's uint32 representation.
			vs.readClock.Set(readerTID, uint32(clock))
		}
	}
	setReadClockEpoch(vs.readClock, e)
	for i := range vs.readEpochs {
		vs.readEpochs[i] = 0
	}
	vs.readerCount = promotedMarker
	vs.readerState.Store(promotedMarker32)
	vs.readEpoch0.Store(0)
}

// ReadClockHappensBefore reports whether every promoted reader happens before
// vc. The comparison is serialized with concurrent reader joins and demotion.
func (vs *VarState) ReadClockHappensBefore(vc *vectorclock.VectorClock) bool {
	vs.mu.lock()
	result := vs.readClock == nil || vs.readClock.HappensBefore(vc)
	vs.mu.unlock()
	return result
}

// FirstConcurrentRead returns one represented read that does not happen before
// vc. The caller normally holds the access lock; mu stabilizes both inline and
// promoted read representations while they are inspected.
func (vs *VarState) FirstConcurrentRead(vc *vectorclock.VectorClock) (epoch.Epoch, bool) {
	vs.mu.lock()
	defer vs.mu.unlock()

	if vs.readerCount == promotedMarker && vs.readClock != nil {
		var concurrent epoch.Epoch
		vs.readClock.Range(func(tid, clock uint32) bool {
			if clock > vc.Get(tid) {
				concurrent = epoch.NewEpoch(tid, uint64(clock))
				return false
			}
			return true
		})
		return concurrent, concurrent != 0
	}

	for i := uint8(0); i < vs.readerCount && i < maxInlineReaders; i++ {
		read := vs.readEpochs[i]
		if i == 0 {
			read = epoch.Epoch(vs.readEpoch0.Load())
		}
		if read != 0 && !read.HappensBefore(vc) {
			return read, true
		}
	}
	return 0, false
}

// Demote clears all read state and demotes back to fast path.
//
// This is called by OnWrite after a write operation to reset read tracking.
// Write dominates all previous reads, so we can safely clear the read state.
//
// A promoted VectorClock is returned to its pool.
func (vs *VarState) Demote() {
	vs.mu.lock()
	defer vs.mu.unlock()

	// Release VectorClock back to pool if promoted.
	if vs.readClock != nil {
		vs.readClock.Release()
	}
	// Clear all inline reader slots.
	for i := range vs.readEpochs {
		vs.readEpochs[i] = 0
	}
	vs.readerCount = 0
	vs.readClock = nil
	vs.readEpoch0.Store(0)
	vs.readerState.Store(0)
}

// Lock-free accessors.

// GetW returns the last write epoch using atomic load (lock-free).
//
// Thread Safety: Lock-free (atomic load).
//
//go:nosplit
func (vs *VarState) GetW() epoch.Epoch {
	return epoch.Epoch(vs.W.Load())
}

// SetW sets the last write epoch using atomic store (lock-free).
//
// Parameters:
//   - e: The epoch to store
//
// Thread Safety: Lock-free (atomic store).
//
//go:nosplit
func (vs *VarState) SetW(e epoch.Epoch) {
	vs.W.Store(uint64(e))
}

// CompareAndSwapW atomically compares and swaps the write epoch (lock-free).
//
// This is used for atomic write epoch updates when racing with other writers.
//
// Parameters:
//   - oldVal: Expected current value
//   - newVal: New value to set
//
// Returns:
//   - true if swap succeeded (current value was 'oldVal')
//   - false if swap failed (current value was not 'oldVal')
//
// Thread Safety: Lock-free (atomic CAS).
//
//go:nosplit
func (vs *VarState) CompareAndSwapW(oldVal, newVal epoch.Epoch) bool {
	return vs.W.CompareAndSwap(uint64(oldVal), uint64(newVal))
}

// Exclusive-writer ownership accessors.

// IsOwned returns true if the variable has an exclusive writer (owned state).
//
// Ownership states:
//   - exclusiveWriter >= 0: Single owner (fast path, skip HB checks)
//   - exclusiveWriter == -1: Shared/multiple writers (full FastTrack)
//   - exclusiveWriter == 0: Uninitialized (no writes yet)
//
// This is used by the detector to decide whether to skip happens-before checks.
//
// Thread Safety: Lock-free (atomic load).
//
//go:nosplit
func (vs *VarState) IsOwned() bool {
	return vs.exclusiveWriter.Load() >= 0
}

// GetExclusiveWriter returns the TID of the exclusive writer, or -1 if shared.
//
// Returns:
//   - TID >= 0: Single exclusive writer (owned)
//   - -1: Shared/multiple writers
//   - 0: Uninitialized (no writes yet)
//
// Thread Safety: Lock-free (atomic load).
//
//go:nosplit
func (vs *VarState) GetExclusiveWriter() int64 {
	return vs.exclusiveWriter.Load()
}

// SetExclusiveWriter sets the exclusive writer TID.
//
// This is called when:
//   - First write: Claim ownership (tid >= 0)
//   - Second writer detected: Promote to shared (tid = -1)
//
// Thread Safety: Lock-free (atomic store).
//
//go:nosplit
func (vs *VarState) SetExclusiveWriter(tid int64) {
	vs.exclusiveWriter.Store(tid)
}

// CompareAndSwapExclusiveWriter atomically compares and swaps the exclusive writer.
//
// This is used to atomically claim ownership when first writing to a variable.
// It solves the TOCTOU race condition where two goroutines both see exclusiveWriter=0
// and both think they're the first writer.
//
// Parameters:
//   - oldVal: Expected current value (typically 0 for first writer claim)
//   - newVal: New value to set (current goroutine's TID)
//
// Returns:
//   - true if swap succeeded (current value was 'oldVal')
//   - false if swap failed (current value was not 'oldVal')
//
// Thread Safety: Lock-free (atomic CAS).
//
//go:nosplit
func (vs *VarState) CompareAndSwapExclusiveWriter(oldVal, newVal int64) bool {
	return vs.exclusiveWriter.CompareAndSwap(oldVal, newVal)
}

// IncrementWriteCount increments the write counter.
//
// This is called on every write to track total write operations.
// Used for statistics and debugging.
//
// Thread Safety: Protected by mutex.
func (vs *VarState) IncrementWriteCount() {
	vs.mu.lock()
	vs.writeCount++
	vs.mu.unlock()
}

// GetWriteCount returns the total number of writes to this variable.
//
// Thread Safety: Protected by mutex.
func (vs *VarState) GetWriteCount() uint32 {
	vs.mu.lock()
	count := vs.writeCount
	vs.mu.unlock()
	return count
}

// String returns a debug representation of the variable state.
//
// Format:
//   - No readers: "W:<epoch> R:[]"
//   - Single reader: "W:<epoch> R:[50@3]"
//   - Multi reader: "W:<epoch> R:[50@3, 60@5, 70@7]"
//   - Promoted: "W:<epoch> R:<vectorclock> [PROMOTED]"
//
// Example:
//   - "W:100@5 R:[50@3]" (single reader, fast path)
//   - "W:100@5 R:[50@3, 60@5]" (2 readers, inline slots)
//   - "W:100@5 R:{0:50, 1:60} [PROMOTED]" (5+ readers, promoted)
//
// This method is only used for debugging and race reporting, not on hot path.
func (vs *VarState) String() string {
	// Note: We manually build the string to avoid fmt import overhead.
	wStr := "W:" + vs.GetW().String()

	vs.mu.lock()
	defer vs.mu.unlock()

	if vs.readerCount == promotedMarker && vs.readClock != nil {
		// Promoted: Show VectorClock.
		return wStr + " R:" + vs.readClock.String() + " [PROMOTED]"
	}

	// Inline slots: Show all active reader epochs.
	rStr := "R:["
	for i := uint8(0); i < vs.readerCount; i++ {
		if i > 0 {
			rStr += ", "
		}
		rStr += vs.readEpochs[i].String()
	}
	rStr += "]"
	return wStr + " " + rStr
}

// Stack trace accessors.

// SetWriteStack records the stack trace hash for a write access.
//
// This is called by the detector on every write to store the stack trace
// for later retrieval during race reporting.
//
// Parameters:
//   - stackHash: Hash returned by stackdepot.CaptureStack()
//
// Thread Safety: Protected by mu.
func (vs *VarState) SetWriteStack(stackHash uint64) {
	vs.mu.lock()
	vs.writeStackHash = stackHash
	vs.mu.unlock()
}

// GetWriteStack retrieves the stack trace hash for the last write.
//
// This is called during race reporting to retrieve the previous write stack.
//
// Returns:
//   - uint64: Hash that can be passed to stackdepot.GetStack()
//   - 0: If no write stack has been captured
//
// Thread Safety: Protected by mu.
func (vs *VarState) GetWriteStack() uint64 {
	vs.mu.lock()
	hash := vs.writeStackHash
	vs.mu.unlock()
	return hash
}

// SetReadStack records the stack trace hash for a read access (read-shared case).
//
// This is called by the detector on read to promoted (read-shared) variables.
// For unpromoted variables, read stack is not stored (not needed for races).
//
// Parameters:
//   - stackHash: Hash returned by stackdepot.CaptureStack()
//
// Thread Safety: Protected by mutex (consistent with other read state).
func (vs *VarState) SetReadStack(stackHash uint64) {
	vs.mu.lock()
	vs.readStackHash = stackHash
	vs.mu.unlock()
}

// GetReadStack retrieves the stack trace hash for the last read.
//
// This is called during race reporting to retrieve the previous read stack.
// Only meaningful for promoted (read-shared) variables.
//
// Returns:
//   - uint64: Hash that can be passed to stackdepot.GetStack()
//   - 0: If no read stack has been captured
//
// Thread Safety: Protected by mutex (consistent with other read state).
func (vs *VarState) GetReadStack() uint64 {
	vs.mu.lock()
	hash := vs.readStackHash
	vs.mu.unlock()
	return hash
}

// Caller-PC accessors.

// SetWritePC stores the program counter (PC) of the write caller.
//
// The full stack is resolved lazily when a race is reported.
//
// Parameters:
//   - pc: Program counter from runtime.Callers(2, pcs[:1])
//
// Thread Safety: Lock-free (atomic store).
//
//go:nosplit
func (vs *VarState) SetWritePC(pc uintptr) {
	vs.writePC.Store(pc)
}

// GetWritePC retrieves the program counter of the last write.
//
// This is called during race reporting to capture full stack lazily.
//
// Returns:
//   - uintptr: Program counter of last write caller
//   - 0: If no write PC has been captured
//
// Thread Safety: Lock-free (atomic load).
//
//go:nosplit
func (vs *VarState) GetWritePC() uintptr {
	return vs.writePC.Load()
}

// SetReadPC stores the program counter (PC) of the read caller.
//
// Parameters:
//   - pc: Program counter from runtime.Callers(2, pcs[:1])
//
// Thread Safety: Lock-free (atomic store).
//
//go:nosplit
func (vs *VarState) SetReadPC(pc uintptr) {
	vs.readPC.Store(pc)
}

// GetReadPC retrieves the program counter of the last read.
//
// This is called during race reporting to capture full stack lazily.
//
// Returns:
//   - uintptr: Program counter of last read caller
//   - 0: If no read PC has been captured
//
// Thread Safety: Lock-free (atomic load).
//
//go:nosplit
func (vs *VarState) GetReadPC() uintptr {
	return vs.readPC.Load()
}
