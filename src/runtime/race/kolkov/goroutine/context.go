package goroutine

import (
	"internal/runtime/atomic"
	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/vectorclock"
	"unsafe"
)

//go:linkname runtimeThrow runtime.throw
func runtimeThrow(s string)

// ReadCacheSlots bounds the exact-address working set. Each entry also carries
// one GC-visible shadow-generation pointer. Atomic-release cache fields are
// appended after this ABI-sensitive group; the runtime depends on the offsets
// of the fields through ReadCacheWidths remaining stable.
const ReadCacheSlots = 4

// ReadCacheIndex returns the direct-mapped slot for an exact address. Folding
// the next two word-index bits preserves distinct slots for naturally aligned
// four-word groups while avoiding systematic aliases between fields 32 bytes
// apart.
//
//go:nosplit
func ReadCacheIndex(addr uintptr) uintptr {
	word := addr >> 3
	return (word ^ (word >> 2)) & (ReadCacheSlots - 1)
}

// AtomicReleaseCacheSlots bounds the fully-associative release working set.
// Collisions only force a canonical join/checkpoint; they never weaken the
// happens-before relation.
const AtomicReleaseCacheSlots = 2

// AtomicLoadCacheSlots bounds the exact atomic-load working set. Three entries
// cover atomic.Value's Store type load plus Load's type and data loads. A miss
// is a performance-only event and takes the detector's locked transaction path.
const AtomicLoadCacheSlots = 3

// AtomicReleaseCacheEntry is context-owned metadata for one exact atomic
// release binding. Release is a GC root rather than a uintptr so explicit
// atomic-release recycling cannot create an untracked pointer identity. A
// matching SeenVersion is weak evidence that the represented snapshot is
// already below the context clock. ExactGeneration is strong evidence only
// while it equals RaceContext.ForeignGeneration.
type AtomicReleaseCacheEntry struct {
	Release         unsafe.Pointer
	Stream          uint64
	SeenVersion     uint64
	ExactGeneration uint64
	Membership      uint8
}

// AtomicLoadCacheEntry is detector-owned metadata for one exact enrolled load
// path. Every pointer is a GC root: Fast retains the immutable lifecycle
// capability identity, State the atomic overlay identity, and Frontier the
// registered per-TID read witness. StateGeneration validates State across a
// quiescent direct Detector.Reset, which can invalidate arena ownership without
// visiting external contexts. The detector is the only package which interprets
// these opaque fields.
type AtomicLoadCacheEntry struct {
	Fast            unsafe.Pointer
	State           unsafe.Pointer
	Frontier        unsafe.Pointer
	Revision        uint64
	Generation      uint64
	PC              uintptr
	StateGeneration uint32
	Mask            uint8
	Internal        bool
}

// RaceContext represents the race detection state for a single goroutine.
//
// Each goroutine has its own RaceContext tracking logical time and happens-before
// relationships. The context maintains both a full vector clock (C) and a cached
// epoch (Epoch) for the current thread.
//
// The epoch cache enables FastTrack's critical optimization: most operations
// (96%+) only need the epoch value, avoiding expensive vector clock operations.
//
// Layout:
//   - TID: monotonic logical goroutine ID
//   - C: hybrid dense/sparse vector clock tracking observed logical IDs
//   - Epoch: Cached value of C[TID] as compact 64-bit epoch
//
// Invariant: Epoch must ALWAYS equal epoch.NewEpoch(TID, C[TID]). The checked
// PreflightClockAdvance/CommitClockAdvance pair maintains it; IncrementClock is
// the convenience wrapper for operations with no other fallible mutation.
type RaceContext struct {
	// TID is a process-lifetime monotonic logical goroutine identifier. IDs
	// are never recycled, because collapsing unrelated lifetimes onto one
	// vector-clock coordinate creates false happens-before edges.
	TID uint32

	// ReadCacheInvalidatedClock is the latest epoch of this context observed by
	// an external one-way synchronization such as finalizer handoff. The cache
	// is usable only at a strictly newer clock. External observers update this
	// marker atomically instead of racing with the context-owned cache fields.
	// It occupies the alignment padding before C on 64-bit systems.
	ReadCacheInvalidatedClock atomic.Uint32

	// C is the full vector clock tracking logical time for all threads.
	// C[i] represents the logical time for thread i.
	// This is used for happens-before checks when epoch fast-path fails.
	C *vectorclock.VectorClock

	// Epoch is the cached epoch for this goroutine: Epoch == C[TID].
	// This enables O(1) access to the current logical time without
	// accessing the full vector clock array.
	//
	// CRITICAL: This field is on the hot path for every memory access!
	// Must be kept in sync with C[TID] at all times.
	Epoch epoch.Epoch

	// ReadCache retains successfully represented exact read addresses in this
	// synchronization epoch. Re-reading a matching slot is redundant: a
	// concurrent writer must conflict with the represented read, while a
	// happens-before-safe writer requires a synchronization event that clears
	// the cache in IncrementClock. ReadCacheStates roots the exact shadow
	// generation which represents each read. The runtime re-resolves the address
	// and elides a read only while that same pointer remains authoritative, so an
	// allocator clear cannot revive an address-only entry. Collisions only reduce
	// optimization coverage.
	ReadCache [ReadCacheSlots]uintptr

	// ReadCacheStates is parallel to ReadCache. unsafe.Pointer keeps a detached
	// generation GC-visible until its owning cache entry is invalidated or
	// replaced; the runtime compares it but never dereferences a stale mapping.
	ReadCacheStates [ReadCacheSlots]unsafe.Pointer

	// ReadCacheWidths is parallel to ReadCache. Compiler scalar hooks use it to
	// distinguish an exact byte read from a 2-, 4-, or 8-byte read at the same
	// start address. The ordinary FastTrack history and lifecycle identity stay
	// anchored at that start; the width describes mixed atomic overlap and local
	// write invalidation.
	ReadCacheWidths [ReadCacheSlots]uint8

	// AtomicReleaseCache is appended after the existing read-cache fields to
	// preserve their runtime ABI offsets. Entries are fully associative and
	// context-owned; eviction is a conservative performance-only fallback.
	AtomicReleaseCache [AtomicReleaseCacheSlots]AtomicReleaseCacheEntry

	// AtomicLoadCache is context-owned and may be read only by the executing
	// logical goroutine. The pointed-to frontier is updated atomically because
	// an ordinary writer later consumes it under the address transaction lock.
	AtomicLoadCache [AtomicLoadCacheSlots]AtomicLoadCacheEntry

	// ForeignGeneration advances for every actual or conservative import into
	// the context's non-own projection. Own-clock commit changes only the owning
	// coordinate and deliberately leaves this generation unchanged.
	ForeignGeneration uint64

	atomicReleaseCacheNext uint8
	AtomicLoadCacheNext    uint8
	freshOnlyOwn           bool
}

// initializeAtomicReleaseTracking establishes the non-zero generation used to
// distinguish an uninitialized weak cache entry from a strong entry.
func (rc *RaceContext) initializeAtomicReleaseTracking(freshOnlyOwn bool) {
	rc.ForeignGeneration = 1
	rc.freshOnlyOwn = freshOnlyOwn
}

// Alloc creates and initializes a new RaceContext for the given thread ID.
//
// The context is initialized with:
//   - TID set to the provided tid
//   - C initialized with C[tid]=1 (this thread at time 1, others at 0)
//   - Epoch set to epoch.NewEpoch(tid, 1) (TID@1)
//
// This represents a newly started goroutine at the beginning of logical time.
//
// IMPORTANT: Clock starts at 1, not 0. This is critical for race detection:
// - Clock 0 means "never happened" (default in VectorClock)
// - Two accesses at clock 0 would appear to "happen-before" each other
// - Starting at 1 ensures unsynchronized accesses are detected as races
//
// Pooling: Uses pooled VectorClock allocation to reduce GC pressure.
// VectorClock is released back to pool when goroutine ends (racegoend).
//
// Example:
//
//	ctx := Alloc(5)
//	// ctx.TID = 5
//	// ctx.C = {5:1, others:0}
//	// ctx.Epoch = 1@5 (clock=1, tid=5)
func Alloc(tid uint32) *RaceContext {
	ctx := &RaceContext{
		TID: tid,
		C:   vectorclock.NewFromPool(),
	}
	ctx.initializeAtomicReleaseTracking(true)
	// Initialize epoch cache to TID@1 (clock 1 for new goroutine).
	// CRITICAL: Clock must start at 1, not 0, to detect unsynchronized races.
	// Clock 0 means "never happened" in HappensBefore check (0 <= 0 is TRUE).
	ctx.C.Set(tid, 1) // Set initial clock in VectorClock
	ctx.Epoch = epoch.NewEpoch(tid, 1)
	return ctx
}

// IncrementClock advances the logical clock for this goroutine.
//
// Per FastTrack (PLDI 2009, Section 3.2), the logical clock is incremented
// ONLY at synchronization events (acquire, release, fork, join), NOT on
// every memory access. This is critical for the same-epoch fast path:
// consecutive accesses within the same sync-free region share the same
// epoch, enabling O(1) same-epoch checks that skip the full detector.
//
// It performs two updates:
//  1. Increments C[TID] in the vector clock
//  2. Updates the cached Epoch to reflect the new C[TID] value
//
// The updates are performed sequentially to maintain the invariant:
//
//	Epoch == epoch.NewEpoch(TID, C[TID])
//
// Performance: Target <200ns/op (VectorClock.Increment + Epoch creation).
//
// Example:
//
//	ctx := Alloc(5)
//	// ctx.C[5] = 1, ctx.Epoch = 1@5
//	ctx.IncrementClock()
//	// ctx.C[5] = 2, ctx.Epoch = 2@5
//	ctx.IncrementClock()
//	// ctx.C[5] = 3, ctx.Epoch = 3@5
func (rc *RaceContext) IncrementClock() {
	next := rc.PreflightClockAdvance()
	rc.CommitClockAdvance(next)
}

// PreflightClockAdvance validates an own-clock advance without changing the
// vector clock, epoch, or any context-owned cache. Compound detector operations
// use the returned successor to order every possible failure before mutation.
func (rc *RaceContext) PreflightClockAdvance() uint64 {
	if rc == nil || rc.C == nil {
		runtimeThrow("race detector clock advance on released context")
	}
	return epoch.NextClock(uint64(rc.C.Get(rc.TID)))
}

// CommitClockAdvance weakens epoch-scoped caches and publishes a previously
// checked successor. A stale or fabricated value fails before any mutation.
func (rc *RaceContext) CommitClockAdvance(next uint64) {
	if rc == nil || rc.C == nil {
		runtimeThrow("race detector clock advance on released context")
	}
	current := uint64(rc.C.Get(rc.TID))
	if current >= epoch.MaxClock || next != current+1 || next > epoch.MaxClock {
		runtimeThrow("race detector non-successor clock commit")
	}

	// A new synchronization epoch makes prior reads non-redundant, but retains
	// their address and width as non-semantic hints. The detector can use a
	// matching hint to materialize a compact history on the next read without
	// making the runtime fast path accept the old epoch's entry.
	rc.WeakenReadCache()

	// Step 1: Publish the checked successor for this thread.
	clock := uint32(next)
	rc.C.Set(rc.TID, clock)

	// Step 2: Update the cached epoch to match C[TID].
	// This maintains the invariant: Epoch == epoch.NewEpoch(TID, C[TID]).
	atomic.Store64((*uint64)(unsafe.Pointer(&rc.Epoch)), uint64(epoch.NewEpoch(rc.TID, next)))

	// An observation of an older epoch cannot order accesses in this new one.
	// Clear its marker when possible, while preserving an observation racing
	// with this increment that already sampled the newly published epoch.
	rc.clearOlderReadCacheInvalidation(clock)
}

// NoteForeignImport invalidates strong atomic-release cache markers while
// preserving their weak dominated-snapshot evidence. Logical generations may
// not wrap: equality after wrap could otherwise revive an obsolete proof.
func (rc *RaceContext) NoteForeignImport() {
	if rc.ForeignGeneration == ^uint64(0) {
		runtimeThrow("race detector foreign-clock generation overflow")
	}
	rc.ForeignGeneration++
	rc.freshOnlyOwn = false
}

// FreshOnlyOwn reports whether the context was constructed with no inherited
// foreign projection and has not imported one since. It is consumed only as a
// proof for the first canonical atomic-release join.
func (rc *RaceContext) FreshOnlyOwn() bool {
	if !rc.freshOnlyOwn {
		return false
	}
	onlyOwn := true
	rc.C.RangeRuns(func(first, last, _ uint32) bool {
		if first != rc.TID || last != rc.TID {
			onlyOwn = false
			return false
		}
		return true
	})
	if onlyOwn {
		rc.C.RangeRetired(func(_, _ uint32) bool {
			onlyOwn = false
			return false
		})
	}
	if !onlyOwn {
		rc.freshOnlyOwn = false
	}
	return onlyOwn
}

// LookupAtomicRelease finds an exact release binding. The pointer, non-ABA
// stream, and current lane membership are all part of the key.
func (rc *RaceContext) LookupAtomicRelease(release unsafe.Pointer, stream uint64, membership uint8) (seenVersion uint64, strong, ok bool) {
	for i := range rc.AtomicReleaseCache {
		entry := &rc.AtomicReleaseCache[i]
		if entry.Release == release && entry.Stream == stream && entry.Membership == membership {
			return entry.SeenVersion, entry.ExactGeneration == rc.ForeignGeneration, true
		}
	}
	return 0, false, false
}

// RecordAtomicRelease updates or inserts one exact binding. Strong records the
// exact foreign-projection proof at the context's current generation; weak
// records retain only the dominated-snapshot invariant.
func (rc *RaceContext) RecordAtomicRelease(release unsafe.Pointer, stream, seenVersion uint64, membership uint8, strong bool) {
	index := -1
	for i := range rc.AtomicReleaseCache {
		entry := &rc.AtomicReleaseCache[i]
		if entry.Release == release && entry.Stream == stream && entry.Membership == membership {
			index = i
			break
		}
		if index < 0 && entry.Release == nil {
			index = i
		}
	}
	if index < 0 {
		index = int(rc.atomicReleaseCacheNext % AtomicReleaseCacheSlots)
		rc.atomicReleaseCacheNext = (rc.atomicReleaseCacheNext + 1) % AtomicReleaseCacheSlots
	}
	entry := &rc.AtomicReleaseCache[index]
	entry.Release = release
	entry.Stream = stream
	entry.SeenVersion = seenVersion
	entry.Membership = membership
	entry.ExactGeneration = 0
	if strong {
		entry.ExactGeneration = rc.ForeignGeneration
	}
}

// InvalidateReadCacheAt prevents cache hits in the observed epoch and every
// earlier epoch. It is safe to call from another goroutine: only this atomic
// marker is externally mutated; ReadCache and its metadata remain owned by rc.
//
// The max publication is important when multiple observers snapshot rc at
// different clocks and finish out of order.
//
//go:nosplit
func (rc *RaceContext) InvalidateReadCacheAt(observed epoch.Epoch) {
	tid, clock64 := observed.Decode()
	if tid != rc.TID || clock64 == 0 {
		return
	}
	clock := uint32(clock64)
	for {
		old := rc.ReadCacheInvalidatedClock.Load()
		if old >= clock {
			return
		}
		if rc.ReadCacheInvalidatedClock.CompareAndSwap(old, clock) {
			return
		}
	}
}

// clearOlderReadCacheInvalidation restores the zero-marker common case after
// synchronization advances the owner beyond an external observation.
//
//go:nosplit
func (rc *RaceContext) clearOlderReadCacheInvalidation(clock uint32) {
	for {
		invalidated := rc.ReadCacheInvalidatedClock.Load()
		if invalidated == 0 || invalidated >= clock {
			return
		}
		if rc.ReadCacheInvalidatedClock.CompareAndSwap(invalidated, 0) {
			return
		}
	}
}

// GetEpoch returns the cached epoch for this goroutine.
//
// This is the CRITICAL HOT PATH operation - called on every memory access
// for race detection. It must be:
//   - O(1): Just a field access, no computation
//   - Zero allocations
//   - Inline-candidate (no function call overhead)
//   - //go:nosplit to prevent stack growth
//
// The cached epoch represents the current logical time for this goroutine as
// a compact 64-bit value (32-bit TID and 32-bit clock).
//
// Performance: Target <1ns/op (single field read).
//
// Example:
//
//	ctx := Alloc(5)
//	e := ctx.GetEpoch()  // Returns 1@5 (clock=1, tid=5)
//	ctx.IncrementClock()
//	e = ctx.GetEpoch()   // Returns 2@5 (clock=2, tid=5)
//
//go:nosplit
func (rc *RaceContext) GetEpoch() epoch.Epoch {
	return rc.Epoch
}

// RecordRead makes subsequent reads of addr redundant while state remains the
// authoritative exact shadow mapping and until the context advances or writes.
// addr is intentionally exact rather than word-aligned so adjacent fields
// retain independent instrumentation.
//
//go:nosplit
func (rc *RaceContext) RecordRead(addr uintptr, state unsafe.Pointer) {
	rc.RecordReadSized(addr, 1, state)
}

// RecordReadSized publishes an exact compiler scalar cache entry. state is the
// authoritative start-address generation; width distinguishes compatible hook
// reuse and lets writes invalidate any overlapping represented atomic lanes.
//
//go:nosplit
func (rc *RaceContext) RecordReadSized(addr, size uintptr, state unsafe.Pointer) {
	if size == 0 || size >= uintptr(ReadCacheWeakWidth) || size-1 > ^uintptr(0)-addr {
		return
	}
	slot := ReadCacheIndex(addr)
	// Publish the GC root before the address discriminator. The context has one
	// logical owner, but this order also keeps raw runtime readers from ever
	// accepting an address paired with the previous slot's state.
	rc.ReadCacheStates[slot] = state
	rc.ReadCacheWidths[slot] = uint8(size)
	rc.ReadCache[slot] = addr
}

// RecordAddressOnlyRead records a redundant compact read no-op without rooting
// its shadow state. This form is valid only after the exact read was already
// represented; runtime address-only elision is consequently limited to storage
// whose lifetime cannot be recycled.
//
//go:nosplit
func (rc *RaceContext) RecordAddressOnlyRead(addr uintptr) {
	rc.RecordRead(addr, nil)
}

// RecordAddressOnlyReadRange records a completed compiler scalar read. Runtime
// fast paths consume this form only for non-reclaimable first-module storage,
// for which allocator-generation revalidation is unnecessary.
//
//go:nosplit
func (rc *RaceContext) RecordAddressOnlyReadRange(addr, size uintptr) {
	if size == 0 || size >= uintptr(ReadCacheWeakWidth) || size-1 > ^uintptr(0)-addr {
		return
	}
	slot := ReadCacheIndex(addr)
	rc.ReadCacheStates[slot] = nil
	rc.ReadCacheWidths[slot] = uint8(size)
	rc.ReadCache[slot] = addr
}

// InvalidateRead removes addr from the cache before a write to that address.
// Writes to other addresses do not invalidate their represented reads.
//
//go:nosplit
func (rc *RaceContext) InvalidateRead(addr uintptr) {
	rc.InvalidateReadRange(addr, 1)
}

// InvalidateReadRange removes only cached exact addresses that overlap
// [addr, addr+size). The subtraction form avoids computing a wrapping end.
// Invalid or wrapping ranges do not mutate the cache.
//
//go:nosplit
func (rc *RaceContext) InvalidateReadRange(addr, size uintptr) {
	if size == 0 || size-1 > ^uintptr(0)-addr {
		return
	}
	for i := range rc.ReadCache {
		cached := rc.ReadCache[i]
		width := uintptr(rc.ReadCacheWidths[i] &^ ReadCacheWeakWidth)
		if cached != 0 && width != 0 && readCacheRangesOverlap(cached, width, addr, size) {
			rc.ReadCache[i] = 0
			rc.ReadCacheStates[i] = nil
			rc.ReadCacheWidths[i] = 0
		}
	}
}

// ReadCacheWeakWidth is the non-semantic-hint marker mirrored by the runtime
// fast path. Compiler scalar widths are always below this reserved bit.
const ReadCacheWeakWidth = uint8(1 << 7)

// HasReadHintSized reports whether the direct-mapped slot retains this exact
// address and width. Both current-epoch entries and weakened prior-epoch hints
// qualify; callers must never treat this as proof that the read is redundant.
//
//go:nosplit
func (rc *RaceContext) HasReadHintSized(addr, size uintptr) bool {
	if size == 0 || size >= uintptr(ReadCacheWeakWidth) {
		return false
	}
	slot := ReadCacheIndex(addr)
	return rc.ReadCache[slot] == addr &&
		uintptr(rc.ReadCacheWidths[slot]&^ReadCacheWeakWidth) == size
}

// HasWeakReadHintSized reports whether the exact hint came from a prior
// synchronization epoch rather than the current epoch's semantic cache.
//
//go:nosplit
func (rc *RaceContext) HasWeakReadHintSized(addr, size uintptr) bool {
	if !rc.HasReadHintSized(addr, size) {
		return false
	}
	slot := ReadCacheIndex(addr)
	return rc.ReadCacheWidths[slot]&ReadCacheWeakWidth != 0
}

// WeakenReadCache preserves only non-semantic address/width hints across a
// synchronization epoch. Clearing the rooted state and marking the width's
// high bit guarantees that runtime exact-width checks cannot accept the entry.
//
//go:nosplit
func (rc *RaceContext) WeakenReadCache() {
	for i := range rc.ReadCache {
		width := rc.ReadCacheWidths[i] &^ ReadCacheWeakWidth
		if rc.ReadCache[i] == 0 || width == 0 {
			rc.ReadCache[i] = 0
			rc.ReadCacheStates[i] = nil
			rc.ReadCacheWidths[i] = 0
			continue
		}
		rc.ReadCacheStates[i] = nil
		rc.ReadCacheWidths[i] = width | ReadCacheWeakWidth
	}
}

// readCacheRangesOverlap reports whether two valid, non-empty half-open ranges
// overlap without computing either potentially wrapping end address.
//
//go:nosplit
func readCacheRangesOverlap(first, firstSize, second, secondSize uintptr) bool {
	if first <= second {
		return second-first < firstSize
	}
	return first-second < secondSize
}

// ClearReadCache invalidates redundant-read elimination.
//
//go:nosplit
func (rc *RaceContext) ClearReadCache() {
	for i := range rc.ReadCache {
		rc.ReadCache[i] = 0
		rc.ReadCacheStates[i] = nil
		rc.ReadCacheWidths[i] = 0
	}
}

// AllocWithStartClock creates a RaceContext with a specific start clock.
//
// Logical IDs are never recycled. The explicit start value is retained for
// tests and context restoration; normal goroutine lifetimes start at one.
//
// Parameters:
//   - tid: Thread ID for this goroutine
//   - startClock: Initial non-zero clock value (zero is normalized to one)
func AllocWithStartClock(tid uint32, startClock uint32) *RaceContext {
	ctx := &RaceContext{
		TID: tid,
		C:   vectorclock.NewFromPool(),
	}
	ctx.initializeAtomicReleaseTracking(true)
	if startClock == 0 {
		startClock = 1
	}
	ctx.C.Set(tid, startClock)
	ctx.Epoch = epoch.NewEpoch(tid, uint64(startClock))
	return ctx
}

// AllocWithParentClock creates a RaceContext that inherits parent's clock.
//
// This is the key function for happens-before at goroutine creation (fork):
//  1. child.C := parent.C (Copy parent's clock - inherit HB relations)
//  2. child.C[child.TID] = startClock (Initialize child's own component)
//  3. child.Epoch = NewEpoch(tid, startClock)
//
// After this, any operation in child "sees" all operations that happened
// in parent before the fork (go func() statement).
//
// Pooling: Uses pooled VectorClock allocation to reduce GC pressure.
// VectorClock is released back to pool when goroutine ends (racegoend).
//
// Parameters:
//   - tid: Thread ID allocated for this child goroutine
//   - parentClock: Snapshot of parent's VectorClock at fork time
//   - startClock: Initial non-zero clock value for this logical ID
//
// Returns:
//   - *RaceContext: Context ready for race detection with inherited HB
//
// Example:
//
//	Parent at fork: clock={1:5, 3:2}
//	Child after AllocWithParentClock(2, parentClock, 1):
//	  clock={1:5, 2:1, 3:2}
//	        ^ inherited from parent
//	             ^ child's own component initialized to startClock
//	                  ^ inherited from parent
func AllocWithParentClock(tid uint32, parentClock *vectorclock.VectorClock, startClock uint32) *RaceContext {
	ctx := &RaceContext{
		TID: tid,
		C:   vectorclock.NewFromPool(),
	}
	ctx.initializeAtomicReleaseTracking(false)

	// Step 1: Inherit parent's clock (HB edge: parent fork -> child start).
	// This copies all components from parent's clock to child's clock.
	if parentClock != nil {
		ctx.C.CopyFrom(parentClock)
	}

	// Step 2: Initialize child's own clock component.
	// Clock zero means "no event", so every live context starts non-zero.
	if startClock == 0 {
		startClock = 1
	}
	ctx.C.Set(tid, startClock)

	// Step 3: Initialize cached epoch.
	ctx.Epoch = epoch.NewEpoch(tid, uint64(startClock))

	return ctx
}
