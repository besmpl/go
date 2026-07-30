package detector

import (
	"internal/runtime/atomic"
	"unsafe"

	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/goroutine"
	"runtime/race/kolkov/shadowmem"
	"runtime/race/kolkov/syncshadow"
)

// Runtime functions via linkname (for sub-package access).

//go:linkname runtimeCallers runtime.Callers
func runtimeCallers(skip int, pc []uintptr) int

//go:linkname printstring runtime.printstring
func printstring(s string)

//go:linkname printuint runtime.printuint
func printuint(v uint64)

// printRaceLine prints a line to stderr.
func printRaceLine(s string) {
	printstring(s)
	printstring("\n")
}

// printRaceLineAddr prints a line with address.
func printRaceLineAddr(prefix string, addr uintptr) {
	printstring(prefix)
	printstring("0x")
	printuint(uint64(addr))
	printstring("\n")
}

//go:linkname runtimeKolkovSpinWait runtime.kolkovSpinWait
func runtimeKolkovSpinWait(cycles uint32, yield bool)

const detectorSpinlockMaxBackoff = uint32(64)

// spinlock is the runtime-compatible detector lock. Failed acquisition polls
// before CAS and uses bounded runtime backoff to avoid invalidating the owner’s
// cache line continuously. runtimeKolkovSpinWait uses bounded procyield, then
// an OS-thread yield at the saturated delay on g0.
type spinlock struct {
	state atomic.Uint32
}

//go:nosplit
func (s *spinlock) lock() {
	for delay := uint32(1); ; {
		if s.state.Load() == 0 && s.state.CompareAndSwap(0, 1) {
			return
		}
		runtimeKolkovSpinWait(delay, delay == detectorSpinlockMaxBackoff)
		if delay < detectorSpinlockMaxBackoff {
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

type reportedRaceKey struct {
	raceType  string
	addr      uintptr
	firstTID  uint32
	secondTID uint32
	firstPC   uintptr
	secondPC  uintptr
	lifecycle uint64
}

type reportedRaceEntry struct {
	hash uint64
	key  reportedRaceKey
}

// reportedRacesMap is a bounded, allocation-on-report deduplication set. Slots
// retain the full key rather than only its hash, so a hash collision can cause
// a duplicate report on probe overflow but can never suppress a distinct race.
type reportedRacesMap struct {
	entries [1024]atomic.Pointer[reportedRaceEntry]
}

func (m *reportedRacesMap) loadOrStore(hash uint64, key reportedRaceKey) (loaded bool) {
	idx := hash & 1023
	var candidate *reportedRaceEntry
	for i := uint64(0); i < 8; i++ {
		slot := (idx + i) & 1023
		existing := m.entries[slot].Load()
		if existing != nil && existing.hash == hash && existing.key == key {
			return true // Already exists
		}
		if existing == nil {
			if candidate == nil {
				candidate = &reportedRaceEntry{hash: hash, key: key}
			}
			if m.entries[slot].CompareAndSwap(nil, candidate) {
				return false // Newly stored
			}
			// CAS failed, check again
			existing = m.entries[slot].Load()
			if existing != nil && existing.hash == hash && existing.key == key {
				return true
			}
		}
	}
	return false // Overflow - treat as not found
}

func (m *reportedRacesMap) reset() {
	for i := range m.entries {
		m.entries[i].Store(nil)
	}
}

// DetectorOptions configures the race detector behavior.
//
// Use NewDetectorWithOptions() to create a detector with custom options.
// For default behavior, use NewDetector() which is equivalent to:
//
//	NewDetectorWithOptions(DetectorOptions{})
//
// Example usage:
//
//	// Default: Full detection (no sampling)
//	d := NewDetector()
//
//	// With sampling: check 1 in 10 accesses.
//	d := NewDetectorWithOptions(DetectorOptions{
//	    SamplingEnabled: true,
//	    SampleRate:      10,
//	})
//
//	// Higher sampling: check 1 in 100 accesses.
//	d := NewDetectorWithOptions(DetectorOptions{
//	    SamplingEnabled: true,
//	    SampleRate:      100,
//	})
//
//nolint:revive // DetectorOptions is more descriptive than Options for public API.
type DetectorOptions struct {
	// SamplingEnabled enables probabilistic sampling. When enabled, only a
	// fraction of memory accesses are checked, so races may be missed.
	// The default is false.
	SamplingEnabled bool

	// SampleRate determines the sampling frequency when SamplingEnabled is true.
	// - Rate=1: Check every access (no sampling, same as disabled)
	// - Rate=10: Check 1 in 10 accesses
	// - Rate=100: Check 1 in 100 accesses
	// - Rate=1000: Check 1 in 1000 accesses
	// Default: 1 (no sampling).
	SampleRate uint64
}

// Detector implements the core FastTrack race detection algorithm.
//
// It maintains global state including shadow memory (tracking access history
// for all memory locations) and goroutine contexts (tracking logical time
// for each thread).
type Detector struct {
	// atomicArena owns every atomic history object and is reset only after the
	// shadow lifecycle/capability gates have quiesced.
	atomicArena *AtomicHistoryArena
	// shadowMemory stores VarState cells for all instrumented addresses.
	// This is the core data structure that tracks the last write and read
	// epochs for every memory location.
	// Uses Shadow interface to allow swapping implementations.
	// Default: PageTableShadow (direct primary pages plus sparse absolute
	// blocks outside the primary window).
	shadowMemory shadowmem.Shadow

	// slotMemory is the concrete word-slot view used to isolate exact lanes
	// before mutation and to traverse copy-on-write range groups.
	slotMemory shadowmem.SlotShadow

	// rangeMemory traverses bulk-equivalent ordinary histories without
	// materializing one slot and state per application word. Keep the concrete
	// type so escape analysis can prove that range visitors do not escape.
	rangeMemory *shadowmem.PageTableShadow

	// syncShadow stores SyncVar cells for all synchronization primitives.
	// This tracks release clocks for mutexes, rwmutexes, channels, etc.
	syncShadow *syncshadow.SyncShadow

	// sampler implements probabilistic sampling.
	// When enabled, only a fraction of memory accesses are checked.
	// It is nil when sampling is disabled.
	sampler *Sampler

	// reportObserver is an optional deterministic test seam. Production leaves
	// it nil and continues to emit reports through runtime.print*.
	reportObserver func(*RaceReport)

	// goroutineCreations retains creation sites for live logical goroutines and
	// a bounded tail of finished ones. It is touched only by lifecycle/reporting
	// paths, never ordinary memory accesses.
	goroutineCreations goroutineCreationRegistry

	// racesDetected counts the total number of races found.
	// This is used for testing and reporting purposes.
	racesDetected int

	// reportedRaces tracks which races have already been reported.
	// This prevents duplicate reports for the same race location.
	reportedRaces reportedRacesMap

	// operationCount tracks synchronization events for periodic overflow checks.
	operationCount atomic.Uint64

	// mu protects racesDetected counter and stats updates.
	mu spinlock
}

const (
	// overflowCheckInterval defines how often synchronization events check for
	// TID or clock overflow.
	overflowCheckInterval = 10000
)

// NewDetector creates and initializes a new race detector instance.
//
// The detector is ready to use immediately after creation.
// It initializes:
//   - Shadow memory for tracking variable access history
//   - Sync shadow memory for tracking synchronization primitives
//
// This is equivalent to NewDetectorWithOptions(DetectorOptions{}).
// For custom configuration (e.g., sampling), use NewDetectorWithOptions.
//
// Example:
//
//	d := NewDetector()
//	ctx := goroutine.Alloc(1)
//	d.OnWrite(0x1234, ctx, 0)  // Detect write to address
//	d.OnAcquire(0x5678, ctx)  // Track mutex lock
func NewDetector() *Detector {
	return NewDetectorWithOptions(DetectorOptions{})
}

// NewDetectorWithOptions creates a race detector with custom configuration.
//
// Sampling trades detection completeness for lower instrumentation work.
//
// Options:
//   - SamplingEnabled: Enable probabilistic sampling
//   - SampleRate: Fraction of accesses to check (e.g., 10 = 1 in 10)
//
// Example usage:
//
//	// Production: Full detection (default)
//	d := NewDetectorWithOptions(DetectorOptions{})
//
//	// Check one in ten accesses.
//	d := NewDetectorWithOptions(DetectorOptions{
//	    SamplingEnabled: true,
//	    SampleRate:      10,
//	})
//
//	// Check one in one hundred accesses.
//	d := NewDetectorWithOptions(DetectorOptions{
//	    SamplingEnabled: true,
//	    SampleRate:      100,
//	})
func NewDetectorWithOptions(opts DetectorOptions) *Detector {
	shadow := shadowmem.NewPageTableShadow()
	d := &Detector{
		atomicArena:  newAtomicHistoryArena(),
		shadowMemory: shadow,
		slotMemory:   shadow,
		rangeMemory:  shadow,
		syncShadow:   syncshadow.NewSyncShadow(),
	}

	// Keep the sampler nil when sampling is disabled.
	if opts.SamplingEnabled {
		d.sampler = NewSampler(SamplerConfig{
			Enabled: true,
			Rate:    opts.SampleRate,
		})
	}

	return d
}

// checkOverflowPeriodically increments the synchronization-event counter and
// periodically reports TID or clock overflow state.
func (d *Detector) checkOverflowPeriodically() {
	count := d.operationCount.Add(1)
	if count%overflowCheckInterval == 0 {
		// Non-hot path: call reporting function (not nosplit).
		d.reportOverflowsIfNeeded()
	}
}

// reportOverflowsIfNeeded checks overflow flags and reports warnings to stderr.
//
// This is called every overflowCheckInterval synchronization events.
// It checks epoch.CheckOverflows() and prints clear, actionable warnings.
func (d *Detector) reportOverflowsIfNeeded() {
	tidOverflow, clockOverflow, tidWarning, clockWarning := epoch.CheckOverflows()

	// CRITICAL: TID overflow detected.
	if tidOverflow {
		printRaceLine("\n==================")
		printRaceLine("CRITICAL: TID OVERFLOW DETECTED!")
		printRaceLine("Program has spawned too many goroutines.")
		printRaceLine("Race detection may produce FALSE NEGATIVES (missed races).")
		printRaceLine("==================\n")
	}

	// CRITICAL: Clock overflow detected.
	if clockOverflow {
		printRaceLine("\n==================")
		printRaceLine("CRITICAL: CLOCK OVERFLOW DETECTED!")
		printRaceLine("Program has executed too many operations.")
		printRaceLine("Race detection may produce FALSE POSITIVES/NEGATIVES.")
		printRaceLine("==================\n")
	}

	// WARNING: TID approaching limit (90% threshold).
	if tidWarning && !tidOverflow {
		printRaceLine("WARNING: TID usage at 90%. Nearing overflow.")
	}

	// WARNING: Clock approaching limit (90% threshold).
	if clockWarning && !clockOverflow {
		printRaceLine("WARNING: Clock usage at 90% (approaching limit).")
	}
}

// captureCallerPC captures the program counter (PC) of the caller.
//
// Memory-access hooks capture only the caller PC. A complete stack is resolved
// only when reporting a race.
//
// The full stack is captured lazily when race is detected, using this PC
// to identify the call site.
//
// Parameters:
//   - skip: Number of stack frames to skip (typically 2: this func + immediate caller)
//
// Returns:
//   - uintptr: Program counter of the caller
//   - 0: If unable to capture (shouldn't happen normally)
//
// Call stack when captureCallerPC is invoked:
//
//	0: runtime.Callers
//	1: captureCallerPC
//	2: OnWrite/OnRead
//	3: api.racewrite/raceread (internal)
//	4: api.RaceWrite/RaceRead (internal)
//	5: race.RaceWrite/RaceRead (internal)
//	6: USER CODE ← This is what we want!
//
// We skip 6 frames to get directly to user code in the standard case.
// For tests or direct API calls, the stack may be shorter, so we capture
// multiple PCs and find the first non-internal frame.
func captureCallerPC() uintptr {
	// Try direct skip first (fastest path).
	// Skip 6 frames: Callers, captureCallerPC, OnWrite, racewrite, RaceWrite, RaceWrite(wrapper)
	var pcs [1]uintptr
	n := runtimeCallers(6, pcs[:])
	if n > 0 && pcs[0] != 0 {
		return pcs[0]
	}

	// Fallback: If stack is shorter (e.g., in tests), try fewer skips.
	// This handles cases where the user calls detector directly.
	n = runtimeCallers(3, pcs[:])
	if n > 0 {
		return pcs[0]
	}
	return 0
}

// OnWrite handles write access to memory at the given address.
//
// This is the memory-write hot path in instrumented code.
//
// Algorithm: FastTrack write transition with exclusive-writer tracking.
//
//  1. Get current goroutine context
//  2. Get or create shadow cell for address
//  3. Get current epoch from context
//  4. [FT WRITE SAME EPOCH] Fast path: If vs.W == currentEpoch, return
//  5. [EXCLUSIVE WRITER] If owned by the same writer, skip redundant HB checks
//  6. Check write-write race: If !vs.W.HappensBefore(ctx.C), report race
//  7. Check read-write race (ADAPTIVE):
//     a. If promoted: Check if readClock happened-before ctx.C
//     b. If not promoted: Check if readEpoch happened-before ctx.C
//  8. Update shadow memory: vs.W = currentEpoch
//  9. Track ownership: first writer claims, a distinct writer marks it shared
//
// 10. Clear read tracking and DEMOTE (write dominates all previous reads)
//
// Parameters:
//   - addr: Memory address being written to
//
// Thread Safety: Safe for concurrent calls from multiple goroutines.
//
// Zero Allocations: This function MUST NOT allocate on the heap.
// All required objects (VarState, RaceContext) are pre-allocated or
// retrieved from pools.
//
//nolint:gocognit,nestif,gocyclo,cyclop // Complex race detection logic requires nested conditionals
func (d *Detector) OnWrite(addr uintptr, ctx *goroutine.RaceContext, pc uintptr) {
	d.onWriteSized(addr, 1, ctx, pc)
}

// OnWriteSized handles a compiler-generated 2-, 4-, or 8-byte scalar write.
// The logical FastTrack transition remains anchored at the scalar start and is
// shared by every covered byte alias. A later partial access copy-on-write
// splits only its subset while retaining the earlier scalar history.
func (d *Detector) OnWriteSized(addr, size uintptr, ctx *goroutine.RaceContext, pc uintptr) {
	if size != 2 && size != 4 && size != 8 {
		return
	}
	d.onWriteSized(addr, size, ctx, pc)
}

func (d *Detector) onWriteSized(addr, size uintptr, ctx *goroutine.RaceContext, pc uintptr) {
	if ctx == nil || size == 0 || size-1 > ^uintptr(0)-addr {
		return
	}
	// Any local write may invalidate the read represented by the per-context
	// redundant-read cache. Runtime fast paths perform the same invalidation
	// before bypassing the detector.
	ctx.InvalidateReadRange(addr, size)

	// Step 0: Sampling check.
	// If sampling is enabled and this access is not sampled, skip detection.
	if d.sampler != nil && !d.sampler.ShouldSample() {
		return
	}

	// Overflow checks run at synchronization events, where clocks advance.

	if pc == 0 {
		pc = captureCallerPC()
	}
	if size != 1 {
		d.applySizedScalarWrite(addr, size, ctx, pc)
		return
	}
	// Fresh and otherwise simple exact histories stay block-compact. The
	// preflight duplicates only allocation-free FastTrack transitions which it
	// can prove conflict-free; every complex/ambiguous case falls through to the
	// existing authoritative detector path.
	if d.rangeMemory.TryCompactWrite(addr, ctx.GetEpoch(), ctx.C, pc) {
		return
	}

	// Step 1: Get or create shadow cell for this address.
	// GetOrCreate is thread-safe and may allocate on first access.
	vs := d.slotMemory.GetOrCreateSlot(addr).Isolate(uint8(addr & 7))
	var pending pendingRangeRace
	d.captureAtomicWriteLocked(addr, 1, vs, ctx, pc, &pending)
	d.applyOrdinaryWriteLocked(addr, vs, ctx, pc, &pending)
	vs.UnlockAccess()
	pending.report(d)

}

// OnRead handles read access to memory at the given address.
//
// This is the memory-read hot path in instrumented code.
//
// Algorithm: FastTrack read transition with exclusive-writer tracking.
//
//  1. Get current goroutine context
//  2. Get or create shadow cell for address
//  3. Get current epoch from context
//  4. [EXCLUSIVE WRITER] If reading an own write, skip the redundant HB check
//  5. Check read-write race: If vs.W != 0 && !vs.W.HappensBefore(ctx.C), report race
//  6. Update read tracking (ADAPTIVE):
//     a. If promoted (vs.IsPromoted()):
//     - Merge current VC into read VC
//     b. If not promoted (fast path):
//     - If same epoch: return
//     - If same TID: update epoch, return
//     - If happens-before: replace epoch, return
//     - Otherwise: PROMOTE to VectorClock
//
// Parameters:
//   - addr: Memory address being read from
//
// Thread Safety: Safe for concurrent calls from multiple goroutines.
//
// Zero Allocations: Fast path allocates nothing. Slow path may allocate VectorClock on promotion.
func (d *Detector) OnRead(addr uintptr, ctx *goroutine.RaceContext, pc uintptr) {
	d.onReadSized(addr, 1, ctx, pc)
}

// OnReadSized is the read counterpart of OnWriteSized.
func (d *Detector) OnReadSized(addr, size uintptr, ctx *goroutine.RaceContext, pc uintptr) {
	if size != 2 && size != 4 && size != 8 {
		return
	}
	d.onReadSized(addr, size, ctx, pc)
}

func (d *Detector) onReadSized(addr, size uintptr, ctx *goroutine.RaceContext, pc uintptr) {
	if ctx == nil || size == 0 || size-1 > ^uintptr(0)-addr {
		return
	}
	// Step 0: Sampling check.
	// If sampling is enabled and this access is not sampled, skip detection.
	if d.sampler != nil && !d.sampler.ShouldSample() {
		return
	}

	// Overflow checks run at synchronization events, where clocks advance.

	if pc == 0 {
		pc = captureCallerPC()
	}
	if size != 1 {
		// internal/race RWMutex markers use the one-byte ABI. Sized compiler
		// accesses can therefore use the physical-alias representation directly.
		d.applySizedScalarRead(addr, size, ctx, pc)
		return
	}
	marker := rwMutexMarkerPC(pc)
	if !marker && ctx.HasWeakReadHintSized(addr, 1) {
		d.applySizedScalarRead(addr, 1, ctx, pc)
		return
	}
	// Marker reads require the mixed atomic/plain classification sidecar and
	// must never enter the compact ordinary-only representation. A successful
	// ordinary compact no-op may seed an address-only cache entry. Changed
	// compact histories remain uncached so their unexposed state can be reused.
	if !marker {
		switch d.rangeMemory.TryCompactRead(addr, ctx.GetEpoch(), ctx.C, pc) {
		case shadowmem.CompactReadExactNoop:
			ctx.RecordAddressOnlyReadRange(addr, size)
			return
		case shadowmem.CompactReadHandled:
			return
		}
	}

	// Step 1: Get or create shadow cell for this address.
	// GetOrCreate is thread-safe and may allocate on first access.
	vs := d.slotMemory.GetOrCreateSlot(addr).Isolate(uint8(addr & 7))
	var pending pendingRangeRace
	d.captureAtomicReadLocked(addr, 1, vs, ctx, pc, marker, &pending)
	d.applyOrdinaryReadLocked(addr, vs, ctx, pc, &pending)
	// Publish both the redundant-read address and the exact state generation
	// while its access lock is retained. ClearRange drains that lock before
	// removing the lane mapping, so a runtime cache hit either re-resolves this
	// pointer before clear or observes the replacement and takes the slow path.
	// This ordering is required even when the transition captured a conflict:
	// reporting is deferred until after publication, and subsequent reads may be
	// elided only once this read is represented in shadow memory.
	// sync.RWMutex marker reads are a distinct reporting class. Caching one
	// would let a later user read at the same address and epoch bypass the
	// detector, leaving only the suppressible marker classification. Skipping
	// publication also preserves an unrelated user entry which collided in the
	// direct-mapped cache.
	if !marker {
		ctx.RecordReadSized(addr, size, unsafe.Pointer(vs))
	}
	vs.UnlockAccess()
	pending.report(d)
}

// happensBeforeWrite reports whether the write epoch is contained in the
// current context's vector clock. It remains as a unit-test and microbenchmark
// seam; production shadow transitions apply the same epoch predicate directly.
func (d *Detector) happensBeforeWrite(prevWrite epoch.Epoch, ctx *goroutine.RaceContext) bool {
	return prevWrite.HappensBefore(ctx.C)
}

// happensBeforeRead reports whether one read epoch is contained in the current
// context's vector clock. It remains as a unit-test and microbenchmark seam;
// production transitions check every witness in promoted read histories.
func (d *Detector) happensBeforeRead(prevRead epoch.Epoch, ctx *goroutine.RaceContext) bool {
	return prevRead.HappensBefore(ctx.C)
}

// reportRace preserves the legacy formatter and counter seam used by unit tests
// and microbenchmarks. Production detection reports through reportRaceV2PC,
// which carries access metadata and deduplicates reports.
//
// Parameters:
//   - raceType: Type of race ("write-write" or "read-write")
//   - addr: Memory address where race occurred
//   - prevEpoch: Epoch of the conflicting previous access
//   - currEpoch: Epoch of the current access
//
// Thread Safety: Uses mutex to prevent interleaved output.
//
// Example Output:
//
//	==================
//	WARNING: DATA RACE
//	Type: write-write
//	Address: 0x12345678
//	Previous access: 10@1 (clock=10, tid=1)
//	Current access:  20@1 (clock=20, tid=1)
//	==================
func (d *Detector) reportRace(raceType string, addr uintptr, prevEpoch, currEpoch epoch.Epoch) {
	// Lock to prevent interleaved output from multiple goroutines.
	d.mu.lock()
	defer d.mu.unlock()

	// Increment race counter for statistics.
	d.racesDetected++

	// Notify the runtime so RaceErrors() returns the correct count.
	kolkovIncrementErrors()

	// Print through the runtime-safe output boundary. The pure formatter is the
	// deterministic seam used by tests; swapping os.Stderr cannot intercept
	// runtime.printstring.
	printstring(formatLegacyRace(raceType, addr, prevEpoch, currEpoch))
}

func formatLegacyRace(raceType string, addr uintptr, prevEpoch, currEpoch epoch.Epoch) string {
	return "==================\n" +
		"WARNING: DATA RACE\n" +
		"Type: " + raceType + "\n" +
		"Address: " + hexReport(addr) + "\n" +
		"Previous access: " + prevEpoch.String() + "\n" +
		"Current access:  " + currEpoch.String() + "\n" +
		"==================\n"
}

// RacesDetected returns the total number of races detected.
//
// This is used for testing and reporting purposes. It provides a simple
// count of how many races were found during execution.
//
// Thread Safety: Safe for concurrent calls (protected by mutex).
//
// Returns:
//   - int: Total number of races detected
func (d *Detector) RacesDetected() int {
	d.mu.lock()
	defer d.mu.unlock()
	return d.racesDetected
}

// OnAcquire handles synchronization acquire operations.
//
// This establishes a happens-before edge from the previous Unlock to this Lock.
// The acquiring thread merges the mutex's release clock into its own clock.
//
// Algorithm: FastTrack [FT ACQUIRE]
//  1. Get lock's SyncVar from sync shadow memory
//  2. If lock has release clock: ctx.C.Join(syncVar.releaseClock)
//  3. ctx.IncrementClock()
//
// This implements: Ct := Ct ⊔ Lm (thread clock joins lock clock).
//
// Parameters:
//   - addr: Address of the mutex being locked
//   - ctx: Current goroutine's RaceContext
//
// Thread Safety: Safe for concurrent calls from multiple goroutines.
//
// Example:
//
//	mu.Lock()  // Compiler inserts: raceacquire(&mu)
//	// OnAcquire merges previous Unlock's clock into current thread
//	x = 42     // Now happens-after previous critical section
func (d *Detector) OnAcquire(addr uintptr, ctx *goroutine.RaceContext) {
	next := ctx.PreflightClockAdvance()
	// Periodic overflow detection runs on synchronization events.
	// TID/clock overflow happens at clock advancement, not memory access.
	d.checkOverflowPeriodically()

	// Step 1: Get or create SyncVar for this mutex address.
	syncVar := d.syncShadow.GetOrCreate(addr)

	// Step 2: Join the lock's release clock while holding the SyncVar lock.
	// This establishes happens-before from the previous Unlock without racing
	// an in-place Release or ReleaseMerge update.
	if syncVar.JoinReleaseClock(ctx.C) {
		// A sync acquire may import an arbitrary foreign projection. Record one
		// conservative invalidation before advancing the context's own clock.
		ctx.NoteForeignImport()
	}

	// Step 3: Increment logical clock to advance time.
	// This must be done AFTER joining to maintain happens-before invariant.
	ctx.CommitClockAdvance(next)
}

// OnRelease handles synchronization release operations.
//
// This creates a happens-before edge that future Lock operations will synchronize with.
// The releasing thread captures its current clock into the mutex's release clock.
//
// Algorithm: FastTrack [FT RELEASE]
//  1. Get lock's SyncVar
//  2. Set syncVar.releaseClock = ctx.C (copy current thread's clock)
//  3. ctx.IncrementClock()
//
// This implements: Lm := Ct (lock clock = thread clock).
//
// Parameters:
//   - addr: Address of the mutex being unlocked
//   - ctx: Current goroutine's RaceContext
//
// Thread Safety: Safe for concurrent calls from multiple goroutines.
//
// Example:
//
//	x = 42       // Write happens-before Unlock
//	mu.Unlock()  // Compiler inserts: racerelease(&mu)
//	// OnRelease captures current clock for next Lock to see
func (d *Detector) OnRelease(addr uintptr, ctx *goroutine.RaceContext) {
	next := ctx.PreflightClockAdvance()
	// Periodic overflow detection runs on synchronization events.
	d.checkOverflowPeriodically()

	// Step 1: Get or create SyncVar for this mutex address.
	syncVar := d.syncShadow.GetOrCreate(addr)

	// Step 2: Set lock's release clock to current thread's clock.
	// This captures the happens-before relationship for future Acquires.
	// Lm := Ct (lock clock = thread clock).
	syncVar.SetReleaseClock(ctx.C)

	// Step 3: Increment logical clock to advance time.
	// This must be done AFTER updating release clock to maintain happens-before.
	ctx.CommitClockAdvance(next)
}

// OnReleaseMerge handles RWMutex read unlock operations.
//
// This is used for RWMutex.RUnlock where multiple readers may have
// overlapping critical sections. We merge the current thread's clock into the
// lock's release clock to capture the union of all happens-before relationships.
//
// Algorithm: FastTrack [FT RELEASE MERGE]
//  1. Get lock's SyncVar
//  2. syncVar.releaseClock = syncVar.releaseClock ⊔ ctx.C (merge clocks)
//  3. ctx.IncrementClock()
//
// This implements: Lm := Lm ⊔ Ct (lock clock merges with thread clock).
//
// Parameters:
//   - addr: Address of the RWMutex being unlocked
//   - ctx: Current goroutine's RaceContext
//
// Thread Safety: Safe for concurrent calls from multiple goroutines.
//
// Example (RWMutex scenario):
//
//	// Reader 1
//	mu.RLock()   // Acquire
//	y = x        // Read
//	mu.RUnlock() // ReleaseMerge (merges Reader 1's clock)
//
//	// Reader 2
//	mu.RLock()   // Acquire
//	z = x        // Read
//	mu.RUnlock() // ReleaseMerge (merges Reader 2's clock)
//
//	// Writer
//	mu.Lock()    // Acquire (sees union of Reader 1 and Reader 2 clocks)
//	x = 42       // Write happens-after both readers
func (d *Detector) OnReleaseMerge(addr uintptr, ctx *goroutine.RaceContext) {
	next := ctx.PreflightClockAdvance()
	// Step 1: Get or create SyncVar for this mutex address.
	syncVar := d.syncShadow.GetOrCreate(addr)

	// Step 2: Merge current thread's clock into lock's release clock.
	// This captures the union of happens-before relationships.
	// Lm := Lm ⊔ Ct (lock clock merges with thread clock).
	syncVar.MergeReleaseClock(ctx.C)

	// Step 3: Increment logical clock to advance time.
	ctx.CommitClockAdvance(next)
}

// ShadowGet returns the VarState for addr without creating it.
// Returns nil if the address has never been accessed.
//
// This is used by the same-epoch fast path in the runtime to check
// whether a systemstack call can be skipped. Read-only, no allocation.
//
//go:nosplit
func (d *Detector) ShadowGet(addr uintptr) *shadowmem.VarState {
	return d.shadowMemory.Get(addr)
}

// GetShadow returns the shadow memory implementation.
// This allows callers to cache a concrete type reference for direct method
// calls, avoiding interface dispatch overhead on the hot path.
func (d *Detector) GetShadow() shadowmem.Shadow {
	return d.shadowMemory
}

// ClearShadowRange clears shadow memory for the given address range.
// Called during memory allocation/free to prevent false positives from
// stale shadow state when the allocator reuses addresses.
func (d *Detector) ClearShadowRange(addr, size uintptr) {
	d.shadowMemory.ClearRange(addr, size)
	d.syncShadow.ClearRange(addr, size)
}

// Reset resets the detector state for testing.
//
// This clears:
//   - All shadow memory cells
//   - All sync shadow memory cells
//   - Race counter
//   - Reported races deduplication map
//   - Goroutine creation metadata
//   - Promotion statistics
//
// Thread Safety: NOT safe for concurrent access.
// The caller must ensure no other goroutines are using the detector.
//
// This is primarily used in test setup/teardown.
func (d *Detector) Reset() {
	d.mu.lock()
	defer d.mu.unlock()
	d.goroutineCreations.reset()

	// Clear shadow memory.
	d.shadowMemory.Reset()
	d.atomicArena.reset()

	// Clear sync shadow memory.
	d.syncShadow.Reset()

	// Reset race counter.
	d.racesDetected = 0

	// Clear reported races map.
	d.reportedRaces.reset()

}

// IsSamplingEnabled returns true if sampling is enabled.
//
// When sampling is enabled, only a fraction of memory accesses are checked.
// This trades detection rate for performance.
//
// Thread Safety: Safe for concurrent calls (read-only).
func (d *Detector) IsSamplingEnabled() bool {
	return d.sampler != nil && d.sampler.IsEnabled()
}

// GetSamplerStats returns sampling statistics.
//
// Returns nil if sampling is disabled.
//
// Thread Safety: Safe for concurrent calls (atomic reads in Sampler).
func (d *Detector) GetSamplerStats() *SamplerStats {
	if d.sampler == nil {
		return nil
	}
	stats := d.sampler.GetStats()
	return &stats
}

// GetSampleRate returns the current sampling rate.
//
// Returns 1 if sampling is disabled (all accesses checked).
//
// Thread Safety: Safe for concurrent calls (read-only).
func (d *Detector) GetSampleRate() uint64 {
	if d.sampler == nil {
		return 1
	}
	return d.sampler.GetEffectiveRate()
}
