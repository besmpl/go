// Package api provides the public runtime API for the Pure-Go Race Detector.
//
// This package implements the entry points called by Go compiler instrumentation
// when code is built with -race flag. These functions are invoked on every memory
// access in instrumented code, making them CRITICAL HOT PATHS.
//
// The API follows the same interface contract as Go's runtime.race* functions,
// ensuring compatibility with existing compiler instrumentation.
//
// Performance Targets (MVP - Phase 1):
//   - raceread:  < 30ns per call (includes OnRead ~21ns)
//   - racewrite: < 25ns per call (includes OnWrite ~17ns)
//   - getCurrentContext (cached): < 5ns
//   - getCurrentContext (first): < 100ns
//
// Runtime Integration:
//   - Goroutine ID via getg().goid runtime bridge (~0ns)
//   - PC capture via sys.GetCallerPC() compiler intrinsic (~0ns)
//   - TID reuse pool for unlimited goroutine support
//   - GoEnd() cleanup for context lifecycle management
package api

import (
	"internal/runtime/atomic"
	"unsafe"

	"runtime/race/kolkov/detector"
	"runtime/race/kolkov/goroutine"
	"runtime/race/kolkov/shadowmem"
	"runtime/race/kolkov/vectorclock"
)

// Ensure unsafe is imported for go:linkname.
var _ = unsafe.Sizeof(0)

// Runtime functions via linkname.
//
//go:linkname printstring runtime.printstring
func printstring(s string)

//go:linkname nanotime runtime.nanotime
func nanotime() int64

//go:linkname runtimeStack runtime.Stack
func runtimeStack(buf []byte, all bool) int

//go:linkname runtimeCaller runtime.Caller
func runtimeCaller(skip int) (pc uintptr, file string, line int, ok bool)

// Helper functions for string/number conversion.

// itoaAPI converts int to string.
func itoaAPI(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// uitoaAPI converts uint64 to string.
func uitoaAPI(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// spinlockAPI is a simple spinlock using atomic operations.
type spinlockAPI struct {
	state atomic.Uint32
}

func (s *spinlockAPI) lock() {
	for !s.state.CompareAndSwap(0, 1) {
		// Spin
	}
}

func (s *spinlockAPI) unlock() {
	s.state.Store(0)
}

// Global detector state.
//
// These variables are initialized once during init() and remain constant
// for the lifetime of the program. The detector itself is thread-safe.
var (
	// enabled controls whether race detection is active.
	// For MVP, this is always true. In Phase 7 (Production), this will
	// be configurable via environment variables (GORACE=...).
	enabled atomic.Uint32 // 0=disabled, 1=enabled

	// contexts is a CAS-based map from goroutine ID to RaceContext.
	// Key: int64 (goroutine ID), Value: *goroutine.RaceContext
	contextsMap contextsMapType

	// nextTID is the atomic counter for allocating thread IDs.
	// Phase 2 Task 2.2: Used for statistics and cleanup trigger.
	// No longer wraps at 256 - TID pool handles reuse.
	nextTID atomic.Uint32

	// det is the global detector instance.
	// All race detection flows through this single instance.
	det *detector.Detector

	// shadow is the cached concrete shadow memory for the same-epoch fast path.
	// Stored as concrete *PageTableShadow to avoid interface dispatch (~5-10ns)
	// on every same-epoch check. Set once during initialization.
	shadow *shadowmem.PageTableShadow


	// === TID Pool Management with Clock Bumping (Phase 2 Task 2.2) ===
	// TID reuse pool supporting unlimited goroutines with safe recycling.
	// When a TID is freed, we record the max clock it reached.
	// When reused, the new goroutine starts its clock ABOVE the previous max,
	// ensuring stale shadow entries are detected as "concurrent" (conservative).

	// freeTIDs is a FIFO queue of recyclable TIDs.
	// FIFO maximizes temporal separation between reuse.
	freeTIDs []uint16

	apiInitCalled atomic.Uint32 // 1 if init() was called

	// tidPoolMu protects freeTIDs and maxClockAtFree.
	// Lock contention is minimal as allocations are rare relative to raceread/racewrite.
	tidPoolMu spinlockAPI

	// tidPoolWarningShown ensures the "nearly exhausted" warning fires only once.
	// Without this, the warning would print on every allocTID() call when < 100 TIDs remain.
	tidPoolWarningShown atomic.Uint32

	// maxClockAtFree records the maximum clock value each TID reached before being freed.
	// The next goroutine assigned this TID must start its clock above this value.
	// Size: MaxThreads * 4 bytes = 4KB.
	maxClockAtFree [vectorclock.MaxThreads]uint32

	// tidToGIDMap maps TID back to GID for cleanup verification.
	// Key: uint16 (TID), Value: int64 (GID).
	// Used during cleanup to identify stale contexts.
	tidToGIDMap tidToGIDMapType

	// allocCounter counts context allocations to trigger periodic cleanup.
	// Every 1000 allocations, we scan for dead goroutines and reclaim TIDs.
	allocCounter atomic.Uint32

	// === Spawn Context Management (GoStart) ===
	// Tracks VectorClock inheritance from parent to child goroutines.

	// spawnContexts stores pending spawn contexts for child goroutines to inherit.
	// Uses slice with mutex for strict FIFO ordering.
	spawnContextsMu    spinlockAPI
	spawnContextsSlice []*spawnInfo

	// nextSpawnID generates unique IDs for spawn contexts.
	nextSpawnID atomic.Uint64

	// spawnContextTTL is the maximum time (in nanoseconds) a spawn context waits for child to claim.
	// After this, the context is cleaned up to prevent memory leaks.
	// 100ms = 100_000_000 ns
	spawnContextTTLNs int64 = 100_000_000
)

// contextsMapType is a CAS-based map from int64 (GID) to *goroutine.RaceContext.
type contextsMapType struct {
	cells [16384]atomic.Pointer[contextCell]
}

type contextCell struct {
	gid int64
	ctx *goroutine.RaceContext
}

func (m *contextsMapType) Load(gid int64) (*goroutine.RaceContext, bool) {
	hash := uint64(gid) * 0x9E3779B97F4A7C15
	idx := hash >> 50 // 14 bits = 16384 slots

	for i := uint64(0); i < 8; i++ {
		slot := (idx + i) & 0x3FFF
		cell := m.cells[slot].Load()
		if cell == nil {
			return nil, false
		}
		if cell.gid == gid {
			return cell.ctx, true
		}
	}
	return nil, false
}

func (m *contextsMapType) Store(gid int64, ctx *goroutine.RaceContext) {
	hash := uint64(gid) * 0x9E3779B97F4A7C15
	idx := hash >> 50

	newCell := &contextCell{gid: gid, ctx: ctx}

	for i := uint64(0); i < 8; i++ {
		slot := (idx + i) & 0x3FFF
		cell := m.cells[slot].Load()

		if cell == nil {
			if m.cells[slot].CompareAndSwap(nil, newCell) {
				return
			}
			cell = m.cells[slot].Load()
		}

		if cell != nil && cell.gid == gid {
			m.cells[slot].Store(newCell)
			return
		}
	}
	// Overflow - store anyway
	m.cells[idx&0x3FFF].Store(newCell)
}

func (m *contextsMapType) LoadAndDelete(gid int64) (*goroutine.RaceContext, bool) {
	hash := uint64(gid) * 0x9E3779B97F4A7C15
	idx := hash >> 50

	for i := uint64(0); i < 8; i++ {
		slot := (idx + i) & 0x3FFF
		cell := m.cells[slot].Load()
		if cell == nil {
			return nil, false
		}
		if cell.gid == gid {
			m.cells[slot].Store(nil)
			return cell.ctx, true
		}
	}
	return nil, false
}

func (m *contextsMapType) Delete(gid int64) {
	m.LoadAndDelete(gid)
}

func (m *contextsMapType) Range(f func(gid int64, ctx *goroutine.RaceContext) bool) {
	for i := range m.cells {
		cell := m.cells[i].Load()
		if cell != nil {
			if !f(cell.gid, cell.ctx) {
				return
			}
		}
	}
}

func (m *contextsMapType) Reset() {
	for i := range m.cells {
		m.cells[i].Store(nil)
	}
}

// tidToGIDMapType is a CAS-based map from uint16 (TID) to int64 (GID).
type tidToGIDMapType struct {
	cells [vectorclock.MaxThreads]atomic.Int64 // Direct indexed by TID
}

func (m *tidToGIDMapType) Store(tid uint16, gid int64) {
	m.cells[tid].Store(gid)
}

func (m *tidToGIDMapType) Load(tid uint16) (int64, bool) {
	gid := m.cells[tid].Load()
	return gid, gid != 0
}

func (m *tidToGIDMapType) Delete(tid uint16) {
	m.cells[tid].Store(0)
}

func (m *tidToGIDMapType) Reset() {
	for i := range m.cells {
		m.cells[i].Store(0)
	}
}

// spawnInfo contains information to pass from parent to child goroutine.
// This enables happens-before tracking across goroutine creation.
type spawnInfo struct {
	parentGID   int64                    // GID of parent goroutine
	childGoid   int64                    // GID of child goroutine (0 = unknown, use FIFO)
	parentClock *vectorclock.VectorClock // Snapshot of parent's clock at fork
	pc          uintptr                  // Program counter of go statement (for stack traces)
	createdAtNs int64                    // Creation time in nanoseconds (for TTL-based cleanup)
	consumed    atomic.Uint32            // 1 if child has claimed this context
}

// init initializes the global race detector.
//
// This runs automatically before main() starts. It sets up:
//   - The global detector instance
//   - The enabled flag (true for MVP)
//   - The TID counter (starts at 0)
//
// The detector is ready to use immediately after init().
func init() {
	ensureInitialized()
}

// ensureInitialized initializes the detector if not already done.
// This is called from init() and also from raceread/racewrite for lazy init.
// Uses CAS to ensure thread-safe initialization.
func ensureInitialized() {
	// Use CAS to ensure only one goroutine initializes
	if !apiInitCalled.CompareAndSwap(0, 1) {
		return // Already initialized or being initialized
	}

	det = detector.NewDetector()

	// Cache concrete shadow memory reference for the same-epoch fast path.
	// Type-assert once here to avoid interface dispatch on every access.
	shadow = det.GetShadow().(*shadowmem.PageTableShadow)

	enabled.Store(1) // 1 = enabled

	// Initialize TID pool - CRITICAL for proper race detection!
	// Without this, allocTID() returns 0 for all goroutines and no races are detected.
	initTIDPool()

	// nextTID starts at 2 (TID 0 is sentinel, TID 1 is for main goroutine).
	nextTID.Store(2)
}

// raceread is called by compiler instrumentation on every read access.
//
// This is the CRITICAL HOT PATH for read operations. It will be invoked
// millions of times during program execution, so performance is paramount.
//
// Flow:
//  1. Check if race detection is enabled (fast atomic load)
//  2. Get or create RaceContext for current goroutine
//  3. Extract program counter (PC) of the access (for future reporting)
//  4. Call detector.OnRead() to check for races
//
// Parameters:
//   - addr: Memory address being read from
//
// Performance: Target <30ns per call (MVP).
//
// Zero Allocations: This function must not allocate on heap after context
// is cached. First call per goroutine may allocate when creating context.
//
// Example (compiler-generated):
//
//	x := *ptr  // Becomes: runtime.raceread(uintptr(unsafe.Pointer(ptr))); x = *ptr
//
// Allow runtime to use this via linkname.
//
//go:linkname raceread
//go:nosplit
func raceread(addr, pc uintptr) {
	// Lazy initialization: If init() hasn't run yet, initialize now.
	// This handles the case where race functions are called before package init().
	if apiInitCalled.Load() == 0 {
		ensureInitialized()
	}

	// Fast path: Check if race detection is enabled.
	// This allows disabling the detector at runtime with minimal overhead.
	if enabled.Load() == 0 {
		return
	}

	// Get RaceContext for current goroutine.
	// This allocates on first call per goroutine (~100ns), then cached (~5ns).
	ctx := getCurrentContext()

	// Perform race detection check.
	// PC is passed through from runtime's sys.GetCallerPC() (~0ns overhead).
	// When pc==0, detector falls back to captureCallerPC() internally.
	det.OnRead(addr, ctx, pc)
}

// racewrite is called by compiler instrumentation on every write access.
//
// This is the CRITICAL HOT PATH for write operations. Like raceread,
// it's called millions of times, so performance is critical.
//
// Flow:
//  1. Check if race detection is enabled (fast atomic load)
//  2. Get or create RaceContext for current goroutine
//  3. Extract program counter (PC) of the access
//  4. Call detector.OnWrite() to check for races
//
// Parameters:
//   - addr: Memory address being written to
//
// Performance: Target <25ns per call (MVP).
//
// Zero Allocations: Must not allocate after context is cached.
//
// Example (compiler-generated):
//
//	*ptr = x  // Becomes: runtime.racewrite(uintptr(unsafe.Pointer(ptr))); *ptr = x
//
// Allow runtime to use this via linkname.
//
//go:linkname racewrite
//go:nosplit
func racewrite(addr, pc uintptr) {
	// Lazy initialization: If init() hasn't run yet, initialize now.
	if apiInitCalled.Load() == 0 {
		ensureInitialized()
	}

	// Fast path: Check if race detection is enabled.
	if enabled.Load() == 0 {
		return
	}

	// Get RaceContext for current goroutine.
	ctx := getCurrentContext()

	// Perform race detection check.
	// PC is passed through from runtime's sys.GetCallerPC() (~0ns overhead).
	// When pc==0, detector falls back to captureCallerPC() internally.
	det.OnWrite(addr, ctx, pc)
}

// === Goroutine Lifecycle (GoStart/GoEnd) ===

// racegostart is called BEFORE creating a new goroutine (go func()).
//
// This function implements the fork semantics from FastTrack algorithm:
//  1. Snapshot current (parent's) VectorClock
//  2. Increment parent's clock (fork is a synchronization event)
//  3. Store snapshot for child to inherit
//
// The snapshot establishes happens-before: all parent's operations before
// the go statement will be visible to the child goroutine.
//
// Parameters:
//   - pc: Program counter of the go statement (for stack traces)
//
// Returns:
//   - uintptr: Spawn ID that can be used for explicit context passing
//
// Performance: ~100ns (VectorClock clone + atomic operations).
//
// Thread Safety: Safe for concurrent calls from multiple goroutines.
//
// Example:
//
//	x = 42                   // Parent writes x
//	go func() {              // racegostart() called here
//	    _ = x                // Child reads x - sees parent's write (no race)
//	}()
//
//go:nosplit
func racegostart(pc uintptr) uintptr {
	if enabled.Load() == 0 {
		return 0
	}

	// Step 1: Get parent's context.
	parentCtx := getCurrentContext()
	parentGID := getGoroutineID()

	// Step 2: Create snapshot of parent's VectorClock.
	// This is the clock the child will inherit.
	spawnClock := parentCtx.C.Clone()

	// Step 3: Increment parent's clock.
	// Parent's subsequent operations have higher clock than fork point.
	// This ensures child doesn't see parent's operations after fork.
	parentCtx.IncrementClock()

	// Step 4: Store spawn context for child to consume (strict FIFO order).
	info := &spawnInfo{
		parentGID:   parentGID,
		parentClock: spawnClock,
		pc:          pc,
		createdAtNs: nanotime(),
	}

	// Append to slice under lock for strict FIFO ordering.
	spawnContextsMu.lock()
	spawnContextsSlice = append(spawnContextsSlice, info)
	spawnContextsMu.unlock()

	// Generate unique spawn ID (for API compatibility, not used for matching).
	spawnID := nextSpawnID.Add(1)

	return uintptr(spawnID)
}

// raceGoStartFromRuntime is called by the runtime's racegostart from systemstack.
// Unlike racegostart, the caller is on g0 so getGoroutineID() would return
// the wrong goroutine. The runtime passes parentGoid explicitly.
//
//go:linkname raceGoStartFromRuntime
//go:nosplit
func raceGoStartFromRuntime(pc uintptr, parentGoid int64) {
	if enabled.Load() == 0 {
		return
	}

	// Look up parent's context using explicit goid.
	parentCtx, ok := contextsMap.Load(parentGoid)
	if !ok {
		// Parent context not found — parent hasn't done any memory accesses yet.
		// Create a fresh spawn context without parent clock inheritance.
		info := &spawnInfo{
			parentGID:   parentGoid,
			parentClock: nil,
			pc:          pc,
			createdAtNs: nanotime(),
		}
		spawnContextsMu.lock()
		spawnContextsSlice = append(spawnContextsSlice, info)
		spawnContextsMu.unlock()
		return
	}

	// Snapshot parent's VectorClock before incrementing.
	spawnClock := parentCtx.C.Clone()

	// Advance parent's clock past the fork point.
	parentCtx.IncrementClock()

	// Store spawn context for child to consume (FIFO).
	info := &spawnInfo{
		parentGID:   parentGoid,
		parentClock: spawnClock,
		pc:          pc,
		createdAtNs: nanotime(),
	}
	spawnContextsMu.lock()
	spawnContextsSlice = append(spawnContextsSlice, info)
	spawnContextsMu.unlock()
}

// raceGoEndFromRuntime is called by the runtime's racegoend.
// Accepts explicit goid since the caller might be on systemstack.
//
//go:linkname raceGoEndFromRuntime
//go:nosplit
func raceGoEndFromRuntime(goid int64) {
	if enabled.Load() == 0 {
		return
	}

	// Load and delete context atomically.
	if ctx, ok := contextsMap.LoadAndDelete(goid); ok {
		// Get current clock before releasing VectorClock (for TID recycling safety).
		var currentClock uint32
		if ctx.C != nil {
			currentClock = ctx.C.Get(ctx.TID)
			ctx.C.Release()
			ctx.C = nil
		}

		// Return TID to pool with clock for safe recycling.
		freeTID(ctx.TID, currentClock)

		// Clean up TID→GID mapping.
		tidToGIDMap.Delete(ctx.TID)
	}
}

// raceGoSetChildID associates the most recently created spawn context with
// the actual child goroutine goid. This is called by the runtime right after
// racegostart, when newg.goid is known.
//
// Without this call, spawn contexts are consumed in FIFO order which causes
// incorrect parent-child matching when runtime creates background goroutines
// (GC, finalizer, etc.) before user goroutines start.
//
//go:linkname raceGoSetChildID
//go:nosplit
func raceGoSetChildID(childGoid int64) {
	if enabled.Load() == 0 {
		return
	}
	spawnContextsMu.lock()
	// Walk backwards to find the most recently added (un-keyed) spawn context.
	for i := len(spawnContextsSlice) - 1; i >= 0; i-- {
		info := spawnContextsSlice[i]
		if info.consumed.Load() == 0 && info.childGoid == 0 {
			info.childGoid = childGoid
			break
		}
	}
	spawnContextsMu.unlock()
}

// raceGoSetChildIDWithCtx associates the most recently created spawn context
// with the actual child goroutine goid AND eagerly creates the child's
// RaceContext. Returns the context pointer as uintptr for direct caching
// in newg.racectx.
//
// T13 optimization: By creating the context here (during goroutine creation),
// we eliminate the first-access slow path that would otherwise run on the
// child's first raceread/racewrite. The context is stored in both:
//   - contextsMap (as *goroutine.RaceContext — visible to GC, dual reference)
//   - g.racectx (as uintptr — invisible to GC, fast path access)
//
// This is safe because contextsMap keeps the context alive until raceGoEnd
// removes it. The GC sees the *RaceContext in contextsMap and won't collect it.
//
//go:linkname raceGoSetChildIDWithCtx
func raceGoSetChildIDWithCtx(childGoid int64) uintptr {
	if enabled.Load() == 0 {
		return 0
	}

	// Step 1: Associate spawn context with childGoid (same as raceGoSetChildID).
	var parentClock *vectorclock.VectorClock
	spawnContextsMu.lock()
	for i := len(spawnContextsSlice) - 1; i >= 0; i-- {
		info := spawnContextsSlice[i]
		if info.consumed.Load() == 0 && info.childGoid == 0 {
			info.childGoid = childGoid
			// Consume the spawn context immediately — the child won't need
			// findAndConsumeSpawnContext since we create its context here.
			if info.consumed.CompareAndSwap(0, 1) {
				parentClock = info.parentClock
			}
			break
		}
	}
	spawnContextsMu.unlock()

	// Step 2: Eagerly create the child's RaceContext.
	tid, startClock := allocTID()

	var ctx *goroutine.RaceContext
	if parentClock != nil {
		ctx = goroutine.AllocWithParentClock(tid, parentClock, startClock)
		// Release the spawn clock clone back to pool (data already copied).
		parentClock.Release()
	} else {
		ctx = goroutine.AllocWithStartClock(tid, startClock)
	}

	// Step 3: Store in contextsMap for GC safety (dual reference).
	contextsMap.Store(childGoid, ctx)

	// Track TID → GID mapping for cleanup.
	tidToGIDMap.Store(tid, childGoid)

	return uintptr(unsafe.Pointer(ctx))
}

// raceInitMainCtx pre-creates the main goroutine's (goid=1) RaceContext.
// Called during raceinit to eliminate the first-access slow path for main.
//
// The main goroutine is special: it has goid=1 and no parent spawn context.
// Returns the context pointer as uintptr for caching in g.racectx.
//
//go:linkname raceInitMainCtx
func raceInitMainCtx() uintptr {
	if apiInitCalled.Load() == 0 {
		ensureInitialized()
	}
	if enabled.Load() == 0 {
		return 0
	}

	// Check if main goroutine context already exists (shouldn't, but be safe).
	var mainGoid int64 = 1
	if ctx, ok := contextsMap.Load(mainGoid); ok {
		return uintptr(unsafe.Pointer(ctx))
	}

	// Allocate TID and create context for main goroutine.
	tid, startClock := allocTID()
	ctx := goroutine.AllocWithStartClock(tid, startClock)

	// Store in contextsMap for GC safety (dual reference).
	contextsMap.Store(mainGoid, ctx)

	// Track TID → GID mapping.
	tidToGIDMap.Store(tid, mainGoid)

	return uintptr(unsafe.Pointer(ctx))
}

// racegoend is called when a goroutine terminates.
//
// This function cleans up resources associated with the goroutine:
//  1. Returns TID to the free pool for reuse
//  2. Removes context from cache
//  3. Cleans up TID→GID mapping
//
// Performance: ~50ns (map operations + TID free).
//
// Thread Safety: Safe for concurrent calls.
//
//go:nosplit
func racegoend() {
	if enabled.Load() == 0 {
		return
	}

	gid := getGoroutineID()

	// Load and delete context atomically.
	if ctx, ok := contextsMap.LoadAndDelete(gid); ok {
		// Get current clock before releasing VectorClock (for TID recycling safety).
		var currentClock uint32
		if ctx.C != nil {
			currentClock = ctx.C.Get(ctx.TID)
			ctx.C.Release()
			ctx.C = nil
		}

		// Return TID to pool with clock for safe recycling.
		freeTID(ctx.TID, currentClock)

		// Clean up TID→GID mapping.
		tidToGIDMap.Delete(ctx.TID)
	}
}

// RaceGoStart is the exported wrapper for racegostart.
// Used by tests and instrumented code that can't call lowercase functions.
func RaceGoStart(pc uintptr) uintptr {
	return racegostart(pc)
}

// RaceGoEnd is the exported wrapper for racegoend.
// Used by tests and instrumented code that can't call lowercase functions.
func RaceGoEnd() {
	racegoend()
}

// findAndConsumeSpawnContext attempts to find and consume a spawn context
// for the current (child) goroutine using heuristic matching.
//
// Heuristic: A newly spawned goroutine calls getCurrentContext() shortly
// after parent called racegostart(). We find the oldest unclaimed spawn
// context and consume it.
//
// Algorithm:
//  1. Lock spawn contexts slice for exclusive access
//  2. Iterate in FIFO order (oldest first - strict ordering!)
//  3. Skip already consumed contexts
//  4. Skip expired contexts (TTL > 100ms)
//  5. First unclaimed, non-expired context is consumed
//  6. Clean up all consumed/expired contexts to prevent memory leaks
//
// CRITICAL: Uses slice iteration instead of sync.Map.Range() because
// sync.Map.Range() iterates in non-deterministic order, which can cause
// child goroutines to receive wrong parent's clock in rapid spawn scenarios.
//
// Returns parent's VectorClock if found, nil otherwise.
func findAndConsumeSpawnContext() *vectorclock.VectorClock {
	// Get this goroutine's goid for targeted lookup.
	myGoid := getGoroutineID()

	spawnContextsMu.lock()
	defer spawnContextsMu.unlock()

	nowNs := nanotime()
	var foundClock *vectorclock.VectorClock

	// First pass: try to find spawn context specifically keyed to this child goid.
	// This is the primary path when raceGoSetChildID was called after racegostart.
	for _, info := range spawnContextsSlice {
		if info.consumed.Load() != 0 {
			continue
		}
		if nowNs-info.createdAtNs > spawnContextTTLNs {
			continue
		}
		if info.childGoid == myGoid {
			if info.consumed.CompareAndSwap(0, 1) {
				foundClock = info.parentClock
				break
			}
		}
	}

	// Fallback: if no goid-keyed context found, try FIFO (for legacy/timer contexts).
	if foundClock == nil {
		for _, info := range spawnContextsSlice {
			if info.consumed.Load() != 0 {
				continue
			}
			if nowNs-info.createdAtNs > spawnContextTTLNs {
				continue
			}
			// Only consume un-keyed contexts (childGoid == 0) in FIFO mode.
			if info.childGoid != 0 {
				continue
			}
			if info.consumed.CompareAndSwap(0, 1) {
				foundClock = info.parentClock
				break
			}
		}
	}

	// Clean up expired and consumed contexts from the slice.
	// This prevents memory leaks and keeps the slice compact.
	// Reuse backing array to avoid allocations.
	validContexts := spawnContextsSlice[:0]
	for _, info := range spawnContextsSlice {
		if info.consumed.Load() == 0 && nowNs-info.createdAtNs <= spawnContextTTLNs {
			validContexts = append(validContexts, info)
		} else if info.parentClock != nil && info != nil {
			// Release expired/consumed spawn clocks back to pool.
			// The consumed ones whose clock was used have already been released
			// by the consumer (raceGoSetChildIDWithCtx / getCurrentContext).
			// This handles expired contexts that were never consumed.
			if info.consumed.Load() == 0 {
				info.parentClock.Release()
			}
			info.parentClock = nil
		}
	}
	spawnContextsSlice = validContexts

	return foundClock
}

// raceacquire is called by compiler instrumentation on mutex lock operations (Phase 4 Task 4.1).
//
// This establishes a happens-before edge from the previous Unlock to this Lock.
// The acquiring thread merges the mutex's release clock into its own clock.
//
// Flow:
//  1. Check if race detection is enabled (fast atomic load)
//  2. Get or create RaceContext for current goroutine
//  3. Call detector.OnAcquire() to establish happens-before
//
// Parameters:
//   - addr: Address of the sync.Mutex being locked
//
// Performance: Target <500ns per call (VectorClock join overhead acceptable).
//
// Zero Allocations: First call per goroutine may allocate context.
// VectorClock join operation is zero-allocation.
//
// Example (compiler-generated):
//
//	mu.Lock()  // Becomes: runtime.raceacquire(uintptr(unsafe.Pointer(&mu))); mu.Lock()
//
// Allow runtime to use this via linkname.
//
//go:linkname raceacquire
//go:nosplit
func raceacquire(addr uintptr) {
	// Fast path: Check if race detection is enabled.
	if enabled.Load() == 0 {
		return
	}

	// Get RaceContext for current goroutine.
	ctx := getCurrentContext()

	// Perform sync acquire tracking.
	// This establishes happens-before from previous Unlock.
	det.OnAcquire(addr, ctx)
}

// racerelease is called by compiler instrumentation on mutex unlock operations (Phase 4 Task 4.1).
//
// This creates a happens-before edge that future Lock operations will synchronize with.
// The releasing thread captures its current clock into the mutex's release clock.
//
// Flow:
//  1. Check if race detection is enabled (fast atomic load)
//  2. Get or create RaceContext for current goroutine
//  3. Call detector.OnRelease() to capture current clock
//
// Parameters:
//   - addr: Address of the sync.Mutex being unlocked
//
// Performance: Target <300ns per call (VectorClock copy overhead acceptable).
//
// Zero Allocations: VectorClock is updated in place (no new allocations after first).
//
// Example (compiler-generated):
//
//	mu.Unlock()  // Becomes: runtime.racerelease(uintptr(unsafe.Pointer(&mu))); mu.Unlock()
//
// Allow runtime to use this via linkname.
//
//go:linkname racerelease
//go:nosplit
func racerelease(addr uintptr) {
	// Fast path: Check if race detection is enabled.
	if enabled.Load() == 0 {
		return
	}

	// Get RaceContext for current goroutine.
	ctx := getCurrentContext()

	// Perform sync release tracking.
	// This captures current clock for next Acquire.
	det.OnRelease(addr, ctx)
}

// racereleasemerge is called by compiler instrumentation on RWMutex unlock operations (Phase 4 Task 4.1).
//
// This is used for RWMutex.Unlock (write unlock) where multiple readers may have
// overlapping critical sections. We merge the current thread's clock into the
// lock's release clock to capture the union of all happens-before relationships.
//
// Flow:
//  1. Check if race detection is enabled (fast atomic load)
//  2. Get or create RaceContext for current goroutine
//  3. Call detector.OnReleaseMerge() to merge current clock
//
// Parameters:
//   - addr: Address of the sync.RWMutex being unlocked
//
// Performance: Target <500ns per call (VectorClock merge overhead acceptable).
//
// Zero Allocations: VectorClock merge is zero-allocation.
//
// Example (compiler-generated):
//
//	mu.RUnlock()  // Becomes: runtime.racereleasemerge(uintptr(unsafe.Pointer(&mu))); mu.RUnlock()
//
// Allow runtime to use this via linkname.
//
//go:linkname racereleasemerge
//go:nosplit
func racereleasemerge(addr uintptr) {
	// Fast path: Check if race detection is enabled.
	if enabled.Load() == 0 {
		return
	}

	// Get RaceContext for current goroutine.
	ctx := getCurrentContext()

	// Perform sync release merge tracking.
	// This merges current clock into lock's release clock.
	det.OnReleaseMerge(addr, ctx)
}

// === g.racectx Fast Path: context pointer passed directly (T9 optimization) ===
// These skip contextsMap lookup entirely — ~5-30ns savings per call.

//go:linkname racereadCtx
//go:nosplit
func racereadCtx(addr, pc, racectx uintptr) {
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	det.OnRead(addr, ctx, pc)
}

//go:linkname racewriteCtx
//go:nosplit
func racewriteCtx(addr, pc, racectx uintptr) {
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	det.OnWrite(addr, ctx, pc)
}

//go:linkname raceacquireCtx
//go:nosplit
func raceacquireCtx(addr, racectx uintptr) {
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	det.OnAcquire(addr, ctx)
}

//go:linkname racereleaseCtx
//go:nosplit
func racereleaseCtx(addr, racectx uintptr) {
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	det.OnRelease(addr, ctx)
}

//go:linkname racereleasemergeCtx
//go:nosplit
func racereleasemergeCtx(addr, racectx uintptr) {
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	det.OnReleaseMerge(addr, ctx)
}

// === Same-Epoch Fast Path (T22 optimization) ===
// These functions check if a memory access can skip the full detector path.
// Called from runtime BEFORE systemstack() to avoid ~60ns closure+stack-switch
// overhead for ~65% of accesses (same-epoch hits).
//
// T26: raceGetShadowPtr returns the uintptr address of *PageTableShadow.
// Called once from runtime.raceinit to cache the value for inline fast path.
//
//go:linkname raceGetShadowPtr
//go:nosplit
func raceGetShadowPtr() uintptr {
	return uintptr(unsafe.Pointer(shadow))
}

// Same-epoch read: if the write epoch's TID+clock matches this goroutine's
// current epoch, then this goroutine was the last writer at the same logical
// time. No other goroutine could have written since, so no read-write race.
//
// Same-epoch write: additionally requires no concurrent readers (readerState==0),
// because a concurrent read from another goroutine could race with this write.

//go:linkname raceSameEpochRead
//go:nosplit
func raceSameEpochRead(addr, racectx uintptr) bool {
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	// Use cached concrete *PageTableShadow to avoid interface dispatch (~5-10ns).
	// shadow is set once during initialization and never changes.
	vs := shadow.Get(addr)
	if vs == nil {
		return false // First access, need full path to create VarState.
	}
	// Same-epoch check: the write epoch stored in shadow matches this
	// goroutine's current epoch exactly (same TID AND same clock).
	//
	// Per FastTrack (PLDI 2009), the clock only advances at synchronization
	// events, so consecutive accesses within the same sync-free region share
	// the same epoch. This makes the same-epoch check effective for ~65% of
	// reads (and ~71% of writes), avoiding the full detector path entirely.
	return vs.W.Load() == uint64(ctx.Epoch)
}

//go:linkname raceSameEpochWrite
//go:nosplit
func raceSameEpochWrite(addr, racectx uintptr) bool {
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	// Use cached concrete *PageTableShadow to avoid interface dispatch (~5-10ns).
	vs := shadow.Get(addr)
	if vs == nil {
		return false // First access, need full path to create VarState.
	}
	// Same-epoch write check: write epoch matches AND no concurrent readers.
	// If there are readers from other goroutines, we must do the full check
	// to detect write-read races.
	return vs.W.Load() == uint64(ctx.Epoch) && vs.GetReaderCount() == 0
}

// === g.racectx Slow Path: creates context, returns pointer for caching ===
// Called on first access per goroutine. Returns context pointer as uintptr
// so runtime can cache it in g.racectx for subsequent fast-path calls.

//go:linkname racereadSlow
func racereadSlow(addr, pc uintptr) uintptr {
	if apiInitCalled.Load() == 0 {
		ensureInitialized()
	}
	if enabled.Load() == 0 {
		return 0
	}
	ctx := getCurrentContext()
	det.OnRead(addr, ctx, pc)
	return uintptr(unsafe.Pointer(ctx))
}

//go:linkname racewriteSlow
func racewriteSlow(addr, pc uintptr) uintptr {
	if apiInitCalled.Load() == 0 {
		ensureInitialized()
	}
	if enabled.Load() == 0 {
		return 0
	}
	ctx := getCurrentContext()
	det.OnWrite(addr, ctx, pc)
	return uintptr(unsafe.Pointer(ctx))
}

//go:linkname raceacquireSlow
func raceacquireSlow(addr uintptr) uintptr {
	if apiInitCalled.Load() == 0 {
		ensureInitialized()
	}
	if enabled.Load() == 0 {
		return 0
	}
	ctx := getCurrentContext()
	det.OnAcquire(addr, ctx)
	return uintptr(unsafe.Pointer(ctx))
}

// === Cross-Goroutine Acquire/Release (for channel sync) ===

// raceAcquireForGoroutine performs an acquire operation on addr on behalf of
// the goroutine identified by goid. This is called by the runtime when one
// goroutine needs to acquire a sync object for another goroutine (e.g.,
// raceacquireg in channel operations where the current goroutine wakes up
// a blocked partner and must transfer HB to the partner, not to itself).
//
//go:linkname raceAcquireForGoroutine
//go:nosplit
func raceAcquireForGoroutine(addr uintptr, goid int64) {
	if enabled.Load() == 0 {
		return
	}
	ctx, ok := contextsMap.Load(goid)
	if !ok {
		// Context not yet created — this goroutine hasn't done any instrumented
		// memory access yet. If goid is the current goroutine (typical for mutex
		// Lock), lazily initialize the context so the acquire is not lost.
		if goid == getGoroutineID() {
			ctx = getCurrentContext()
		}
		if ctx == nil {
			return
		}
	}
	det.OnAcquire(addr, ctx)
}

// raceReleaseForGoroutine performs a release operation on addr on behalf of
// the goroutine identified by goid. This is the release counterpart of
// raceAcquireForGoroutine — used in racereleaseg where the current goroutine
// releases a sync object on behalf of a different goroutine.
//
//go:linkname raceReleaseForGoroutine
//go:nosplit
func raceReleaseForGoroutine(addr uintptr, goid int64) {
	if enabled.Load() == 0 {
		return
	}
	ctx, ok := contextsMap.Load(goid)
	if !ok {
		if goid == getGoroutineID() {
			ctx = getCurrentContext()
		}
		if ctx == nil {
			return
		}
	}
	det.OnRelease(addr, ctx)
}

// raceReleaseMergeForGoroutine performs a release-merge operation on addr on
// behalf of the goroutine identified by goid. This is the release-merge
// counterpart of raceReleaseForGoroutine -- used in racereleasemergeg where
// the current goroutine releases on behalf of a different goroutine.
//
//go:linkname raceReleaseMergeForGoroutine
//go:nosplit
func raceReleaseMergeForGoroutine(addr uintptr, goid int64) {
	if enabled.Load() == 0 {
		return
	}
	ctx, ok := contextsMap.Load(goid)
	if !ok {
		if goid == getGoroutineID() {
			ctx = getCurrentContext()
		}
		if ctx == nil {
			return
		}
	}
	det.OnReleaseMerge(addr, ctx)
}

// === Channel Synchronization API (Phase 4 Task 4.2) ===

// racechansendbefore is called by compiler instrumentation BEFORE channel send (Phase 4 Task 4.2).
//
// This is called before the send operation blocks/completes. For MVP, this is
// a no-op placeholder. Future phases could use this for validation or optimizations.
//
// Flow:
//  1. Check if race detection is enabled (fast atomic load)
//  2. Get or create RaceContext for current goroutine
//  3. Call detector.OnChannelSendBefore()
//
// Parameters:
//   - ch: Address of the channel being sent to
//
// Performance: Target <100ns per call (minimal overhead).
//
// Example (compiler-generated):
//
//	ch <- value  // Becomes: runtime.racechansendbefore(&ch); ...; runtime.racechansendafter(&ch)
//
//go:nosplit
func racechansendbefore(ch uintptr) {
	// Fast path: Check if race detection is enabled.
	if enabled.Load() == 0 {
		return
	}

	// Get RaceContext for current goroutine.
	ctx := getCurrentContext()

	// Perform channel send before tracking (MVP: no-op).
	det.OnChannelSendBefore(ch, ctx)
}

// racechansendafter is called by compiler instrumentation AFTER channel send completes (Phase 4 Task 4.2).
//
// This establishes a happens-before edge from the sender to future receivers.
// The sender's clock is captured into the channel's sendClock.
//
// Flow:
//  1. Check if race detection is enabled (fast atomic load)
//  2. Get or create RaceContext for current goroutine
//  3. Call detector.OnChannelSendAfter() to capture sender's clock
//
// Parameters:
//   - ch: Address of the channel being sent to
//
// Performance: Target <500ns per call (VectorClock copy overhead acceptable).
//
// Zero Allocations: First call may allocate ChannelState. Subsequent calls update in place.
//
// Example (compiler-generated):
//
//	ch <- value  // Becomes: runtime.racechansendbefore(&ch); ...; runtime.racechansendafter(&ch)
//
//go:nosplit
func racechansendafter(ch uintptr) {
	// Fast path: Check if race detection is enabled.
	if enabled.Load() == 0 {
		return
	}

	// Get RaceContext for current goroutine.
	ctx := getCurrentContext()

	// Perform channel send after tracking.
	// This captures sender's clock for receiver to see.
	det.OnChannelSendAfter(ch, ctx)
}

// racechanrecvbefore is called by compiler instrumentation BEFORE channel receive (Phase 4 Task 4.2).
//
// This is called before the receive operation blocks/completes. For MVP, this is
// a no-op placeholder. Future phases could use this for validation or optimizations.
//
// Flow:
//  1. Check if race detection is enabled (fast atomic load)
//  2. Get or create RaceContext for current goroutine
//  3. Call detector.OnChannelRecvBefore()
//
// Parameters:
//   - ch: Address of the channel being received from
//
// Performance: Target <100ns per call (minimal overhead).
//
// Example (compiler-generated):
//
//	value := <-ch  // Becomes: runtime.racechanrecvbefore(&ch); ...; runtime.racechanrecvafter(&ch)
//
//go:nosplit
func racechanrecvbefore(ch uintptr) {
	// Fast path: Check if race detection is enabled.
	if enabled.Load() == 0 {
		return
	}

	// Get RaceContext for current goroutine.
	ctx := getCurrentContext()

	// Perform channel receive before tracking (MVP: no-op).
	det.OnChannelRecvBefore(ch, ctx)
}

// racechanrecvafter is called by compiler instrumentation AFTER channel receive completes (Phase 4 Task 4.2).
//
// This establishes a happens-before edge from the sender to the receiver.
// The receiver merges the sender's clock to observe all the sender's work.
//
// Flow:
//  1. Check if race detection is enabled (fast atomic load)
//  2. Get or create RaceContext for current goroutine
//  3. Call detector.OnChannelRecvAfter() to merge sender's clock
//
// Parameters:
//   - ch: Address of the channel being received from
//
// Performance: Target <500ns per call (VectorClock join overhead acceptable).
//
// Zero Allocations: VectorClock join is zero-allocation (in-place update).
//
// Example (compiler-generated):
//
//	value := <-ch  // Becomes: runtime.racechanrecvbefore(&ch); ...; runtime.racechanrecvafter(&ch)
//
//go:nosplit
func racechanrecvafter(ch uintptr) {
	// Fast path: Check if race detection is enabled.
	if enabled.Load() == 0 {
		return
	}

	// Get RaceContext for current goroutine.
	ctx := getCurrentContext()

	// Perform channel receive after tracking.
	// This merges sender's clock into receiver.
	det.OnChannelRecvAfter(ch, ctx)
}

// racechanclose is called by compiler instrumentation when channel is closed (Phase 4 Task 4.2).
//
// This establishes a happens-before edge from the closer to all future receives.
// The closer's clock is captured into the channel's closeClock.
//
// Flow:
//  1. Check if race detection is enabled (fast atomic load)
//  2. Get or create RaceContext for current goroutine
//  3. Call detector.OnChannelClose() to capture closer's clock
//
// Parameters:
//   - ch: Address of the channel being closed
//
// Performance: Target <300ns per call (VectorClock copy overhead acceptable).
//
// Zero Allocations: First call allocates VectorClock for closeClock.
//
// Example (compiler-generated):
//
//	close(ch)  // Becomes: runtime.racechanclose(&ch); close(ch)
//
//go:nosplit
func racechanclose(ch uintptr) {
	// Fast path: Check if race detection is enabled.
	if enabled.Load() == 0 {
		return
	}

	// Get RaceContext for current goroutine.
	ctx := getCurrentContext()

	// Perform channel close tracking.
	// This captures closer's clock for future receives.
	det.OnChannelClose(ch, ctx)
}

// === WaitGroup Synchronization API (Phase 4 Task 4.3) ===

// racewaitgroupadd is called by compiler instrumentation on WaitGroup.Add(delta) (Phase 4 Task 4.3).
//
// This tracks WaitGroup counter increments. While Add() doesn't establish
// happens-before on its own, we track the counter for optional validation
// and debugging.
//
// Flow:
//  1. Check if race detection is enabled (fast atomic load)
//  2. Get or create RaceContext for current goroutine
//  3. Call detector.OnWaitGroupAdd() to track counter
//
// Parameters:
//   - wg: Address of the sync.WaitGroup
//   - delta: The delta to add to the counter
//
// Performance: Target <200ns per call (minimal overhead).
//
// Zero Allocations: First call per goroutine may allocate context.
//
// Example (compiler-generated):
//
//	wg.Add(1)  // Becomes: runtime.racewaitgroupadd(uintptr(unsafe.Pointer(&wg)), 1); wg.Add(1)
//
//go:nosplit
//nolint:unused // Called by compiler instrumentation, not directly from code
func racewaitgroupadd(wg uintptr, delta int) {
	// Fast path: Check if race detection is enabled.
	if enabled.Load() == 0 {
		return
	}

	// Get RaceContext for current goroutine.
	ctx := getCurrentContext()

	// Perform WaitGroup add tracking.
	det.OnWaitGroupAdd(wg, delta, ctx)
}

// racewaitgroupdone is called by compiler instrumentation on WaitGroup.Done() (Phase 4 Task 4.3).
//
// This is the critical happens-before operation: Done() captures the current
// thread's clock and merges it into the WaitGroup's doneClock. When Wait()
// returns, it will merge this doneClock, establishing happens-before.
//
// Flow:
//  1. Check if race detection is enabled (fast atomic load)
//  2. Get or create RaceContext for current goroutine
//  3. Call detector.OnWaitGroupDone() to merge clock into doneClock
//
// Parameters:
//   - wg: Address of the sync.WaitGroup
//
// Performance: Target <500ns per call (VectorClock merge overhead acceptable).
//
// Zero Allocations: VectorClock merge is zero-allocation.
//
// Example (compiler-generated):
//
//	wg.Done()  // Becomes: runtime.racewaitgroupdone(uintptr(unsafe.Pointer(&wg))); wg.Done()
//
//go:nosplit
//nolint:unused // Called by compiler instrumentation, not directly from code
func racewaitgroupdone(wg uintptr) {
	// Fast path: Check if race detection is enabled.
	if enabled.Load() == 0 {
		return
	}

	// Get RaceContext for current goroutine.
	ctx := getCurrentContext()

	// Perform WaitGroup done tracking.
	// This merges current thread's clock into doneClock.
	det.OnWaitGroupDone(wg, ctx)
}

// racewaitgroupwaitbefore is called by compiler instrumentation BEFORE WaitGroup.Wait() blocks (Phase 4 Task 4.3).
//
// This is called before Wait() blocks waiting for all Done() calls.
// For MVP, this is primarily a placeholder for future optimizations or validation.
//
// Flow:
//  1. Check if race detection is enabled (fast atomic load)
//  2. Get or create RaceContext for current goroutine
//  3. Call detector.OnWaitGroupWaitBefore()
//
// Parameters:
//   - wg: Address of the sync.WaitGroup
//
// Performance: Target <100ns per call (minimal overhead).
//
// Example (compiler-generated):
//
//	wg.Wait()  // Becomes: runtime.racewaitgroupwaitbefore(&wg); ...; runtime.racewaitgroupwaitafter(&wg)
//
//go:nosplit
//nolint:unused // Called by compiler instrumentation, not directly from code
func racewaitgroupwaitbefore(wg uintptr) {
	// Fast path: Check if race detection is enabled.
	if enabled.Load() == 0 {
		return
	}

	// Get RaceContext for current goroutine.
	ctx := getCurrentContext()

	// Perform WaitGroup wait before tracking (MVP: minimal).
	det.OnWaitGroupWaitBefore(wg, ctx)
}

// racewaitgroupwaitafter is called by compiler instrumentation AFTER WaitGroup.Wait() returns (Phase 4 Task 4.3).
//
// This is the critical happens-before establishment: the waiter merges all
// accumulated Done() clocks into its own clock. After this, all writes
// done before Done() are visible to the waiter.
//
// Flow:
//  1. Check if race detection is enabled (fast atomic load)
//  2. Get or create RaceContext for current goroutine
//  3. Call detector.OnWaitGroupWaitAfter() to merge doneClock
//
// Parameters:
//   - wg: Address of the sync.WaitGroup
//
// Performance: Target <500ns per call (VectorClock merge overhead acceptable).
//
// Zero Allocations: VectorClock merge is zero-allocation.
//
// Example (compiler-generated):
//
//	wg.Wait()  // Becomes: runtime.racewaitgroupwaitbefore(&wg); ...; runtime.racewaitgroupwaitafter(&wg)
//	// After Wait() returns, waiter can safely read child goroutines' writes
//
//go:nosplit
//nolint:unused // Called by compiler instrumentation, not directly from code
func racewaitgroupwaitafter(wg uintptr) {
	// Fast path: Check if race detection is enabled.
	if enabled.Load() == 0 {
		return
	}

	// Get RaceContext for current goroutine.
	ctx := getCurrentContext()

	// Perform WaitGroup wait after tracking.
	// This merges accumulated doneClock into waiter's clock.
	det.OnWaitGroupWaitAfter(wg, ctx)
}

// getCurrentContext returns the RaceContext for the current goroutine.
//
// This function maintains a per-goroutine context cache in the global
// contexts sync.Map. On first access, it:
//  1. Extracts goroutine ID (via fast assembly on amd64, ~1ns)
//  2. Tries to find spawn context from parent (GoStart inheritance)
//  3. Allocates a TID from the reuse pool (0-255)
//  4. Creates a RaceContext for that TID (with or without parent clock)
//  5. Caches it in the map
//
// On subsequent accesses, it just does a map lookup (~5ns).
//
// GoStart Inheritance (NEW):
//   - If racegostart() was called before spawning this goroutine,
//     child inherits parent's VectorClock establishing happens-before.
//   - This prevents false positives for patterns like:
//     x = 42; go func() { _ = x }()
//
// Performance:
//   - First call per goroutine: ~100ns (includes TID allocation from pool)
//   - Cached calls: ~5ns (sync.Map load operation)
//
// TID Allocation (Phase 2 Task 2.2):
//   - TIDs allocated from reuse pool (supports unlimited goroutines)
//   - Periodic cleanup (every 1000 allocations) reclaims TIDs from dead goroutines
//   - If pool exhausted, cleanup triggered immediately
//
// Thread Safety: Safe for concurrent calls from multiple goroutines.
func getCurrentContext() *goroutine.RaceContext {
	// Step 1: Get goroutine ID for current goroutine.
	// Phase 2.1: Fast assembly implementation on amd64 (~1ns).
	// Fallback: runtimeStack parsing on other architectures (~4.7µs).
	gid := getGoroutineID()

	// Step 2: Try to load existing context from cache (fast path).
	// contextsMap.Load returns *goroutine.RaceContext directly (no type assertion needed).
	if ctx, ok := contextsMap.Load(gid); ok {
		return ctx
	}

	// Step 3: Slow path - allocate new context for this goroutine.
	// This happens once per goroutine at first access.

	// Step 3a: Try to find spawn context from parent (GoStart inheritance).
	// If parent called racegostart() before spawning us, we inherit their clock.
	parentClock := findAndConsumeSpawnContext()

	// Allocate TID from reuse pool with clock bumping for safe recycling.
	tid, startClock := allocTID()

	// Create new RaceContext for this goroutine.
	var ctx *goroutine.RaceContext
	if parentClock != nil {
		// GoStart path: inherit parent's clock with recycling-safe startClock.
		ctx = goroutine.AllocWithParentClock(tid, parentClock, startClock)
		// Release the spawn clock clone back to pool (data already copied).
		parentClock.Release()
	} else {
		// Legacy path: fresh clock with recycling-safe startClock.
		ctx = goroutine.AllocWithStartClock(tid, startClock)
	}

	// Store in cache for future accesses.
	// sync.Map.Store is thread-safe and handles concurrent stores gracefully.
	contextsMap.Store(gid, ctx)

	// Track TID → GID mapping for cleanup.
	tidToGIDMap.Store(tid, gid)

	// Trigger periodic cleanup to reclaim TIDs from dead goroutines.
	maybeCleanup()

	return ctx
}

// === TID Pool Management Functions (Phase 2 Task 2.2) ===

// initTIDPool initializes the TID reuse pool with all available TIDs (0-255).
//
// This is called once during Init() to set up the free TID stack.
// All 256 TIDs are initially available for allocation.
//
// TIDs are stored in ascending order [1, 2, ..., MaxThreads-1] so allocation
// proceeds 1, 2, 3, ... via FIFO pop from front.
//
// CRITICAL: TID 0 is RESERVED as "no owner" sentinel in SmartTrack
// ownership tracking (VarState.exclusiveWriter). Allocating TID 0 to a
// goroutine would make CAS(0, 0) a no-op, preventing ownership claims
// and causing missed race detections.
//
// Thread Safety: NOT thread-safe. Must be called during initialization only.
func initTIDPool() {
	tidPoolMu.lock()
	defer tidPoolMu.unlock()

	// Initialize free TID pool with TIDs [1, 2, ..., MaxThreads-1].
	// TID 0 is excluded — it serves as "no exclusive writer" sentinel.
	// TIDs must be < vectorclock.MaxThreads to fit in VectorClock array.
	maxTID := vectorclock.MaxThreads - 1
	freeTIDs = make([]uint16, maxTID)
	for i := 0; i < maxTID; i++ {
		//nolint:gosec // G115: Safe conversion, i+1 is always <= MaxThreads-1
		freeTIDs[i] = uint16(i + 1)
	}
}

// allocTID allocates a TID from the free pool with clock bumping for safe recycling.
//
// Returns (tid, startClock) where startClock is the initial clock value the
// new goroutine must use. For fresh TIDs, startClock=1. For recycled TIDs,
// startClock = maxClockAtFree[tid] + 1, ensuring stale shadow entries are
// always detected as "concurrent" (no false negatives).
//
// Algorithm:
//  1. Lock the pool
//  2. Try FIFO pop from recycled TIDs (with clock bumping)
//  3. If empty, trigger cleanup and retry
//  4. Graceful degradation if all TIDs exhausted
//
// Pool depletion warning: When fewer than 50 TIDs remain (~5% of MaxThreads),
// a warning is printed. This is a meaningful indicator of ACTUAL exhaustion,
// as opposed to TID-value-based warnings which fire falsely with FIFO recycling.
//
// Performance: ~50ns (mutex lock + queue pop + array read).
//
// Thread Safety: Safe for concurrent calls (protected by tidPoolMu).
func allocTID() (uint16, uint32) {
	tidPoolMu.lock()

	// Fast path: TID available in pool.
	if len(freeTIDs) > 0 {
		// Warn once when pool is nearly depleted (< 50 TIDs remaining, ~5% of MaxThreads).
		// This indicates real TID exhaustion, not just high TID values from FIFO cycling.
		// The warning fires only once to avoid spamming on every allocTID() call.
		if len(freeTIDs) < 50 && tidPoolWarningShown.CompareAndSwap(0, 1) {
			printstring("WARNING: race detector TID pool nearly exhausted (< 100 TIDs remaining)\n")
		}
		tid := freeTIDs[0]
		freeTIDs = freeTIDs[1:]
		startClock := maxClockAtFree[tid] + 1
		tidPoolMu.unlock()
		return tid, startClock
	}

	// Slow path: Pool exhausted - trigger cleanup.
	tidPoolMu.unlock()

	// DISABLED: cleanupDeadGoroutines uses runtime.Stack() which can cause
	// "stopTheWorld: holding locks" errors when called during race detection.
	// For now, just fall through to graceful degradation.
	// TODO: Implement lock-free cleanup mechanism.
	// cleanupDeadGoroutines()

	// Retry allocation after cleanup.
	tidPoolMu.lock()
	defer tidPoolMu.unlock()

	if len(freeTIDs) > 0 {
		tid := freeTIDs[0]
		freeTIDs = freeTIDs[1:]
		startClock := maxClockAtFree[tid] + 1
		return tid, startClock
	}

	// Pool still exhausted after cleanup - graceful degradation.
	printstring("WARNING: race detector TID pool exhausted, reusing TID 0 (detection may be incomplete)\n")
	return 0, 1
}

// freeTID returns a TID to the free pool with clock recording for safe recycling.
//
// The currentClock is the maximum clock value this TID reached. When the TID
// is later recycled, the new goroutine will start its clock above this value,
// ensuring stale shadow entries are correctly identified as "concurrent".
//
// Performance: ~35ns (mutex lock + array write + slice append).
//
// Thread Safety: Safe for concurrent calls (protected by tidPoolMu).
func freeTID(tid uint16, currentClock uint32) {
	tidPoolMu.lock()
	defer tidPoolMu.unlock()

	// Record max clock for safe recycling (clock bumping).
	if currentClock > maxClockAtFree[tid] {
		maxClockAtFree[tid] = currentClock
	}

	// Push TID to FIFO queue for temporal separation.
	//nolint:makezero // Intentional append to initialized slice (TID pool)
	freeTIDs = append(freeTIDs, tid)
}

// maybeCleanup triggers periodic cleanup of dead goroutines.
//
// Cleanup is triggered every 1000 context allocations to amortize the cost.
// The cleanup runs in a background goroutine to avoid blocking allocations.
//
// Cleanup overhead: ~1ms per 1000 goroutines scanned.
// Amortized overhead: ~0.1% (1ms / 1000 allocations).
//
// Thread Safety: Safe for concurrent calls (uses atomic counter).
func maybeCleanup() {
	// Increment allocation counter.
	count := allocCounter.Add(1)

	// DISABLED: Background cleanup also uses runtime.Stack() which can cause issues.
	// TODO: Implement lock-free cleanup mechanism.
	// const cleanupInterval = 1000
	// if count%cleanupInterval == 0 {
	//     go cleanupDeadGoroutines()
	// }
	_ = count
}

// cleanupDeadGoroutines scans the contexts map and reclaims TIDs from dead goroutines.
//
// Algorithm:
//  1. Get list of all live goroutine IDs via runtimeStack()
//  2. Build a set of live GIDs for O(1) lookup
//  3. Scan contexts map for GIDs not in the live set
//  4. For each dead goroutine, free its TID and remove context
//
// Performance:
//   - runtimeStack(all=true): ~1ms for 1000 goroutines
//   - Set construction: ~10µs for 1000 goroutines
//   - contextsMap.Range: ~50µs for 1000 contexts
//   - Total: ~1ms for 1000 goroutines
//
// Thread Safety: Safe for concurrent calls. Uses sync.Map which handles
// concurrent reads/writes/deletes gracefully.
func cleanupDeadGoroutines() {
	// Step 1: Get list of all live goroutine IDs.
	// This is the expensive part (~1ms for 1000 goroutines).
	liveGIDs := getLiveGoroutineIDs()

	// Step 2: Build set for O(1) lookup.
	liveSet := make(map[int64]bool, len(liveGIDs))
	for _, gid := range liveGIDs {
		liveSet[gid] = true
	}

	// Step 3: Scan contexts and remove dead goroutines.
	contextsMap.Range(func(gid int64, ctx *goroutine.RaceContext) bool {
		// Check if goroutine is still alive.
		if !liveSet[gid] {
			// Get current clock before freeing (for TID recycling safety).
			var currentClock uint32
			if ctx.C != nil {
				currentClock = ctx.C.Get(ctx.TID)
			}

			// Goroutine is dead - reclaim its TID with clock.
			freeTID(ctx.TID, currentClock)

			// Remove from contexts map.
			contextsMap.Delete(gid)

			// Remove from TID → GID mapping.
			tidToGIDMap.Delete(ctx.TID)
		}

		// Continue iteration.
		return true
	})
}

// getLiveGoroutineIDs returns a list of all live goroutine IDs.
//
// This uses runtimeStack(all=true) to get a stack trace for ALL goroutines,
// then parses the output to extract GIDs.
//
// Performance: ~1ms for 1000 goroutines.
// This is the main cost of cleanup, which is why we amortize it over 1000 allocations.
//
// Thread Safety: Safe for concurrent calls (runtimeStack is thread-safe).
//
// Returns:
//   - []int64: List of all live goroutine IDs
func getLiveGoroutineIDs() []int64 {
	// Allocate buffer for stack traces.
	// 1MB should be enough for ~1000 goroutines with typical stack depths.
	// If buffer is too small, runtimeStack returns truncated output,
	// but we'll still get GIDs for all goroutines in the trace.
	buf := make([]byte, 1024*1024) // 1MB

	// Get stack traces for ALL goroutines.
	// all=true is critical - we need every goroutine's stack.
	n := runtimeStack(buf, true)

	// Parse stack dump to extract all GIDs.
	return parseAllGIDs(buf[:n])
}

// parseAllGIDs parses runtimeStack(all=true) output to extract all goroutine IDs.
//
// Input format (example):
//
//	goroutine 1 [running]:
//	main.main()
//	    /path/to/main.go:10 +0x20
//
//	goroutine 5 [chan receive]:
//	main.worker()
//	    /path/to/main.go:20 +0x40
//
// We extract: [1, 5, ...]
//
// Algorithm:
//  1. Split buffer into lines
//  2. Find lines starting with "goroutine "
//  3. Parse the GID from each line
//
// Performance: ~100µs for 1000 goroutines.
//
// Parameters:
//   - buf: Stack trace buffer from runtimeStack(all=true)
//
// Returns:
//   - []int64: List of goroutine IDs
func parseAllGIDs(buf []byte) []int64 {
	var gids []int64

	// Split into lines.
	// runtimeStack output has one "goroutine N [state]:" line per goroutine.
	i := 0
	for i < len(buf) {
		// Find next newline.
		end := i
		for end < len(buf) && buf[end] != '\n' {
			end++
		}

		// Extract line.
		line := buf[i:end]

		// Check if this is a "goroutine N" line.
		if len(line) >= 10 && string(line[:10]) == "goroutine " {
			// Parse GID from this line.
			gid := parseGID(line)
			if gid != 0 {
				gids = append(gids, gid)
			}
		}

		// Move to next line.
		i = end + 1
	}

	return gids
}

// NOTE: getGoroutineID() and parseGID() are defined in goid_generic.go
// getGoroutineIDFast() uses runtime bridge (getg().goid) via goid_runtime.go

// getcallerpc returns the program counter (PC) of the caller.
//
// This extracts the PC of the memory access that triggered raceread/racewrite.
// The PC can be used to get source location information for race reports.
//
// Call Stack:
//
//	0: getcallerpc()
//	1: raceread() or racewrite()
//	2: instrumented code (the actual memory access)
//
// We want the PC at level 2, so we call runtimeCaller(2).
//
// Performance: ~50ns (runtimeCaller overhead).
//
// MVP: PC is extracted but not used in reporting yet.
// Phase 7: PC will be passed to detector for stack trace generation.
//
// Returns:
//   - uintptr: Program counter of the memory access
//
//nolint:unparam // Return value will be used in Phase 7 for stack traces.
func getcallerpc() uintptr {
	// runtimeCaller(2) skips:
	//   - getcallerpc (this function) - skip 0
	//   - raceread/racewrite - skip 1
	//   - returns: instrumented code - skip 2
	_, _, pc, ok := runtimeCaller(2)
	if !ok {
		return 0
	}
	return uintptr(pc)
}

// raceClearShadow clears shadow memory for the given address range.
// Called by runtime's racemalloc/racefree to prevent false positives
// from stale shadow state when addresses are reused by the allocator.
//
//go:linkname raceClearShadow
//go:nosplit
func raceClearShadow(addr, size uintptr) {
	if enabled.Load() == 0 {
		return
	}
	det.ClearShadowRange(addr, size)
}

// Enable turns on race detection.
//
// This is currently a no-op for MVP (always enabled), but provides the
// API hook for Phase 7 when we implement runtime enable/disable.
//
// Thread Safety: Safe for concurrent calls.
func Enable() {
	enabled.Store(1)
}

// Disable turns off race detection.
//
// After calling Disable(), raceread/racewrite become no-ops (fast return).
// This can be used to disable race detection for performance-critical sections.
//
// Thread Safety: Safe for concurrent calls.
//
// Example:
//
//	race.Disable()
//	// ... performance-critical code with known-safe access patterns ...
//	race.Enable()
func Disable() {
	enabled.Store(0)
}

// RacesDetected returns the total number of races detected.
//
// This is exported for testing and statistics purposes.
//
// Thread Safety: Safe for concurrent calls.
//
// Returns:
//   - int: Total number of races detected since initialization
func RacesDetected() int {
	return det.RacesDetected()
}

// RaceRead is an exported wrapper for raceread, for demonstration purposes.
//
// In production code, you should compile with -race flag, which automatically
// instruments all memory accesses. This function is provided for examples
// and testing purposes only.
//
// Parameters:
//   - addr: Memory address being read from
func RaceRead(addr uintptr) {
	raceread(addr, 0) // 0 = use fallback PC capture in detector
}

// RaceWrite is an exported wrapper for racewrite, for demonstration purposes.
//
// In production code, you should compile with -race flag, which automatically
// instruments all memory accesses. This function is provided for examples
// and testing purposes only.
//
// Parameters:
//   - addr: Memory address being written to
func RaceWrite(addr uintptr) {
	racewrite(addr, 0) // 0 = use fallback PC capture in detector
}

// RaceAcquire is an exported wrapper for raceacquire, for demonstration purposes (Phase 4 Task 4.1).
//
// In production code, you should compile with -race flag, which automatically
// instruments mutex operations. This function is provided for examples
// and testing purposes only.
//
// Parameters:
//   - addr: Address of the mutex being locked
func RaceAcquire(addr uintptr) {
	raceacquire(addr)
}

// RaceRelease is an exported wrapper for racerelease, for demonstration purposes (Phase 4 Task 4.1).
//
// In production code, you should compile with -race flag, which automatically
// instruments mutex operations. This function is provided for examples
// and testing purposes only.
//
// Parameters:
//   - addr: Address of the mutex being unlocked
func RaceRelease(addr uintptr) {
	racerelease(addr)
}

// RaceReleaseMerge is an exported wrapper for racereleasemerge, for demonstration purposes (Phase 4 Task 4.1).
//
// In production code, you should compile with -race flag, which automatically
// instruments RWMutex operations. This function is provided for examples
// and testing purposes only.
//
// Parameters:
//   - addr: Address of the RWMutex being unlocked
func RaceReleaseMerge(addr uintptr) {
	racereleasemerge(addr)
}

// === Exported Channel API Functions (Phase 4 Task 4.2) ===

// RaceChannelSendBefore is an exported wrapper for racechansendbefore, for demonstration purposes.
//
// In production code, you should compile with -race flag, which automatically
// instruments channel operations. This function is provided for examples
// and testing purposes only.
//
// Parameters:
//   - ch: Address of the channel being sent to
func RaceChannelSendBefore(ch uintptr) {
	racechansendbefore(ch)
}

// RaceChannelSendAfter is an exported wrapper for racechansendafter, for demonstration purposes.
//
// In production code, you should compile with -race flag, which automatically
// instruments channel operations. This function is provided for examples
// and testing purposes only.
//
// Parameters:
//   - ch: Address of the channel being sent to
func RaceChannelSendAfter(ch uintptr) {
	racechansendafter(ch)
}

// RaceChannelRecvBefore is an exported wrapper for racechanrecvbefore, for demonstration purposes.
//
// In production code, you should compile with -race flag, which automatically
// instruments channel operations. This function is provided for examples
// and testing purposes only.
//
// Parameters:
//   - ch: Address of the channel being received from
func RaceChannelRecvBefore(ch uintptr) {
	racechanrecvbefore(ch)
}

// RaceChannelRecvAfter is an exported wrapper for racechanrecvafter, for demonstration purposes.
//
// In production code, you should compile with -race flag, which automatically
// instruments channel operations. This function is provided for examples
// and testing purposes only.
//
// Parameters:
//   - ch: Address of the channel being received from
func RaceChannelRecvAfter(ch uintptr) {
	racechanrecvafter(ch)
}

// RaceChannelClose is an exported wrapper for racechanclose, for demonstration purposes.
//
// In production code, you should compile with -race flag, which automatically
// instruments channel operations. This function is provided for examples
// and testing purposes only.
//
// Parameters:
//   - ch: Address of the channel being closed
func RaceChannelClose(ch uintptr) {
	racechanclose(ch)
}

// Reset resets the detector state for testing.
//
// This clears all shadow memory, resets the race counter, and clears
// the goroutine context cache. It's primarily used in test setup/teardown.
//
// Thread Safety: NOT safe for concurrent access.
// The caller must ensure no other goroutines are using the detector.
func Reset() {
	det.Reset()
	// Clear goroutine contexts.
	contextsMap.Reset()
	// Clear TID → GID mapping.
	tidToGIDMap.Reset()
	// Reset TID counter.
	nextTID.Store(0)
	// Reset allocation counter.
	allocCounter.Store(0)
	// Clear spawn context tracking.
	spawnContextsMu.lock()
	spawnContextsSlice = nil
	spawnContextsMu.unlock()
	nextSpawnID.Store(0)
	// Reset maxClockAtFree for clean state.
	maxClockAtFree = [vectorclock.MaxThreads]uint32{}
	// Reinitialize TID pool for tests.
	// Tests call Reset() but expect to be able to allocate TIDs afterwards.
	initTIDPool()
}

// Init initializes the race detector for use.
//
// This function sets up the race detector runtime and makes it ready to
// track memory accesses. It should be called at the start of your program,
// typically in main() or init().
//
// Init() performs the following initialization steps:
//  1. Enables race detection
//  2. Resets the TID counter to 0
//  3. Creates a fresh detector instance (with optional sampling from env)
//  4. Initializes the TID reuse pool (Phase 2 Task 2.2)
//  5. Allocates a RaceContext for the main goroutine with TID=0
//
// Environment Variables (v0.3.0):
//
//	RACEDETECTOR_SAMPLE_RATE=N  - Enable sampling with rate N (1=disabled, 10=1/10, 100=1/100)
//	                             This trades detection rate for performance (~50-90% overhead reduction).
//	                             Example: RACEDETECTOR_SAMPLE_RATE=10 ./myprogram
//
// Main Goroutine Convention:
// By convention, the main goroutine (the one calling Init) always receives
// TID=0. This is consistent with Go's runtime.raceinit behavior and helps
// identify the main goroutine in race reports.
//
// Init() is idempotent - calling it multiple times is safe and will
// re-initialize the detector with fresh state.
//
// Thread Safety: NOT safe for concurrent calls.
// Init() should only be called during program startup before any
// goroutines are spawned.
//
// Example:
//
//	func main() {
//	    race.Init()
//	    defer race.Fini()
//
//	    // Your program code here...
//	}
//
// Example with sampling:
//
//	$ RACEDETECTOR_SAMPLE_RATE=10 ./myprogram  # Check 1 in 10 accesses
func Init() {
	// Enable race detection.
	enabled.Store(1)

	// Reset TID counter to 0.
	nextTID.Store(0)

	// Reset allocation counter for cleanup trigger.
	allocCounter.Store(0)

	// Create a fresh detector instance.
	// Note: Sampling configuration is disabled in runtime context (no os.Getenv).
	det = detector.NewDetector()
	shadow = det.GetShadow().(*shadowmem.PageTableShadow)

	// Clear any existing goroutine contexts.
	contextsMap.Reset()

	// Clear TID → GID mapping.
	tidToGIDMap.Reset()

	// Clear spawn context tracking (GoStart).
	spawnContextsMu.lock()
	spawnContextsSlice = nil
	spawnContextsMu.unlock()
	nextSpawnID.Store(0)

	// Initialize TID reuse pool (Phase 2 Task 2.2).
	// This sets up the free TID stack with all 256 TIDs available.
	initTIDPool()

	// Reset maxClockAtFree for clean state.
	maxClockAtFree = [vectorclock.MaxThreads]uint32{}

	// Allocate RaceContext for the main goroutine.
	// CRITICAL: Main goroutine gets TID=1, NOT TID=0.
	// TID=0 is reserved as sentinel value meaning "no exclusive writer" in SmartTrack.
	// Using TID=0 for main would cause SmartTrack to incorrectly treat main's writes
	// as "no writer present", missing races when child goroutines write.
	gid := getGoroutineID()
	mainCtx := goroutine.AllocWithStartClock(1, 1) // TID=1, startClock=1
	contextsMap.Store(gid, mainCtx)

	// Track main goroutine in TID → GID mapping.
	tidToGIDMap.Store(1, gid)

	// Remove TID 1 from the free pool (already allocated to main goroutine).
	// TID 0: Already excluded by initTIDPool() (reserved as sentinel)
	// TID 1: Already allocated to main goroutine above
	tidPoolMu.lock()
	// Pool is [1, 2, 3, ..., MaxThreads-1]. Remove first element (TID 1).
	if len(freeTIDs) >= 1 && freeTIDs[0] == 1 {
		freeTIDs = freeTIDs[1:] // Now: [2, 3, 4, ..., MaxThreads-1]
	}
	tidPoolMu.unlock()

	// Set nextTID to 2 so that the next spawned goroutine gets TID >= 2.
	// TID 0: Reserved as sentinel (never allocate)
	// TID 1: Main goroutine (already allocated above)
	// TID 2+: Child goroutines (allocated dynamically)
	nextTID.Store(2)
}

// Fini finalizes the race detector and prints a summary report.
//
// This function should be called at the end of your program, typically
// using defer in main() right after Init(). It performs cleanup and
// prints a summary of race detection results to stderr.
//
// The summary report includes:
//   - Total number of races detected (if any)
//   - Success message if no races were found
//
// After Fini() is called, the detector is disabled and raceread/racewrite
// become no-ops. If you need to re-enable detection, call Init() again.
//
// Thread Safety: Safe to call multiple times, but only the first call
// will print the summary. Subsequent calls are no-ops.
//
// Example:
//
//	func main() {
//	    race.Init()
//	    defer race.Fini()
//
//	    // Your program code here...
//	}
//	// On exit, Fini() prints:
//	// ==================
//	// Race Detector Report
//	// ==================
//	// ✓ No data races detected.
//	// ==================
//
//go:linkname Fini
func Fini() {
	// Disable race detection first.
	// This ensures no more race checks happen while we're printing the report.
	enabled.Store(0)

	// Get the total number of races detected.
	racesDetected := det.RacesDetected()

	// Print summary report to stderr using runtime print functions.
	printstring("\n")
	printstring("==================\n")
	printstring("Race Detector Report\n")
	printstring("==================\n")

	if racesDetected == 0 {
		// Success case - no races found.
		printstring("No data races detected.\n")
	} else {
		// Warning case - races were detected.
		printstring("WARNING: ")
		printstring(itoaAPI(racesDetected))
		printstring(" data race(s) detected!\n")
		printstring("\nSee above for details.\n")
	}

	printstring("==================\n\n")
}
