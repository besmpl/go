// Package api provides the public runtime API for the Pure-Go Race Detector.
//
// This package implements the entry points called by Go compiler instrumentation
// when code is built with -race flag. These functions are invoked on every memory
// access in instrumented code, making them CRITICAL HOT PATHS.
//
// The API follows the same interface contract as Go's runtime.race* functions,
// ensuring compatibility with existing compiler instrumentation.
//
// Runtime Integration:
//   - Goroutine ID via getg().goid runtime bridge (~0ns)
//   - PC capture via sys.GetCallerPC() compiler intrinsic (~0ns)
//   - Monotonic logical IDs with hybrid dense/sparse vector clocks
//   - GoEnd() cleanup for context lifecycle management
package api

import (
	"internal/runtime/atomic"
	"unsafe"

	"runtime/race/kolkov/detector"
	"runtime/race/kolkov/epoch"
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

//go:linkname runtimeThrow runtime.throw
func runtimeThrow(s string)

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
	// enabled controls whether race detection is active. Enable and Disable
	// update it directly; runtime initialization enables detection by default.
	enabled atomic.Uint32 // 0=disabled, 1=enabled

	// contextsMap retains every live RaceContext by goroutine ID. g.racectx is
	// stored as uintptr and is therefore not a GC root; this map must never
	// silently drop or overwrite a live entry.
	// Key: int64 (goroutine ID), Value: *goroutine.RaceContext
	contextsMap contextsMapType

	// lifecycleMu linearizes logical-ID allocation, context publication/end,
	// detached-context publication/end, and finalizer snapshots. pendingTIDs
	// covers the small allocation-to-publication window so a finalizer never
	// mistakes an allocated identity for an ended one. Memory is proportional
	// to concurrent unpublished allocations, not process-lifetime history.
	lifecycleMu spinlockAPI
	pendingTIDs map[uint32]struct{}

	// lifecycleState separates global recording policy from the per-goroutine
	// suppression implemented by runtime.RaceDisable. DirtyDisabled cannot be
	// published Enabled again without a quiescent drain through Reset or Init.
	lifecycleState atomic.Uint32

	// detachedContexts roots temporary logical contexts used when one runtime
	// goroutine executes independent cleanup callbacks. Those contexts live
	// outside contextsMap because the goroutine's ordinary context must remain
	// rooted while temporarily replaced in g.racectx.
	detachedContexts map[uintptr]*goroutine.RaceContext

	// nextTID allocates process-lifetime monotonic logical IDs. Reusing a
	// vector-clock coordinate for an unrelated goroutine lifetime is unsound.
	nextTID tidHighWater

	// det is the global detector instance.
	// All race detection flows through this single instance.
	det *detector.Detector

	// shadow is the cached concrete shadow memory for the same-epoch fast path.
	// Stored as concrete *PageTableShadow to avoid interface dispatch (~5-10ns)
	// on every same-epoch check. Set once during initialization.
	shadow *shadowmem.PageTableShadow

	// apiInitCalled distinguishes initialization in progress from a detector
	// that is ready for allocator and goroutine lifecycle callbacks.
	// 0 = not started, 1 = initializing, 2 = ready.
	apiInitCalled atomic.Uint32

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

type detectorLifecycle uint32

const (
	lifecycleVirgin detectorLifecycle = iota
	lifecycleEnabled
	lifecycleDirtyDisabled
	lifecycleResetting
)

const exhaustedTIDHighWater = uint64(1) << epoch.TIDBits

// tidHighWater keeps the process-lifetime allocator in an atomic uint64 while
// retaining the uint32 diagnostic seam used by older package tests. Production
// allocation and exhaustion checks always use the full-width methods.
type tidHighWater struct {
	value atomic.Uint64
}

func (h *tidHighWater) Load() uint32 { return uint32(h.value.Load()) }
func (h *tidHighWater) CompareAndSwap(old, new uint32) bool {
	return h.value.CompareAndSwap(uint64(old), uint64(new))
}
func (h *tidHighWater) load64() uint64       { return h.value.Load() }
func (h *tidHighWater) store64(value uint64) { h.value.Store(value) }
func (h *tidHighWater) reserve() (uint32, bool) {
	for {
		current := h.value.Load()
		if current >= exhaustedTIDHighWater-1 {
			return 0, false
		}
		next := current + 1
		if h.value.CompareAndSwap(current, next) {
			return uint32(next), true
		}
	}
}

// Kept as an alias for package tests and diagnostics that inspect detached
// roots. It is the same lifecycle registry lock, not an independent lock.
var detachedContextsMu = &lifecycleMu

const contextMapShardCount = 64

// contextsMapType is a sharded map from int64 (GID) to RaceContext. The
// detector's scalar hot path uses g.racectx directly, so lifecycle operations
// favor collision safety and GC rooting over a fixed-capacity probe table.
type contextsMapType struct {
	shards [contextMapShardCount]contextMapShard
}

type contextMapShard struct {
	mu      spinlockAPI
	entries map[int64]*goroutine.RaceContext
}

type contextCell struct {
	gid int64
	ctx *goroutine.RaceContext
}

func (m *contextsMapType) shard(gid int64) *contextMapShard {
	hash := uint64(gid) * 0x9E3779B97F4A7C15
	return &m.shards[hash>>(64-6)]
}

func (m *contextsMapType) Load(gid int64) (*goroutine.RaceContext, bool) {
	shard := m.shard(gid)
	shard.mu.lock()
	ctx, ok := shard.entries[gid]
	shard.mu.unlock()
	return ctx, ok
}

func (m *contextsMapType) Store(gid int64, ctx *goroutine.RaceContext) {
	lifecycleMu.lock()
	m.storeLocked(gid, ctx)
	if ctx != nil && ctx.TID != 0 {
		delete(pendingTIDs, ctx.TID)
	}
	lifecycleMu.unlock()
}

func (m *contextsMapType) storeLocked(gid int64, ctx *goroutine.RaceContext) {
	shard := m.shard(gid)
	shard.mu.lock()
	if shard.entries == nil {
		shard.entries = make(map[int64]*goroutine.RaceContext)
	}
	shard.entries[gid] = ctx
	shard.mu.unlock()
}

func (m *contextsMapType) LoadAndDelete(gid int64) (*goroutine.RaceContext, bool) {
	lifecycleMu.lock()
	ctx, ok := m.loadAndDeleteLocked(gid)
	lifecycleMu.unlock()
	return ctx, ok
}

func (m *contextsMapType) loadAndDeleteLocked(gid int64) (*goroutine.RaceContext, bool) {
	shard := m.shard(gid)
	shard.mu.lock()
	ctx, ok := shard.entries[gid]
	if ok {
		delete(shard.entries, gid)
	}
	shard.mu.unlock()
	return ctx, ok
}

func (m *contextsMapType) Delete(gid int64) {
	m.LoadAndDelete(gid)
}

func (m *contextsMapType) Range(f func(gid int64, ctx *goroutine.RaceContext) bool) {
	// Callbacks may delete entries, so do not invoke f while holding a shard.
	lifecycleMu.lock()
	var snapshot []contextCell
	m.rangeLocked(func(gid int64, ctx *goroutine.RaceContext) bool {
		snapshot = append(snapshot, contextCell{gid: gid, ctx: ctx})
		return true
	})
	lifecycleMu.unlock()
	for _, cell := range snapshot {
		if !f(cell.gid, cell.ctx) {
			return
		}
	}
}

func (m *contextsMapType) rangeLocked(f func(gid int64, ctx *goroutine.RaceContext) bool) {
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.lock()
		for gid, ctx := range shard.entries {
			if !f(gid, ctx) {
				shard.mu.unlock()
				return
			}
		}
		shard.mu.unlock()
	}
}

func (m *contextsMapType) Reset() {
	lifecycleMu.lock()
	m.resetLocked()
	lifecycleMu.unlock()
}

func (m *contextsMapType) resetLocked() {
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.lock()
		shard.entries = nil
		shard.mu.unlock()
	}
}

// spawnInfo contains information to pass from parent to child goroutine.
// This enables happens-before tracking across goroutine creation.
type spawnInfo struct {
	id          uint64                   // Stable token returned by racegostart
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
//   - The enabled flag
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

	// Static zero is the only initialization of the process-lifetime high-water.
	// No later lifecycle transition may rewind it.

	// Publish fully initialized lifecycle state before enabling user events.
	apiInitCalled.Store(2)
	lifecycleState.Store(uint32(lifecycleEnabled))
	enabled.Store(1) // publish readiness last
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

	parentCtx := getCurrentContext()
	parentGID := getGoroutineID()
	return enqueueSpawn(pc, parentGID, parentCtx)
}

func enqueueSpawn(pc uintptr, parentGID int64, parentCtx *goroutine.RaceContext) uintptr {
	var spawnClock *vectorclock.VectorClock
	if parentCtx != nil && parentCtx.C != nil {
		next := parentCtx.PreflightClockAdvance()
		// Fork snapshots must not turn the live parent's dense tail into a
		// copy-on-write source after its clock-advance preflight. The parent
		// commits immediately below, so publish a fully detached child image.
		spawnClock = parentCtx.C.CloneDetached()
		parentCtx.CommitClockAdvance(next)
	}
	info := &spawnInfo{
		id:          nextSpawnID.Add(1),
		parentGID:   parentGID,
		parentClock: spawnClock,
		pc:          pc,
		createdAtNs: nanotime(),
	}

	spawnContextsMu.lock()
	spawnContextsSlice = append(spawnContextsSlice, info)
	spawnContextsMu.unlock()

	return uintptr(info.id)
}

// raceGoStartFromRuntime is called by the runtime's racegostart from systemstack.
// Unlike racegostart, the caller is on g0 so getGoroutineID() would return
// the wrong goroutine. The runtime passes parentGoid explicitly.
//
//go:linkname raceGoStartFromRuntime
//go:nosplit
func raceGoStartFromRuntime(pc uintptr, parentGoid int64) uintptr {
	if enabled.Load() == 0 {
		return 0
	}

	// Look up parent's context using explicit goid.
	parentCtx, _ := contextsMap.Load(parentGoid)
	return enqueueSpawn(pc, parentGoid, parentCtx)
}

// raceGoStartFromContext starts a child from an explicit temporary context.
// Runtime callbacks use this when they execute on g0 with g0.racectx set.
//
//go:linkname raceGoStartFromContext
//go:nocheckptr
func raceGoStartFromContext(pc, parentCtx uintptr) uintptr {
	if enabled.Load() == 0 || parentCtx <= 1 {
		return 0
	}
	return enqueueSpawn(pc, 0, (*goroutine.RaceContext)(unsafe.Pointer(parentCtx)))
}

// raceGoEndFromRuntime is called by the runtime's racegoend.
// Accepts explicit goid since the caller might be on systemstack.
//
//go:linkname raceGoEndFromRuntime
//go:nosplit
func raceGoEndFromRuntime(goid int64) {
	// Context retirement is lifecycle maintenance rather than a detector
	// event. It must run even while access and synchronization events are
	// disabled, otherwise g.racectx is cleared while its registry root leaks.
	if ctx, ok := contextsMap.Load(goid); ok {
		// Publish the finished status before removing the live context root, so
		// report snapshots cannot observe a retired goroutine as still running.
		det.RetireGoroutineCreation(ctx.TID)
		if removed, removedOK := contextsMap.LoadAndDelete(goid); removedOK {
			detector.DeactivateAtomicLoadCache(removed)
			if removed.C != nil {
				removed.C.Release()
				removed.C = nil
			}
		}
	}
}

// raceContextStartFromRuntime creates an independent temporary logical
// context inheriting spawnctx's happens-before history. The parent advances
// past the fork point, just like an ordinary goroutine start. The returned
// uintptr is cached in g.racectx, while detachedContexts supplies the GC root.
//
//go:linkname raceContextStartFromRuntime
//go:nocheckptr
func raceContextStartFromRuntime(creationPC uintptr, spawnctx uintptr) uintptr {
	if enabled.Load() == 0 {
		return 0
	}

	var parent *goroutine.RaceContext
	var parentNext uint64
	if spawnctx > 1 {
		parent = (*goroutine.RaceContext)(unsafe.Pointer(spawnctx))
		if parent.C != nil {
			parentNext = parent.PreflightClockAdvance()
		}
	}
	tid, startClock := allocTID()
	var ctx *goroutine.RaceContext
	if parent != nil {
		if parentNext != 0 {
			// Copy before advancing: the child observes everything before the
			// fork, while later parent operations remain unordered with it.
			ctx = goroutine.AllocWithParentClock(tid, parent.C, startClock)
			parent.CommitClockAdvance(parentNext)
		}
	}
	if ctx == nil {
		ctx = goroutine.AllocWithStartClock(tid, startClock)
	}
	det.RegisterGoroutineCreation(tid, creationPC)

	ptr := uintptr(unsafe.Pointer(ctx))
	detachedContextsMu.lock()
	if detachedContexts == nil {
		detachedContexts = make(map[uintptr]*goroutine.RaceContext)
	}
	detachedContexts[ptr] = ctx
	delete(pendingTIDs, tid)
	detachedContextsMu.unlock()
	return ptr
}

// raceContextEndFromRuntime ends a temporary context and relinquishes its
// vector clock. It intentionally does not join back into the parent: cleanup
// callbacks are independent logical goroutines, matching TSAN's go-end model.
//
//go:linkname raceContextEndFromRuntime
//go:nocheckptr
func raceContextEndFromRuntime(racectx uintptr) {
	if racectx <= 1 {
		return
	}
	detachedContextsMu.lock()
	ctx := detachedContexts[racectx]
	if ctx != nil {
		det.RetireGoroutineCreation(ctx.TID)
		delete(detachedContexts, racectx)
	}
	detachedContextsMu.unlock()
	if ctx != nil {
		detector.DeactivateAtomicLoadCache(ctx)
		if ctx.C != nil {
			ctx.C.Release()
			ctx.C = nil
		}
	}
}

type claimedSpawnContext struct {
	parentClock *vectorclock.VectorClock
	creationPC  uintptr
	found       bool
}

// claimSpawnContextByID transfers one spawn clock and its creation PC to the
// eager child-context creator. The entry is removed while holding the slice
// lock, so a returned clock has exactly one owner and no stale pointer can
// remain after that owner releases it to the VectorClock pool.
func claimSpawnContextByID(spawnID uintptr, childGoid int64) claimedSpawnContext {
	if spawnID == 0 {
		return claimedSpawnContext{}
	}

	spawnContextsMu.lock()
	defer spawnContextsMu.unlock()
	return claimSpawnContextByIDLocked(&spawnContextsSlice, spawnID, childGoid)
}

func claimSpawnContextByIDLocked(contexts *[]*spawnInfo, spawnID uintptr, childGoid int64) claimedSpawnContext {
	for i, info := range *contexts {
		if info.id != uint64(spawnID) || !info.consumed.CompareAndSwap(0, 1) {
			continue
		}

		info.childGoid = childGoid
		claimed := claimedSpawnContext{
			parentClock: info.parentClock,
			creationPC:  info.pc,
			found:       true,
		}
		info.parentClock = nil

		copy((*contexts)[i:], (*contexts)[i+1:])
		last := len(*contexts) - 1
		(*contexts)[last] = nil
		*contexts = (*contexts)[:last]
		return claimed
	}
	return claimedSpawnContext{}
}

// These clock-only wrappers preserve the package's diagnostic/test seam while
// production child initialization claims the complete lifecycle record.
func consumeSpawnContextByID(spawnID uintptr, childGoid int64) *vectorclock.VectorClock {
	return claimSpawnContextByID(spawnID, childGoid).parentClock
}

func consumeSpawnContextByIDLocked(contexts *[]*spawnInfo, spawnID uintptr, childGoid int64) *vectorclock.VectorClock {
	return claimSpawnContextByIDLocked(contexts, spawnID, childGoid).parentClock
}

// raceGoSetChildIDWithCtx associates the spawn context identified by spawnID
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
func raceGoSetChildIDWithCtx(childGoid int64, spawnID uintptr) uintptr {
	if apiInitCalled.Load() != 2 || detectorLifecycle(lifecycleState.Load()) == lifecycleResetting {
		return 0
	}

	// Reserve the identity before consuming the spawn record. Exhaustion must
	// fail without removing or releasing a pending parent clock.
	tid, startClock := allocTID()

	// Step 1: Consume the exact context returned by racegostart. Creation can
	// proceed concurrently on multiple Ps, so "most recent" is not a stable
	// parent-child association between the two runtime callbacks. Context
	// binding is lifecycle maintenance, so it remains active while access and
	// synchronization events are disabled.
	claimed := claimSpawnContextByID(spawnID, childGoid)
	parentClock := claimed.parentClock

	// Step 2: Eagerly create the child's RaceContext.
	var ctx *goroutine.RaceContext
	if parentClock != nil {
		ctx = goroutine.AllocWithParentClock(tid, parentClock, startClock)
		// Release the spawn clock clone back to pool (data already copied).
		parentClock.Release()
	} else {
		ctx = goroutine.AllocWithStartClock(tid, startClock)
	}
	det.RegisterGoroutineCreation(tid, claimed.creationPC)

	// Step 3: Store in contextsMap for GC safety (dual reference).
	contextsMap.Store(childGoid, ctx)

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

	return uintptr(unsafe.Pointer(ctx))
}

// racegoend is called when a goroutine terminates.
//
// This function removes the GC root and releases the context's vector clock.
// Logical TIDs are deliberately never recycled.
//
// Thread Safety: Safe for concurrent calls.
//
//go:nosplit
func racegoend() {
	gid := getGoroutineID()

	// Keep lifecycle cleanup active while event recording is disabled, matching
	// the explicit runtime callback above.
	if ctx, ok := contextsMap.Load(gid); ok {
		det.RetireGoroutineCreation(ctx.TID)
		if removed, removedOK := contextsMap.LoadAndDelete(gid); removedOK {
			detector.DeactivateAtomicLoadCache(removed)
			if removed.C != nil {
				removed.C.Release()
				removed.C = nil
			}
		}
	}
}

// contextEpochSnapshot atomically reads the owning component published in a
// RaceContext. Contexts mutate their own epochs without taking a global lock;
// finalizer handoff needs only this component, not a concurrent traversal of
// their mutable vector-clock maps.
func contextEpochSnapshot(ctx *goroutine.RaceContext) epoch.Epoch {
	if ctx == nil {
		return 0
	}
	return epoch.Epoch(atomic.Load64((*uint64)(unsafe.Pointer(&ctx.Epoch))))
}

// raceFinalizerGoFromRuntime performs the one-way handoff used by TSAN's
// finalizer-goroutine callback. The current callback context observes the last
// epoch of every live logical context. Never-reused IDs that ended before the
// snapshot are represented compactly as immutable +infinity intervals; no
// clock is joined back, so later unsynchronized accesses in other goroutines
// can still race with it.
//
//go:linkname raceFinalizerGoFromRuntime
//go:nocheckptr
func raceFinalizerGoFromRuntime(racectx uintptr) {
	if racectx <= 1 {
		return
	}
	target := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	if target.C == nil {
		return
	}
	targetNext := target.PreflightClockAdvance()

	lifecycleMu.lock()
	highwater := nextTID.load64()
	type liveEpochSnapshot struct {
		ctx *goroutine.RaceContext
		e   epoch.Epoch
	}
	liveEpochs := make([]liveEpochSnapshot, 0, len(pendingTIDs)+1)
	snapshotEpoch := func(ctx *goroutine.RaceContext) {
		e := contextEpochSnapshot(ctx)
		if e == 0 {
			return
		}
		tid, _ := e.Decode()
		if tid == 0 || uint64(tid) > highwater {
			return
		}
		liveEpochs = append(liveEpochs, liveEpochSnapshot{ctx: ctx, e: e})
	}
	contextsMap.rangeLocked(func(_ int64, ctx *goroutine.RaceContext) bool {
		snapshotEpoch(ctx)
		return true
	})

	// Temporary cleanup contexts are rooted separately from contextsMap.
	for _, ctx := range detachedContexts {
		snapshotEpoch(ctx)
	}
	liveTIDs := make([]uint32, 0, len(liveEpochs)+len(pendingTIDs))
	for tid := range pendingTIDs {
		if tid != 0 && uint64(tid) <= highwater {
			liveTIDs = append(liveTIDs, tid)
		}
	}
	foreignImport := false
	for _, snapshot := range liveEpochs {
		tid, clock := snapshot.e.Decode()
		liveTIDs = append(liveTIDs, tid)
		if uint32(clock) > target.C.Get(tid) && tid != target.TID {
			foreignImport = true
		}
	}
	sortUint32s(liveTIDs)
	retired := retiredComplement(uint32(highwater), liveTIDs)
	if len(retired) != 0 {
		// Retirement is a foreign +infinity import even when every finite live
		// epoch was already dominated by target.
		foreignImport = true
	}
	if foreignImport && target.ForeignGeneration == ^uint64(0) {
		runtimeThrow("race detector foreign-clock generation overflow")
	}
	// Joining these epochs into the finalizer orders its next write after source
	// reads already represented by their caches. Invalidate each source while the
	// lifecycle lock still pins its context; no source pointer escapes the
	// snapshot critical section.
	for i := range liveEpochs {
		liveEpochs[i].ctx.InvalidateReadCacheAt(liveEpochs[i].e)
		liveEpochs[i].ctx = nil
	}
	lifecycleMu.unlock()

	// The copied epochs and retired ranges define the snapshot's linearization
	// point. Vector-clock mutation occurs after all fallible preflight checks and
	// without retaining pointers that an end callback may release.
	for _, snapshot := range liveEpochs {
		tid, clock := snapshot.e.Decode()
		if uint32(clock) > target.C.Get(tid) {
			target.C.Set(tid, uint32(clock))
		}
	}
	target.C.RetireRanges(retired)
	if foreignImport {
		target.NoteForeignImport()
	}

	// The snapshot import may reshape the target's sparse overlay. Re-reserve
	// the allocation-free own-coordinate successor after the final mutation.
	target.PreflightClockAdvance()
	target.CommitClockAdvance(targetNext)
}

// sortUint32s is an in-place heap sort. This package is part of runtime and
// cannot depend on sort; deterministic O(n log n) ordering is sufficient for
// the uncommon finalizer snapshot path.
func sortUint32s(values []uint32) {
	siftDown := func(root, end int) {
		for root*2+1 <= end {
			child := root*2 + 1
			if child+1 <= end && values[child] < values[child+1] {
				child++
			}
			if values[root] >= values[child] {
				return
			}
			values[root], values[child] = values[child], values[root]
			root = child
		}
	}
	for start := len(values)/2 - 1; start >= 0; start-- {
		siftDown(start, len(values)-1)
	}
	for end := len(values) - 1; end > 0; end-- {
		values[0], values[end] = values[end], values[0]
		siftDown(0, end-1)
	}
}

func retiredComplement(highwater uint32, sortedLive []uint32) []vectorclock.RetiredRange {
	if highwater == 0 {
		return nil
	}
	ranges := make([]vectorclock.RetiredRange, 0, len(sortedLive)+1)
	cursor := uint64(1)
	limit := uint64(highwater)
	for _, live := range sortedLive {
		tid := uint64(live)
		if tid < cursor || tid > limit {
			continue
		}
		if cursor < tid {
			ranges = append(ranges, vectorclock.RetiredRange{First: uint32(cursor), Last: uint32(tid - 1)})
		}
		cursor = tid + 1
	}
	if cursor <= limit {
		ranges = append(ranges, vectorclock.RetiredRange{First: uint32(cursor), Last: highwater})
	}
	return ranges
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

// findAndClaimSpawnContext attempts to find and consume a spawn context
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
// Returns the complete claimed spawn lifecycle record, if any.
func findAndClaimSpawnContext() claimedSpawnContext {
	// Get this goroutine's goid for targeted lookup.
	myGoid := getGoroutineID()

	spawnContextsMu.lock()
	defer spawnContextsMu.unlock()

	nowNs := nanotime()
	var claimed claimedSpawnContext

	// First pass: try to find a spawn context explicitly keyed to this child goid.
	for _, info := range spawnContextsSlice {
		if info.consumed.Load() != 0 {
			continue
		}
		if nowNs-info.createdAtNs > spawnContextTTLNs {
			continue
		}
		if info.childGoid == myGoid {
			if info.consumed.CompareAndSwap(0, 1) {
				claimed = claimedSpawnContext{
					parentClock: info.parentClock,
					creationPC:  info.pc,
					found:       true,
				}
				info.parentClock = nil
				break
			}
		}
	}

	// Fallback: if no goid-keyed context found, try FIFO (for legacy/timer contexts).
	if !claimed.found {
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
				claimed = claimedSpawnContext{
					parentClock: info.parentClock,
					creationPC:  info.pc,
					found:       true,
				}
				info.parentClock = nil
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
		} else if info != nil && info.parentClock != nil {
			// Release expired/consumed spawn clocks back to pool.
			// Consumed clocks are detached above and owned by their caller.
			// Any pointer still present here belongs to an expired entry.
			info.parentClock.Release()
			info.parentClock = nil
		}
	}
	spawnContextsSlice = validContexts

	return claimed
}

// findAndConsumeSpawnContext is the clock-only compatibility seam used by
// package tests. Child initialization consumes the complete spawn record.
func findAndConsumeSpawnContext() *vectorclock.VectorClock {
	return findAndClaimSpawnContext().parentClock
}

// raceacquire is called by compiler instrumentation on mutex lock operations.
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

// racerelease is called by compiler instrumentation on mutex unlock operations.
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

// racereleasemerge is called by compiler instrumentation on RWMutex read unlock operations.
//
// This is used for RWMutex.RUnlock where multiple readers may have
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

//go:linkname racereadSizedCtx
//go:nosplit
func racereadSizedCtx(addr, size, pc, racectx uintptr) {
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	det.OnReadSized(addr, size, ctx, pc)
}

//go:linkname racewriteCtx
//go:nosplit
func racewriteCtx(addr, pc, racectx uintptr) {
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	det.OnWrite(addr, ctx, pc)
}

//go:linkname racewriteSizedCtx
//go:nosplit
func racewriteSizedCtx(addr, size, pc, racectx uintptr) {
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	det.OnWriteSized(addr, size, ctx, pc)
}

func detectReadRange(addr, size, pc uintptr, ctx *goroutine.RaceContext) {
	det.OnReadRange(addr, size, ctx, pc)
}

func detectWriteRange(addr, size, pc uintptr, ctx *goroutine.RaceContext) {
	det.OnWriteRange(addr, size, ctx, pc)
}

//go:linkname racereadRangeCtx
//go:nosplit
func racereadRangeCtx(addr, size, pc, racectx uintptr) {
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	detectReadRange(addr, size, pc, ctx)
}

//go:linkname racewriteRangeCtx
//go:nosplit
func racewriteRangeCtx(addr, size, pc, racectx uintptr) {
	ctx := (*goroutine.RaceContext)(unsafe.Pointer(racectx))
	detectWriteRange(addr, size, pc, ctx)
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

//go:linkname racereadSizedSlow
func racereadSizedSlow(addr, size, pc uintptr) uintptr {
	if apiInitCalled.Load() == 0 {
		ensureInitialized()
	}
	if enabled.Load() == 0 {
		return 0
	}
	ctx := getCurrentContext()
	det.OnReadSized(addr, size, ctx, pc)
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

//go:linkname racewriteSizedSlow
func racewriteSizedSlow(addr, size, pc uintptr) uintptr {
	if apiInitCalled.Load() == 0 {
		ensureInitialized()
	}
	if enabled.Load() == 0 {
		return 0
	}
	ctx := getCurrentContext()
	det.OnWriteSized(addr, size, ctx, pc)
	return uintptr(unsafe.Pointer(ctx))
}

//go:linkname racereadRangeSlow
func racereadRangeSlow(addr, size, pc uintptr) uintptr {
	if apiInitCalled.Load() == 0 {
		ensureInitialized()
	}
	if enabled.Load() == 0 {
		return 0
	}
	ctx := getCurrentContext()
	detectReadRange(addr, size, pc, ctx)
	return uintptr(unsafe.Pointer(ctx))
}

//go:linkname racewriteRangeSlow
func racewriteRangeSlow(addr, size, pc uintptr) uintptr {
	if apiInitCalled.Load() == 0 {
		ensureInitialized()
	}
	if enabled.Load() == 0 {
		return 0
	}
	ctx := getCurrentContext()
	detectWriteRange(addr, size, pc, ctx)
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
// The runtime invokes this only while its scheduler protocol keeps the target
// goroutine alive and unable to complete raceGoEndFromRuntime. contextsMap.Load
// does not otherwise pin the returned context.
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
// The target-liveness precondition is the same as raceAcquireForGoroutine.
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
// The target-liveness precondition is the same as raceAcquireForGoroutine.
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

// getCurrentContext returns the RaceContext for the current goroutine.
//
// This function maintains a per-goroutine context cache in the sharded
// contextsMap registry. On first access, it:
//  1. Extracts the goroutine ID through the runtime bridge when available
//  2. Tries to find spawn context from parent (GoStart inheritance)
//  3. Allocates a never-reused logical TID
//  4. Creates a RaceContext for that TID (with or without parent clock)
//  5. Publishes it in the registry
//
// Subsequent calls through this fallback perform goroutine-ID extraction and
// a lookup in the appropriate registry shard. Runtime-instrumented accesses
// normally use the RaceContext cached directly in g.racectx instead.
//
// GoStart inheritance:
//   - If racegostart() was called before spawning this goroutine,
//     child inherits parent's VectorClock establishing happens-before.
//   - This prevents false positives for patterns like:
//     x = 42; go func() { _ = x }()
//
// Logical IDs are allocated monotonically for the process lifetime. Higher IDs
// use VectorClock's sparse tier rather than aliasing an existing coordinate.
//
// Thread Safety: Safe for concurrent calls from multiple goroutines.
func getCurrentContext() *goroutine.RaceContext {
	// Step 1: Get goroutine ID for current goroutine.
	// Use the runtime bridge to read getg().goid without parsing a stack trace.
	// The generic fallback parses the current runtime stack header.
	gid := getGoroutineID()

	// Step 2: Try to load existing context from cache (fast path).
	// contextsMap.Load returns *goroutine.RaceContext directly (no type assertion needed).
	if ctx, ok := contextsMap.Load(gid); ok {
		return ctx
	}

	// Step 3: Slow path - allocate new context for this goroutine.
	// This happens once per goroutine at first access.

	// Reserve a never-reused logical ID before consuming a spawn record. TID
	// exhaustion must leave the pending fork clock published and claimable.
	tid, startClock := allocTID()

	// Step 3a: Try to find spawn context from parent (GoStart inheritance).
	// If parent called racegostart() before spawning us, we inherit their clock.
	claimed := findAndClaimSpawnContext()
	parentClock := claimed.parentClock

	// Create new RaceContext for this goroutine.
	var ctx *goroutine.RaceContext
	if parentClock != nil {
		// GoStart path: inherit parent's clock.
		ctx = goroutine.AllocWithParentClock(tid, parentClock, startClock)
		// Release the spawn clock clone back to pool (data already copied).
		parentClock.Release()
	} else {
		// Legacy path: fresh clock.
		ctx = goroutine.AllocWithStartClock(tid, startClock)
	}
	det.RegisterGoroutineCreation(tid, claimed.creationPC)

	// Publish the context under the lifecycle and registry-shard locks.
	contextsMap.Store(gid, ctx)

	return ctx
}

// === Logical TID Allocation ===

// allocTID assigns a process-lifetime monotonic logical identity. startClock is
// always 1 because no unrelated lifetime can occupy the same vector-clock
// coordinate. The uint32 space is intentionally a hard correctness boundary.
func allocTID() (tid uint32, startClock uint32) {
	lifecycleMu.lock()
	var ok bool
	tid, ok = nextTID.reserve()
	if !ok {
		lifecycleMu.unlock()
		runtimeThrow("race detector exhausted logical goroutine IDs")
	}
	if pendingTIDs == nil {
		pendingTIDs = make(map[uint32]struct{})
	}
	pendingTIDs[tid] = struct{}{}
	lifecycleMu.unlock()
	return tid, 1
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
// Returns:
//   - uintptr: Program counter of the memory access
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
	if apiInitCalled.Load() != 2 {
		return
	}

	// Address-lifetime transitions are allocator maintenance, not user memory
	// events. Stale access and synchronization history must not survive an
	// allocation/free that happens while event recording is disabled.
	det.ClearShadowRange(addr, size)
}

// Enable turns on race detection for package tests and benchmarks.
//
// Enable is not a sound resume boundary after tracked program execution was
// globally disabled: intervening accesses were intentionally unobserved and
// may have superseded shadow or synchronization history. Call Init to begin a
// fresh detection lifetime. Enable cannot prove global quiescence and therefore
// fails closed for DirtyDisabled or Resetting state; callers must invoke Reset
// explicitly at a proven-quiescent boundary.
func Enable() {
	switch detectorLifecycle(lifecycleState.Load()) {
	case lifecycleEnabled:
		enabled.Store(1)
	case lifecycleDirtyDisabled, lifecycleResetting:
		runtimeThrow("race detector dirty state requires quiescent Reset before Enable")
	}
}

// Disable turns off race detection for package tests and benchmarks.
//
// After calling Disable(), raceread/racewrite become no-ops (fast return).
// It is a terminal or quiescent test switch, not the implementation of
// runtime.RaceDisable. The latter is per-goroutine and continues memory-history
// maintenance while suppressing synchronization edges.
//
// The flag update is atomic, but callers must quiesce detector users before a
// later Reset or Init.
func Disable() {
	lifecycleMu.lock()
	// Stop new event entry even if a diagnostic test directly changed the fast
	// flag. DirtyDisabled remains idempotent across repeated Fini calls.
	enabled.Store(0)
	if detectorLifecycle(lifecycleState.Load()) != lifecycleResetting {
		lifecycleState.Store(uint32(lifecycleDirtyDisabled))
	}
	lifecycleMu.unlock()
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

// RaceAcquire is an exported wrapper for raceacquire.
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

// RaceRelease is an exported wrapper for racerelease.
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

// RaceReleaseMerge is an exported wrapper for racereleasemerge.
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

// Reset resets the detector state for testing.
//
// This clears all shadow memory, resets the race counter, and clears the
// goroutine context cache. Logical IDs remain process-lifetime monotonic;
// resetting detector state must not make unrelated lifetimes share a vector
// clock coordinate. It's primarily used in test setup/teardown.
//
// Thread Safety: NOT safe for concurrent access.
// The caller must ensure no other goroutines are using the detector.
func Reset() {
	resetLifecycle(false, false)
}

// Init initializes the race detector for use.
//
// This function sets up the race detector runtime and makes it ready to
// track memory accesses. It should be called at the start of your program,
// typically in main() or init().
//
// Init() performs the following initialization steps:
//  1. Enables race detection
//  2. Preserves the process-lifetime logical-ID high-water mark
//  3. Creates a fresh detector instance with sampling disabled
//  4. Allocates a RaceContext for the calling goroutine with the next logical ID
//
// Logical ID convention:
// TID 0 is reserved as the uninitialized ownership sentinel. The goroutine
// calling Init receives the next process-lifetime logical ID.
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
func Init() {
	resetLifecycle(true, true)
}

// resetLifecycle is called only at caller-proven quiescent boundaries. It
// detaches every API-owned root while holding lifecycleMu, releases detached
// ownership after unlocking, resets detector state, and publishes readiness
// and Enabled last. Process-lifetime TIDs are deliberately not touched.
func resetLifecycle(freshDetector, createCallerContext bool) {
	if createCallerContext {
		// Init must fail for identity exhaustion before disabling, detaching, or
		// resetting any published detector state. Init is a caller-proven
		// quiescent boundary, so this check remains valid until allocTID below.
		lifecycleMu.lock()
		exhausted := nextTID.load64() >= exhaustedTIDHighWater-1
		lifecycleMu.unlock()
		if exhausted {
			runtimeThrow("race detector exhausted logical goroutine IDs")
		}
	}
	enabled.Store(0)
	apiInitCalled.Store(1)

	lifecycleMu.lock()
	lifecycleState.Store(uint32(lifecycleResetting))
	var contexts []*goroutine.RaceContext
	contextsMap.rangeLocked(func(_ int64, ctx *goroutine.RaceContext) bool {
		contexts = append(contexts, ctx)
		return true
	})
	contextsMap.resetLocked()
	for _, ctx := range detachedContexts {
		contexts = append(contexts, ctx)
	}
	detachedContexts = nil
	pendingTIDs = nil

	// Structural child binding checks Resetting before reserving a TID. Holding
	// the spawn lock here transfers every pending clock into this reset exactly
	// once before new recording is published.
	spawnContextsMu.lock()
	spawns := spawnContextsSlice
	spawnContextsSlice = nil
	spawnContextsMu.unlock()
	lifecycleMu.unlock()

	seen := make(map[*goroutine.RaceContext]struct{}, len(contexts))
	for _, ctx := range contexts {
		if ctx == nil {
			continue
		}
		if _, duplicate := seen[ctx]; duplicate {
			continue
		}
		seen[ctx] = struct{}{}
		detector.DeactivateAtomicLoadCache(ctx)
		if ctx.C != nil {
			ctx.C.Release()
			ctx.C = nil
		}
	}
	for _, info := range spawns {
		if info != nil && info.parentClock != nil {
			info.parentClock.Release()
			info.parentClock = nil
		}
	}
	if det != nil {
		det.Reset()
	}
	if freshDetector || det == nil {
		det = detector.NewDetector()
	}
	shadow = det.GetShadow().(*shadowmem.PageTableShadow)
	nextSpawnID.Store(0)
	if createCallerContext {
		// Create and root the caller before publishing Enabled. TID zero remains
		// reserved by current FastTrack ownership encodings.
		gid := getGoroutineID()
		tid, startClock := allocTID()
		ctx := goroutine.AllocWithStartClock(tid, startClock)
		contextsMap.Store(gid, ctx)
	}

	lifecycleMu.lock()
	lifecycleState.Store(uint32(lifecycleEnabled))
	apiInitCalled.Store(2)
	enabled.Store(1)
	lifecycleMu.unlock()
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
// Repeated calls are permitted. Each call disables the detector and prints
// the current summary; calls are not coalesced into a single report.
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
	runtimeFini()

	// Get the total number of races detected.
	racesDetected := det.RacesDetected()

	// Keep runtime.printstring as the output boundary. The pure formatter makes
	// the exact summary contract deterministic in tests.
	printstring(formatFiniSummary(racesDetected))
}

// runtimeFini disables the detector without printing the API summary. Runtime
// race finalization uses this path because production -race programs, like the
// ThreadSanitizer backend, are silent when no race is detected. Race reports
// themselves are emitted immediately by the detector.
//
//go:linkname runtimeFini
func runtimeFini() {
	Disable()
}

func formatFiniSummary(racesDetected int) string {
	summary := "\n==================\n" +
		"Race Detector Report\n" +
		"==================\n"
	if racesDetected == 0 {
		summary += "No data races detected.\n"
	} else {
		summary += "WARNING: " + itoaAPI(racesDetected) + " data race(s) detected!\n" +
			"\nSee above for details.\n"
	}
	return summary + "==================\n\n"
}
