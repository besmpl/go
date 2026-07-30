//go:build amd64 || arm64

package shadowmem

import (
	"internal/runtime/atomic"
	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/vectorclock"
)

// Page table constants for two-level direct-mapped shadow memory.
const (
	// l1Size is the number of L1 page directory entries.
	// 65536 entries * 8 bytes = 512KB fixed overhead.
	l1Size = 65536

	// l2Size is the number of aligned application words represented by an L2
	// page. Each page covers 2MiB of application memory.
	l2Size = 1 << 18

	l1Shift = 21
	l2Mask  = l2Size - 1

	ptTotalCoverage = uintptr(l1Size) << l1Shift

	// A 4KiB application block contains 512 aligned words. A bulk range keeps
	// one ordinary default history per block and materializes word slots only
	// when scalar, partial, atomic, or clear semantics make a word diverge.
	rangeBlockShift    = 12
	rangeBlockSize     = uintptr(1) << rangeBlockShift
	rangeBlockWords    = rangeBlockSize / 8
	rangeBlocksPerPage = (uintptr(1) << l1Shift) / rangeBlockSize
	rangeBlockWordMask = rangeBlockWords - 1

	// Addresses outside the ABI-mirrored 128GiB window use a sparse directory
	// keyed by absolute 4KiB block. Keeping collisions at block granularity
	// prevents a large range from building quadratic per-word CAS chains.
	externalBlockBuckets = 1 << 16
	externalBlockMask    = externalBlockBuckets - 1
)

// shadowPage covers 2MiB of application memory. slotTables must remain first:
// runtime/race_kolkov.go directly loads this ABI-mirrored pointer directory.
type shadowPage struct {
	slotTables [rangeBlocksPerPage]atomic.Pointer[blockSlotTable]
	blocks     [rangeBlocksPerPage]rangeBlock
}

// blockSlotTable is allocated only after a primary or external block
// materializes its first word. Compact/default-only range blocks need history
// but no 4KiB slot pointer plane, which is the common generated-code shape.
type blockSlotTable struct {
	slots [rangeBlockWords]atomic.Pointer[shadowSlot]
}

// externalShadowBlock is the sparse out-of-window equivalent of one primary
// page block. Its 4KiB word-slot plane is lazy so one distant compact range
// commits only the small directory cell and exact block history.
type externalShadowBlock struct {
	slots   atomic.Pointer[blockSlotTable]
	history rangeBlock
}

type externalBlockCell struct {
	base  uintptr
	next  *externalBlockCell
	block externalShadowBlock
}

type blockView struct {
	slotTableOwner *atomic.Pointer[blockSlotTable]
	history        *rangeBlock
}

// slotTable returns the lazily published per-block word-slot table. create is
// used only while materializing under the owning block lock; lookup, compact,
// default, and allocator-clear paths keep an absent table allocation-free.
func (view blockView) slotTable(create bool) []atomic.Pointer[shadowSlot] {
	table := view.slotTableOwner.Load()
	if table == nil && create {
		candidate := new(blockSlotTable)
		if view.slotTableOwner.CompareAndSwap(nil, candidate) {
			table = candidate
		} else {
			table = view.slotTableOwner.Load()
		}
	}
	if table == nil {
		return nil
	}
	return table.slots[:]
}

func (view blockView) loadSlot(wordIdx uintptr) *ShadowSlot {
	slots := view.slotTable(false)
	if slots == nil {
		return nil
	}
	return slots[wordIdx].Load()
}

// PageTableShadow implements direct-mapped shadow memory for a centered 128GiB
// window plus a sparse absolute-block directory for other mapped regions.
//
// All fields retain the private runtime fast-path ABI. The 64K sparse heads use
// no more fixed space than the obsolete word-level CAS fallback they replace,
// while keeping bounded runtime lookups effective for large sequential ranges.
type PageTableShadow struct {
	base atomic.Uintptr

	pages [l1Size]atomic.Pointer[shadowPage]

	external [externalBlockBuckets]atomic.Pointer[externalBlockCell]
}

// NewPageTableShadow creates a ready-to-use page table shadow.
func NewPageTableShadow() *PageTableShadow {
	return &PageTableShadow{}
}

// initBase sets the immutable primary-window base on first access.
func (pt *PageTableShadow) initBase(addr uintptr) {
	aligned := addr &^ (uintptr(1)<<l1Shift - 1)
	headroom := ptTotalCoverage / 4
	if aligned > headroom {
		aligned -= headroom
	} else {
		// Zero is the initialization sentinel. Keep the low-address window
		// aligned to both primary pages and range blocks so their word/block
		// indices describe the same absolute application addresses.
		aligned = uintptr(1) << l1Shift
	}
	pt.base.CompareAndSwap(0, aligned)
}

func (pt *PageTableShadow) primaryPage(addr uintptr, create bool) (*shadowPage, uintptr, bool) {
	base := pt.base.Load()
	if base == 0 {
		if !create {
			return nil, 0, false
		}
		pt.initBase(addr)
		base = pt.base.Load()
	}
	if addr < base {
		return nil, 0, false
	}
	offset := addr - base
	if offset >= ptTotalCoverage {
		return nil, 0, false
	}

	pageIdx := offset >> l1Shift
	page := pt.pages[pageIdx].Load()
	if page == nil && create {
		candidate := new(shadowPage)
		if pt.pages[pageIdx].CompareAndSwap(nil, candidate) {
			page = candidate
		} else {
			page = pt.pages[pageIdx].Load()
		}
	}
	if page == nil {
		return nil, 0, false
	}
	return page, (offset >> 3) & l2Mask, true
}

func externalBlockHash(base uintptr) uintptr {
	return uintptr(fastHash(base)) & externalBlockMask
}

func (pt *PageTableShadow) externalBlock(addr uintptr, create bool) *externalShadowBlock {
	base := addr &^ (rangeBlockSize - 1)
	bucket := &pt.external[externalBlockHash(base)]
	for {
		head := bucket.Load()
		for cell := head; cell != nil; cell = cell.next {
			if cell.base == base {
				return &cell.block
			}
		}
		if !create {
			return nil
		}
		candidate := &externalBlockCell{base: base, next: head}
		if bucket.CompareAndSwap(head, candidate) {
			return &candidate.block
		}
	}
}

func (pt *PageTableShadow) blockFor(addr uintptr, create bool) (blockView, bool) {
	base := pt.base.Load()
	if create && base == 0 {
		pt.initBase(addr)
		base = pt.base.Load()
	}
	inPrimary := base != 0 && addr >= base && addr-base < ptTotalCoverage
	if inPrimary {
		page, wordIdx, ok := pt.primaryPage(addr, create)
		if !ok {
			return blockView{}, false
		}
		blockIdx := wordIdx / rangeBlockWords
		return blockView{
			slotTableOwner: &page.slotTables[blockIdx],
			history:        &page.blocks[blockIdx],
		}, true
	}
	block := pt.externalBlock(addr, create)
	if block == nil {
		return blockView{}, false
	}
	return blockView{slotTableOwner: &block.slots, history: &block.history}, true
}

// materializeSlotLocked publishes one complete word override while holding the
// block lock. Every retained lane starts in one clone of the current block
// default, then exact compact memberships override it. clearMask lanes are nil
// at publication, making a partial clear's destination authoritative before
// the source memberships are retired.
//
// The returned bool reports whether this call published the slot. Although all
// materializers use the block lock, the CAS remains the defensive publication
// boundary mirrored by the runtime fast path.
func materializeSlotLocked(view blockView, wordIdx uintptr, clearMask uint8) (*ShadowSlot, bool) {
	if slot := view.loadSlot(wordIdx); slot != nil {
		return slot, false
	}

	slot := &ShadowSlot{}
	compact := view.history.compact.Load()
	wordOffset := wordIdx * shadowSlotLanes
	tombstones := uint8(0)
	if compact != nil {
		tombstones = compact.wordTombstoneMask(wordOffset)
	}
	defaultState := view.history.state.Load()
	if defaultState != nil {
		// Retain the default transaction through destination publication. A
		// runtime shortcut which resolved the default must either complete
		// before this point or fail mapping revalidation against the new slot.
		defaultState.LockAccess()
		state := defaultState.CloneOrdinaryLocked()
		for lane := range slot.states {
			if (clearMask|tombstones)&(uint8(1)<<lane) == 0 {
				slot.states[lane].Store(state)
			}
		}
	}
	if compact != nil {
		// Compact membership overrides the block default. Build the complete
		// authoritative word before publication so no reader can observe a
		// partially moved history.
		compact.materializeWord(wordOffset, slot)
		for lane := range slot.states {
			if clearMask&(uint8(1)<<lane) != 0 {
				slot.states[lane].Store(nil)
			}
		}
	}
	slots := view.slotTable(true)
	if slots[wordIdx].CompareAndSwap(nil, slot) {
		if compact != nil {
			// Publication is the move linearization point. Only now may the old
			// compact memberships be retired; a materialized word remains
			// authoritative, including nil lanes, for the block lifetime.
			compact.retireWord(wordOffset)
		}
		if defaultState != nil {
			defaultState.UnlockAccess()
		}
		return slot, true
	}
	if defaultState != nil {
		defaultState.UnlockAccess()
	}
	return slots[wordIdx].Load(), false
}

// materializeSlot publishes one word override while holding the block lock.
func materializeSlot(view blockView, wordIdx uintptr) *ShadowSlot {
	if slot := view.loadSlot(wordIdx); slot != nil {
		return slot
	}

	view.history.mu.lock()
	defer view.history.mu.unlock()
	slot, _ := materializeSlotLocked(view, wordIdx, 0)
	return slot
}

// GetOrCreateSlot returns the materialized word slot containing addr.
func (pt *PageTableShadow) GetOrCreateSlot(addr uintptr) *ShadowSlot {
	view, _ := pt.blockFor(addr, true)
	return materializeSlot(view, (addr>>3)&rangeBlockWordMask)
}

// GetSlot returns a materialized word override, or nil when the word still uses
// its block default.
func (pt *PageTableShadow) GetSlot(addr uintptr) *ShadowSlot {
	view, ok := pt.blockFor(addr, false)
	if !ok {
		return nil
	}
	return view.loadSlot((addr >> 3) & rangeBlockWordMask)
}

// MaterializeReadHintSlot publishes the compact word containing addr.
func (pt *PageTableShadow) MaterializeReadHintSlot(addr uintptr) bool {
	view, ok := pt.blockFor(addr, false)
	if !ok {
		return false
	}
	wordIdx := (addr >> 3) & rangeBlockWordMask
	if view.loadSlot(wordIdx) != nil {
		return true
	}
	view.history.mu.lock()
	defer view.history.mu.unlock()
	if view.loadSlot(wordIdx) != nil {
		return true
	}
	_, published := materializeSlotLocked(view, wordIdx, 0)
	return published
}

// readHintSlotCandidate roots the one state whose complete materialized-slot
// reference mask is exactly the hinted scalar range. The slot lock makes this
// initial snapshot coherent with copy-on-write, but is deliberately released
// before the caller waits for the state transaction lock.
func (pt *PageTableShadow) readHintSlotCandidate(addr, size uintptr) (*ShadowSlot, *VarState, uint8, bool) {
	if size == 0 || size > shadowSlotLanes || size > shadowSlotLanes-(addr&7) {
		return nil, nil, 0, false
	}
	slot := pt.GetSlot(addr)
	if slot == nil {
		return nil, nil, 0, false
	}
	first := uint8(addr & 7)
	mask := uint8(((uint16(1) << size) - 1) << first)

	slot.mu.lock()
	defer slot.mu.unlock()
	if pt.GetSlot(addr) != slot {
		return nil, nil, 0, false
	}
	state := slot.State(first)
	if state == nil || slot.referenceMask(state) != mask {
		return nil, nil, 0, false
	}
	return slot, state, mask, true
}

// lockReadHintSlotCandidate waits without retaining the slot lock, then
// revalidates the complete mapping using atomic pointer loads while accessMu
// prevents clear or copy-on-write from redirecting candidate. It must not take
// the slot or block lock while state is held: their established order is
// block -> slot -> access.
func (pt *PageTableShadow) lockReadHintSlotCandidate(addr uintptr, slot *ShadowSlot, state *VarState, mask uint8) (locked, retry bool) {
	state.LockAccess()
	first := uint8(addr & 7)
	if pt.GetSlot(addr) != slot ||
		slot.State(first) != state ||
		slot.referenceMask(state) != mask {
		state.UnlockAccess()
		return false, true
	}
	// Atomic sidecars require the authoritative mixed-access visitor. Attachment
	// and replacement are serialized by the access lock held here.
	if state.GetAtomicState() != nil {
		state.UnlockAccess()
		return false, false
	}
	return true, false
}

// LockReadHintSlotRange locks the exact ordinary state for a repeated weak-hint
// scalar read. A stale candidate is retried once; all other shapes and atomic
// overlays fall back to the authoritative range protocol in the detector.
func (pt *PageTableShadow) LockReadHintSlotRange(addr, size uintptr) (*VarState, bool) {
	for attempt := 0; attempt < 2; attempt++ {
		slot, state, mask, ok := pt.readHintSlotCandidate(addr, size)
		if !ok {
			return nil, false
		}
		if locked, retry := pt.lockReadHintSlotCandidate(addr, slot, state, mask); locked {
			return state, true
		} else if !retry {
			return nil, false
		}
	}
	return nil, false
}

// GetOrCreate returns the exact lane state, materializing its complete word.
func (pt *PageTableShadow) GetOrCreate(addr uintptr) *VarState {
	return pt.GetOrCreateSlot(addr).GetOrCreateLane(uint8(addr & 7))
}

// Get returns the exact represented state without materializing a word.
func (pt *PageTableShadow) Get(addr uintptr) *VarState {
	view, ok := pt.blockFor(addr, false)
	if !ok {
		return nil
	}
	if slot := view.loadSlot((addr >> 3) & rangeBlockWordMask); slot != nil {
		return slot.State(uint8(addr & 7))
	}
	if compact := view.history.compact.Load(); compact != nil {
		if compact.palette.Load() != nil {
			// Dense palette records deliberately carry no VarState per shape or
			// anchor. Direct Get is a cold diagnostic/API boundary once active
			// has closed the runtime shortcut, so materialize only this word.
			return materializeSlot(view, (addr>>3)&rangeBlockWordMask).State(uint8(addr & 7))
		}
		if state, authoritative := compact.lookupExact(addr); authoritative {
			return state
		}
		// Materialization publishes the complete destination slot before it
		// retires the source membership. A lookup which observed the old nil
		// slot but missed after retirement must retry the destination rather
		// than falling through to an unrelated block default.
		if slot := view.loadSlot((addr >> 3) & rangeBlockWordMask); slot != nil {
			return slot.State(uint8(addr & 7))
		}
	}
	return view.history.state.Load()
}

// TryCompactWrite applies an allocation-free simple FastTrack write transition
// to an unmaterialized exact start address. It returns false without changing
// history when the address inherits a block default, the transition is complex,
// or the bounded compact representation is full; callers then use the ordinary
// slot protocol. Scalar hooks carry no width, so only addr itself is recorded.
func (pt *PageTableShadow) TryCompactWrite(addr uintptr, current epoch.Epoch, clock *vectorclock.VectorClock, pc uintptr) bool {
	view, _ := pt.blockFor(addr, true)
	wordIdx := (addr >> 3) & rangeBlockWordMask
	if view.loadSlot(wordIdx) != nil {
		return false
	}

	view.history.mu.lock()
	defer view.history.mu.unlock()
	if view.loadSlot(wordIdx) != nil {
		return false
	}

	compact := view.history.compact.Load()
	if compact == nil {
		if view.history.state.Load() != nil {
			return false
		}
		compact = newCompactGroups()
		// Publish the permanent header before membership. moveDescriptor sets
		// its active word before the mapping becomes visible, so runtime lookup
		// either sees the prior default-only state or fails closed.
		view.history.compact.Store(compact)
	} else if palette := compact.palette.Load(); palette != nil {
		if palette.owner(addr) == compactPaletteDefault && view.history.state.Load() != nil {
			return false
		}
	} else {
		group, overlap := compact.lookupGroup(addr)
		if overlap {
			return false
		}
		if group == nil && !compact.isTombstone(addr) && view.history.state.Load() != nil {
			return false
		}
	}
	_, ok := compact.tryWrite(addr, current, clock, pc)
	return ok
}

// TryCompactRead is the read counterpart of TryCompactWrite. Only
// CompactReadExactNoop may seed Detector's redundant-read cache. Handled
// transitions stay uncached so their unexposed state remains eligible for
// allocation-free in-place reuse; unstable or unsupported probes return Miss.
func (pt *PageTableShadow) TryCompactRead(addr uintptr, current epoch.Epoch, clock *vectorclock.VectorClock, pc uintptr) CompactReadResult {
	view, _ := pt.blockFor(addr, true)
	wordIdx := (addr >> 3) & rangeBlockWordMask
	if view.loadSlot(wordIdx) != nil {
		return CompactReadMiss
	}

	view.history.mu.lock()
	defer view.history.mu.unlock()
	if view.loadSlot(wordIdx) != nil {
		return CompactReadMiss
	}

	compact := view.history.compact.Load()
	if compact == nil {
		if view.history.state.Load() != nil {
			return CompactReadMiss
		}
		compact = newCompactGroups()
		view.history.compact.Store(compact)
	}
	var sourceState *VarState
	var sourceDescriptor compactHistoryDescriptor
	var denseSource bool
	palette := compact.palette.Load()
	if palette == nil {
		source, overlap := compact.lookupGroup(addr)
		if overlap {
			return CompactReadMiss
		}
		if source == nil && !compact.isTombstone(addr) && view.history.state.Load() != nil {
			return CompactReadMiss
		}
		if source != nil {
			sourceState = source.state.Load()
		}
	} else {
		if palette.owner(addr) == compactPaletteDefault && view.history.state.Load() != nil {
			return CompactReadMiss
		}
		// Dense histories have no per-address VarState pointer to compare. The
		// descriptor is their complete immutable semantic key, including the
		// allocator lifecycle, so equality across the locked transition is the
		// exact counterpart of sparse source-state identity.
		sourceDescriptor, denseSource = palette.descriptor(addr)
	}
	state, ok := compact.tryRead(addr, current, clock, pc)
	if !ok {
		return CompactReadMiss
	}
	if denseSource {
		destinationDescriptor, represented := palette.descriptor(addr)
		if represented && destinationDescriptor == sourceDescriptor {
			return CompactReadExactNoop
		}
		return CompactReadHandled
	}
	if sourceState != nil && state == sourceState {
		return CompactReadExactNoop
	}
	return CompactReadHandled
}

// AccessRange visits every distinct ordinary-history group intersecting the
// range. Completely covered 4KiB blocks transition one default plus their
// materialized word overrides; edge fragments retain exact per-word masks.
func (pt *PageTableShadow) AccessRange(addr, size uintptr, visit func(word uintptr, mask uint8, state *VarState)) {
	if size == 0 || size-1 > ^uintptr(0)-addr {
		return
	}

	current, remaining := addr, size
	if offset := current & (rangeBlockSize - 1); offset != 0 {
		count := rangeBlockSize - offset
		if count > remaining {
			count = remaining
		}
		pt.accessExactRange(current, count, visit)
		current += count
		remaining -= count
	}
	for remaining >= rangeBlockSize {
		view, _ := pt.blockFor(current, true)
		accessFullBlock(view, current, visit)
		current += rangeBlockSize
		remaining -= rangeBlockSize
	}
	if remaining != 0 {
		pt.accessExactRange(current, remaining, visit)
	}
}

func (pt *PageTableShadow) accessExactRange(addr, size uintptr, visit func(word uintptr, mask uint8, state *VarState)) {
	for current, remaining := addr, size; remaining != 0; {
		lane := current & 7
		count := uintptr(8) - lane
		if count > remaining {
			count = remaining
		}
		mask := uint8(((uint16(1) << count) - 1) << lane)
		word := current &^ uintptr(7)
		slot := pt.GetOrCreateSlot(current)
		slot.AccessGroups(mask, func(groupMask uint8, state *VarState) {
			visit(word, groupMask, state)
		})
		current += count
		remaining -= count
	}
}

func accessFullBlock(view blockView, base uintptr, visit func(word uintptr, mask uint8, state *VarState)) {
	view.history.mu.lock()
	defer view.history.mu.unlock()

	compact := view.history.compact.Load()
	if compact == nil {
		if view.history.state.Load() != nil {
			runtimeThrow("race detector block default published before compact clear metadata")
		}
		// Install allocation-free partial-clear metadata before a block default
		// can become visible. AccessRange is a normal detector boundary where
		// allocation is permitted; allocator ClearRange is not.
		compact = newCompactGroups()
		view.history.compact.Store(compact)
	}
	if compact.palette.Load() != nil {
		// Dense compaction handles every supported full-block transition before
		// this generic callback path. A conflict or unsupported state is rare;
		// make exact word slots first so the established COW visitor remains the
		// single fallback oracle without requiring an unbounded stack plan.
		for i := uintptr(0); i < rangeBlockWords; i++ {
			wordOffset := uintptr(i) * shadowSlotLanes
			if view.loadSlot(i) == nil &&
				(compact.coveredWord(wordOffset)|compact.wordTombstoneMask(wordOffset)) != 0 {
				materializeSlotLocked(view, i, 0)
			}
		}
	}

	// A tombstone is an authoritative zero history which deliberately does not
	// inherit the block default. Full-range access must transition it too. This
	// boundary may allocate, so first publish complete word slots and retire the
	// tombstones; the ordered traversal below then has only slots, compact
	// groups, and at most one default equivalence class.
	for i := uintptr(0); i < rangeBlockWords; i++ {
		wordOffset := uintptr(i) * shadowSlotLanes
		if view.loadSlot(i) == nil && compact.wordTombstoneMask(wordOffset) != 0 {
			materializeSlotLocked(view, i, 0)
		}
	}

	var accesses [compactGroupCapacity]compactAccess
	accessCount := compact.snapshotAccesses(base, &accesses)
	nextCompact := 0
	defaultVisited := false
	for i := uintptr(0); i < rangeBlockWords; i++ {
		word := base + uintptr(i)*8
		slot := view.loadSlot(i)
		if slot != nil {
			if nextCompact < accessCount && accesses[nextCompact].anchor < word+8 {
				runtimeThrow("race detector compact membership overlaps materialized word")
			}
			slot.accessGroupsBlockLocked(0xff, func(mask uint8, state *VarState) {
				visit(word, mask, state)
			})
			continue
		}

		covered := compact.coveredWord(uintptr(i) * shadowSlotLanes)
		defaultMask := ^covered
		defaultLane := uint8(shadowSlotLanes)
		if !defaultVisited && defaultMask != 0 {
			defaultLane = firstLane(defaultMask)
		}

		// Merge compact equivalence-class representatives with the single
		// default representative in exact lane order. A compact class is visited
		// once at its lowest member; covered lanes in later words are excluded
		// from default inheritance without another callback.
		for nextCompact < accessCount && accesses[nextCompact].anchor < word+8 {
			entry := &accesses[nextCompact]
			if entry.anchor < word {
				runtimeThrow("race detector compact access order regressed")
			}
			compactLane := uint8(entry.anchor & (shadowSlotLanes - 1))
			if defaultLane < compactLane {
				accessBlockDefaultLocked(view.history, compact, word, defaultMask, visit)
				defaultVisited = true
				defaultLane = shadowSlotLanes
			}
			visit(word, uint8(1)<<compactLane, entry.state)
			nextCompact++
		}
		if defaultLane < shadowSlotLanes {
			accessBlockDefaultLocked(view.history, compact, word, defaultMask, visit)
			defaultVisited = true
		}
	}
	if nextCompact != accessCount {
		runtimeThrow("race detector compact access escaped block")
	}
	compact.commitAccesses(&accesses, accessCount)
}

// accessBlockDefaultLocked applies one block-wide default transition at its
// lowest actual uncovered address. The caller holds the block lock.
func accessBlockDefaultLocked(history *rangeBlock, compact *compactGroups, word uintptr, mask uint8, visit func(word uintptr, mask uint8, state *VarState)) {
	state := history.state.Load()
	if state == nil {
		state = compact.newGenerationState()
	}
	state.LockAccess()
	visit(word, mask, state)
	state.UnlockAccess()
	if compact.palette.Load() == nil {
		// Allocator clear cannot allocate. Publish the ready exact-zero plane at
		// this ordinary detector boundary before the bitmap default becomes visible.
		compact.provisionTombstones()
	}
	history.state.Store(state)
}

// ClearRange forgets exact histories in [addr, addr+size). A full block drops
// its default and clears materialized overrides in place. A partial clear of a
// compact/default-only block records exact zero directly in its compact
// metadata; blocks with materialized words retain the exact per-word fallback.
func (pt *PageTableShadow) ClearRange(addr, size uintptr) {
	if size == 0 || size-1 > ^uintptr(0)-addr {
		return
	}

	current, remaining := addr, size
	if offset := current & (rangeBlockSize - 1); offset != 0 {
		count := rangeBlockSize - offset
		if count > remaining {
			count = remaining
		}
		pt.clearExactRange(current, count)
		current += count
		remaining -= count
	}
	for remaining >= rangeBlockSize {
		if view, ok := pt.blockFor(current, false); ok {
			clearFullBlock(view)
		}
		current += rangeBlockSize
		remaining -= rangeBlockSize
	}
	if remaining != 0 {
		pt.clearExactRange(current, remaining)
	}
}

func (pt *PageTableShadow) clearExactRange(addr, size uintptr) {
	// The ordinary allocator shape is one sub-block object backed only by a
	// compact/default history. Its absent slot table is permanent while the
	// block lock is held, so drain the default transaction and mutate compact
	// metadata once instead of repeating both operations for every aligned word.
	// Once any word has materialized, keep the established per-word path below:
	// published slots remain authoritative for the lifetime of the block.
	if size != 0 && size <= rangeBlockSize-(addr&(rangeBlockSize-1)) {
		if view, ok := pt.blockFor(addr, false); ok && view.slotTable(false) == nil {
			view.history.mu.lock()
			if view.slotTable(false) == nil {
				clearUnmaterializedRangeBlockLocked(view, addr, size)
				view.history.mu.unlock()
				return
			}
			view.history.mu.unlock()
		}
	}

	for current, remaining := addr, size; remaining != 0; {
		lane := current & 7
		count := uintptr(8) - lane
		if count > remaining {
			count = remaining
		}
		mask := uint8(((uint16(1) << count) - 1) << lane)
		view, ok := pt.blockFor(current, false)
		if ok {
			wordIdx := (current >> 3) & rangeBlockWordMask
			view.history.mu.lock()
			slot := view.loadSlot(wordIdx)
			if slot != nil {
				slot.clearMaskBlockLocked(mask)
			} else {
				clearUnmaterializedRangeBlockLocked(view, current, count)
			}
			view.history.mu.unlock()
		}
		current += count
		remaining -= count
	}
}

// clearUnmaterializedRangeBlockLocked clears one exact in-block range whose
// selected words have no published slot. The caller holds the block lock.
func clearUnmaterializedRangeBlockLocked(view blockView, addr, size uintptr) {
	compact := view.history.compact.Load()
	defaultState := view.history.state.Load()
	if compact == nil {
		if defaultState != nil {
			runtimeThrow("race detector block default missing compact clear metadata")
		}
		return
	}

	// Allocator clears can run from GC sweep, where heap allocation is
	// forbidden. Drain the inherited default while compact tombstones become
	// authoritative, then retire exact memberships and advance the generation.
	if defaultState != nil {
		if binding := defaultState.atomicState.Load(); binding != nil {
			binding.escapeIncompatible()
		}
		defaultState.LockAccess()
		if binding := defaultState.atomicState.Load(); binding != nil {
			binding.escapeIncompatible()
		}
	}
	compact.clearRangeKnownDefault(addr, size, defaultState != nil)
	if defaultState != nil {
		defaultState.UnlockAccess()
	}
}

func clearFullBlock(view blockView) {
	view.history.mu.lock()
	slots := view.slotTable(false)
	for i := range slots {
		if slot := slots[i].Load(); slot != nil {
			// Close and drain any retained atomic-only transaction before the
			// clear linearizes for this word. Keep the empty slot authoritative:
			// a caller may have resolved its pointer before the block lock and
			// begin a new access after clear releases slot.mu. Detaching here
			// would orphan that post-clear history and any newly enrolled atomic
			// capability. Empty materialized lanes remain exact zero history.
			slot.clearMaskBlockLocked(0xff)
		}
	}
	// The block default is also an access-locked transaction object. Runtime
	// scalar shortcuts may only observe it, but a full range operation can be
	// publishing through it while ClearRange arrives. Drain and remove it before
	// compact membership: a lock-free lookup which misses a just-retired compact
	// bit must never fall through to this unrelated old default.
	if state := view.history.state.Load(); state != nil {
		state.LockAccess()
		view.history.state.Store(nil)
		state.UnlockAccess()
	} else {
		view.history.state.Store(nil)
	}
	if compact := view.history.compact.Load(); compact != nil {
		// Full clear drains every compact transaction, drops all exact
		// memberships/tombstones, and advances the block-owned allocator
		// generation. Keep the compact object published permanently so runtime
		// shortcuts cannot fall back to stale state after this lifecycle.
		compact.reset()
	}
	view.history.mu.unlock()
}

// Reset clears all primary and sparse shadow memory. The Shadow contract
// requires callers to quiesce concurrent accesses first.
func (pt *PageTableShadow) Reset() {
	for i := range pt.pages {
		pt.pages[i].Store(nil)
	}
	for i := range pt.external {
		pt.external[i].Store(nil)
	}
	pt.base.Store(0)
}
