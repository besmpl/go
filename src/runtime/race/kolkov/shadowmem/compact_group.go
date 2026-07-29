//go:build amd64 || arm64

package shadowmem

import (
	"internal/runtime/atomic"
	"math/bits"

	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/vectorclock"
)

// compactGroupCapacity bounds the number of resident exact ordinary-history
// classes in one 4 KiB application block. Empty classes form a small descriptor
// working set and are recycled when a new live class needs their slot. Gob's
// generated access loops use more than eight distinct PCs, so sixteen avoids
// systematic overflow while keeping a lock-free lookup bounded.
const compactGroupCapacity = 16

const (
	compactMembershipBits  = int(rangeBlockSize)
	compactMembershipWords = compactMembershipBits / 64
)

// compactHistoryKey is the complete supported ordinary FastTrack history.
// Allocator lifetime is deliberately kept in compactHistoryDescriptor: it is
// not ordinary semantics, but it must participate in published-group equality
// so an address lifetime can never be joined back across a clear.
//
// Stack histories, atomic overlays, promoted reads, and multiple inline reads
// are not representable here. compactDescriptorFromState rejects them rather
// than manufacturing an incomplete key.
type compactHistoryKey struct {
	write           epoch.Epoch
	read            epoch.Epoch
	exclusiveWriter int64
	writePC         uintptr
	readPC          uintptr
	writeCount      uint32
}

type compactHistoryDescriptor struct {
	history   compactHistoryKey
	lifecycle lifecycleID
}

// compactGroup owns one current VarState at a time and an exact membership
// bitmap over the 4096 compiler-provided anchor bytes in its application
// block. A state returned to an unlocked lookup or range visitor is permanently
// immutable. An unexposed state may be reinitialized in place while the group's
// publication revision is odd and either its sole member is being retargeted or
// it has no authoritative members.
//
// Membership words are atomic because PageTable lookups do not take the block
// lock. All mutation methods otherwise require the owning rangeBlock lock.
type compactGroup struct {
	state atomic.Pointer[VarState]
	// exposed records whether lookupExact has returned the current state to an
	// unlocked caller. It is reset only when a fresh state pointer is published.
	exposed atomic.Uint32
	members [compactMembershipWords]atomic.Uint64

	// The fields below are read and written only while the block lock is held.
	descriptor compactHistoryDescriptor
	joinable   bool
	retired    bool
}

// compactGroups is allocated lazily and then remains attached to its block.
// lifecycle is one allocator generation shared by virgin anchors, allowing
// their first equal W-only or R-only transitions to join without allocating a
// VarState per byte. A full clear drains reset and advances this generation.
type compactGroups struct {
	// active is the runtime fast-path gate and deliberately remains at offset
	// zero. Headers may be installed eagerly so allocator clear never allocates;
	// active stays zero while the block contains only its ordinary default. It
	// is published before the first membership or tombstone and never clears,
	// including across reset, so runtime lookup is permanently conservative
	// after the block has ever held exact compact metadata.
	active atomic.Uint32
	groups [compactGroupCapacity]atomic.Pointer[compactGroup]
	// tombstones are exact authoritative zero histories installed by partial
	// allocator clears. They are embedded so racefree/GC sweep never allocates.
	// A normal detector access may later move a tombstone into a fresh compact
	// history; a materialized slot retires both forms after publication.
	tombstones [compactMembershipWords]atomic.Uint64
	// revision is an even/odd publication sequence. It prevents a lock-free
	// lookup from combining observations from opposite sides of a move into an
	// absence which never existed. Writers are serialized by rangeBlock.mu.
	revision atomic.Uint64
	// palette is a one-way dense representation attempted before allocating a
	// seventh simultaneous bitmap group. Unsupported histories may keep using
	// the bitmap representation through its physical capacity of sixteen. The
	// runtime ABI depends only on active at offset zero.
	palette   atomic.Pointer[compactPalette]
	lifecycle lifecycleID
}

// compactAccess is one COW snapshot used by full-block traversal. Callers can
// merge these bounded entries with materialized slots in global address order,
// invoke their visitors, and commit only after every source group was visited.
// Delayed commit prevents an early convergent transition from hiding a later
// source group and therefore a possible conflict.
type compactAccess struct {
	group  *compactGroup
	anchor uintptr
	state  *VarState
}

func newCompactGroups() *compactGroups {
	return &compactGroups{lifecycle: allocateLifecycleID()}
}

func (c *compactGroups) ensureLifecycle() lifecycleID {
	if c.lifecycle == (lifecycleID{}) {
		c.lifecycle = allocateLifecycleID()
	}
	return c.lifecycle
}

// newGenerationState returns an unpublished zero state in this block's
// allocator lifetime. It is used for uncovered/default history so later
// compact adoption cannot accidentally cross a generation boundary.
// The caller holds the block lock and owns the returned mutable state.
func (c *compactGroups) newGenerationState() *VarState {
	return &VarState{lifecycleID: c.ensureLifecycle()}
}

// activate permanently closes the runtime block-default fast path before an
// exact compact mapping can become visible. Store is intentionally stronger
// than the minimum release ordering and pairs with the runtime's atomic load.
func (c *compactGroups) activate() {
	c.active.Store(1)
}

func compactAnchor(anchor uintptr) uintptr {
	return anchor & (rangeBlockSize - 1)
}

func compactBit(anchor uintptr) (word int, mask uint64) {
	offset := compactAnchor(anchor)
	return int(offset >> 6), uint64(1) << (offset & 63)
}

func compactMember(g *compactGroup, anchor uintptr) bool {
	word, mask := compactBit(anchor)
	return g != nil && g.members[word].Load()&mask != 0
}

func compactSetMember(g *compactGroup, anchor uintptr) {
	word, mask := compactBit(anchor)
	for {
		old := g.members[word].Load()
		if old&mask != 0 || g.members[word].CompareAndSwap(old, old|mask) {
			return
		}
	}
}

func compactClearMember(g *compactGroup, anchor uintptr) {
	word, mask := compactBit(anchor)
	for {
		old := g.members[word].Load()
		if old&mask == 0 || g.members[word].CompareAndSwap(old, old&^mask) {
			return
		}
	}
}

func compactClearMembershipMask(g *compactGroup, word int, mask uint64) {
	for {
		old := g.members[word].Load()
		if old&mask == 0 || g.members[word].CompareAndSwap(old, old&^mask) {
			return
		}
	}
}

func compactAtomicSet(word *atomic.Uint64, mask uint64) {
	for {
		old := word.Load()
		if old&mask == mask || word.CompareAndSwap(old, old|mask) {
			return
		}
	}
}

func compactAtomicClear(word *atomic.Uint64, mask uint64) {
	for {
		old := word.Load()
		if old&mask == 0 || word.CompareAndSwap(old, old&^mask) {
			return
		}
	}
}

func (c *compactGroups) beginMutation() {
	c.revision.Add(1)
}

func (c *compactGroups) endMutation() {
	c.revision.Add(1)
}

func (g *compactGroup) firstAnchor() (uintptr, bool) {
	if g == nil {
		return 0, false
	}
	for word := range g.members {
		value := g.members[word].Load()
		if value != 0 {
			return uintptr(word*64 + bits.TrailingZeros64(value)), true
		}
	}
	return 0, false
}

func (g *compactGroup) empty() bool {
	_, ok := g.firstAnchor()
	return !ok
}

func (g *compactGroup) soleMember(anchor uintptr) bool {
	want := compactAnchor(anchor)
	for word := range g.members {
		value := g.members[word].Load()
		if word == int(want>>6) {
			value &^= uint64(1) << (want & 63)
		}
		if value != 0 {
			return false
		}
	}
	return compactMember(g, want)
}

// lookup returns the current exact state without taking the block lock. A move
// publishes destination membership before retiring source membership; fixed
// slot-order lookup therefore observes either the complete old mapping or the
// complete new mapping, never an absence gap.
//
// When a caller first observed a nil materialized slot, a nil result must be
// followed by one slot reload: materialization publishes the slot before it
// retires compact membership, so that retry closes the only lookup race.
//
//go:nosplit
func (c *compactGroups) lookup(anchor uintptr) *VarState {
	state, _ := c.lookupExact(anchor)
	return state
}

// lookupExact distinguishes an authoritative zero tombstone from a never
// represented anchor. Active membership is checked first: clear publishes its
// tombstone before retiring the source, and a fresh access publishes its
// destination before clearing the tombstone, so overlap always resolves to a
// complete ordinary history.
//
// A concurrent mutation returns an authoritative conservative miss instead of
// spinning. Detector transitions hold the block lock and therefore observe a
// stable revision; unlocked page-table/runtime probes must not fall through to
// a potentially unrelated block default while publication is in flight.
//
//go:nosplit
func (c *compactGroups) lookupExact(anchor uintptr) (*VarState, bool) {
	if c == nil {
		return nil, false
	}
	before := c.revision.Load()
	if before&1 != 0 {
		return nil, true
	}
	if palette := c.palette.Load(); palette != nil {
		state, authoritative, owner := palette.lookup(compactAnchor(anchor))
		after := c.revision.Load()
		if before != after || after&1 != 0 || palette.owner(compactAnchor(anchor)) != owner {
			return nil, true
		}
		return state, authoritative
	}
	var group *compactGroup
	var state *VarState
	for i := range c.groups {
		candidateGroup := c.groups[i].Load()
		if !compactMember(candidateGroup, anchor) {
			continue
		}
		if candidate := candidateGroup.state.Load(); candidate != nil {
			group = candidateGroup
			state = candidate
			break
		}
	}
	tombstone := c.isTombstone(anchor)
	if group != nil {
		// Publish exposure before validating the mapping. A sole-member writer
		// begins an odd revision before consulting this bit, so it may reuse the
		// state only when no successful unlocked lookup could have retained it.
		group.exposed.Store(1)
	}
	after := c.revision.Load()
	if before != after || after&1 != 0 ||
		group != nil && (!compactMember(group, anchor) || group.state.Load() != state) {
		return nil, true
	}
	if state != nil {
		return state, true
	}
	return nil, tombstone
}

//go:nosplit
func (c *compactGroups) isTombstone(anchor uintptr) bool {
	if c == nil {
		return false
	}
	if palette := c.palette.Load(); palette != nil {
		return palette.owner(compactAnchor(anchor)) == compactPaletteTombstone
	}
	word, mask := compactBit(anchor)
	return c.tombstones[word].Load()&mask != 0
}

// tombstoned is the page-table spelling of isTombstone.
//
//go:nosplit
func (c *compactGroups) tombstoned(anchor uintptr) bool {
	return c.isTombstone(anchor)
}

func (c *compactGroups) setTombstone(anchor uintptr) {
	word, mask := compactBit(anchor)
	compactAtomicSet(&c.tombstones[word], mask)
}

func (c *compactGroups) clearTombstone(anchor uintptr) {
	word, mask := compactBit(anchor)
	compactAtomicClear(&c.tombstones[word], mask)
}

// lookupGroup is the block-locked form used by transitions. overlap=false is
// part of the representation invariant; a detected overlap fails closed.
func (c *compactGroups) lookupGroup(anchor uintptr) (found *compactGroup, overlap bool) {
	for i := range c.groups {
		group := c.groups[i].Load()
		if !compactMember(group, anchor) {
			continue
		}
		if found != nil {
			return nil, true
		}
		found = group
	}
	return found, false
}

// compactDescriptorFromState snapshots every supported ordinary-history
// field. Callers either hold accessMu or pass an immutable published state.
func compactDescriptorFromState(state *VarState) (compactHistoryDescriptor, bool) {
	if state == nil || state.atomicState.Load() != nil || state.lifecycleID == (lifecycleID{}) {
		return compactHistoryDescriptor{}, false
	}

	state.mu.lock()
	defer state.mu.unlock()

	if state.readClock != nil || state.readerCount > 1 ||
		state.readerState.Load() != uint32(state.readerCount) ||
		state.writeStackHash != 0 || state.readStackHash != 0 {
		return compactHistoryDescriptor{}, false
	}

	read := epoch.Epoch(0)
	switch state.readerCount {
	case 0:
		if state.readEpoch0.Load() != 0 {
			return compactHistoryDescriptor{}, false
		}
	case 1:
		read = epoch.Epoch(state.readEpoch0.Load())
		if read == 0 || state.readEpochs[0] != read {
			return compactHistoryDescriptor{}, false
		}
	default:
		return compactHistoryDescriptor{}, false
	}
	for i := int(state.readerCount); i < len(state.readEpochs); i++ {
		if state.readEpochs[i] != 0 {
			return compactHistoryDescriptor{}, false
		}
	}

	write := epoch.Epoch(state.W.Load())
	owner := state.exclusiveWriter.Load()
	if owner < -1 {
		return compactHistoryDescriptor{}, false
	}
	if write == 0 {
		if owner != 0 || state.writeCount != 0 || state.writePC.Load() != 0 {
			return compactHistoryDescriptor{}, false
		}
	} else if owner >= 0 {
		writeTID, _ := write.Decode()
		if owner != int64(writeTID) {
			return compactHistoryDescriptor{}, false
		}
	}

	return compactHistoryDescriptor{
		history: compactHistoryKey{
			write:           write,
			read:            read,
			exclusiveWriter: owner,
			writePC:         state.writePC.Load(),
			readPC:          state.readPC.Load(),
			writeCount:      state.writeCount,
		},
		lifecycle: state.lifecycleID,
	}, true
}

func initializeCompactState(state *VarState, descriptor compactHistoryDescriptor) {
	// Set every ordinary-history field explicitly. In particular, do not assign
	// an atomic-containing VarState as a struct: the full-capacity reuse path
	// reinitializes an existing object while publication is held at odd revision.
	state.W.Store(0)
	state.exclusiveWriter.Store(0)
	state.writePC.Store(0)
	state.readPC.Store(0)
	state.readEpoch0.Store(0)
	state.readerState.Store(0)
	state.writeCount = 0
	for i := range state.readEpochs {
		state.readEpochs[i] = 0
	}
	state.readerCount = 0
	state.lifecycleID = descriptor.lifecycle
	state.readClock = nil
	state.writeStackHash = 0
	state.readStackHash = 0
	state.atomicState.Store(nil)

	key := descriptor.history
	state.W.Store(uint64(key.write))
	state.exclusiveWriter.Store(key.exclusiveWriter)
	state.writePC.Store(key.writePC)
	state.readPC.Store(key.readPC)
	state.writeCount = key.writeCount
	if key.read != 0 {
		state.readEpoch0.Store(uint64(key.read))
		state.readerState.Store(1)
		state.readEpochs[0] = key.read
		state.readerCount = 1
	}

}

func compactStateFromDescriptor(descriptor compactHistoryDescriptor) *VarState {
	state := new(VarState)
	initializeCompactState(state, descriptor)
	return state
}

func compactHappensBefore(e epoch.Epoch, clock *vectorclock.VectorClock) bool {
	return e == 0 || clock != nil && e.HappensBefore(clock)
}

func (key compactHistoryKey) afterRead(current epoch.Epoch, clock *vectorclock.VectorClock, pc uintptr) (compactHistoryKey, bool) {
	if current == 0 || !compactHappensBefore(key.write, clock) {
		return compactHistoryKey{}, false
	}
	next := key
	next.readPC = pc
	if key.read == current {
		return next, true
	}
	if key.read == 0 {
		next.read = current
		return next, true
	}
	existingTID, _ := key.read.Decode()
	currentTID, _ := current.Decode()
	if existingTID == currentTID || compactHappensBefore(key.read, clock) {
		next.read = current
		return next, true
	}
	return compactHistoryKey{}, false
}

func (key compactHistoryKey) afterWrite(current epoch.Epoch, clock *vectorclock.VectorClock, pc uintptr) (compactHistoryKey, bool) {
	if current == 0 || !compactHappensBefore(key.write, clock) || !compactHappensBefore(key.read, clock) {
		return compactHistoryKey{}, false
	}
	next := key
	if key.write == current && key.read == 0 {
		next.writePC = pc
		return next, true
	}

	currentTID, _ := current.Decode()
	if next.exclusiveWriter == 0 {
		next.exclusiveWriter = int64(currentTID)
	} else if next.exclusiveWriter > 0 && next.exclusiveWriter != int64(currentTID) {
		next.exclusiveWriter = -1
	}
	next.write = current
	next.writeCount++
	next.writePC = pc
	next.read = 0
	// Demote deliberately leaves readPC intact in VarState; it remains part of
	// the complete key even when there is no represented read epoch.
	return next, true
}

func (c *compactGroups) findDescriptor(descriptor compactHistoryDescriptor, exclude *compactGroup) *compactGroup {
	for i := range c.groups {
		group := c.groups[i].Load()
		if group == nil || group == exclude || group.retired || !group.joinable ||
			group.descriptor != descriptor || group.state.Load() == nil {
			continue
		}
		// Empty joinable groups are bounded descriptor caches. Their immutable
		// state can be reused when a later anchor repeats this pipeline stage.
		return group
	}
	return nil
}

func (c *compactGroups) reusableSlot() int {
	// Once the bitmap representation reaches the palette storage crossover,
	// reuse an existing empty object before consuming another physical slot.
	// This keeps temporal descriptor churn cheap without forcing dense storage.
	if c.allocatedGroupCount() >= compactPaletteBitmapCrossover {
		for i := range c.groups {
			group := c.groups[i].Load()
			if group != nil && group.empty() && (group.retired || group.joinable) {
				return i
			}
		}
	}
	// Prefer never-used and explicitly retired slots. This preserves any small
	// exact-descriptor working set while the table still has disposable space.
	for i := range c.groups {
		group := c.groups[i].Load()
		if group == nil || group.retired && group.empty() {
			return i
		}
	}
	// Capacity is a bound on live equivalence classes, not on every descriptor
	// the block has ever visited. An empty joinable group has no authoritative
	// members, so its cache slot may be recycled for a new live descriptor.
	for i := range c.groups {
		group := c.groups[i].Load()
		if group != nil && !group.retired && group.joinable && group.empty() {
			return i
		}
	}
	return -1
}

// recycleGroup retargets an already-published but empty group. The owning block
// lock is held and the compact publication revision is odd. A state which has
// never escaped lookup/range publication can be reinitialized in place. Once a
// state has been exposed, its pointer is immutable forever, so recycling keeps
// that old object intact and publishes fresh storage instead.
func (g *compactGroup) recycleGroup(descriptor compactHistoryDescriptor) *VarState {
	state := g.state.Load()
	if state != nil && g.exposed.Load() == 0 {
		initializeCompactState(state, descriptor)
	} else {
		state = compactStateFromDescriptor(descriptor)
		g.state.Store(state)
		g.exposed.Store(0)
	}
	g.descriptor = descriptor
	g.joinable = true
	g.retired = false
	return state
}

// moveDescriptor atomically redirects one exact bit to descriptor. Existing
// destination groups, including empty cached descriptors, are joined without
// allocating. A VarState and group are allocated only after proving a new exact
// key is required. At capacity, a sole source is COW-replaced in its existing
// group; every other failure leaves the old mapping unchanged so the caller can
// materialize the word. An exact descriptor match is already the complete
// represented transition, so it returns the published immutable state without
// changing membership or allocating.
func (c *compactGroups) moveDescriptor(anchor uintptr, source *compactGroup, descriptor compactHistoryDescriptor) (*VarState, bool) {
	if source != nil && source.joinable && source.descriptor == descriptor {
		state := source.state.Load()
		return state, state != nil
	}

	if destination := c.findDescriptor(descriptor, source); destination != nil {
		// Destination first, source second: lock-free lookup always sees at least
		// one complete mapping during the move. An emptied source remains a
		// bounded joinable cache for its immutable state and descriptor.
		c.activate()
		c.beginMutation()
		compactSetMember(destination, anchor)
		if source != nil {
			compactClearMember(source, anchor)
		}
		c.clearTombstone(anchor)
		c.endMutation()
		return destination.state.Load(), true
	}

	slot := c.reusableSlot()
	if slot >= 0 && c.groups[slot].Load() == nil && c.allocatedGroupCount() >= compactPaletteBitmapCrossover {
		if source != nil && source.soleMember(anchor) {
			// Reuse the sole source in place instead of allocating a seventh
			// object merely to leave the source empty.
			slot = -1
		} else if palette := c.upgradePalette(); palette != nil {
			return palette.moveAnchor(c, compactAnchor(anchor), descriptor)
		}
	}
	if slot < 0 {
		// With no descriptor slot available, a sole-member source can change
		// keys in place at the group level. Replace the immutable state pointer
		// and descriptor under the publication sequence; membership does not
		// move, and readers which retained the old state still see it unchanged.
		if source == nil || !source.soleMember(anchor) {
			if palette := c.upgradePalette(); palette != nil {
				return palette.moveAnchor(c, compactAnchor(anchor), descriptor)
			}
			return nil, false
		}
		c.activate()
		c.beginMutation()
		state := source.state.Load()
		if state == nil {
			c.endMutation()
			return nil, false
		}
		if source.exposed.Load() == 0 {
			// No unlocked caller can hold this pointer, so reuse its storage. The
			// descriptor transition preserves the allocator lifecycle.
			initializeCompactState(state, descriptor)
		} else {
			// An exposed state is immutable forever. Publish a fresh replacement
			// and make exposure describe that new current pointer.
			state = compactStateFromDescriptor(descriptor)
			source.state.Store(state)
			source.exposed.Store(0)
		}
		source.descriptor = descriptor
		source.joinable = true
		source.retired = false
		c.clearTombstone(anchor)
		c.endMutation()
		return state, true
	}
	group := c.groups[slot].Load()
	if group == nil {
		state := compactStateFromDescriptor(descriptor)
		group = &compactGroup{descriptor: descriptor, joinable: true}
		group.state.Store(state)
		compactSetMember(group, anchor)
		c.activate()
		c.beginMutation()
		c.groups[slot].Store(group)
		if source != nil {
			compactClearMember(source, anchor)
		}
		c.clearTombstone(anchor)
		c.endMutation()
		return state, true
	}

	// The selected existing group is empty, so no lookup can newly expose its
	// state. Enter the odd revision before consulting exposed to also exclude a
	// successful lookup which started before the membership was retired.
	c.activate()
	c.beginMutation()
	state := group.recycleGroup(descriptor)
	compactSetMember(group, anchor)
	if source != nil {
		compactClearMember(source, anchor)
	}
	c.clearTombstone(anchor)
	c.endMutation()
	return state, true
}

// tryRead applies the supported simple FastTrack read transition to an exact
// compact anchor. It handles virgin zero, W-only, R-only, and W+single-R
// histories. Conflicts, concurrent-reader promotion, unsupported metadata,
// overlap, and capacity overflow fail without mutation. A true no-op succeeds
// with the source's exact immutable state and does not mutate or materialize.
// The caller holds the block lock and has excluded atomic/RWMutex markers.
func (c *compactGroups) tryRead(anchor uintptr, current epoch.Epoch, clock *vectorclock.VectorClock, pc uintptr) (*VarState, bool) {
	if c == nil {
		return nil, false
	}
	if palette := c.palette.Load(); palette != nil {
		return palette.tryScalar(c, compactAnchor(anchor), current, clock, pc, false)
	}
	source, overlap := c.lookupGroup(anchor)
	if overlap {
		return nil, false
	}
	descriptor := compactHistoryDescriptor{lifecycle: c.ensureLifecycle()}
	if source != nil {
		if !source.joinable || source.state.Load() == nil {
			return nil, false
		}
		descriptor = source.descriptor
	}
	next, ok := descriptor.history.afterRead(current, clock, pc)
	if !ok {
		return nil, false
	}
	descriptor.history = next
	return c.moveDescriptor(anchor, source, descriptor)
}

// tryWrite is the write counterpart of tryRead. Its key transition exactly
// mirrors applyOrdinaryWriteLocked for supported histories.
func (c *compactGroups) tryWrite(anchor uintptr, current epoch.Epoch, clock *vectorclock.VectorClock, pc uintptr) (*VarState, bool) {
	if c == nil {
		return nil, false
	}
	if palette := c.palette.Load(); palette != nil {
		return palette.tryScalar(c, compactAnchor(anchor), current, clock, pc, true)
	}
	source, overlap := c.lookupGroup(anchor)
	if overlap {
		return nil, false
	}
	descriptor := compactHistoryDescriptor{lifecycle: c.ensureLifecycle()}
	if source != nil {
		if !source.joinable || source.state.Load() == nil {
			return nil, false
		}
		descriptor = source.descriptor
		// Alternating/mutable hot locations should become authoritative word
		// slots instead of permanently paying the bounded compact scan. Gob's
		// critical bulk flow is write-then-read; a later write after any read
		// frontier is conservatively promoted without changing compact history.
		if descriptor.history.read != 0 && source.soleMember(anchor) {
			return nil, false
		}
	}
	next, ok := descriptor.history.afterWrite(current, clock, pc)
	if !ok {
		return nil, false
	}
	descriptor.history = next
	// Preserve the scalar write hotness policy: an identical compact write
	// promotes through the ordinary slot path. Read no-ops instead remain
	// compact because Detector can safely publish their exact immutable state
	// in the redundant-read cache.
	if source != nil && source.descriptor == descriptor && source.soleMember(anchor) {
		return nil, false
	}
	return c.moveDescriptor(anchor, source, descriptor)
}

// groupWordMask returns the exact lanes represented by group in the aligned
// application word at wordOffset. wordOffset is a byte offset or absolute
// address; only its low block bits are used.
func groupWordMask(group *compactGroup, wordOffset uintptr) uint8 {
	base := compactAnchor(wordOffset) &^ uintptr(7)
	mask := uint8(0)
	for lane := uintptr(0); lane < 8; lane++ {
		if compactMember(group, base+lane) {
			mask |= uint8(1) << lane
		}
	}
	return mask
}

// coveredWord returns the union of compact membership in one aligned word.
// The caller holds the block lock; lock-free readers may also safely call it.
func (c *compactGroups) coveredWord(wordOffset uintptr) uint8 {
	if c == nil {
		return 0
	}
	if palette := c.palette.Load(); palette != nil {
		return palette.coveredWord(wordOffset)
	}
	mask := uint8(0)
	for i := range c.groups {
		mask |= groupWordMask(c.groups[i].Load(), wordOffset)
	}
	return mask
}

// wordTombstoneMask returns exact authoritative-zero lanes in one aligned
// word. Default materialization must exclude these lanes before publishing its
// slot; compact histories, if any, retain precedence.
func (c *compactGroups) wordTombstoneMask(wordOffset uintptr) uint8 {
	if c == nil {
		return 0
	}
	if palette := c.palette.Load(); palette != nil {
		return palette.tombstoneWord(wordOffset)
	}
	base := compactAnchor(wordOffset) &^ uintptr(7)
	mask := uint8(0)
	for lane := uintptr(0); lane < 8; lane++ {
		if c.isTombstone(base + lane) {
			mask |= uint8(1) << lane
		}
	}
	return mask
}

// materializeWord clones each intersecting compact history once into an
// unpublished empty slot. The caller publishes the fully initialized slot and
// only then calls retireWord; this ordering preserves exact lookup semantics.
func (c *compactGroups) materializeWord(wordOffset uintptr, slot *ShadowSlot) {
	if c == nil || slot == nil {
		return
	}
	if palette := c.palette.Load(); palette != nil {
		palette.materializeWord(wordOffset, slot)
		return
	}
	for i := range c.groups {
		group := c.groups[i].Load()
		mask := groupWordMask(group, wordOffset)
		if mask == 0 {
			continue
		}
		state := group.state.Load()
		if state == nil {
			continue
		}
		state.LockAccess()
		clone := state.CloneOrdinaryLocked()
		state.UnlockAccess()
		for lane := uint8(0); lane < shadowSlotLanes; lane++ {
			if mask&(uint8(1)<<lane) != 0 {
				slot.states[lane].Store(clone)
			}
		}
	}
}

// retireWord permanently removes compact membership for one materialized word.
func (c *compactGroups) retireWord(wordOffset uintptr) {
	c.retireRange(wordOffset&^uintptr(7), 8)
}

// retireRange removes exact memberships in one block-local range. Partial
// clear/materialization callers must first publish permanent authoritative word
// slots; removed anchors must never fall back to compact/default history.
func (c *compactGroups) retireRange(offset, size uintptr) {
	if c == nil || size == 0 {
		return
	}
	if palette := c.palette.Load(); palette != nil {
		c.beginMutation()
		palette.setRangeOwner(offset, size, compactPaletteDefault)
		c.endMutation()
		return
	}
	c.beginMutation()
	c.removeMembershipRange(offset, size)
	c.clearTombstoneRangeRaw(offset, size)
	c.endMutation()
}

// clearRange is the conservative allocation-free clear used by direct callers.
// It always advances the lifecycle, including for a repeated tombstone clear.
func (c *compactGroups) clearRange(offset, size uintptr) {
	c.clearRangeWithHistory(offset, size, true, true)
}

// clearRangeKnownDefault is the allocation-free compact half of a partial
// allocator clear. The caller holds the block lock and drains the block default
// separately when selected anchors can inherit it. A clear with neither an
// inherited default nor represented compact history has no report identity to
// retire, so repeating that virgin/tombstone clear can retain the lifecycle.
func (c *compactGroups) clearRangeKnownDefault(offset, size uintptr, defaultHasHistory bool) {
	c.clearRangeWithHistory(offset, size, defaultHasHistory, false)
}

func (c *compactGroups) clearRangeWithHistory(offset, size uintptr, defaultHasHistory, forceAdvance bool) {
	if c == nil || size == 0 {
		return
	}
	if palette := c.palette.Load(); palette != nil {
		palette.clearRange(c, offset, size, defaultHasHistory, forceAdvance)
		return
	}
	start, end := compactRange(offset, size)
	if start == end {
		return
	}
	advanceLifecycle := forceAdvance
	if !advanceLifecycle && defaultHasHistory {
		for anchor := start; anchor < end; anchor++ {
			if !c.isTombstone(anchor) {
				advanceLifecycle = true
				break
			}
		}
	}
	var locked [compactGroupCapacity]*VarState
	lockedCount := 0
	for i := range c.groups {
		group := c.groups[i].Load()
		if group == nil || !group.intersects(start, end) {
			continue
		}
		advanceLifecycle = true
		state := group.state.Load()
		if state == nil {
			continue
		}
		duplicate := false
		for j := 0; j < lockedCount; j++ {
			if locked[j] == state {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		if binding := state.atomicState.Load(); binding != nil {
			binding.escapeIncompatible()
		}
		state.LockAccess()
		if binding := state.atomicState.Load(); binding != nil {
			binding.escapeIncompatible()
		}
		locked[lockedCount] = state
		lockedCount++
	}
	c.activate()
	c.beginMutation()
	c.setTombstoneRange(start, end-start)
	c.removeMembershipRange(start, end-start)
	if advanceLifecycle {
		c.lifecycle = allocateLifecycleID()
	}
	c.endMutation()
	for i := lockedCount - 1; i >= 0; i-- {
		locked[i].UnlockAccess()
	}
}

func compactRange(offset, size uintptr) (start, end uintptr) {
	start = compactAnchor(offset)
	if size > rangeBlockSize-start {
		size = rangeBlockSize - start
	}
	return start, start + size
}

func (g *compactGroup) intersects(start, end uintptr) bool {
	if g == nil || start >= end {
		return false
	}
	firstWord := int(start >> 6)
	lastWord := int((end - 1) >> 6)
	for word := firstWord; word <= lastWord; word++ {
		value := g.members[word].Load()
		wordStart := uintptr(word * 64)
		lo, hi := start, end
		if lo < wordStart {
			lo = wordStart
		}
		if limit := wordStart + 64; hi > limit {
			hi = limit
		}
		width := hi - lo
		var mask uint64
		if width == 64 {
			mask = ^uint64(0)
		} else {
			mask = ((uint64(1) << width) - 1) << (lo & 63)
		}
		if value&mask != 0 {
			return true
		}
	}
	return false
}

func (c *compactGroups) setTombstoneRange(offset, size uintptr) {
	start, end := compactRange(offset, size)
	if start == end {
		return
	}
	if palette := c.palette.Load(); palette != nil {
		c.activate()
		palette.setRangeOwner(start, end-start, compactPaletteTombstone)
		return
	}
	c.activate()
	c.updateTombstoneRange(start, end, true)
}

// publishTombstoneRange is the allocation-free publication primitive used by
// page-table clear after it has drained compact/default publishers. Callers
// normally prefer clearRange, which also retires intersecting memberships and
// advances the generation as one protocol.
func (c *compactGroups) publishTombstoneRange(offset, size uintptr) {
	c.beginMutation()
	c.setTombstoneRange(offset, size)
	c.endMutation()
}

// publishTombstoneMask is the aligned-word form used by exact clear loops.
func (c *compactGroups) publishTombstoneMask(wordOffset uintptr, mask uint8) {
	if mask == 0 {
		return
	}
	c.activate()
	c.beginMutation()
	base := compactAnchor(wordOffset) &^ uintptr(7)
	if palette := c.palette.Load(); palette != nil {
		for lane := uintptr(0); lane < 8; lane++ {
			if mask&(uint8(1)<<lane) != 0 {
				palette.setRangeOwner(base+lane, 1, compactPaletteTombstone)
			}
		}
		c.endMutation()
		return
	}
	for lane := uintptr(0); lane < 8; lane++ {
		if mask&(uint8(1)<<lane) != 0 {
			c.setTombstone(base + lane)
		}
	}
	c.endMutation()
}

func (c *compactGroups) clearTombstoneRange(offset, size uintptr) {
	c.beginMutation()
	c.clearTombstoneRangeRaw(offset, size)
	c.endMutation()
}

func (c *compactGroups) clearTombstoneRangeRaw(offset, size uintptr) {
	start, end := compactRange(offset, size)
	if palette := c.palette.Load(); palette != nil {
		for anchor := start; anchor < end; anchor++ {
			if palette.owner(anchor) == compactPaletteTombstone {
				palette.setOwner(anchor, compactPaletteDefault)
			}
		}
		return
	}
	c.updateTombstoneRange(start, end, false)
}

// clearTombstoneMask is the destination-published counterpart of
// publishTombstoneMask.
func (c *compactGroups) clearTombstoneMask(wordOffset uintptr, mask uint8) {
	c.beginMutation()
	base := compactAnchor(wordOffset) &^ uintptr(7)
	if palette := c.palette.Load(); palette != nil {
		for lane := uintptr(0); lane < 8; lane++ {
			if mask&(uint8(1)<<lane) != 0 && palette.owner(base+lane) == compactPaletteTombstone {
				palette.setOwner(base+lane, compactPaletteDefault)
			}
		}
		c.endMutation()
		return
	}
	for lane := uintptr(0); lane < 8; lane++ {
		if mask&(uint8(1)<<lane) != 0 {
			c.clearTombstone(base + lane)
		}
	}
	c.endMutation()
}

func (c *compactGroups) updateTombstoneRange(start, end uintptr, set bool) {
	for current := start; current < end; {
		word := int(current >> 6)
		wordEnd := (current | 63) + 1
		if wordEnd > end {
			wordEnd = end
		}
		width := wordEnd - current
		var mask uint64
		if width == 64 {
			mask = ^uint64(0)
		} else {
			mask = ((uint64(1) << width) - 1) << (current & 63)
		}
		if set {
			compactAtomicSet(&c.tombstones[word], mask)
		} else {
			compactAtomicClear(&c.tombstones[word], mask)
		}
		current = wordEnd
	}
}

func (c *compactGroups) removeMembershipRange(offset, size uintptr) {
	if palette := c.palette.Load(); palette != nil {
		palette.setRangeOwner(offset, size, compactPaletteDefault)
		return
	}
	start := compactAnchor(offset)
	if size > rangeBlockSize-start {
		size = rangeBlockSize - start
	}
	end := start + size
	for i := range c.groups {
		group := c.groups[i].Load()
		if group == nil {
			continue
		}
		for current := start; current < end; {
			word := int(current >> 6)
			wordEnd := (current | 63) + 1
			if wordEnd > end {
				wordEnd = end
			}
			width := wordEnd - current
			var mask uint64
			if width == 64 {
				mask = ^uint64(0)
			} else {
				mask = ((uint64(1) << width) - 1) << (current & 63)
			}
			compactClearMembershipMask(group, word, mask)
			current = wordEnd
		}
		if group.empty() {
			group.retired = true
			group.joinable = false
		}
	}
}

// snapshotAccesses clones active source groups and sorts the bounded result by
// the lowest exact application address. The caller holds the block lock. Clone
// states remain mutable and unpublished until commitAccesses.
func (c *compactGroups) snapshotAccesses(blockBase uintptr, dst *[compactGroupCapacity]compactAccess) int {
	if c == nil || dst == nil {
		return 0
	}
	count := 0
	for i := range c.groups {
		group := c.groups[i].Load()
		first, ok := group.firstAnchor()
		if !ok {
			continue
		}
		state := group.state.Load()
		if state == nil {
			continue
		}
		state.LockAccess()
		clone := state.CloneOrdinaryLocked()
		state.UnlockAccess()
		clone.LockAccess()
		dst[count] = compactAccess{group: group, anchor: blockBase + first, state: clone}
		count++
	}
	for i := 1; i < count; i++ {
		entry := dst[i]
		j := i
		for j > 0 && entry.anchor < dst[j-1].anchor {
			dst[j] = dst[j-1]
			j--
		}
		dst[j] = entry
	}
	return count
}

// commitAccesses publishes every already-visited COW state, then merges exact
// supported descriptors which converged. Delaying all publication/merging
// until the complete batch was visited preserves one callback per source
// history and cannot hide a conflict.
func (c *compactGroups) commitAccesses(src *[compactGroupCapacity]compactAccess, count int) {
	if c == nil || src == nil {
		return
	}
	if count > len(src) {
		count = len(src)
	}
	for i := 0; i < count; i++ {
		entry := &src[i]
		if entry.group == nil || entry.state == nil {
			continue
		}
		// Ordinary full-range visitors cannot create an atomic overlay: compact
		// admission excluded one and atomic capture only mutates an existing
		// overlay. Refuse silent publication if that boundary is ever violated.
		if entry.state.atomicState.Load() != nil {
			runtimeThrow("race detector compact range transition created atomic history")
		}
		descriptor, ok := compactDescriptorFromState(entry.state)
		entry.group.joinable = ok
		if ok {
			entry.group.descriptor = descriptor
		} else {
			entry.group.descriptor = compactHistoryDescriptor{lifecycle: entry.state.lifecycleID}
		}
	}
	c.beginMutation()
	for i := 0; i < count; i++ {
		entry := &src[i]
		if entry.group != nil && entry.state != nil {
			entry.group.state.Store(entry.state)
			// The range callback received entry.state before commit and may retain
			// it, so publication must preserve it as externally exposed.
			entry.group.exposed.Store(1)
		}
	}
	c.mergeEquivalent()
	c.endMutation()
	for i := 0; i < count; i++ {
		entry := &src[i]
		if entry.group != nil && entry.state != nil {
			entry.state.UnlockAccess()
		}
	}
}

// mergeEquivalent coalesces convergent exact keys after all source groups were
// visited. Destination bits are published before source retirement.
func (c *compactGroups) mergeEquivalent() {
	for i := 0; i < len(c.groups); i++ {
		destination := c.groups[i].Load()
		if destination == nil || destination.retired || !destination.joinable || destination.empty() {
			continue
		}
		for j := i + 1; j < len(c.groups); j++ {
			source := c.groups[j].Load()
			if source == nil || source.retired || !source.joinable || source.empty() || source.descriptor != destination.descriptor {
				continue
			}
			for word := range source.members {
				value := source.members[word].Load()
				for value != 0 {
					bit := bits.TrailingZeros64(value)
					anchor := uintptr(word*64 + bit)
					compactSetMember(destination, anchor)
					compactClearMember(source, anchor)
					value &^= uint64(1) << bit
				}
			}
			source.retired = true
			source.joinable = false
		}
	}
}

// accessAll is the compact-only traversal convenience. Page-table full-block
// traversal normally uses snapshotAccesses directly so it can globally
// interleave compact representatives with materialized slots and defaults.
func (c *compactGroups) accessAll(blockBase uintptr, visit func(word uintptr, mask uint8, state *VarState)) {
	if c == nil || visit == nil {
		return
	}
	var accesses [compactGroupCapacity]compactAccess
	count := c.snapshotAccesses(blockBase, &accesses)
	for i := 0; i < count; i++ {
		entry := &accesses[i]
		offset := entry.anchor - blockBase
		visit(entry.anchor&^uintptr(7), uint8(1)<<(offset&7), entry.state)
	}
	c.commitAccesses(&accesses, count)
}

// reset is the full-block clear operation. It closes atomic capabilities,
// drains every distinct access transaction, removes all memberships and group
// pointers, then advances the block generation. The compactGroups allocation
// itself remains permanent so external lock-free readers never dereference a
// reclaimed header.
func (c *compactGroups) reset() {
	if c == nil {
		return
	}
	if palette := c.palette.Load(); palette != nil {
		palette.reset(c)
		return
	}
	newLifecycle := allocateLifecycleID()
	var locked [compactGroupCapacity]*VarState
	lockedCount := 0
	for i := range c.groups {
		group := c.groups[i].Load()
		if group == nil || group.empty() {
			continue
		}
		state := group.state.Load()
		if state == nil {
			continue
		}
		duplicate := false
		for j := 0; j < lockedCount; j++ {
			if locked[j] == state {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		if binding := state.atomicState.Load(); binding != nil {
			binding.escapeIncompatible()
		}
		state.LockAccess()
		if binding := state.atomicState.Load(); binding != nil {
			binding.escapeIncompatible()
		}
		locked[lockedCount] = state
		lockedCount++
	}
	c.beginMutation()
	for i := range c.groups {
		group := c.groups[i].Load()
		if group != nil {
			for word := range group.members {
				group.members[word].Store(0)
			}
		}
		c.groups[i].Store(nil)
	}
	for word := range c.tombstones {
		c.tombstones[word].Store(0)
	}
	c.lifecycle = newLifecycle
	c.endMutation()
	for i := lockedCount - 1; i >= 0; i-- {
		locked[i].UnlockAccess()
	}
}
