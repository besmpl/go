//go:build amd64 || arm64

package shadowmem

import (
	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/vectorclock"
)

// compactRangeSourceCapacity includes every compact group plus the block
// default and the authoritative-zero tombstone class.
const compactRangeSourceCapacity = compactGroupCapacity + 2

type compactRangeSourceKind uint8

const (
	compactRangeGroupSource compactRangeSourceKind = iota
	compactRangeTombstoneSource
	compactRangeDefaultSource
)

type compactRangeSource struct {
	group      *compactGroup
	descriptor compactHistoryDescriptor
	next       compactHistoryDescriptor
	target     int
	kind       compactRangeSourceKind
}

type compactRangeDestination struct {
	descriptor compactHistoryDescriptor
	group      *compactGroup
	newGroup   *compactGroup
	slot       int
	recycle    bool
}

func compactRangeWordMask(offset, size uintptr, word int) uint64 {
	start := offset
	end := offset + size
	wordStart := uintptr(word) << 6
	wordEnd := wordStart + 64
	if start < wordStart {
		start = wordStart
	}
	if end > wordEnd {
		end = wordEnd
	}
	if start >= end {
		return 0
	}
	width := end - start
	if width == 64 {
		return ^uint64(0)
	}
	return ((uint64(1) << width) - 1) << (start & 63)
}

func compactRangeGroupEmptyAfter(group *compactGroup, offset, size uintptr, keepSelected bool) bool {
	if group == nil {
		return true
	}
	for word := 0; word < compactMembershipWords; word++ {
		value := group.membershipWord(word)
		if !keepSelected {
			value &^= compactRangeWordMask(offset, size, word)
		}
		if value != 0 {
			return false
		}
	}
	return true
}

func compactRangeSourceIndex(sources *[compactRangeSourceCapacity]compactRangeSource, count int, group *compactGroup) int {
	for i := 0; i < count; i++ {
		if sources[i].group == group {
			return i
		}
	}
	return -1
}

// finishCompactRange unlocks the materialized block default after a compact
// range transaction. Range hooks run on the runtime system stack, where a
// heap-backed defer is illegal; keep every exit explicit through this helper.
func finishCompactRange(defaultState *VarState, result bool) bool {
	if defaultState != nil {
		defaultState.UnlockAccess()
	}
	return result
}

// finishCompactRangeBlock is the block-lock counterpart of finishCompactRange.
func finishCompactRangeBlock(block *rangeBlock, result bool) bool {
	block.mu.unlock()
	return result
}

// compactRangeUniformNoop proves that every selected byte resolves through one
// exact immutable bitmap state and that the requested transition does not
// change it. The caller holds the owning block lock, so a successful proof can
// bypass the general fixed-stack transaction planner without publication.
// Defaults, tombstones, overlaps, unsupported states, and real transitions all
// deliberately fall through to the complete planner.
func (c *compactGroups) compactRangeUniformNoop(offset, size uintptr, current epoch.Epoch, clock *vectorclock.VectorClock, pc uintptr, write bool) bool {
	firstWord := int(offset >> 6)
	lastWord := int((offset + size - 1) >> 6)
	var source *compactGroup

	for i := 0; i < compactGroupCapacity; i++ {
		group := c.groupLoad(i)
		if group == nil {
			continue
		}
		intersects := false
		for word := firstWord; word <= lastWord; word++ {
			selected := compactRangeWordMask(offset, size, word)
			if group.membershipWord(word)&selected != 0 {
				intersects = true
				break
			}
		}
		if !intersects {
			continue
		}
		if source != nil || group.retired || !group.joinable {
			return false
		}
		source = group
	}
	if source == nil {
		return false
	}
	for word := firstWord; word <= lastWord; word++ {
		selected := compactRangeWordMask(offset, size, word)
		if source.membershipWord(word)&selected != selected || c.tombstoneWord(word)&selected != 0 {
			return false
		}
	}

	state := source.state.Load()
	descriptor, ok := compactDescriptorFromState(state)
	if !ok || descriptor != source.descriptor || state != source.state.Load() {
		return false
	}
	var next compactHistoryKey
	if write {
		next, ok = descriptor.history.afterWrite(current, clock, pc)
	} else {
		next, ok = descriptor.history.afterRead(current, clock, pc)
	}
	return ok && next == descriptor.history
}

// tryRange applies one conflict-free ordinary transition to every exact byte
// in a block-local fragment. Admission and planning happen before publication;
// a false result never changes compact membership or ordinary history.
func (c *compactGroups) tryRange(offset, size uintptr, defaultState *VarState, current epoch.Epoch, clock *vectorclock.VectorClock, pc uintptr, write bool) bool {
	if c == nil || current == 0 || size == 0 || offset >= rangeBlockSize || size > rangeBlockSize-offset {
		return false
	}
	if palette := c.palette.Load(); palette != nil {
		if defaultState != nil {
			defaultState.LockAccess()
		}
		return finishCompactRange(defaultState, palette.tryRangeLocked(c, offset, size, defaultState, current, clock, pc, write))
	}
	if c.compactRangeUniformNoop(offset, size, current, clock, pc, write) {
		return true
	}

	var sources [compactRangeSourceCapacity]compactRangeSource
	sourceCount := 0
	firstWord := int(offset >> 6)
	lastWord := int((offset + size - 1) >> 6)

	// Partition selected bytes by their exact published compact group. Validate
	// each immutable descriptor instead of trusting stale classification fields.
	for i := 0; i < compactGroupCapacity; i++ {
		group := c.groupLoad(i)
		if group == nil {
			continue
		}
		intersects := false
		for word := firstWord; word <= lastWord; word++ {
			intersection := group.membershipWord(word) & compactRangeWordMask(offset, size, word)
			if intersection == 0 {
				continue
			}
			intersects = true
			for j := 0; j < sourceCount; j++ {
				previous := sources[j].group
				if previous != nil && previous.membershipWord(word)&intersection != 0 {
					return false
				}
			}
		}
		if !intersects {
			continue
		}
		if group.retired || !group.joinable {
			return false
		}
		state := group.state.Load()
		descriptor, ok := compactDescriptorFromState(state)
		if !ok || descriptor != group.descriptor {
			return false
		}
		sources[sourceCount] = compactRangeSource{
			group:      group,
			descriptor: descriptor,
			target:     -1,
		}
		sourceCount++
	}

	// Compact history and tombstones must never overlap. Remaining tombstones
	// form one exact zero class in the current allocator generation.
	tombstoneSelected := false
	for word := firstWord; word <= lastWord; word++ {
		mask := c.tombstoneWord(word) & compactRangeWordMask(offset, size, word)
		if mask == 0 {
			continue
		}
		tombstoneSelected = true
		for i := 0; i < sourceCount; i++ {
			if group := sources[i].group; group != nil && group.membershipWord(word)&mask != 0 {
				return false
			}
		}
	}
	if tombstoneSelected {
		if c.lifecycle == (lifecycleID{}) {
			return false
		}
		sources[sourceCount] = compactRangeSource{
			descriptor: compactHistoryDescriptor{lifecycle: c.lifecycle},
			target:     -1,
			kind:       compactRangeTombstoneSource,
		}
		sourceCount++
	}

	// Every selected byte not covered above inherits the block default (or the
	// virgin zero class). Keep a materialized default transaction locked through
	// destination publication so its descriptor cannot change under the plan.
	defaultSelected := false
	for word := firstWord; word <= lastWord; word++ {
		selected := compactRangeWordMask(offset, size, word)
		covered := c.tombstoneWord(word) & selected
		for i := 0; i < sourceCount; i++ {
			if group := sources[i].group; group != nil {
				covered |= group.membershipWord(word) & selected
			}
		}
		if selected&^covered != 0 {
			defaultSelected = true
			break
		}
	}
	if defaultState != nil {
		defaultState.LockAccess()
	}
	if defaultSelected {
		var descriptor compactHistoryDescriptor
		var ok bool
		if defaultState != nil {
			descriptor, ok = compactDescriptorFromState(defaultState)
		} else if c.lifecycle != (lifecycleID{}) {
			descriptor, ok = compactHistoryDescriptor{lifecycle: c.lifecycle}, true
		}
		if !ok {
			return finishCompactRange(defaultState, false)
		}
		sources[sourceCount] = compactRangeSource{
			descriptor: descriptor,
			target:     -1,
			kind:       compactRangeDefaultSource,
		}
		sourceCount++
	}

	// Compute every post-transition descriptor before choosing or allocating a
	// destination. Conflict, promotion, and other non-representable transitions
	// fail here without any publication side effect.
	for i := 0; i < sourceCount; i++ {
		source := &sources[i]
		var next compactHistoryKey
		var ok bool
		if write {
			next, ok = source.descriptor.history.afterWrite(current, clock, pc)
		} else {
			next, ok = source.descriptor.history.afterRead(current, clock, pc)
		}
		if !ok {
			return finishCompactRange(defaultState, false)
		}
		source.next = source.descriptor
		source.next.history = next
	}

	var destinations [compactRangeSourceCapacity]compactRangeDestination
	destinationCount := 0
	for i := 0; i < sourceCount; i++ {
		source := &sources[i]
		target := -1
		for j := 0; j < destinationCount; j++ {
			if destinations[j].descriptor == source.next {
				target = j
				break
			}
		}
		if target < 0 {
			target = destinationCount
			destinations[target] = compactRangeDestination{descriptor: source.next, slot: -1}
			destinationCount++
		}
		source.target = target
	}
	// Prefer an already-published joinable descriptor, including an empty cached
	// group. A source which is about to lose its final member cannot be recycled
	// as a different destination in this transaction.
	for i := 0; i < destinationCount; i++ {
		destination := &destinations[i]
		for slot := 0; slot < compactGroupCapacity; slot++ {
			group := c.groupLoad(slot)
			if group == nil || group.retired || !group.joinable || group.state.Load() == nil || group.descriptor != destination.descriptor {
				continue
			}
			if sourceIndex := compactRangeSourceIndex(&sources, sourceCount, group); sourceIndex >= 0 {
				source := &sources[sourceIndex]
				keepSelected := source.next == source.descriptor
				if compactRangeGroupEmptyAfter(group, offset, size, keepSelected) && !keepSelected {
					continue
				}
			}
			destination.group = group
			break
		}
	}

	// Reserve every exact destination first, even when it is an empty cached
	// group. A later unequal destination must not recycle that same slot.
	var reserved [compactGroupCapacity]bool
	for i := 0; i < destinationCount; i++ {
		group := destinations[i].group
		if group == nil {
			continue
		}
		for slot := 0; slot < compactGroupCapacity; slot++ {
			if c.groupLoad(slot) == group {
				reserved[slot] = true
				break
			}
		}
	}

	// Prefer nil and retired-empty slots, retaining a small exact-descriptor
	// working set while disposable capacity remains. Once those are exhausted,
	// recycle empty joinable caches: capacity bounds simultaneously live history
	// classes rather than all history ever observed. Complete every reservation
	// before allocating or mutating so capacity failure is pointer-identical.
	allocatedGroups := c.allocatedGroupCount()
	for i := 0; i < destinationCount; i++ {
		destination := &destinations[i]
		if destination.group != nil {
			continue
		}
		if allocatedGroups >= compactPaletteBitmapCrossover {
			for slot := 0; slot < compactGroupCapacity; slot++ {
				if reserved[slot] {
					continue
				}
				group := c.groupLoad(slot)
				if group != nil && group.empty() && (group.retired || group.joinable) {
					destination.slot = slot
					reserved[slot] = true
					break
				}
			}
		}
		for slot := 0; slot < compactGroupCapacity; slot++ {
			if destination.slot >= 0 {
				break
			}
			if reserved[slot] {
				continue
			}
			group := c.groupLoad(slot)
			if group == nil || group.retired && group.empty() {
				destination.slot = slot
				reserved[slot] = true
				break
			}
		}
		if destination.slot < 0 {
			for slot := 0; slot < compactGroupCapacity; slot++ {
				if reserved[slot] {
					continue
				}
				group := c.groupLoad(slot)
				if group != nil && !group.retired && group.joinable && group.empty() {
					destination.slot = slot
					reserved[slot] = true
					break
				}
			}
		}
		if destination.slot < 0 {
			if palette := c.upgradePalette(); palette != nil {
				return finishCompactRange(defaultState, palette.tryRangeLocked(c, offset, size, defaultState, current, clock, pc, write))
			}
			return finishCompactRange(defaultState, false)
		}
	}
	newGroups := 0
	for i := 0; i < destinationCount; i++ {
		destination := &destinations[i]
		if destination.group == nil && destination.slot >= 0 && c.groupLoad(destination.slot) == nil {
			newGroups++
		}
	}
	if allocatedGroups+newGroups > compactPaletteBitmapCrossover {
		if palette := c.upgradePalette(); palette != nil {
			return finishCompactRange(defaultState, palette.tryRangeLocked(c, offset, size, defaultState, current, clock, pc, write))
		}
	}

	// Capacity is now proven. Allocate and fully initialize genuinely new slots
	// before publication. Existing empty group objects are retained; their state
	// is chosen under the odd revision so the exposure handshake is authoritative.
	for i := 0; i < destinationCount; i++ {
		destination := &destinations[i]
		if destination.group != nil {
			continue
		}
		if group := c.groupLoad(destination.slot); group != nil {
			destination.group = group
			destination.recycle = true
			continue
		}
		group := newCompactGroup(destination.descriptor)
		destination.newGroup = group
		destination.group = group
	}

	// The plan is complete. For each bitmap word, snapshot every source before
	// publishing any destination. Then publish all destination masks before
	// retiring source memberships/tombstones. Word-local snapshots are required
	// when multiple sources converge or a source is also an existing destination.
	// The single odd/even revision prevents unlocked lookup from observing a gap.
	c.activate()
	c.beginMutation()
	for i := 0; i < destinationCount; i++ {
		destination := &destinations[i]
		if destination.recycle {
			destination.group.recycleGroup(destination.descriptor)
		}
	}
	for i := 0; i < destinationCount; i++ {
		destination := &destinations[i]
		if destination.newGroup != nil {
			c.groupStore(destination.slot, destination.newGroup)
		}
	}
	var sourceMasks [compactRangeSourceCapacity]uint64
	for word := firstWord; word <= lastWord; word++ {
		selected := compactRangeWordMask(offset, size, word)
		covered := uint64(0)
		defaultSource := -1
		for i := 0; i < sourceCount; i++ {
			source := &sources[i]
			var mask uint64
			switch source.kind {
			case compactRangeGroupSource:
				mask = source.group.membershipWord(word) & selected
			case compactRangeTombstoneSource:
				mask = c.tombstoneWord(word) & selected
			case compactRangeDefaultSource:
				defaultSource = i
			}
			sourceMasks[i] = mask
			covered |= mask
		}
		if defaultSource >= 0 {
			sourceMasks[defaultSource] = selected &^ covered
		}

		for i := 0; i < sourceCount; i++ {
			if mask := sourceMasks[i]; mask != 0 {
				compactSetMembershipMask(destinations[sources[i].target].group, word, mask)
			}
		}
		for i := 0; i < sourceCount; i++ {
			mask := sourceMasks[i]
			if mask == 0 {
				continue
			}
			source := &sources[i]
			switch source.kind {
			case compactRangeGroupSource:
				if destinations[source.target].group != source.group {
					compactClearMembershipMask(source.group, word, mask)
				}
			case compactRangeTombstoneSource:
				plane := c.tombstones.Load()
				if plane == nil {
					runtimeThrow("race detector compact range lost tombstone plane")
				}
				compactAtomicClear(&plane[word], mask)
			}
		}
	}
	// Empty supported sources remain joinable as a small descriptor working set;
	// a later unrelated destination may recycle them once no other slot remains.
	c.endMutation()
	return finishCompactRange(defaultState, true)
}

func (pt *PageTableShadow) tryCompactRange(addr, size uintptr, current epoch.Epoch, clock *vectorclock.VectorClock, pc uintptr, write bool) bool {
	if current == 0 || size == 0 || size > rangeBlockSize || size-1 > ^uintptr(0)-addr ||
		addr&(rangeBlockSize-1)+size > rangeBlockSize {
		return false
	}
	view, _ := pt.blockFor(addr, true)
	firstWord := (addr >> 3) & rangeBlockWordMask
	lastWord := ((addr + size - 1) >> 3) & rangeBlockWordMask
	if slots := view.slotTable(false); slots != nil {
		for word := firstWord; word <= lastWord; word++ {
			if slots[word].Load() != nil {
				return false
			}
		}
	}

	view.history.mu.lock()
	if slots := view.slotTable(false); slots != nil {
		for word := firstWord; word <= lastWord; word++ {
			if slots[word].Load() != nil {
				return finishCompactRangeBlock(view.history, false)
			}
		}
	}

	compact := view.history.compact.Load()
	newHeader := false
	if compact == nil {
		if view.history.state.Load() != nil || current == 0 {
			return finishCompactRangeBlock(view.history, false)
		}
		compact = newCompactGroups()
		newHeader = true
	}
	if preparePaletteMutationLocked(view, compact, firstWord, lastWord) {
		return finishCompactRangeBlock(view.history, false)
	}
	if !compact.tryRange(addr&(rangeBlockSize-1), size, view.history.state.Load(), current, clock, pc, write) {
		return finishCompactRangeBlock(view.history, false)
	}
	if newHeader {
		// The private header already contains a complete even-revision mapping.
		// Publishing it last makes a failed first admission pointer-identical and
		// exposes no partially initialized compact metadata on success.
		view.history.compact.Store(compact)
	}
	return finishCompactRangeBlock(view.history, true)
}

// TryCompactReadRange applies an exact conflict-free read to one partial 4 KiB
// fragment. False requests fallback through AccessRange for the same fragment.
func (pt *PageTableShadow) TryCompactReadRange(addr, size uintptr, current epoch.Epoch, clock *vectorclock.VectorClock, pc uintptr) bool {
	return pt.tryCompactRange(addr, size, current, clock, pc, false)
}

// TryCompactWriteRange is the write counterpart of TryCompactReadRange.
func (pt *PageTableShadow) TryCompactWriteRange(addr, size uintptr, current epoch.Epoch, clock *vectorclock.VectorClock, pc uintptr) bool {
	return pt.tryCompactRange(addr, size, current, clock, pc, true)
}
