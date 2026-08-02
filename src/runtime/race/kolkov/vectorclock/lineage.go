package vectorclock

import "internal/runtime/atomic"

const (
	lineageBlockEntries                        = 256
	lineageBlockPageEntries                    = 256
	lineageEntryPrevBits                       = 21
	lineageEntryDeltaBits                      = 10
	lineageEntryPrevMask                uint32 = 1<<lineageEntryPrevBits - 1
	lineageEntryDeltaMask               uint32 = 1<<lineageEntryDeltaBits - 1
	lineageEntryOverflowFlag            uint32 = 1 << 31
	lineageEntryCheckpointPeriod               = 16
	lineageHeadCompactShift                    = lineageEntryPrevBits
	lineageMinRotation                         = 4096
	lineageRotationEntriesPerCoordinate        = 128
	// A segment's delta entries occupy at most 4 MiB, plus sparse exact
	// checkpoints. Blocks are allocated lazily, so raising the ceiling does
	// not increase the small-lineage
	// footprint; it only avoids repeatedly rebuilding a large exact anchor.
	lineageMaxRotation    = 1 << 20
	lineageMaxDenseHeads  = 65536
	lineageBlockPageCount = (lineageMaxRotation/lineageBlockEntries - 1 +
		lineageBlockPageEntries - 1) / lineageBlockPageEntries
)

type lineageEntry struct {
	// word is either {10-bit clock delta, 21-bit previous local index}, or the
	// overflow flag plus a one-based exact-checkpoint index.
	word uint32
}

type lineageOverflow struct {
	clock, prevSame uint32
}

type lineageBlock struct {
	entries [lineageBlockEntries]lineageEntry
}

type lineageBlockPage struct {
	blocks [lineageBlockPageEntries]atomic.Pointer[lineageBlock]
}

type lineageOverflowBlock struct {
	entries [lineageBlockEntries]lineageOverflow
}

type lineageOverflowBlockPage struct {
	blocks [lineageBlockPageEntries]atomic.Pointer[lineageOverflowBlock]
}

type lineageHead struct {
	// state packs the one-based local index in the low 21 bits and the
	// writer-owned exact-checkpoint distance above it. Readers mask the index;
	// only the lineage writer uses the distance and clock.
	state atomic.Uint32
	clock uint32
}

func (h *lineageHead) load() (index uint32, compactEntries uint8) {
	state := h.state.Load()
	return state & lineageEntryPrevMask, uint8(state >> lineageHeadCompactShift)
}

func (h *lineageHead) publish(index, clock uint32, compactEntries uint8) {
	h.clock = clock
	h.state.Store(index | uint32(compactEntries)<<lineageHeadCompactShift)
}

type lineageCell struct {
	// key is uint64(tid)+1, reserving zero for an empty cell.
	key  atomic.Uint64
	head lineageHead
}

type lineageSegment struct {
	family        *lineageFamily
	anchor        *clockImage
	anchorVersion uint64
	lastVersion   uint64 // writer-owned
	firstBlock    lineageBlock
	blockPages    [lineageBlockPageCount]atomic.Pointer[lineageBlockPage]
	firstOverflow atomic.Pointer[lineageOverflowBlock]
	overflowPages [lineageBlockPageCount]atomic.Pointer[lineageOverflowBlockPage]
	overflows     uint32 // writer-owned
	denseBase     uint32
	denseHeads    []lineageHead
	cells         []lineageCell
	cellMask      uint64
	sparseHeads   uint64 // writer-owned number of occupied sparse cells
	entries       uint32 // writer-owned
	rotateAt      uint64
	refs          atomic.Int32
	sealed        atomic.Bool
}

// lineageFamily is deliberately pointer-identified. Versions in one family
// remain comparable in O(1) across segment rotations; branches start a new
// family because their subsequent versions are not ordered with the parent.
type lineageFamily struct{ identity byte }

// lineageIndexHint describes only coordinates updated in the previous
// segment. Immutable anchor coordinates need no mutable heads: Get can read
// them directly from clockImage. Sizing the next mutable index from observed
// updates avoids rebuilding a wide per-TID index for a compact inherited
// anchor while still adapting geometrically to genuinely wide update cohorts.
type lineageIndexHint struct {
	first, last uint32
	count       uint64
	have        bool
}

func (hint *lineageIndexHint) add(tid uint32) {
	if !hint.have {
		hint.first, hint.last, hint.have = tid, tid, true
	} else {
		if tid < hint.first {
			hint.first = tid
		}
		if tid > hint.last {
			hint.last = tid
		}
	}
	hint.count++
}

// CausalView is an exact immutable {segment, version} pin. Copying a view does
// not create a reference: call Retain before independently storing a copy, and
// call Release exactly once for each retained ownership reference.
type CausalView struct {
	segment *lineageSegment
	version uint64
}

// ClockLineage serializes a single append stream. Pinned Get is lock-free.
// The writer guard is an internal atomic spin guard so this primitive is usable
// from detector/runtime paths without sync locks or map growth reentrancy.
type ClockLineage struct {
	writer   atomic.Uint32
	family   *lineageFamily
	current  *lineageSegment
	version  uint64
	sealed   bool
	released bool
}

// PreparedCausalAppend is a pre-hardware lineage transaction. PrepareAppend
// performs every operation which may allocate or rotate while holding the
// lineage writer guard. The caller may then execute hardware and finish with
// Commit or CommitPinned, neither of which allocates or rotates. Abort releases
// the guard when hardware was not executed.
//
// A prepared transaction must not be copied and must be finished exactly once.
type PreparedCausalAppend struct {
	lineage        *ClockLineage
	head           *lineageHead
	sparse         *lineageCell
	tid            uint32
	clock          uint32
	entryWord      uint32
	previous       uint32
	checkpoint     *lineageOverflow
	compactEntries uint8
	found          bool
	needed         bool
}

func (l *ClockLineage) lock() {
	for !l.writer.CompareAndSwap(0, 1) {
	}
}
func (l *ClockLineage) unlock() { l.writer.Store(0) }

func NewClockLineage(anchor *VectorClock) *ClockLineage {
	return newClockLineage(newClockImage(anchor))
}

func NewClockLineageFromSnapshot(anchor *ClockSnapshot) *ClockLineage {
	return newClockLineage(newClockImageFromSnapshot(anchor))
}

func newClockLineage(anchor *clockImage) *ClockLineage {
	family := new(lineageFamily)
	return &ClockLineage{family: family, current: newLineageSegment(family, anchor, 0, lineageIndexHint{})}
}

func lineageRotationThreshold(coordinates uint64) uint64 {
	if coordinates > uint64(lineageMaxRotation)/lineageRotationEntriesPerCoordinate {
		return lineageMaxRotation
	}
	rotateAt := coordinates * lineageRotationEntriesPerCoordinate
	if rotateAt < lineageMinRotation {
		return lineageMinRotation
	}
	return rotateAt
}

func newLineageSegment(family *lineageFamily, anchor *clockImage, version uint64, hint lineageIndexHint) *lineageSegment {
	coordinates := anchor.coordinateCount()
	rotateAt := lineageRotationThreshold(coordinates)
	// Dense heads describe likely mutable coordinates, not immutable anchor
	// coordinates. A compact anchor can contain tens of thousands of inherited
	// TIDs while the publication stream repeatedly updates only one owner.
	var denseBase uint32
	var denseHeads []lineageHead
	if hint.have {
		span := uint64(hint.last) - uint64(hint.first) + 1
		if span <= lineageMaxDenseHeads && span <= hint.count*2 {
			denseBase = hint.first
			denseHeads = make([]lineageHead, span)
		}
	}
	// Sparse capacity grows from the distinct mutable cohort observed in the
	// prior segment. The eightfold geometric prediction avoids rotation storms
	// for all-unique streams, but new/single-owner lineages stay at 16 cells.
	sparseHint := hint.count * 4
	if denseHeads != nil && sparseHint > 4096 {
		sparseHint = 4096
	} else if sparseHint > 16384 {
		sparseHint = 16384
	}
	capacity := uint64(16)
	for capacity < sparseHint*2 {
		capacity <<= 1
	}
	segment := &lineageSegment{
		family: family, anchor: anchor, anchorVersion: version, lastVersion: version,
		denseBase: denseBase, denseHeads: denseHeads,
		cells:    make([]lineageCell, int(capacity)),
		cellMask: capacity - 1, rotateAt: rotateAt,
	}
	segment.refs.Store(1)
	return segment
}

func (s *lineageSegment) blockAt(blockIndex uint32) *lineageBlock {
	if blockIndex == 0 {
		return &s.firstBlock
	}
	extra := blockIndex - 1
	page := s.blockPages[extra/lineageBlockPageEntries].Load()
	if page == nil {
		return nil
	}
	return page.blocks[extra%lineageBlockPageEntries].Load()
}

// ensureBlock is writer-only. Publishing the page before an entry block is
// safe because readers treat a nil block as an entry that did not yet exist.
func (s *lineageSegment) ensureBlock(blockIndex uint32) *lineageBlock {
	if blockIndex == 0 {
		return &s.firstBlock
	}
	extra := blockIndex - 1
	pageSlot := &s.blockPages[extra/lineageBlockPageEntries]
	page := pageSlot.Load()
	if page == nil {
		page = new(lineageBlockPage)
		pageSlot.Store(page)
	}
	blockSlot := &page.blocks[extra%lineageBlockPageEntries]
	block := blockSlot.Load()
	if block == nil {
		block = new(lineageBlock)
		blockSlot.Store(block)
	}
	return block
}

func (s *lineageSegment) overflowBlockAt(blockIndex uint32) *lineageOverflowBlock {
	if blockIndex == 0 {
		return s.firstOverflow.Load()
	}
	extra := blockIndex - 1
	page := s.overflowPages[extra/lineageBlockPageEntries].Load()
	if page == nil {
		return nil
	}
	return page.blocks[extra%lineageBlockPageEntries].Load()
}

func (s *lineageSegment) ensureOverflowBlock(blockIndex uint32) *lineageOverflowBlock {
	if blockIndex == 0 {
		block := s.firstOverflow.Load()
		if block == nil {
			block = new(lineageOverflowBlock)
			s.firstOverflow.Store(block)
		}
		return block
	}
	extra := blockIndex - 1
	pageSlot := &s.overflowPages[extra/lineageBlockPageEntries]
	page := pageSlot.Load()
	if page == nil {
		page = new(lineageOverflowBlockPage)
		pageSlot.Store(page)
	}
	blockSlot := &page.blocks[extra%lineageBlockPageEntries]
	block := blockSlot.Load()
	if block == nil {
		block = new(lineageOverflowBlock)
		blockSlot.Store(block)
	}
	return block
}

func (s *lineageSegment) overflowAt(index uint32) *lineageOverflow {
	if index == 0 {
		return nil
	}
	zero := index - 1
	block := s.overflowBlockAt(zero / lineageBlockEntries)
	if block == nil {
		return nil
	}
	return &block.entries[zero%lineageBlockEntries]
}

func lineageHash(tid uint32) uint64 { return snapshotPriority(tid) }

func (s *lineageSegment) cell(tid uint32) (*lineageCell, bool) {
	key := uint64(tid) + 1
	for probe := uint64(0); probe <= s.cellMask; probe++ {
		cell := &s.cells[(lineageHash(tid)+probe)&s.cellMask]
		published := cell.key.Load()
		if published == key {
			return cell, true
		}
		if published == 0 {
			return cell, false
		}
	}
	return nil, false
}

func (s *lineageSegment) head(tid uint32) (*lineageHead, bool, *lineageCell) {
	if tid >= s.denseBase && uint64(tid-s.denseBase) < uint64(len(s.denseHeads)) {
		head := &s.denseHeads[tid-s.denseBase]
		index, _ := head.load()
		return head, index != 0, nil
	}
	cell, found := s.cell(tid)
	if cell == nil {
		return nil, false, nil
	}
	return &cell.head, found, cell
}

func (s *lineageSegment) entryAt(index uint32) *lineageEntry {
	if index == 0 {
		return nil
	}
	zero := index - 1
	block := s.blockAt(zero / lineageBlockEntries)
	if block == nil {
		return nil
	}
	return &block.entries[zero%lineageBlockEntries]
}

func (s *lineageSegment) get(tid uint32, version uint64) uint32 {
	headCell, found, _ := s.head(tid)
	if found {
		head, _ := headCell.load()
		target := uint64(0)
		if version > s.anchorVersion {
			target = version - s.anchorVersion
		}
		for uint64(head) > target {
			head = s.entryPrev(head)
		}
		return s.entryClock(tid, head)
	}
	return s.anchor.Get(tid)
}

func (s *lineageSegment) entryPrev(index uint32) uint32 {
	if index == 0 {
		return 0
	}
	entry := s.entryAt(index)
	if entry == nil {
		runtimeThrow("race detector missing clock-lineage entry")
	}
	word := entry.word
	if word&lineageEntryOverflowFlag == 0 {
		return word & lineageEntryPrevMask
	}
	overflow := s.overflowAt(word &^ lineageEntryOverflowFlag)
	if overflow == nil {
		runtimeThrow("race detector missing clock-lineage overflow")
	}
	return overflow.prevSame
}

func (s *lineageSegment) entryClock(tid, index uint32) uint32 {
	if index == 0 {
		return s.anchor.Get(tid)
	}
	sum := uint64(0)
	for index != 0 {
		entry := s.entryAt(index)
		if entry == nil {
			runtimeThrow("race detector missing clock-lineage entry")
		}
		word := entry.word
		if word&lineageEntryOverflowFlag != 0 {
			overflow := s.overflowAt(word &^ lineageEntryOverflowFlag)
			if overflow == nil || uint64(overflow.clock)+sum > uint64(^uint32(0)) {
				runtimeThrow("race detector invalid clock-lineage checkpoint")
			}
			return overflow.clock + uint32(sum)
		}
		sum += uint64((word >> lineageEntryPrevBits) & lineageEntryDeltaMask)
		index = word & lineageEntryPrevMask
	}
	anchor := s.anchor.Get(tid)
	if uint64(anchor)+sum > uint64(^uint32(0)) {
		runtimeThrow("race detector invalid clock-lineage compact chain")
	}
	return anchor + uint32(sum)
}

func (s *lineageSegment) prepareEntry(prev, previousClock, clock uint32, compactEntries uint8) (word uint32, nextCompact uint8, checkpoint *lineageOverflow) {
	delta := clock - previousClock
	// Callers admit only strictly monotone updates, and published heads are
	// already masked to the segment-bounded local-index field.
	if delta <= lineageEntryDeltaMask && compactEntries < lineageEntryCheckpointPeriod-1 {
		return prev | delta<<lineageEntryPrevBits, compactEntries + 1, nil
	}
	index := s.overflows + 1
	zero := index - 1
	block := s.ensureOverflowBlock(zero / lineageBlockEntries)
	checkpoint = &block.entries[zero%lineageBlockEntries]
	return lineageEntryOverflowFlag | index, 0, checkpoint
}

func (s *lineageSegment) publishEntry(entry *lineageEntry, checkpoint *lineageOverflow, word, prev, clock uint32) {
	if word&lineageEntryOverflowFlag != 0 {
		index := word &^ lineageEntryOverflowFlag
		if checkpoint == nil || index != s.overflows+1 {
			runtimeThrow("race detector invalid prepared clock-lineage overflow")
		}
		*checkpoint = lineageOverflow{clock: clock, prevSame: prev}
		s.overflows = index
	}
	entry.word = word
}

func (s *lineageSegment) append(version uint64, tid, clock uint32) {
	head, found, sparse := s.head(tid)
	if head == nil {
		runtimeThrow("race detector clock-lineage index full")
	}
	if !found {
		// Publishing the key before the head is safe: views which can exist at
		// this point predate this TID's first update and therefore use anchor.
		if sparse != nil {
			sparse.key.Store(uint64(tid) + 1)
			s.sparseHeads++
		}
	}
	index := s.entries + 1
	blockIndex := (index - 1) / lineageBlockEntries
	block := s.ensureBlock(blockIndex)
	entry := &block.entries[(index-1)%lineageBlockEntries]
	prev, previousCompact := head.load()
	previousClock := head.clock
	if !found {
		previousClock = s.anchor.Get(tid)
	}
	word, compactEntries, checkpoint := s.prepareEntry(prev, previousClock, clock, previousCompact)
	s.publishEntry(entry, checkpoint, word, prev, clock)
	// Publishing the one-based index makes the immutable entry and optional
	// exact checkpoint visible. Writer-only head fields are never read here.
	head.publish(index, clock, compactEntries)
	s.entries = index
	s.lastVersion = version
}

func (s *lineageSegment) appendPrepared(version uint64, tid, clock, word, previous uint32, checkpoint *lineageOverflow, compactEntries uint8, head *lineageHead, sparse *lineageCell, found bool) {
	if head == nil {
		runtimeThrow("race detector lost prepared clock-lineage head")
	}
	if !found {
		if sparse != nil {
			sparse.key.Store(uint64(tid) + 1)
			s.sparseHeads++
		}
	}
	index := s.entries + 1
	block := s.blockAt((index - 1) / lineageBlockEntries)
	if block == nil {
		runtimeThrow("race detector lost prepared clock-lineage block")
	}
	entry := &block.entries[(index-1)%lineageBlockEntries]
	s.publishEntry(entry, checkpoint, word, previous, clock)
	head.publish(index, clock, compactEntries)
	s.entries = index
	s.lastVersion = version
}

func (l *ClockLineage) Version() uint64 {
	l.lock()
	version := l.version
	l.unlock()
	return version
}

// Reserve preallocates typed entry blocks for up to count subsequent
// non-dominated appends in the current segment. It lets runtime integrations
// make a pinned transaction's Append allocation-free; rotation remains an
// explicit slow path. Excess capacity beyond the current rotation boundary is
// intentionally not retained.
func (l *ClockLineage) Reserve(count uint64) bool {
	l.lock()
	defer l.unlock()
	if l.released || l.sealed || l.current == nil {
		return false
	}
	s := l.current
	remaining := s.rotateAt - uint64(s.entries)
	if count > remaining {
		return false
	}
	needed := (uint64(s.entries) + count + lineageBlockEntries - 1) / lineageBlockEntries
	for i := uint64(0); i < needed; i++ {
		s.ensureBlock(uint32(i))
	}
	// Every reserved append may require an exact checkpoint: the clock delta
	// can exceed the compact field even when the previous compact chain is
	// short. Reserve that worst case so the allocation-free contract does not
	// depend on future clock values.
	neededOverflows := (uint64(s.overflows) + count + lineageBlockEntries - 1) / lineageBlockEntries
	for i := uint64(0); i < neededOverflows; i++ {
		s.ensureOverflowBlock(uint32(i))
	}
	return true
}

// PrepareAppend prepares one exact point-max update before hardware executes.
// The returned transaction owns the lineage writer guard until Commit,
// CommitPinned, or Abort. needed is false for a dominated/zero/retired update;
// such a valid transaction must still be finished.
func (l *ClockLineage) PrepareAppend(tid, clock uint32) (prepared PreparedCausalAppend, needed bool) {
	l.lock()
	if l.released || l.sealed || l.current == nil {
		l.unlock()
		return PreparedCausalAppend{}, false
	}
	if uint64(l.current.entries) >= l.current.rotateAt {
		l.rotateLocked()
	}
	head, found, sparse := l.current.head(tid)
	old := l.current.anchor.Get(tid)
	if found {
		old = head.clock
	}
	needed = clock != 0 && old != ^uint32(0) && clock > old
	entryWord := uint32(0)
	previous := uint32(0)
	previousClock := uint32(0)
	var checkpoint *lineageOverflow
	compactEntries := uint8(0)
	if needed {
		if !found && sparse != nil && l.current.sparseHeads*2 >= uint64(len(l.current.cells)) {
			l.rotateLocked()
			head, found, sparse = l.current.head(tid)
		}
		if l.version == ^uint64(0) {
			runtimeThrow("race detector clock-lineage version overflow")
		}
		// Reserve the precise block needed by Commit. The published block pointer
		// is stable for the lifetime of this segment.
		index := l.current.entries + 1
		blockIndex := (index - 1) / lineageBlockEntries
		l.current.ensureBlock(blockIndex)
		var previousCompact uint8
		previous, previousCompact = head.load()
		previousClock = head.clock
		if !found {
			previousClock = old
		}
		entryWord, compactEntries, checkpoint = l.current.prepareEntry(previous, previousClock, clock, previousCompact)
	}
	return PreparedCausalAppend{
		lineage: l, head: head, sparse: sparse, tid: tid, clock: clock,
		entryWord: entryWord, previous: previous,
		checkpoint: checkpoint, compactEntries: compactEntries, found: found, needed: needed,
	}, needed
}

func (prepared *PreparedCausalAppend) Valid() bool {
	return prepared != nil && prepared.lineage != nil
}

func (prepared *PreparedCausalAppend) Needed() bool {
	return prepared != nil && prepared.lineage != nil && prepared.needed
}

// Commit publishes a prepared update without allocation or rotation.
func (prepared *PreparedCausalAppend) Commit() (uint64, bool) {
	view, appended := prepared.commit(false)
	return view.version, appended
}

// CommitPinned is Commit plus a retained exact resulting view.
func (prepared *PreparedCausalAppend) CommitPinned() (CausalView, bool) {
	return prepared.commit(true)
}

// CommitOwned publishes a prepared update and advances caller's owned
// publication while the writer guard is still held. It creates no temporary
// retained view: same-segment advancement has zero reference atomics, while a
// preparation-time rotation transfers ownership retain-new-before-release-old.
func (prepared *PreparedCausalAppend) CommitOwned(view *CausalView) (uint64, bool) {
	if prepared == nil || prepared.lineage == nil {
		return 0, false
	}
	l := prepared.lineage
	if view == nil || view.Valid() && view.segment.family != l.family {
		*prepared = PreparedCausalAppend{}
		l.unlock()
		return l.version, false
	}
	borrowed := CausalView{segment: l.current, version: l.version}
	if prepared.needed {
		l.version++
		l.current.appendPrepared(l.version, prepared.tid, prepared.clock, prepared.entryWord, prepared.previous, prepared.checkpoint, prepared.compactEntries, prepared.head, prepared.sparse, prepared.found)
		borrowed.version = l.version
	}
	appended := prepared.needed
	if !view.AdvanceTo(borrowed) {
		// The family was validated above; only an impossible dead segment could
		// reject advancement after the metadata commit.
		runtimeThrow("race detector prepared owned publication advance failed")
	}
	*prepared = PreparedCausalAppend{}
	l.unlock()
	return borrowed.version, appended
}

func (prepared *PreparedCausalAppend) commit(pin bool) (CausalView, bool) {
	if prepared == nil || prepared.lineage == nil {
		return CausalView{}, false
	}
	l := prepared.lineage
	view := CausalView{segment: l.current, version: l.version}
	if prepared.needed {
		l.version++
		l.current.appendPrepared(l.version, prepared.tid, prepared.clock, prepared.entryWord, prepared.previous, prepared.checkpoint, prepared.compactEntries, prepared.head, prepared.sparse, prepared.found)
		view.version = l.version
	}
	if pin {
		view.Retain()
	}
	needed := prepared.needed
	*prepared = PreparedCausalAppend{}
	l.unlock()
	return view, needed
}

// Abort releases a prepared transaction without publishing its point update.
func (prepared *PreparedCausalAppend) Abort() {
	if prepared == nil || prepared.lineage == nil {
		return
	}
	l := prepared.lineage
	*prepared = PreparedCausalAppend{}
	l.unlock()
}

func (l *ClockLineage) Append(tid, clock uint32) (uint64, bool) {
	l.lock()
	view, appended := l.appendLocked(tid, clock, false)
	version := view.version
	if !view.Valid() {
		version = l.version
	}
	l.unlock()
	return version, appended
}

func (l *ClockLineage) AppendPinned(tid, clock uint32) (CausalView, bool) {
	l.lock()
	view, appended := l.appendLocked(tid, clock, true)
	l.unlock()
	return view, appended
}

// AppendOwned appends one point update and advances caller's owned publication
// handle without creating a temporary retained view. An empty handle adopts
// the current family; a non-empty handle must already belong to this lineage.
// Same-segment advancement performs no reference atomics, while a rotation
// transfers ownership with retain-new-before-release-old.
func (l *ClockLineage) AppendOwned(view *CausalView, tid, clock uint32) (uint64, bool) {
	l.lock()
	defer l.unlock()
	if view == nil || l.released || l.current == nil ||
		view.Valid() && view.segment.family != l.family {
		return l.version, false
	}
	borrowed, appended := l.appendLocked(tid, clock, false)
	if !borrowed.Valid() || !view.AdvanceTo(borrowed) {
		return l.version, false
	}
	return borrowed.version, appended
}

func (l *ClockLineage) appendLocked(tid, clock uint32, pin bool) (CausalView, bool) {
	if l.released || l.sealed || l.current == nil {
		return CausalView{}, false
	}
	if uint64(l.current.entries) >= l.current.rotateAt {
		l.rotateLocked()
	}
	old := l.current.get(tid, l.version)
	if clock == 0 || old == ^uint32(0) || clock <= old {
		view := CausalView{segment: l.current, version: l.version}
		if pin {
			view.Retain()
		}
		return view, false
	}
	// The fixed index never grows. Rotate early if a pathological all-unique
	// stream reaches half load before the normal event threshold.
	_, found, sparse := l.current.head(tid)
	if !found && sparse != nil && l.current.sparseHeads*2 >= uint64(len(l.current.cells)) {
		l.rotateLocked()
	}
	if l.version == ^uint64(0) {
		runtimeThrow("race detector clock-lineage version overflow")
	}
	l.version++
	l.current.append(l.version, tid, clock)
	view := CausalView{segment: l.current, version: l.version}
	if pin {
		view.Retain()
	}
	return view, true
}

func (l *ClockLineage) Pin() CausalView {
	l.lock()
	var view CausalView
	if !l.released && l.current != nil {
		view = CausalView{segment: l.current, version: l.version}
		view.Retain()
	}
	l.unlock()
	return view
}

func (l *ClockLineage) PinVersion(version uint64) CausalView {
	l.lock()
	var view CausalView
	if !l.released && l.current != nil && version >= l.current.anchorVersion && version <= l.version {
		view = CausalView{segment: l.current, version: version}
		view.Retain()
	}
	l.unlock()
	return view
}

func (l *ClockLineage) Rotate() bool {
	l.lock()
	defer l.unlock()
	if l.released || l.sealed || l.current == nil {
		return false
	}
	l.rotateLocked()
	return true
}

// Checkpoint publishes an unconditional new same-family version with the same
// exact logical image. It is used when an external weak generation must remain
// distinguishable even though no finite coordinate changed.
func (l *ClockLineage) Checkpoint() bool {
	l.lock()
	defer l.unlock()
	if l.released || l.sealed || l.current == nil {
		return false
	}
	if l.version == ^uint64(0) {
		runtimeThrow("race detector clock-lineage version overflow")
	}
	l.version++
	l.rotateLocked()
	return true
}

// Reanchor replaces the open segment with a monotone exact image while
// preserving family identity and the global version. It is intended for slow
// foreign-join paths: a non-dominating replacement is rejected because it
// would invalidate same-family version dominance.
func (l *ClockLineage) Reanchor(anchor *VectorClock) bool {
	l.lock()
	defer l.unlock()
	return l.reanchorLocked(anchor, false)
}

// ReanchorOwned performs an exact monotone reanchor and advances caller's
// owned publication without a temporary pin. forceVersion publishes a distinct
// same-family version even when anchor is logically equal, for weak generation
// boundaries which must remain distinguishable.
func (l *ClockLineage) ReanchorOwned(view *CausalView, anchor *VectorClock, forceVersion bool) bool {
	l.lock()
	defer l.unlock()
	if view == nil || l.released || l.current == nil ||
		view.Valid() && view.segment.family != l.family {
		return false
	}
	if !l.reanchorLocked(anchor, forceVersion) {
		return false
	}
	borrowed := CausalView{segment: l.current, version: l.version}
	return view.AdvanceTo(borrowed)
}

func (l *ClockLineage) reanchorLocked(anchor *VectorClock, forceVersion bool) bool {
	if l.released || l.sealed || l.current == nil || anchor == nil {
		return false
	}
	current := CausalView{segment: l.current, version: l.version}.materialize()
	defer current.Release()
	if !current.LessOrEqual(anchor) {
		return false
	}
	// A strictly richer image is a new family version. Keeping the old version
	// would make reverse same-family dominance appear true for an older view.
	equal := anchor.LessOrEqual(current)
	if equal && !forceVersion {
		return true
	}
	if l.version == ^uint64(0) {
		runtimeThrow("race detector clock-lineage version overflow")
	}
	l.version++
	old := l.current
	old.sealed.Store(true)
	l.current = newLineageSegment(l.family, newClockImage(anchor), l.version, lineageIndexHint{})
	old.release()
	return true
}

func (l *ClockLineage) rotateLocked() {
	old := l.current
	var hint lineageIndexHint
	anchorClock := old.anchor.materialize()
	ranges := make([]FiniteRange, 0, len(old.denseHeads)+len(old.cells))
	for offset := range old.denseHeads {
		if index, _ := old.denseHeads[offset].load(); index != 0 {
			tid := old.denseBase + uint32(offset)
			hint.add(tid)
			ranges = append(ranges, FiniteRange{First: tid, Last: tid, Clock: old.denseHeads[offset].clock})
		}
	}
	for i := range old.cells {
		if index, _ := old.cells[i].head.load(); index != 0 {
			// Heads are exact maxima because Append accepts only monotone updates.
			key := old.cells[i].key.Load()
			tid := uint32(key - 1)
			hint.add(tid)
			ranges = append(ranges, FiniteRange{First: tid, Last: tid, Clock: old.cells[i].head.clock})
		}
	}
	// As in CausalView.materialize, one sorted structural merge avoids
	// repeatedly coalescing the growing anchor for every mutable coordinate.
	sortFiniteByFirst(ranges)
	anchorClock.JoinRanges(ranges)
	anchor := newClockImage(anchorClock)
	anchorClock.Release()
	old.sealed.Store(true)
	l.current = newLineageSegment(l.family, anchor, l.version, hint)
	old.release()
}

func (l *ClockLineage) Seal() CausalView {
	l.lock()
	defer l.unlock()
	if l.released || l.current == nil {
		return CausalView{}
	}
	l.sealed = true
	l.current.sealed.Store(true)
	view := CausalView{segment: l.current, version: l.version}
	view.Retain()
	return view
}

func (l *ClockLineage) Release() {
	l.lock()
	defer l.unlock()
	if l.released {
		return
	}
	l.released, l.sealed = true, true
	if l.current != nil {
		l.current.sealed.Store(true)
		l.current.release()
		l.current = nil
	}
}

func (s *lineageSegment) retain() bool {
	for refs := s.refs.Load(); refs > 0; refs = s.refs.Load() {
		if refs == int32(^uint32(0)>>1) {
			runtimeThrow("race detector clock-lineage reference overflow")
		}
		if s.refs.CompareAndSwap(refs, refs+1) {
			return true
		}
	}
	return false
}
func (s *lineageSegment) release() {
	if s.refs.Add(-1) < 0 {
		runtimeThrow("race detector clock-lineage reference underflow")
	}
}

func (v CausalView) Valid() bool     { return v.segment != nil }
func (v CausalView) Version() uint64 { return v.version }

// SameFamily reports whether two views belong to one linear version family.
// It is a constant-time identity check and remains true across rotation.
func (v CausalView) SameFamily(other CausalView) bool {
	return v.segment != nil && other.segment != nil && v.segment.family == other.segment.family
}

// Dominates is an O(1) sufficient and exact ordering test for same-family
// views. Unrelated families return false and require an ordinary clock compare.
func (v CausalView) Dominates(other CausalView) bool {
	return v.SameFamily(other) && v.version >= other.version
}

func (v CausalView) Get(tid uint32) uint32 {
	if v.segment == nil {
		return 0
	}
	return v.segment.get(tid, v.version)
}
func (v *CausalView) Retain() bool {
	return v.segment != nil && v.segment.retain()
}

// Duplicate creates one independently releasable copy.
func (v CausalView) Duplicate() (CausalView, bool) {
	if v.segment == nil || !v.segment.retain() {
		return CausalView{}, false
	}
	return v, true
}

// AdvanceTo replaces this owned view with a dominating same-family view. A
// same-segment advance only updates the version and performs no reference
// atomics. A cross-segment advance retains the replacement before releasing
// the old segment. An empty receiver adopts one retained ownership reference.
func (v *CausalView) AdvanceTo(other CausalView) bool {
	if v == nil || !other.Valid() {
		return false
	}
	if !v.Valid() {
		if !other.segment.retain() {
			return false
		}
		*v = other
		return true
	}
	if !other.Dominates(*v) {
		return false
	}
	if v.segment == other.segment {
		v.version = other.version
		return true
	}
	if !other.segment.retain() {
		return false
	}
	old := v.segment
	*v = other
	old.release()
	return true
}
func (v *CausalView) Release() {
	if v == nil || v.segment == nil {
		return
	}
	segment := v.segment
	v.segment, v.version = nil, 0
	segment.release()
}

func (v CausalView) materialize() *VectorClock {
	if v.segment == nil {
		return NewFromPool()
	}
	vc := v.segment.anchor.materialize()
	// Resolve every mutable coordinate at the pinned version, then merge the
	// sorted points in one structural pass. Calling Set for each head repeatedly
	// coalesces and reclassifies the growing sparse representation, making a
	// wide immutable view quadratic to materialize.
	// Index capacity predicts future update breadth, not the number of heads
	// visible in this pinned version. In particular, a freshly rotated wide
	// segment deliberately keeps a large empty index. Start with a small bounded
	// buffer and grow only when the view actually contains many mutable heads.
	ranges := make([]FiniteRange, 0, 64)
	for offset := range v.segment.denseHeads {
		if index, _ := v.segment.denseHeads[offset].load(); index != 0 {
			tid := v.segment.denseBase + uint32(offset)
			if clock := v.segment.get(tid, v.version); clock != 0 {
				ranges = append(ranges, FiniteRange{First: tid, Last: tid, Clock: clock})
			}
		}
	}
	for i := range v.segment.cells {
		if key := v.segment.cells[i].key.Load(); key != 0 {
			tid := uint32(key - 1)
			if clock := v.segment.get(tid, v.version); clock != 0 {
				ranges = append(ranges, FiniteRange{First: tid, Last: tid, Clock: clock})
			}
		}
	}
	sortFiniteByFirst(ranges)
	vc.JoinRanges(ranges)
	return vc
}

func (v CausalView) Snapshot() *ClockSnapshot {
	clock := v.materialize()
	snapshot := clock.Freeze()
	clock.Release()
	return snapshot
}

// IsRetired reports the exact immutable retirement state at this version.
func (v CausalView) IsRetired(tid uint32) bool {
	return v.segment != nil && v.segment.anchor.IsRetired(tid)
}

// anchorDominatesAlignedBlock is a borrowed, allocation-free lower-bound
// proof. Every point update after a segment anchor is monotonic, so a block
// dominated by the anchor is dominated by every version pinned in the segment.
func (v CausalView) anchorDominatesAlignedBlock(first, clock uint32) bool {
	return v.segment != nil && v.segment.anchor.dominatesAlignedBlock(first, clock)
}

// MaxTID returns the largest finite or retired TID in the exact view.
func (v CausalView) MaxTID() uint32 {
	clock := v.materialize()
	max := clock.GetMaxTID()
	clock.Release()
	return max
}

// JoinInto is the general foreign-family integration seam.
func (v CausalView) JoinInto(dst *VectorClock) {
	if dst != nil && v.segment != nil {
		clock := v.materialize()
		dst.Join(clock)
		clock.Release()
	}
}
func (v CausalView) RangeRuns(visit func(first, last, clock uint32) bool) {
	clock := v.materialize()
	clock.RangeRuns(visit)
	clock.Release()
}

// Range visits every finite coordinate in ascending TID order.
func (v CausalView) Range(visit func(tid, clock uint32) bool) {
	v.RangeRuns(func(first, last, clock uint32) bool {
		for tid := uint64(first); tid <= uint64(last); tid++ {
			if !visit(uint32(tid), clock) {
				return false
			}
		}
		return true
	})
}
func (v CausalView) RangeRetired(visit func(first, last uint32) bool) {
	clock := v.materialize()
	clock.RangeRetired(visit)
	clock.Release()
}
func (v CausalView) Branch() *ClockLineage {
	clock := v.materialize()
	lineage := NewClockLineage(clock)
	clock.Release()
	return lineage
}
