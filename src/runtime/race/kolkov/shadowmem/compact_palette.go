//go:build amd64 || arm64

package shadowmem

import (
	"internal/runtime/atomic"
	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/vectorclock"
)

// The dense palette is the second compact representation for one 4 KiB
// application block. Eight uint8 shape owners share each atomic word. An
// aligned word with one exact descriptor fuses its owner and both clock lows in
// that word; heterogeneous words expand into lazy 128-anchor packed chunks.
// The shape carries presence,
// TID and the high 16 clock bits. Thus sequential clocks do not manufacture a
// VarState or descriptor object per byte, while sparse blocks which cross over
// from six bitmap groups pay only for clock regions they use.
const (
	compactPaletteDefault    = uint8(0)
	compactPaletteTombstone  = uint8(1)
	compactPaletteFirstShape = uint8(2)
	// Owners 254 and 255 are reserved as the fast-read and fused-word tags,
	// leaving IDs 2..253. Reserving the complete top-byte value makes a tagged
	// word unambiguous even when its other seven bytes contain arbitrary owners.
	compactPaletteMaxShapes = 252

	compactPaletteOwnerWords        = int(rangeBlockSize / 8)
	compactPaletteClockChunkShift   = 7
	compactPaletteClockChunkAnchors = 1 << compactPaletteClockChunkShift
	compactPaletteClockChunkWords   = compactPaletteClockChunkAnchors / 4
	compactPaletteClockChunks       = int(rangeBlockSize / compactPaletteClockChunkAnchors)
	compactPaletteShapeHashBuckets  = 64
	compactPaletteShapeChunkRecords = 8
	compactPaletteShapeChunks       = (compactPaletteMaxShapes + compactPaletteShapeChunkRecords - 1) / compactPaletteShapeChunkRecords

	// Six bitmap group/state pairs occupy roughly the same storage as the palette
	// owner root. Beyond that crossover, recycle an empty bitmap group first and
	// otherwise attempt the palette before allocating a seventh simultaneous
	// group. Unsupported histories may continue to the physical capacity of 16.
	compactPaletteBitmapCrossover = 6
)

const (
	compactPaletteHasWrite = uint8(1) << iota
	compactPaletteHasRead
)

type compactPaletteShape struct {
	writeTID        uint32
	readTID         uint32
	exclusiveWriter int64
	writePC         uintptr
	readPC          uintptr
	writeCount      uint32
	writeHigh       uint16
	readHigh        uint16
	lifecycle       lifecycleID
	flags           uint8
}

type compactPaletteShapeRecord struct {
	shape    compactPaletteShape
	members  uint16 // protected by the owning rangeBlock lock
	reserved uint16
	hashNext uint8
	freeNext uint8
	used     bool
	free     bool
}

type compactPaletteShapeChunk struct {
	records [compactPaletteShapeChunkRecords]compactPaletteShapeRecord
}

type compactPaletteClockChunk struct {
	write   [compactPaletteClockChunkWords]atomic.Uint64
	read    [compactPaletteClockChunkWords]atomic.Uint64
	uniform [compactPaletteClockChunkAnchors / 8]atomic.Uint64
}

type compactPalette struct {
	owners         [compactPaletteOwnerWords]atomic.Uint64
	clocks         [compactPaletteClockChunks]atomic.Pointer[compactPaletteClockChunk]
	shapes         [compactPaletteShapeChunks - 1]atomic.Pointer[compactPaletteShapeChunk]
	initialShapes  compactPaletteShapeChunk
	fastReadPC     atomic.Uintptr
	fastReadActive atomic.Uint32
	fastReadUsers  atomic.Uint32
	hash           [compactPaletteShapeHashBuckets]uint8
	plan           uint16
	nextShape      uint8 // first never-installed shape-record index
	freeHead       uint8 // block-locked list of zero-member candidates
	zeroOwner      uint8 // immutable pinned zero-history lifecycle shape
}

func paletteShapeFromDescriptor(descriptor compactHistoryDescriptor) (compactPaletteShape, uint16, uint16) {
	shape := compactPaletteShape{
		exclusiveWriter: descriptor.history.exclusiveWriter,
		writePC:         descriptor.history.writePC,
		readPC:          descriptor.history.readPC,
		writeCount:      descriptor.history.writeCount,
		lifecycle:       descriptor.lifecycle,
	}
	var writeLow, readLow uint16
	if descriptor.history.write != 0 {
		tid, clock := descriptor.history.write.Decode()
		shape.flags |= compactPaletteHasWrite
		shape.writeTID = tid
		shape.writeHigh = uint16(clock >> 16)
		writeLow = uint16(clock)
	}
	if descriptor.history.read != 0 {
		tid, clock := descriptor.history.read.Decode()
		shape.flags |= compactPaletteHasRead
		shape.readTID = tid
		shape.readHigh = uint16(clock >> 16)
		readLow = uint16(clock)
	}
	return shape, writeLow, readLow
}

func (shape compactPaletteShape) descriptor(writeLow, readLow uint16) compactHistoryDescriptor {
	descriptor := compactHistoryDescriptor{
		history: compactHistoryKey{
			exclusiveWriter: shape.exclusiveWriter,
			writePC:         shape.writePC,
			readPC:          shape.readPC,
			writeCount:      shape.writeCount,
		},
		lifecycle: shape.lifecycle,
	}
	if shape.flags&compactPaletteHasWrite != 0 {
		descriptor.history.write = epoch.NewEpoch(shape.writeTID, uint64(shape.writeHigh)<<16|uint64(writeLow))
	}
	if shape.flags&compactPaletteHasRead != 0 {
		descriptor.history.read = epoch.NewEpoch(shape.readTID, uint64(shape.readHigh)<<16|uint64(readLow))
	}
	return descriptor
}

func compactPaletteHash(shape compactPaletteShape) uint8 {
	x := uint64(shape.writeTID)<<32 | uint64(shape.readTID)
	x ^= uint64(shape.exclusiveWriter) * 0x9e3779b97f4a7c15
	x ^= uint64(shape.writePC) + uint64(shape.readPC)*0xbf58476d1ce4e5b9
	x ^= uint64(shape.writeCount)<<17 | uint64(shape.writeHigh)<<1 | uint64(shape.readHigh)<<33
	x ^= shape.lifecycle.uint64()*0x94d049bb133111eb | uint64(shape.flags)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	return uint8(x) & (compactPaletteShapeHashBuckets - 1)
}

func (p *compactPalette) shapeRecord(id uint8, create bool) *compactPaletteShapeRecord {
	if id < compactPaletteFirstShape {
		return nil
	}
	index := int(id - compactPaletteFirstShape)
	if index >= compactPaletteMaxShapes {
		return nil
	}
	chunkIndex := index / compactPaletteShapeChunkRecords
	if chunkIndex == 0 {
		return &p.initialShapes.records[index%compactPaletteShapeChunkRecords]
	}
	chunk := p.shapes[chunkIndex-1].Load()
	if chunk == nil && create {
		chunk = new(compactPaletteShapeChunk)
		p.shapes[chunkIndex-1].Store(chunk)
	}
	if chunk == nil {
		return nil
	}
	return &chunk.records[index%compactPaletteShapeChunkRecords]
}

func (p *compactPalette) findShape(shape compactPaletteShape) uint8 {
	return p.findShapeInBucket(shape, compactPaletteHash(shape))
}

// findShapeInBucket is the block-locked lookup form used when a transition
// also needs the target bucket for publication. Keeping the hash at the caller
// avoids expanding the packed lifecycle once for lookup and twice more when a
// sole source record is retargeted.
func (p *compactPalette) findShapeInBucket(shape compactPaletteShape, bucket uint8) uint8 {
	for id := p.hash[bucket]; id != 0; {
		record := p.shapeRecord(id, false)
		if record != nil && record.used && record.shape == shape {
			return id
		}
		if record == nil {
			return 0
		}
		id = record.hashNext
	}
	return 0
}

func (p *compactPalette) removeShapeHash(id uint8, record *compactPaletteShapeRecord) {
	if record == nil || !record.used {
		return
	}
	bucket := compactPaletteHash(record.shape)
	link := &p.hash[bucket]
	for *link != 0 {
		current := *link
		candidate := p.shapeRecord(current, false)
		if current == id {
			*link = candidate.hashNext
			record.hashNext = 0
			return
		}
		link = &candidate.hashNext
	}
}

// ensureShape is block-locked. Zero-member shape records are bounded caches
// and may be retargeted without changing any published anchor.
func (p *compactPalette) markShapeFree(id uint8, record *compactPaletteShapeRecord) {
	if id < compactPaletteFirstShape || record == nil || record.free {
		return
	}
	record.freeNext = p.freeHead
	record.free = true
	p.freeHead = id
}

func (p *compactPalette) takeFreeShape() uint8 {
	for p.freeHead != 0 {
		id := p.freeHead
		record := p.shapeRecord(id, false)
		if record == nil {
			p.freeHead = 0
			return 0
		}
		p.freeHead = record.freeNext
		record.freeNext = 0
		record.free = false
		if record.members == 0 {
			return id
		}
	}
	return 0
}

func (p *compactPalette) releaseShapeMembers(id uint8, count uint16) {
	if id < compactPaletteFirstShape || count == 0 {
		return
	}
	record := p.shapeRecord(id, false)
	if record == nil || record.members < count {
		runtimeThrow("race detector dense palette member count underflow")
	}
	record.members -= count
	if record.members == 0 {
		p.markShapeFree(id, record)
	}
}

func (p *compactPalette) ensureShape(shape compactPaletteShape) uint8 {
	if id := p.findShape(shape); id != 0 {
		return id
	}
	if id := p.takeFreeShape(); id != 0 {
		record := p.shapeRecord(id, false)
		if record.used {
			p.removeShapeHash(id, record)
		}
		record.shape = shape
		record.used = true
		bucket := compactPaletteHash(shape)
		record.hashNext = p.hash[bucket]
		p.hash[bucket] = id
		return id
	}
	for index := int(p.nextShape); index < compactPaletteMaxShapes; index++ {
		id := uint8(index) + compactPaletteFirstShape
		record := p.shapeRecord(id, false)
		if record != nil && record.used {
			p.nextShape = uint8(index + 1)
			continue
		}
		p.nextShape = uint8(index + 1)
		if record == nil {
			record = p.shapeRecord(id, true)
		}
		record.shape = shape
		record.members = 0
		record.used = true
		bucket := compactPaletteHash(shape)
		record.hashNext = p.hash[bucket]
		p.hash[bucket] = id
		return id
	}
	for index := 0; index < int(p.nextShape); index++ {
		id := uint8(index) + compactPaletteFirstShape
		record := p.shapeRecord(id, false)
		if record == nil || record.members != 0 {
			continue
		}
		if record.used {
			p.removeShapeHash(id, record)
		}
		record.shape = shape
		record.members = 0
		record.used = true
		bucket := compactPaletteHash(shape)
		record.hashNext = p.hash[bucket]
		p.hash[bucket] = id
		return id
	}
	return 0
}

func (p *compactPalette) ensureShapeForPlan(shape compactPaletteShape, generation uint16) uint8 {
	if id := p.findShape(shape); id != 0 {
		p.shapeRecord(id, false).reserved = generation
		return id
	}
	for index := 0; index < compactPaletteMaxShapes; index++ {
		id := uint8(index) + compactPaletteFirstShape
		record := p.shapeRecord(id, false)
		if record != nil && (record.members != 0 || record.reserved == generation) {
			continue
		}
		if record == nil {
			record = p.shapeRecord(id, true)
		} else if record.used {
			p.removeShapeHash(id, record)
		}
		if uint8(index+1) > p.nextShape {
			p.nextShape = uint8(index + 1)
		}
		record.shape = shape
		record.members = 0
		record.used = true
		record.reserved = generation
		bucket := compactPaletteHash(shape)
		record.hashNext = p.hash[bucket]
		p.hash[bucket] = id
		return id
	}
	return 0
}

// owner is safe for lock-free diagnostic lookup. Semantic transitions are
// block-locked and bracket owner/clock publication with compactGroups.revision.
func (p *compactPalette) owner(anchor uintptr) uint8 {
	anchor = compactAnchor(anchor)
	word := p.owners[anchor>>3].Load()
	if owner, _, mask, ok := compactPaletteDecodeFastReadWord(word); ok {
		if mask&(uint8(1)<<(anchor&7)) != 0 {
			return owner
		}
		return compactPaletteTombstone
	}
	if owner, _, _, ok := compactPaletteDecodeFusedOwnerWord(word); ok {
		return owner
	}
	return uint8(word >> ((anchor & 7) * 8))
}

const (
	compactPaletteFusedOwnerTag        = uint64(0xff) << 56
	compactPaletteFusedOwnerValidation = uint64(0x5aa5) << 40
	compactPaletteFastReadTag          = uint64(0xfe) << 56
)

// compactPaletteFastReadWord is the exact zero-history read form for one
// aligned application word. The pinned owner supplies allocator lifecycle,
// while the word carries a complete admitted epoch and the still-live lane
// mask. The complement becomes authoritative zero after a partial clear.
func compactPaletteFastReadWord(owner uint8, read epoch.Epoch, mask uint8) (uint64, bool) {
	tid, clock := read.Decode()
	if owner < compactPaletteFirstShape || owner == 0xff || read == 0 ||
		tid > uint32(^uint16(0)) || clock >= 1<<24 || mask == 0 {
		return 0, false
	}
	return compactPaletteFastReadTag | uint64(mask)<<48 | uint64(clock)<<24 |
		uint64(tid)<<8 | uint64(owner), true
}

func compactPaletteDecodeFastReadWord(word uint64) (owner uint8, read epoch.Epoch, mask uint8, ok bool) {
	if word&compactPaletteFusedOwnerTag != compactPaletteFastReadTag {
		return 0, 0, 0, false
	}
	owner = uint8(word)
	mask = uint8(word >> 48)
	if owner < compactPaletteFirstShape || owner == 0xff || mask == 0 {
		return 0, 0, 0, false
	}
	tid := uint32(uint16(word >> 8))
	clock := uint64(word>>24) & (1<<24 - 1)
	read = epoch.NewEpoch(tid, clock)
	return owner, read, mask, read != 0
}

func compactPaletteFusedOwnerWord(owner uint8, writeLow, readLow uint16) uint64 {
	if owner < compactPaletteFirstShape || owner == 0xff {
		runtimeThrow("race detector dense palette invalid fused owner")
	}
	return compactPaletteFusedOwnerTag | compactPaletteFusedOwnerValidation |
		uint64(readLow)<<24 | uint64(writeLow)<<8 | uint64(owner)
}

func compactPaletteDecodeFusedOwnerWord(word uint64) (owner uint8, writeLow, readLow uint16, ok bool) {
	if word&compactPaletteFusedOwnerTag != compactPaletteFusedOwnerTag ||
		word&(uint64(0xffff)<<40) != compactPaletteFusedOwnerValidation {
		return 0, 0, 0, false
	}
	owner = uint8(word)
	if owner < compactPaletteFirstShape || owner == 0xff {
		return 0, 0, 0, false
	}
	return owner, uint16(word >> 8), uint16(word >> 24), true
}

func (p *compactPalette) storeFusedOwnerWord(anchor uintptr, owner uint8, writeLow, readLow uint16) {
	anchor = compactAnchor(anchor) &^ uintptr(7)
	// Any old sidecar is ignored while fused, but clear its uniform tag so a
	// later expansion cannot observe stale metadata between publications.
	p.clearUniformLowWord(anchor)
	p.owners[anchor>>3].Store(compactPaletteFusedOwnerWord(owner, writeLow, readLow))
}

// expandFusedOwnerWord publishes exact explicit lows for all eight lanes before
// replacing the fused tag with ordinary owner bytes. Published callers hold the
// block lock and are inside an odd revision; private migration is unreachable.
func (p *compactPalette) expandFusedOwnerWord(anchor uintptr) bool {
	anchor = compactAnchor(anchor) &^ uintptr(7)
	cell := &p.owners[anchor>>3]
	owner, writeLow, readLow, ok := compactPaletteDecodeFusedOwnerWord(cell.Load())
	if !ok {
		return false
	}
	clocks, index := p.clockStorage(anchor, true)
	base := index &^ uintptr(7)
	wantWrite := compactPalettePackedLow(writeLow)
	wantRead := compactPalettePackedLow(readLow)
	for offset := uintptr(0); offset < 8; offset += 4 {
		clocks.write[(base+offset)>>2].Store(wantWrite)
		clocks.read[(base+offset)>>2].Store(wantRead)
	}
	clocks.uniform[index>>3].Store(0)
	cell.Store(uint64(owner) * 0x0101010101010101)
	return true
}

func (p *compactPalette) setOwner(anchor uintptr, owner uint8) {
	anchor = compactAnchor(anchor)
	cell := &p.owners[anchor>>3]
	shift := (anchor & 7) * 8
	mask := uint64(0xff) << shift
	for {
		if p.expandFusedOwnerWord(anchor) {
			continue
		}
		old := cell.Load()
		next := old&^mask | uint64(owner)<<shift
		if old == next {
			return
		}
		// A scalar owner change splits the aligned word. Publish an exact
		// per-anchor low plane before invalidating its uniform-word metadata.
		// Semantic callers hold the block lock and bracket this publication with
		// the block revision; private migration uses the same ordering before the
		// palette is reachable.
		p.expandUniformLowWord(anchor)
		if cell.CompareAndSwap(old, next) {
			return
		}
	}
}

func compactPaletteLaneMask(lane, count, bits uintptr) uint64 {
	width := count * bits
	if width == 64 {
		return ^uint64(0)
	}
	return ((uint64(1) << width) - 1) << (lane * bits)
}

func (p *compactPalette) rangeOwnerEqual(start, end uintptr, owner uint8) bool {
	want := uint64(owner) * 0x0101010101010101
	for start < end {
		lane := start & 7
		count := uintptr(8) - lane
		if count > end-start {
			count = end - start
		}
		word := p.owners[start>>3].Load()
		if fusedOwner, _, _, ok := compactPaletteDecodeFusedOwnerWord(word); ok {
			if fusedOwner != owner {
				return false
			}
		} else if mask := compactPaletteLaneMask(lane, count, 8); word&mask != want&mask {
			return false
		}
		start += count
	}
	return true
}

func (p *compactPalette) setUniformRangeOwner(start, end uintptr, owner uint8) {
	want := uint64(owner) * 0x0101010101010101
	for start < end {
		lane := start & 7
		count := uintptr(8) - lane
		if count > end-start {
			count = end - start
		}
		cell := &p.owners[start>>3]
		mask := compactPaletteLaneMask(lane, count, 8)
		if mask == ^uint64(0) {
			if owner < compactPaletteFirstShape {
				p.clearUniformLowWord(start)
			} else {
				// This helper changes ownership only. Preserve a fused word's
				// exact low clocks before replacing its packed descriptor.
				p.expandFusedOwnerWord(start)
				p.expandUniformLowWord(start)
			}
			cell.Store(want)
		} else {
			p.expandFusedOwnerWord(start)
			p.expandUniformLowWord(start)
			for {
				old := cell.Load()
				next := old&^mask | want&mask
				if old == next || cell.CompareAndSwap(old, next) {
					break
				}
			}
		}
		start += count
	}
}

func compactPaletteLow(word *atomic.Uint64, index uintptr) uint16 {
	return uint16(word.Load() >> ((index & 3) * 16))
}

func compactPaletteSetLow(word *atomic.Uint64, index uintptr, low uint16) {
	shift := (index & 3) * 16
	mask := uint64(0xffff) << shift
	for {
		old := word.Load()
		next := old&^mask | uint64(low)<<shift
		if old == next || word.CompareAndSwap(old, next) {
			return
		}
	}
}

func (p *compactPalette) clockStorage(anchor uintptr, create bool) (*compactPaletteClockChunk, uintptr) {
	anchor = compactAnchor(anchor)
	cell := &p.clocks[anchor>>compactPaletteClockChunkShift]
	clocks := cell.Load()
	if clocks == nil && create {
		clocks = new(compactPaletteClockChunk)
		cell.Store(clocks)
	}
	return clocks, anchor & (compactPaletteClockChunkAnchors - 1)
}

const compactPaletteUniformLowValid = uint64(1) << 63

func compactPaletteUniformLows(writeLow, readLow uint16) uint64 {
	return compactPaletteUniformLowValid | uint64(writeLow) | uint64(readLow)<<16
}

func compactPaletteDecodeUniformLows(value uint64) (writeLow, readLow uint16, ok bool) {
	if value&compactPaletteUniformLowValid == 0 {
		return 0, 0, false
	}
	return uint16(value), uint16(value >> 16), true
}

func (p *compactPalette) uniformLowWord(anchor uintptr, create bool) *atomic.Uint64 {
	clocks, index := p.clockStorage(anchor, create)
	if clocks == nil {
		return nil
	}
	return &clocks.uniform[index>>3]
}

func (p *compactPalette) clearUniformLowWord(anchor uintptr) {
	if word := p.uniformLowWord(anchor, false); word != nil {
		word.Store(0)
	}
}

// expandUniformLowWord is block-locked for published palettes. Explicit lows
// are installed first, then the valid bit is cleared, so a racing diagnostic
// reader sees either the old exact uniform value or the new exact lane values;
// the surrounding revision still rejects a read concurrent with mutation.
func (p *compactPalette) expandUniformLowWord(anchor uintptr) {
	clocks, index := p.clockStorage(anchor, false)
	if clocks == nil {
		return
	}
	word := &clocks.uniform[index>>3]
	writeLow, readLow, ok := compactPaletteDecodeUniformLows(word.Load())
	if !ok {
		return
	}
	base := index &^ uintptr(7)
	wantWrite := compactPalettePackedLow(writeLow)
	wantRead := compactPalettePackedLow(readLow)
	for offset := uintptr(0); offset < 8; offset += 4 {
		clocks.write[(base+offset)>>2].Store(wantWrite)
		clocks.read[(base+offset)>>2].Store(wantRead)
	}
	word.Store(0)
}

func (p *compactPalette) lows(anchor uintptr) (uint16, uint16) {
	anchor = compactAnchor(anchor)
	if _, writeLow, readLow, ok := compactPaletteDecodeFusedOwnerWord(p.owners[anchor>>3].Load()); ok {
		return writeLow, readLow
	}
	clocks, index := p.clockStorage(anchor, false)
	if clocks == nil {
		return 0, 0
	}
	if writeLow, readLow, ok := compactPaletteDecodeUniformLows(clocks.uniform[index>>3].Load()); ok {
		return writeLow, readLow
	}
	return compactPaletteLow(&clocks.write[index>>2], index), compactPaletteLow(&clocks.read[index>>2], index)
}

func (p *compactPalette) setLows(anchor uintptr, writeLow, readLow uint16) {
	p.expandFusedOwnerWord(anchor)
	p.expandUniformLowWord(anchor)
	clocks, index := p.clockStorage(anchor, true)
	compactPaletteSetLow(&clocks.write[index>>2], index, writeLow)
	compactPaletteSetLow(&clocks.read[index>>2], index, readLow)
}

func compactPalettePackedLow(low uint16) uint64 {
	return uint64(low) * 0x0001000100010001
}

func compactPaletteLowWordEqual(word *atomic.Uint64, lane, count uintptr, want uint64) bool {
	mask := compactPaletteLaneMask(lane, count, 16)
	return word.Load()&mask == want&mask
}

func compactPaletteSetLowWord(word *atomic.Uint64, lane, count uintptr, want uint64) {
	mask := compactPaletteLaneMask(lane, count, 16)
	if mask == ^uint64(0) {
		word.Store(want)
		return
	}
	for {
		old := word.Load()
		next := old&^mask | want&mask
		if old == next || word.CompareAndSwap(old, next) {
			return
		}
	}
}

func (p *compactPalette) rangeLowsEqual(start, end uintptr, writeLow, readLow uint16) bool {
	wantWrite := compactPalettePackedLow(writeLow)
	wantRead := compactPalettePackedLow(readLow)
	for start < end {
		clocks, index := p.clockStorage(start, false)
		chunkEnd := end
		if limit := start + compactPaletteClockChunkAnchors - index; chunkEnd > limit {
			chunkEnd = limit
		}
		for start < chunkEnd {
			if _, fusedWrite, fusedRead, ok := compactPaletteDecodeFusedOwnerWord(p.owners[start>>3].Load()); ok {
				if fusedWrite != writeLow || fusedRead != readLow {
					return false
				}
				count := uintptr(8) - (start & 7)
				if count > chunkEnd-start {
					count = chunkEnd - start
				}
				start += count
				index += count
				continue
			}
			if clocks != nil {
				if uniformWrite, uniformRead, ok := compactPaletteDecodeUniformLows(clocks.uniform[index>>3].Load()); ok {
					if uniformWrite != writeLow || uniformRead != readLow {
						return false
					}
					count := uintptr(8) - (index & 7)
					if count > chunkEnd-start {
						count = chunkEnd - start
					}
					start += count
					index += count
					continue
				}
			}
			lane := index & 3
			count := uintptr(4) - lane
			if count > chunkEnd-start {
				count = chunkEnd - start
			}
			if clocks == nil {
				if writeLow != 0 || readLow != 0 {
					return false
				}
			} else if !compactPaletteLowWordEqual(&clocks.write[index>>2], lane, count, wantWrite) ||
				!compactPaletteLowWordEqual(&clocks.read[index>>2], lane, count, wantRead) {
				return false
			}
			start += count
			index += count
		}
	}
	return true
}

func (p *compactPalette) setRangeLows(start, end uintptr, writeLow, readLow uint16) {
	for start < end {
		lane := start & 7
		count := uintptr(8) - lane
		if count > end-start {
			count = end - start
		}
		if lane == 0 && count == 8 {
			cell := &p.owners[start>>3]
			if owner, _, _, ok := compactPaletteDecodeFusedOwnerWord(cell.Load()); ok {
				cell.Store(compactPaletteFusedOwnerWord(owner, writeLow, readLow))
				start += count
				continue
			}
			clocks, index := p.clockStorage(start, true)
			clocks.uniform[index>>3].Store(compactPaletteUniformLows(writeLow, readLow))
			start += count
			continue
		}
		p.expandFusedOwnerWord(start)
		p.expandUniformLowWord(start)
		clocks, index := p.clockStorage(start, true)
		for remaining := count; remaining != 0; {
			lowLane := index & 3
			lowCount := uintptr(4) - lowLane
			if lowCount > remaining {
				lowCount = remaining
			}
			compactPaletteSetLowWord(&clocks.write[index>>2], lowLane, lowCount, compactPalettePackedLow(writeLow))
			compactPaletteSetLowWord(&clocks.read[index>>2], lowLane, lowCount, compactPalettePackedLow(readLow))
			index += lowCount
			remaining -= lowCount
		}
		start += count
	}
}

func (p *compactPalette) setRangeReadLows(start, end uintptr, readLow uint16) {
	for start < end {
		lane := start & 7
		count := uintptr(8) - lane
		if count > end-start {
			count = end - start
		}
		if lane == 0 && count == 8 {
			cell := &p.owners[start>>3]
			if owner, writeLow, _, ok := compactPaletteDecodeFusedOwnerWord(cell.Load()); ok {
				cell.Store(compactPaletteFusedOwnerWord(owner, writeLow, readLow))
				start += count
				continue
			}
			if clocks, index := p.clockStorage(start, false); clocks != nil {
				uniform := &clocks.uniform[index>>3]
				if writeLow, _, ok := compactPaletteDecodeUniformLows(uniform.Load()); ok {
					uniform.Store(compactPaletteUniformLows(writeLow, readLow))
					start += count
					continue
				}
			}
		}
		p.expandFusedOwnerWord(start)
		p.expandUniformLowWord(start)
		clocks, index := p.clockStorage(start, true)
		for remaining := count; remaining != 0; {
			lowLane := index & 3
			lowCount := uintptr(4) - lowLane
			if lowCount > remaining {
				lowCount = remaining
			}
			compactPaletteSetLowWord(&clocks.read[index>>2], lowLane, lowCount, compactPalettePackedLow(readLow))
			index += lowCount
			remaining -= lowCount
		}
		start += count
	}
}

// setUniformDescriptorRange is the combined publication seam for a range with
// one owner and one exact low pair. Complete aligned words need no clock chunk;
// partial words expand first and retain ordinary owner bytes.
func (p *compactPalette) setUniformDescriptorRange(start, end uintptr, owner uint8, writeLow, readLow uint16) {
	for start < end {
		lane := start & 7
		count := uintptr(8) - lane
		if count > end-start {
			count = end - start
		}
		if lane == 0 && count == 8 {
			p.storeFusedOwnerWord(start, owner, writeLow, readLow)
		} else {
			p.setRangeLows(start, start+count, writeLow, readLow)
			p.setUniformRangeOwner(start, start+count, owner)
		}
		start += count
	}
}

func (p *compactPalette) descriptor(anchor uintptr) (compactHistoryDescriptor, bool) {
	anchor = compactAnchor(anchor)
	if owner, read, mask, ok := compactPaletteDecodeFastReadWord(p.owners[anchor>>3].Load()); ok {
		if mask&(uint8(1)<<(anchor&7)) == 0 {
			return compactHistoryDescriptor{}, false
		}
		record := p.shapeRecord(owner, false)
		if record == nil || !record.used || record.shape.flags != 0 || record.shape.lifecycle == (lifecycleID{}) {
			return compactHistoryDescriptor{}, false
		}
		return compactHistoryDescriptor{
			history:   compactHistoryKey{read: read, readPC: p.fastReadPC.Load()},
			lifecycle: record.shape.lifecycle,
		}, true
	}
	owner := p.owner(anchor)
	if owner < compactPaletteFirstShape {
		return compactHistoryDescriptor{}, false
	}
	record := p.shapeRecord(owner, false)
	if record == nil || !record.used {
		return compactHistoryDescriptor{}, false
	}
	w, r := p.lows(anchor)
	return record.shape.descriptor(w, r), true
}

func (p *compactPalette) hasFastReadWord(offset uintptr) bool {
	if p == nil {
		return false
	}
	_, _, _, ok := compactPaletteDecodeFastReadWord(p.owners[compactAnchor(offset)>>3].Load())
	return ok
}

// tryFastZeroRead applies one exact aligned read to a palette default word
// without taking rangeBlock.mu or allocating a per-reader shape. Any competing
// canonical mutation changes the block revision or owner word and makes the
// optimistic result retry through the canonical path.
func (p *compactPalette) tryFastZeroRead(view blockView, compact *compactGroups, addr uintptr, current epoch.Epoch, clock *vectorclock.VectorClock, pc uintptr) bool {
	if p == nil || compact == nil || p.fastReadActive.Load() == 0 || p.zeroOwner < compactPaletteFirstShape ||
		addr&7 != 0 || view.history.state.Load() != nil || view.loadSlot((addr>>3)&rangeBlockWordMask) != nil {
		return false
	}
	p.fastReadUsers.Add(1)
	if p.fastReadActive.Load() == 0 {
		p.fastReadUsers.Add(-1)
		return false
	}
	result := p.tryFastZeroReadEnrolled(view, compact, addr, current, clock, pc)
	p.fastReadUsers.Add(-1)
	return result
}

func (p *compactPalette) tryFastZeroReadEnrolled(view blockView, compact *compactGroups, addr uintptr, current epoch.Epoch, clock *vectorclock.VectorClock, pc uintptr) bool {
	if existing := p.fastReadPC.Load(); existing != pc {
		if existing != 0 || pc == 0 || !p.fastReadPC.CompareAndSwap(0, pc) {
			return false
		}
	}
	wordIndex := compactAnchor(addr) >> 3
	cell := &p.owners[wordIndex]
	revision := compact.revision.Load()
	if revision&1 != 0 || compact.palette.Load() != p {
		return false
	}
	for {
		old := cell.Load()
		var next uint64
		if old == 0 {
			var ok bool
			next, ok = compactPaletteFastReadWord(p.zeroOwner, current, 0xff)
			if !ok {
				return false
			}
		} else {
			owner, prior, mask, ok := compactPaletteDecodeFastReadWord(old)
			if !ok || owner != p.zeroOwner || mask != 0xff {
				return false
			}
			priorTID, _ := prior.Decode()
			currentTID, _ := current.Decode()
			if prior != current && priorTID != currentTID &&
				(clock == nil || !prior.HappensBefore(clock)) {
				return false
			}
			var representable bool
			next, representable = compactPaletteFastReadWord(owner, current, mask)
			if !representable {
				return false
			}
			if next == old {
				break
			}
		}
		if cell.CompareAndSwap(old, next) {
			break
		}
	}
	word := cell.Load()
	_, represented, mask, ok := compactPaletteDecodeFastReadWord(word)
	return ok && mask == 0xff && represented != 0 && p.fastReadActive.Load() != 0 &&
		compact.palette.Load() == p && compact.revision.Load() == revision &&
		view.history.state.Load() == nil && view.loadSlot((addr>>3)&rangeBlockWordMask) == nil
}

func (p *compactPalette) disableFastReads() {
	if p == nil {
		return
	}
	p.fastReadActive.Store(0)
	for delay := uint32(1); p.fastReadUsers.Load() != 0; {
		runtimeKolkovSpinWait(delay, delay == spinlockMaxBackoff)
		if delay < spinlockMaxBackoff {
			delay <<= 1
		}
	}
}

// lookup deliberately returns a conservative authoritative miss for a dense
// shape. PageTable.Get materializes its exact eight-byte word under the block
// lock; the runtime fast path never calls it after active becomes nonzero.
func (p *compactPalette) lookup(anchor uintptr) (*VarState, bool, uint8) {
	owner := p.owner(anchor)
	switch owner {
	case compactPaletteDefault:
		return nil, false, owner
	case compactPaletteTombstone:
		return nil, true, owner
	default:
		return nil, true, owner
	}
}

func (p *compactPalette) moveAnchor(c *compactGroups, anchor uintptr, descriptor compactHistoryDescriptor) (*VarState, bool) {
	shape, writeLow, readLow := paletteShapeFromDescriptor(descriptor)
	old := p.owner(anchor)
	target := p.findShape(shape)
	reuseOld := false
	if target == 0 {
		available := false
		for index := 0; index < compactPaletteMaxShapes; index++ {
			record := p.shapeRecord(uint8(index)+compactPaletteFirstShape, false)
			if record == nil || record.members == 0 {
				available = true
				break
			}
		}
		if !available {
			// Capacity bounds simultaneous live shapes, not historical keys. If
			// this anchor is the sole member of its source shape, retarget that
			// record in place just as the bitmap representation retargets a sole
			// source group. Its owner ID and member count remain unchanged.
			record := p.shapeRecord(old, false)
			if old < compactPaletteFirstShape || record == nil || !record.used || record.members != 1 {
				return nil, false
			}
			reuseOld = true
		}
	}
	c.beginMutation()
	if target == 0 {
		if reuseOld {
			record := p.shapeRecord(old, false)
			p.removeShapeHash(old, record)
			record.shape = shape
			record.hashNext = p.hash[compactPaletteHash(shape)]
			p.hash[compactPaletteHash(shape)] = old
			target = old
		} else {
			target = p.ensureShape(shape)
			if target == 0 {
				runtimeThrow("race detector dense palette lost reserved scalar shape")
			}
		}
	}
	p.setLows(anchor, writeLow, readLow)
	p.setOwner(anchor, target)
	if old != target {
		p.releaseShapeMembers(old, 1)
		p.shapeRecord(target, false).members++
	}
	c.endMutation()
	// Scalar compact callers may compare pointer identity only to classify a
	// no-op. A changed dense state intentionally remains uncached and returns nil.
	return nil, true
}

func (p *compactPalette) tryScalar(c *compactGroups, anchor uintptr, current epoch.Epoch, clock *vectorclock.VectorClock, pc uintptr, write bool) (*VarState, bool) {
	owner := p.owner(anchor)
	descriptor := compactHistoryDescriptor{lifecycle: c.ensureLifecycle()}
	if owner >= compactPaletteFirstShape {
		var ok bool
		descriptor, ok = p.descriptor(anchor)
		if !ok {
			return nil, false
		}
	}
	var next compactHistoryKey
	var ok bool
	if write {
		if descriptor.history.read != 0 && !descriptor.history.sameReader(current) &&
			owner >= compactPaletteFirstShape && p.shapeRecord(owner, false).members == 1 {
			return nil, false
		}
		next, ok = descriptor.history.afterWrite(current, clock, pc)
	} else {
		next, ok = descriptor.history.afterRead(current, clock, pc)
	}
	if !ok {
		return nil, false
	}
	nextDescriptor := descriptor
	nextDescriptor.history = next
	if write && nextDescriptor == descriptor && owner >= compactPaletteFirstShape && p.shapeRecord(owner, false).members == 1 {
		return nil, false
	}
	if nextDescriptor == descriptor {
		return nil, true
	}
	return p.moveAnchor(c, anchor, nextDescriptor)
}

// tryUniformRangeLocked handles a range whose exact anchors all have one
// descriptor. Compiler scalar accesses make this the overwhelmingly common
// changed-range shape: all 2, 4, or 8 byte aliases were published together and
// remain one equivalence class until a partial access splits them.
//
// Requiring equal owners and equal packed clock lows is an exact descriptor
// proof, not a shape-only approximation. In particular, it preserves the
// per-anchor same-epoch-write distinction which requires the general planner
// for heterogeneous lows. A missing target may reuse the source record only
// when the selected anchors exhaust its complete membership; otherwise this
// helper leaves every bit unchanged and lets the general capacity preflight run.
func (p *compactPalette) tryUniformRangeLocked(c *compactGroups, start, end uintptr, defaultDescriptor compactHistoryDescriptor, current epoch.Epoch, clock *vectorclock.VectorClock, pc uintptr, write bool) (handled, ok bool) {
	if start == end {
		return true, false
	}

	owner := p.owner(start)
	descriptor := defaultDescriptor
	var source *compactPaletteShapeRecord
	var sourceWriteLow, sourceReadLow uint16
	switch owner {
	case compactPaletteDefault:
	case compactPaletteTombstone:
		descriptor = compactHistoryDescriptor{lifecycle: c.ensureLifecycle()}
	default:
		source = p.shapeRecord(owner, false)
		if source == nil || !source.used {
			return false, false
		}
		sourceWriteLow, sourceReadLow = p.lows(start)
	}

	if !p.rangeOwnerEqual(start, end, owner) {
		return false, false
	}
	if source != nil && !p.rangeLowsEqual(start, end, sourceWriteLow, sourceReadLow) {
		return false, false
	}
	if !write && source != nil && source.shape.flags&compactPaletteHasRead != 0 && source.shape.readPC == pc {
		currentTID, currentClock := current.Decode()
		if source.shape.readTID == currentTID && source.shape.readHigh == uint16(currentClock>>16) {
			writeOrdered := source.shape.flags&compactPaletteHasWrite == 0
			if !writeOrdered {
				write := epoch.NewEpoch(source.shape.writeTID, uint64(source.shape.writeHigh)<<16|uint64(sourceWriteLow))
				writeOrdered = compactHappensBefore(write, clock)
			}
			if !writeOrdered {
				return true, false
			}

			// A same-reader transition changes only the exact low clock bits.
			// Owner, shape, lifecycle, reporting PC, and write frontier remain
			// unchanged, so avoid descriptor expansion and palette hash churn.
			readLow := uint16(currentClock)
			if readLow != sourceReadLow {
				c.activate()
				c.beginMutation()
				p.setRangeReadLows(start, end, readLow)
				c.endMutation()
			}
			return true, true
		}
	}
	if source != nil {
		descriptor = source.shape.descriptor(sourceWriteLow, sourceReadLow)
	}

	var next compactHistoryKey
	if write {
		next, ok = descriptor.history.afterWrite(current, clock, pc)
	} else {
		next, ok = descriptor.history.afterRead(current, clock, pc)
	}
	if !ok {
		return true, false
	}
	if next == descriptor.history {
		// Match the scalar hot-write policy: when this sized access owns the
		// complete equivalence class, let authoritative fallback materialize it
		// once rather than rebuilding the same dense uniform proof forever.
		if write && source != nil && source.members == uint16(end-start) {
			return true, false
		}
		return true, true
	}
	nextDescriptor := descriptor
	nextDescriptor.history = next
	targetShape, writeLow, readLow := paletteShapeFromDescriptor(nextDescriptor)
	targetBucket := compactPaletteHash(targetShape)
	target := p.findShapeInBucket(targetShape, targetBucket)
	count := uint16(end - start)

	// If no cached target exists, retargeting the source record is sound only
	// when no anchor outside this transaction can still resolve through it.
	// Keeping the owner ID also avoids publishing thousands of redundant owner
	// stores for a full-equivalence-class range.
	if target == 0 {
		if source == nil {
			// A uniform default or tombstone range has exactly one destination.
			// Reserve it directly; only heterogeneous ranges need the general
			// three-pass capacity planner below.
			target = p.ensureShape(targetShape)
			if target == 0 {
				return true, false
			}
			c.activate()
			c.beginMutation()
			p.setUniformDescriptorRange(start, end, target, writeLow, readLow)
			p.shapeRecord(target, false).members += count
			c.endMutation()
			return true, true
		}
		if source.members != count {
			return false, false
		}
		c.activate()
		c.beginMutation()
		p.removeShapeHash(owner, source)
		source.shape = targetShape
		source.hashNext = p.hash[targetBucket]
		p.hash[targetBucket] = owner
		p.setUniformDescriptorRange(start, end, owner, writeLow, readLow)
		c.endMutation()
		return true, true
	}

	c.activate()
	c.beginMutation()
	p.setUniformDescriptorRange(start, end, target, writeLow, readLow)
	if target != owner {
		if source != nil {
			p.releaseShapeMembers(owner, count)
		}
		p.shapeRecord(target, false).members += count
	}
	c.endMutation()
	return true, true
}

// tryRangeLocked performs a three-pass transaction. The first pass validates
// every exact anchor (not merely every shape), which is essential because two
// anchors with the same shape can take different same-epoch-write branches.
// The second pass reserves every target shape and clock chunk. Only then does
// the odd-revision third pass publish lows followed by owners.
func (p *compactPalette) tryRangeLocked(c *compactGroups, offset, size uintptr, defaultState *VarState, current epoch.Epoch, clock *vectorclock.VectorClock, pc uintptr, write bool) bool {
	start, end := compactRange(offset, size)
	if current == 0 || start == end {
		return false
	}
	defaultDescriptor := compactHistoryDescriptor{lifecycle: c.ensureLifecycle()}
	if defaultState != nil {
		var ok bool
		defaultDescriptor, ok = compactDescriptorFromState(defaultState)
		if !ok {
			return false
		}
	}
	if handled, ok := p.tryUniformRangeLocked(c, start, end, defaultDescriptor, current, clock, pc, write); handled {
		return ok
	}
	descriptorAt := func(anchor uintptr) (compactHistoryDescriptor, bool) {
		switch owner := p.owner(anchor); owner {
		case compactPaletteDefault:
			return defaultDescriptor, true
		case compactPaletteTombstone:
			return compactHistoryDescriptor{lifecycle: c.ensureLifecycle()}, true
		default:
			return p.descriptor(anchor)
		}
	}
	transition := func(descriptor compactHistoryDescriptor) (compactHistoryDescriptor, bool) {
		var next compactHistoryKey
		var ok bool
		if write {
			next, ok = descriptor.history.afterWrite(current, clock, pc)
		} else {
			next, ok = descriptor.history.afterRead(current, clock, pc)
		}
		if !ok {
			return compactHistoryDescriptor{}, false
		}
		descriptor.history = next
		return descriptor, true
	}
	changed := false
	for anchor := start; anchor < end; anchor++ {
		descriptor, ok := descriptorAt(anchor)
		if !ok {
			return false
		}
		next, ok := transition(descriptor)
		if !ok {
			return false
		}
		changed = changed || next != descriptor
	}
	if !changed {
		return true
	}
	// Prove target-shape capacity without allocating or mutating the palette.
	// One representative anchor per distinct target bounds comparisons by the
	// palette's 253-shape ceiling and keeps the system-stack frame below 8 KiB.
	var representatives [compactPaletteMaxShapes]uintptr
	representativeCount := 0
	for anchor := start; anchor < end; anchor++ {
		descriptor, _ := descriptorAt(anchor)
		next, _ := transition(descriptor)
		shape, _, _ := paletteShapeFromDescriptor(next)
		duplicate := false
		for i := 0; i < representativeCount; i++ {
			previousDescriptor, _ := descriptorAt(representatives[i])
			previousNext, _ := transition(previousDescriptor)
			previousShape, _, _ := paletteShapeFromDescriptor(previousNext)
			if previousShape == shape {
				duplicate = true
				break
			}
		}
		if !duplicate {
			if representativeCount == len(representatives) {
				return false
			}
			representatives[representativeCount] = anchor
			representativeCount++
		}
	}
	available := 0
	for index := 0; index < compactPaletteMaxShapes; index++ {
		record := p.shapeRecord(uint8(index)+compactPaletteFirstShape, false)
		if record == nil || record.members == 0 {
			available++
		}
	}
	missing, cachedTargets := 0, 0
	for i := 0; i < representativeCount; i++ {
		descriptor, _ := descriptorAt(representatives[i])
		next, _ := transition(descriptor)
		shape, _, _ := paletteShapeFromDescriptor(next)
		if id := p.findShape(shape); id == 0 {
			missing++
		} else if p.shapeRecord(id, false).members == 0 {
			// A cached target is already one of the available records but must
			// remain reserved while unequal missing targets are installed.
			cachedTargets++
		}
	}
	if missing > available-cachedTargets {
		return false
	}
	c.activate()
	c.beginMutation()
	p.plan++
	if p.plan == 0 {
		p.plan = 1
		for index := 0; index < compactPaletteMaxShapes; index++ {
			if record := p.shapeRecord(uint8(index)+compactPaletteFirstShape, false); record != nil {
				record.reserved = 0
			}
		}
	}
	generation := p.plan
	// Reserve every already-cached target before installing any missing target.
	// Otherwise input order could let an early missing shape recycle a later
	// cached target and make the capacity proof false.
	for i := 0; i < representativeCount; i++ {
		descriptor, _ := descriptorAt(representatives[i])
		next, _ := transition(descriptor)
		shape, _, _ := paletteShapeFromDescriptor(next)
		if id := p.findShape(shape); id != 0 {
			p.shapeRecord(id, false).reserved = generation
		}
	}
	for anchor := start; anchor < end; anchor++ {
		descriptor, _ := descriptorAt(anchor)
		next, _ := transition(descriptor)
		shape, _, _ := paletteShapeFromDescriptor(next)
		if p.ensureShapeForPlan(shape, generation) == 0 {
			runtimeThrow("race detector dense palette lost reserved range shape")
		}
		p.clockStorage(anchor, true)
	}
	for anchor := start; anchor < end; anchor++ {
		descriptor, _ := descriptorAt(anchor)
		next, _ := transition(descriptor)
		shape, writeLow, readLow := paletteShapeFromDescriptor(next)
		target := p.findShape(shape)
		old := p.owner(anchor)
		p.setLows(anchor, writeLow, readLow)
		p.setOwner(anchor, target)
		if old != target {
			p.releaseShapeMembers(old, 1)
			p.shapeRecord(target, false).members++
		}
	}
	c.endMutation()
	return true
}

func (p *compactPalette) coveredWord(wordOffset uintptr) uint8 {
	base := compactAnchor(wordOffset) &^ uintptr(7)
	var mask uint8
	for lane := uintptr(0); lane < 8; lane++ {
		if p.owner(base+lane) >= compactPaletteFirstShape {
			mask |= 1 << lane
		}
	}
	return mask
}

func (p *compactPalette) tombstoneWord(wordOffset uintptr) uint8 {
	base := compactAnchor(wordOffset) &^ uintptr(7)
	var mask uint8
	for lane := uintptr(0); lane < 8; lane++ {
		if p.owner(base+lane) == compactPaletteTombstone {
			mask |= 1 << lane
		}
	}
	return mask
}

func (p *compactPalette) materializeWord(wordOffset uintptr, slot *ShadowSlot) {
	base := compactAnchor(wordOffset) &^ uintptr(7)
	var descriptors [8]compactHistoryDescriptor
	var states [8]*VarState
	for lane := uintptr(0); lane < 8; lane++ {
		descriptor, ok := p.descriptor(base + lane)
		if !ok {
			continue
		}
		for previous := uintptr(0); previous < lane; previous++ {
			if states[previous] != nil && descriptors[previous] == descriptor {
				states[lane] = states[previous]
				break
			}
		}
		if states[lane] == nil {
			states[lane] = compactStateFromDescriptor(descriptor)
		}
		descriptors[lane] = descriptor
		slot.states[lane].Store(states[lane])
	}
}

func (p *compactPalette) setRangeOwner(offset, size uintptr, owner uint8) {
	start, end := compactRange(offset, size)
	for start < end {
		lane := start & 7
		count := uintptr(8) - lane
		if count > end-start {
			count = end - start
		}
		cell := &p.owners[start>>3]
		if fastOwner, read, readMask, ok := compactPaletteDecodeFastReadWord(cell.Load()); ok &&
			(owner == compactPaletteDefault || owner == compactPaletteTombstone) {
			selected := uint8(((uint16(1) << count) - 1) << lane)
			readMask &^= selected
			if readMask == 0 {
				cell.Store(uint64(compactPaletteTombstone) * 0x0101010101010101)
			} else {
				word, representable := compactPaletteFastReadWord(fastOwner, read, readMask)
				if !representable {
					runtimeThrow("race detector dense palette lost fast-read word")
				}
				cell.Store(word)
			}
			start += count
			continue
		}
		for anchor := start; anchor < start+count; anchor++ {
			old := p.owner(anchor)
			if old >= compactPaletteFirstShape {
				record := p.shapeRecord(old, false)
				if record != nil && record.members != 0 {
					p.releaseShapeMembers(old, 1)
				}
			}
		}
		p.setUniformRangeOwner(start, start+count, owner)
		start += count
	}
}

func (p *compactPalette) clearRange(c *compactGroups, offset, size uintptr, defaultHasHistory, forceAdvance bool) {
	start, end := compactRange(offset, size)
	if start == end {
		return
	}
	p.disableFastReads()
	target := compactPaletteDefault
	if defaultHasHistory {
		target = compactPaletteTombstone
	}
	advanceLifecycle := forceAdvance
	mutate := forceAdvance
	for anchor := start; anchor < end; anchor++ {
		owner := p.owner(anchor)
		if owner != target {
			mutate = true
		}
		if owner >= compactPaletteFirstShape || owner == compactPaletteDefault && defaultHasHistory {
			advanceLifecycle = true
		}
	}
	if !mutate && !advanceLifecycle {
		return
	}
	c.activate()
	c.beginMutation()
	p.setRangeOwner(start, end-start, target)
	if advanceLifecycle {
		c.lifecycle = allocateLifecycleID()
	}
	c.endMutation()
}

func (p *compactPalette) reset(c *compactGroups) {
	p.disableFastReads()
	c.beginMutation()
	c.palette.Store(nil)
	for i := 0; i < compactInlineGroups; i++ {
		c.groups[i].Store(nil)
	}
	c.overflow.Store(nil)
	c.tombstones.Store(nil)
	c.lifecycle = allocateLifecycleID()
	c.endMutation()
}

// paletteMigrationValid is an allocation-free first pass. Failed speculative
// early admission must not add a palette object or clock plane to an
// unsupported block; it leaves the established bitmap oracle untouched.
func paletteMigrationValid(c *compactGroups) bool {
	var occupied [compactMembershipWords]uint64
	for i := 0; i < compactGroupCapacity; i++ {
		group := c.groupLoad(i)
		if group == nil || group.empty() {
			continue
		}
		state := group.state.Load()
		if group.retired || !group.joinable || state == nil {
			return false
		}
		descriptor, ok := compactDescriptorFromState(state)
		if !ok || descriptor != group.descriptor {
			return false
		}
		for word := 0; word < compactMembershipWords; word++ {
			members := group.membershipWord(word)
			if occupied[word]&members != 0 {
				return false
			}
			occupied[word] |= members
		}
	}
	if tombstones := c.tombstones.Load(); tombstones != nil {
		for word := range tombstones {
			value := tombstones[word].Load()
			if occupied[word]&value != 0 {
				return false
			}
		}
	}
	return true
}

// densePaletteFromBitmaps builds a private exact palette after the allocation-
// free validation pass. Publication and old bitmap retirement are performed by
// compactGroups.upgradePalette.
func densePaletteFromBitmaps(c *compactGroups) *compactPalette {
	if !paletteMigrationValid(c) {
		return nil
	}
	p := new(compactPalette)
	p.zeroOwner = p.ensureShape(compactPaletteShape{lifecycle: c.lifecycle})
	if p.zeroOwner == 0 {
		return nil
	}
	// The zero-lifecycle record is immutable for the palette lifetime. Fast
	// read words carry their own membership mask, so the sentinel keeps normal
	// shape recycling from repurposing this exact lifecycle witness.
	p.shapeRecord(p.zeroOwner, false).members = ^uint16(0)
	p.fastReadActive.Store(1)
	for i := 0; i < compactGroupCapacity; i++ {
		group := c.groupLoad(i)
		if group == nil || group.empty() {
			continue
		}
		shape, writeLow, readLow := paletteShapeFromDescriptor(group.descriptor)
		owner := p.ensureShape(shape)
		if owner == 0 {
			return nil
		}
		for word := 0; word < compactMembershipWords; word++ {
			members := group.membershipWord(word)
			// Bitmap membership words and owner words are both naturally aligned.
			// Install complete eight-anchor classes directly in the root so the
			// private migration never allocates a clock chunk for boxed uint64s.
			for lanes := uintptr(0); lanes < 64; lanes += 8 {
				mask := uint64(0xff) << lanes
				if members&mask != mask {
					continue
				}
				anchor := uintptr(word*64) + lanes
				p.storeFusedOwnerWord(anchor, owner, writeLow, readLow)
				p.shapeRecord(owner, false).members += 8
				members &^= mask
			}
			for members != 0 {
				bit := uint(bitsTrailingZeros64(members))
				anchor := uintptr(word*64) + uintptr(bit)
				p.setLows(anchor, writeLow, readLow)
				p.setOwner(anchor, owner)
				p.shapeRecord(owner, false).members++
				members &^= uint64(1) << bit
			}
		}
	}
	if tombstones := c.tombstones.Load(); tombstones != nil {
		for word := range tombstones {
			value := tombstones[word].Load()
			for value != 0 {
				bit := uint(bitsTrailingZeros64(value))
				anchor := uintptr(word*64) + uintptr(bit)
				p.setOwner(anchor, compactPaletteTombstone)
				value &^= uint64(1) << bit
			}
		}
	}
	return p
}

// Keep math/bits out of migration's hot compilation unit call graph.
func bitsTrailingZeros64(value uint64) int {
	if value == 0 {
		return 64
	}
	n := 0
	for value&1 == 0 {
		value >>= 1
		n++
	}
	return n
}

func (c *compactGroups) allocatedGroupCount() int {
	count := 0
	for i := 0; i < compactGroupCapacity; i++ {
		if c.groupLoad(i) != nil {
			count++
		}
	}
	return count
}

func (c *compactGroups) upgradePalette() *compactPalette {
	if palette := c.palette.Load(); palette != nil {
		return palette
	}
	palette := densePaletteFromBitmaps(c)
	if palette == nil {
		return nil
	}
	c.activate()
	c.beginMutation()
	c.palette.Store(palette)
	for i := 0; i < compactInlineGroups; i++ {
		c.groups[i].Store(nil)
	}
	c.overflow.Store(nil)
	c.tombstones.Store(nil)
	c.endMutation()
	return palette
}
