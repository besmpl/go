//go:build amd64 || arm64

package shadowmem

import (
	"testing"
	"unsafe"

	"runtime/race/kolkov/epoch"
)

func TestCompactPaletteStorageCrossover(t *testing.T) {
	if got, want := unsafe.Sizeof(compactPalette{}), uintptr(4680); got != want {
		t.Fatalf("palette root size=%d, want %d", got, want)
	}
	if got, want := unsafe.Sizeof(compactPaletteClockChunk{}), uintptr(512); got != want {
		t.Fatalf("palette clock chunk size=%d, want %d", got, want)
	}
}

func TestPageTableCompactPaletteReadNoopClassification(t *testing.T) {
	pt := NewPageTableShadow()
	const base = uintptr(0x580000)
	current := compactTestEpoch(9, 2)
	clock := compactTestClock(current)

	// Seven simultaneous history classes cross the six-group bitmap threshold
	// and place the target in the dense representation exercised by globals in
	// BenchmarkRaceRead.
	for i := uintptr(0); i <= compactPaletteBitmapCrossover; i++ {
		addr := base + i*17
		if !pt.TryCompactWrite(addr, current, clock, 0xa000+i) {
			t.Fatalf("dense seed %d failed", i)
		}
	}
	view, _ := pt.blockFor(base, false)
	compact := view.history.compact.Load()
	if compact == nil || compact.palette.Load() == nil {
		t.Fatal("test setup did not install the dense palette")
	}

	const firstReadPC = uintptr(0xb001)
	if cacheable, ok := pt.TryCompactRead(base, current, clock, firstReadPC); !ok || cacheable {
		t.Fatalf("changed dense read = (%v,%v), want (false,true)", cacheable, ok)
	}
	if cacheable, ok := pt.TryCompactRead(base, current, clock, firstReadPC); !ok || !cacheable {
		t.Fatalf("exact dense read no-op = (%v,%v), want (true,true)", cacheable, ok)
	}

	const changedReadPC = uintptr(0xb002)
	if cacheable, ok := pt.TryCompactRead(base, current, clock, changedReadPC); !ok || cacheable {
		t.Fatalf("changed-PC dense read = (%v,%v), want (false,true)", cacheable, ok)
	}
	if cacheable, ok := pt.TryCompactRead(base, current, clock, changedReadPC); !ok || !cacheable {
		t.Fatalf("repeated changed-PC dense read = (%v,%v), want (true,true)", cacheable, ok)
	}
	if slot := pt.GetSlot(base); slot != nil {
		t.Fatalf("dense read classification materialized exact slot %p", slot)
	}
}

func paletteTestGroups(t *testing.T) *compactGroups {
	t.Helper()
	groups := newCompactGroups()
	current := compactTestEpoch(9, 1)
	clock := compactTestClock(current)
	for i := 0; i <= compactGroupCapacity; i++ {
		if _, ok := groups.tryWrite(uintptr(i*17), current, clock, uintptr(100+i)); !ok {
			t.Fatalf("history %d failed", i)
		}
	}
	if groups.palette.Load() == nil {
		t.Fatal("dense palette not installed")
	}
	return groups
}

func installPaletteShape(t *testing.T, palette *compactPalette, anchor uintptr, descriptor compactHistoryDescriptor) uint8 {
	t.Helper()
	shape, writeLow, readLow := paletteShapeFromDescriptor(descriptor)
	owner := palette.ensureShape(shape)
	if owner == 0 {
		t.Fatalf("no palette record for anchor %d descriptor %+v", anchor, descriptor)
	}
	palette.setLows(anchor, writeLow, readLow)
	palette.setOwner(anchor, owner)
	palette.shapeRecord(owner, false).members++
	return owner
}

func TestCompactPaletteUniformMissingTargetAdoption(t *testing.T) {
	groups := newCompactGroups()
	palette := new(compactPalette)
	groups.palette.Store(palette)
	current := epoch.NewEpoch(31, 0x10002)
	clock := compactTestClock(current)
	const (
		start = uintptr(37)
		size  = uintptr(8)
		pc    = uintptr(0x7310)
	)

	if !groups.tryRange(start, size, nil, current, clock, pc, true) {
		t.Fatal("uniform default range fell back")
	}
	owner := palette.owner(start)
	record := palette.shapeRecord(owner, false)
	if owner < compactPaletteFirstShape || record == nil || record.members != uint16(size) {
		t.Fatalf("adopted owner=%d record=%+v, want one %d-member shape", owner, record, size)
	}
	if palette.plan != 0 {
		t.Fatalf("uniform adoption entered the general planner: generation=%d", palette.plan)
	}
	for anchor := start; anchor < start+size; anchor++ {
		descriptor, ok := palette.descriptor(anchor)
		if !ok || descriptor.history.write != current || descriptor.history.writePC != pc ||
			descriptor.history.writeCount != 1 || descriptor.lifecycle != groups.lifecycle ||
			palette.owner(anchor) != owner {
			t.Fatalf("anchor %d descriptor=%+v ok=%v owner=%d, want W=%v PC=%#x lifecycle=%v owner=%d",
				anchor, descriptor, ok, palette.owner(anchor), current, pc, groups.lifecycle, owner)
		}
	}

	beforeLifecycle := groups.lifecycle
	groups.clearRange(start, size)
	if groups.lifecycle == beforeLifecycle {
		t.Fatal("conservative clear did not advance the tombstone lifecycle")
	}
	reader := epoch.NewEpoch(32, 0x20003)
	const readPC = uintptr(0x7320)
	if !groups.tryRange(start, size, nil, reader, compactTestClock(reader), readPC, false) {
		t.Fatal("uniform tombstone range fell back")
	}
	if palette.plan != 0 {
		t.Fatalf("tombstone adoption entered the general planner: generation=%d", palette.plan)
	}
	for anchor := start; anchor < start+size; anchor++ {
		descriptor, ok := palette.descriptor(anchor)
		if !ok || descriptor.history.read != reader || descriptor.history.readPC != readPC ||
			descriptor.lifecycle != groups.lifecycle {
			t.Fatalf("tombstone anchor %d descriptor=%+v ok=%v, want R=%v PC=%#x lifecycle=%v",
				anchor, descriptor, ok, reader, readPC, groups.lifecycle)
		}
	}
}

func TestCompactPaletteUniformMissingTargetCapacityFailureIsAtomic(t *testing.T) {
	groups := newCompactGroups()
	palette := new(compactPalette)
	groups.palette.Store(palette)
	current := epoch.NewEpoch(33, 7)
	lifecycle := groups.ensureLifecycle()
	for i := 0; i < compactPaletteMaxShapes; i++ {
		descriptor := compactHistoryDescriptor{
			history: compactHistoryKey{
				write:           current,
				exclusiveWriter: 33,
				writePC:         uintptr(0x7400 + i),
				writeCount:      1,
			},
			lifecycle: lifecycle,
		}
		installPaletteShape(t, palette, uintptr(i), descriptor)
	}
	const (
		start = uintptr(3000)
		size  = uintptr(8)
	)
	beforeRevision := groups.revision.Load()
	beforeHash := palette.hash
	beforePlan, beforeNext, beforeFree := palette.plan, palette.nextShape, palette.freeHead
	if groups.tryRange(start, size, nil, current, compactTestClock(current), 0x7fff, true) {
		t.Fatal("uniform default range unexpectedly fit at live-shape capacity")
	}
	if groups.revision.Load() != beforeRevision || palette.hash != beforeHash || palette.plan != beforePlan ||
		palette.nextShape != beforeNext || palette.freeHead != beforeFree {
		t.Fatalf("failed adoption mutated metadata: revision %d/%d plan %d/%d next %d/%d free %d/%d",
			groups.revision.Load(), beforeRevision, palette.plan, beforePlan,
			palette.nextShape, beforeNext, palette.freeHead, beforeFree)
	}
	for anchor := start; anchor < start+size; anchor++ {
		if owner := palette.owner(anchor); owner != compactPaletteDefault {
			t.Fatalf("failed adoption published owner %d at anchor %d", owner, anchor)
		}
		if writeLow, readLow := palette.lows(anchor); writeLow != 0 || readLow != 0 {
			t.Fatalf("failed adoption published lows (%d,%d) at anchor %d", writeLow, readLow, anchor)
		}
	}
}

func TestCompactPaletteFreeListSkipsRevivedRecords(t *testing.T) {
	palette := new(compactPalette)
	lifecycle := allocateLifecycleID()
	shape := func(pc uintptr) compactPaletteShape {
		value, _, _ := paletteShapeFromDescriptor(compactHistoryDescriptor{
			history: compactHistoryKey{
				write:           epoch.NewEpoch(34, 1),
				exclusiveWriter: 34,
				writePC:         pc,
				writeCount:      1,
			},
			lifecycle: lifecycle,
		})
		return value
	}

	shapeA, shapeB := shape(0x7501), shape(0x7502)
	idA, idB := palette.ensureShape(shapeA), palette.ensureShape(shapeB)
	palette.shapeRecord(idA, false).members = 1
	palette.shapeRecord(idB, false).members = 1
	palette.releaseShapeMembers(idA, 1)
	if palette.freeHead != idA || !palette.shapeRecord(idA, false).free {
		t.Fatalf("first free record: head=%d record=%+v, want %d", palette.freeHead, palette.shapeRecord(idA, false), idA)
	}

	// Finding a cached zero-member shape can revive it while its validated list
	// entry remains pending. A later pop must skip the now-live record.
	if got := palette.ensureShape(shapeA); got != idA {
		t.Fatalf("cached shape owner=%d, want %d", got, idA)
	}
	palette.shapeRecord(idA, false).members = 1
	palette.releaseShapeMembers(idB, 1)
	idC := palette.ensureShape(shape(0x7503))
	if idC != idB {
		t.Fatalf("new shape recycled owner=%d, want newest free owner %d", idC, idB)
	}
	palette.shapeRecord(idC, false).members = 1
	idD := palette.ensureShape(shape(0x7504))
	if idD == idA || idD == idB {
		t.Fatalf("free-list pop recycled a live owner: got %d, live %d/%d", idD, idA, idB)
	}

	palette.releaseShapeMembers(idA, 1)
	shapeE := shape(0x7505)
	if got := palette.ensureShape(shapeE); got != idA {
		t.Fatalf("released revived owner recycled as %d, want %d", got, idA)
	}
	if palette.findShape(shapeA) != 0 || palette.findShape(shapeE) != idA {
		t.Fatal("recycling left a stale or missing palette hash entry")
	}
}

func TestCompactPaletteFreeListSurvivesPlanReservations(t *testing.T) {
	palette := new(compactPalette)
	shape := func(pc uintptr) compactPaletteShape {
		return compactPaletteShape{writeTID: 35, writePC: pc, writeCount: 1, lifecycle: allocateLifecycleID(), flags: compactPaletteHasWrite}
	}
	idA, idB := palette.ensureShape(shape(0x7601)), palette.ensureShape(shape(0x7602))
	palette.shapeRecord(idA, false).members = 1
	palette.shapeRecord(idB, false).members = 1
	palette.releaseShapeMembers(idA, 1)
	palette.releaseShapeMembers(idB, 1)
	if got := palette.ensureShapeForPlan(shape(0x7603), 1); got != idA {
		t.Fatalf("first planned shape owner=%d, want %d", got, idA)
	}
	if got := palette.ensureShapeForPlan(shape(0x7604), 1); got != idB {
		t.Fatalf("second planned shape owner=%d, want %d", got, idB)
	}
	palette.shapeRecord(idA, false).members = 1
	palette.shapeRecord(idB, false).members = 1
	if got := palette.ensureShape(shape(0x7605)); got == idA || got == idB {
		t.Fatalf("free-list metadata overrode live plan reservations: got %d, live %d/%d", got, idA, idB)
	}
}

func TestCompactPalettePlanGenerationWrap(t *testing.T) {
	groups := newCompactGroups()
	palette := new(compactPalette)
	groups.palette.Store(palette)
	writer := epoch.NewEpoch(40, 3)
	lifecycle := groups.ensureLifecycle()
	for anchor := uintptr(0); anchor < 2; anchor++ {
		installPaletteShape(t, palette, anchor, compactHistoryDescriptor{
			history: compactHistoryKey{
				write:           writer,
				exclusiveWriter: 40,
				writePC:         0x7901 + anchor,
				writeCount:      1,
			},
			lifecycle: lifecycle,
		})
	}
	var oldOwners [2]uint8
	for anchor := range oldOwners {
		oldOwners[anchor] = palette.owner(uintptr(anchor))
		palette.shapeRecord(oldOwners[anchor], false).reserved = 1
	}
	palette.plan = ^uint16(0)
	reader := epoch.NewEpoch(41, 5)
	clock := compactTestClock(writer, reader)
	if !groups.tryRange(0, 2, nil, reader, clock, 0x7910, false) {
		t.Fatal("heterogeneous transition failed across plan-generation wrap")
	}
	if palette.plan != 1 {
		t.Fatalf("wrapped plan generation=%d, want 1", palette.plan)
	}
	for anchor, oldOwner := range oldOwners {
		if record := palette.shapeRecord(oldOwner, false); record.reserved != 0 {
			t.Fatalf("old owner %d retained reservation %d after wrap", oldOwner, record.reserved)
		}
		descriptor, ok := palette.descriptor(uintptr(anchor))
		if !ok || descriptor.history.write != writer || descriptor.history.read != reader || descriptor.history.readPC != 0x7910 {
			t.Fatalf("anchor %d descriptor=%+v ok=%v after wrap", anchor, descriptor, ok)
		}
	}
}

func TestCompactPaletteResetBuildsAcyclicFreeList(t *testing.T) {
	groups := newCompactGroups()
	palette := new(compactPalette)
	groups.palette.Store(palette)
	const records = 20
	var owners [records]uint8
	for i := 0; i < records; i++ {
		owners[i] = palette.ensureShape(compactPaletteShape{
			writeTID:  42,
			writePC:   uintptr(0x7a00 + i),
			lifecycle: allocateLifecycleID(),
			flags:     compactPaletteHasWrite,
		})
		palette.shapeRecord(owners[i], false).members = 1
	}
	for i, owner := range owners {
		if i%2 == 0 {
			palette.releaseShapeMembers(owner, 1)
		}
	}
	palette.reset(groups)
	var seen [256]bool
	count := 0
	for owner := palette.freeHead; owner != 0; {
		if seen[owner] {
			t.Fatalf("free-list cycle or duplicate at owner %d", owner)
		}
		seen[owner] = true
		record := palette.shapeRecord(owner, false)
		if record == nil || !record.free || record.members != 0 {
			t.Fatalf("invalid reset free owner %d: %+v", owner, record)
		}
		owner = record.freeNext
		count++
		if count > compactPaletteMaxShapes {
			t.Fatal("free list exceeded the bounded shape capacity")
		}
	}
	// Shape storage is allocated in chunks; reset can also make the unused tail
	// in the final allocated chunk immediately reusable.
	want := (records + compactPaletteShapeChunkRecords - 1) / compactPaletteShapeChunkRecords * compactPaletteShapeChunkRecords
	if count != want {
		t.Fatalf("reset free records=%d, want %d allocated records", count, want)
	}
}

func TestCompactPaletteTIDZeroRoundTrip(t *testing.T) {
	descriptor := compactHistoryDescriptor{
		history: compactHistoryKey{
			write:           epoch.NewEpoch(0, 0x10002),
			read:            epoch.NewEpoch(0, 0x20003),
			exclusiveWriter: -1,
			writePC:         0x7010,
			readPC:          0x7020,
			writeCount:      9,
		},
		lifecycle: allocateLifecycleID(),
	}
	shape, writeLow, readLow := paletteShapeFromDescriptor(descriptor)
	if shape.flags != compactPaletteHasWrite|compactPaletteHasRead || shape.writeTID != 0 || shape.readTID != 0 {
		t.Fatalf("TID-zero presence = flags %#x writeTID %d readTID %d", shape.flags, shape.writeTID, shape.readTID)
	}
	if got := shape.descriptor(writeLow, readLow); got != descriptor {
		t.Fatalf("TID-zero round trip = %+v, want %+v", got, descriptor)
	}
}

func TestCompactPaletteExactClockCardinality(t *testing.T) {
	for _, count := range []int{17, 65, 257} {
		groups := newCompactGroups()
		for i := 1; i <= count; i++ {
			current := epoch.NewEpoch(7, uint64(i))
			if _, ok := groups.tryWrite(uintptr(i), current, compactTestClock(current), 0x7000); !ok {
				t.Fatalf("count %d history %d failed", count, i)
			}
		}
		palette := groups.palette.Load()
		if palette == nil {
			t.Fatalf("count %d did not upgrade", count)
		}
		for _, i := range []int{1, count / 2, count} {
			descriptor, ok := palette.descriptor(uintptr(i))
			want := epoch.NewEpoch(7, uint64(i))
			if !ok || descriptor.history.write != want {
				t.Fatalf("count %d anchor %d write=%v ok=%v, want %v", count, i, descriptor.history.write, ok, want)
			}
		}
	}
}

func TestCompactPaletteScalarRecyclesSoleSourceAtCapacity(t *testing.T) {
	groups := newCompactGroups()
	palette := new(compactPalette)
	groups.palette.Store(palette)
	groups.activate()
	current := epoch.NewEpoch(7, 3)
	lifecycle := groups.ensureLifecycle()
	var sourceShape compactPaletteShape
	var sourceOwner uint8
	for i := 0; i < compactPaletteMaxShapes; i++ {
		descriptor := compactHistoryDescriptor{
			history: compactHistoryKey{
				write:           current,
				exclusiveWriter: 7,
				writePC:         uintptr(0x8000 + i),
				writeCount:      1,
			},
			lifecycle: lifecycle,
		}
		owner := installPaletteShape(t, palette, uintptr(i), descriptor)
		if i == 0 {
			sourceShape, _, _ = paletteShapeFromDescriptor(descriptor)
			sourceOwner = owner
		}
	}
	if _, ok := groups.tryRead(0, current, compactTestClock(current), 0x9000); !ok {
		t.Fatal("sole-source transition fell back at the live-shape capacity")
	}
	if palette.owner(0) != sourceOwner || palette.findShape(sourceShape) != 0 {
		t.Fatalf("sole source was not retargeted in place: owner=%d want=%d oldShape=%d",
			palette.owner(0), sourceOwner, palette.findShape(sourceShape))
	}
	record := palette.shapeRecord(sourceOwner, false)
	descriptor, ok := palette.descriptor(0)
	if !ok || record.members != 1 || descriptor.history.write != current ||
		descriptor.history.read != current || descriptor.history.readPC != 0x9000 {
		t.Fatalf("retargeted descriptor=%+v ok=%v members=%d", descriptor, ok, record.members)
	}
}

func TestCompactPaletteUniformRangeRecyclesExhaustedSourceAtCapacity(t *testing.T) {
	groups := newCompactGroups()
	palette := new(compactPalette)
	groups.palette.Store(palette)
	groups.activate()
	current := epoch.NewEpoch(7, 3)
	lifecycle := groups.ensureLifecycle()
	sourceDescriptor := compactHistoryDescriptor{
		history: compactHistoryKey{
			write:           current,
			exclusiveWriter: 7,
			writePC:         0x8000,
			writeCount:      1,
		},
		lifecycle: lifecycle,
	}
	var sourceOwner uint8
	for anchor := uintptr(0); anchor < 8; anchor++ {
		owner := installPaletteShape(t, palette, anchor, sourceDescriptor)
		if anchor == 0 {
			sourceOwner = owner
		} else if owner != sourceOwner {
			t.Fatalf("uniform source owner=%d, want %d", owner, sourceOwner)
		}
	}
	sourceShape, _, _ := paletteShapeFromDescriptor(sourceDescriptor)
	for i := 1; i < compactPaletteMaxShapes; i++ {
		descriptor := sourceDescriptor
		descriptor.history.writePC += uintptr(i)
		installPaletteShape(t, palette, uintptr(7+i), descriptor)
	}
	if record := palette.shapeRecord(sourceOwner, false); record == nil || record.members != 8 {
		t.Fatalf("source members=%v, want 8", record)
	}

	const readPC = uintptr(0x9000)
	if !groups.tryRange(0, 8, nil, current, compactTestClock(current), readPC, false) {
		t.Fatal("uniform exhausted-source transition fell back at live-shape capacity")
	}
	want := sourceDescriptor
	want.history, _ = want.history.afterRead(current, compactTestClock(current), readPC)
	if palette.findShape(sourceShape) != 0 {
		t.Fatal("exhausted source shape remained in the palette hash")
	}
	for anchor := uintptr(0); anchor < 8; anchor++ {
		got, ok := palette.descriptor(anchor)
		if !ok || got != want || palette.owner(anchor) != sourceOwner {
			t.Fatalf("anchor %d descriptor=%+v ok=%v owner=%d, want %+v owner=%d",
				anchor, got, ok, palette.owner(anchor), want, sourceOwner)
		}
	}
}

func TestCompactPaletteBitmapCrossoverAndEmptyDonor(t *testing.T) {
	current := epoch.NewEpoch(7, 4)
	clock := compactTestClock(current)

	t.Run("seventh simultaneous class upgrades", func(t *testing.T) {
		groups := newCompactGroups()
		for i := 0; i < compactPaletteBitmapCrossover; i++ {
			if _, ok := groups.tryWrite(uintptr(i), current, clock, uintptr(0xd000+i)); !ok {
				t.Fatalf("seed class %d failed", i)
			}
		}
		if groups.palette.Load() != nil || groups.allocatedGroupCount() != compactPaletteBitmapCrossover {
			t.Fatalf("premature crossover: palette=%p groups=%d", groups.palette.Load(), groups.allocatedGroupCount())
		}
		if _, ok := groups.tryWrite(100, current, clock, 0xd100); !ok || groups.palette.Load() == nil {
			t.Fatalf("seventh class admission ok=%v palette=%p", ok, groups.palette.Load())
		}
	})

	t.Run("empty donor stays bitmap", func(t *testing.T) {
		groups := newCompactGroups()
		for i := 0; i < compactPaletteBitmapCrossover; i++ {
			if _, ok := groups.tryWrite(uintptr(i), current, clock, uintptr(0xe000+i)); !ok {
				t.Fatalf("seed class %d failed", i)
			}
		}
		if _, ok := groups.tryWrite(0, current, clock, 0xe001); !ok {
			t.Fatal("convergence failed")
		}
		if _, ok := groups.tryWrite(100, current, clock, 0xe100); !ok {
			t.Fatal("empty-donor admission failed")
		}
		if groups.palette.Load() != nil || groups.allocatedGroupCount() != compactPaletteBitmapCrossover {
			t.Fatalf("empty donor triggered dense allocation: palette=%p groups=%d", groups.palette.Load(), groups.allocatedGroupCount())
		}
	})

	t.Run("unsupported migration continues bitmap", func(t *testing.T) {
		groups := newCompactGroups()
		for i := 0; i < compactPaletteBitmapCrossover; i++ {
			if _, ok := groups.tryWrite(uintptr(i), current, clock, uintptr(0xf000+i)); !ok {
				t.Fatalf("seed class %d failed", i)
			}
		}
		blocker, overlap := groups.lookupGroup(0)
		if blocker == nil || overlap {
			t.Fatalf("blocker=%p overlap=%v", blocker, overlap)
		}
		blocker.joinable = false
		if _, ok := groups.tryWrite(100, current, clock, 0xf100); !ok {
			t.Fatal("unsupported block did not continue to physical capacity")
		}
		if groups.palette.Load() != nil || groups.allocatedGroupCount() != compactPaletteBitmapCrossover+1 {
			t.Fatalf("unsupported crossover: palette=%p groups=%d", groups.palette.Load(), groups.allocatedGroupCount())
		}
		blocker.joinable = true
	})
}

func TestCompactPaletteRangeCrossover(t *testing.T) {
	groups := newCompactGroups()
	current := epoch.NewEpoch(8, 5)
	clock := compactTestClock(current)
	for i := 0; i < compactPaletteBitmapCrossover; i++ {
		if _, ok := groups.tryWrite(uintptr(i), current, clock, uintptr(0x11000+i)); !ok {
			t.Fatalf("seed class %d failed", i)
		}
	}
	if !groups.tryRange(3000, 3, nil, current, clock, 0x11100, true) {
		t.Fatal("seventh-class range admission failed")
	}
	palette := groups.palette.Load()
	if palette == nil {
		t.Fatal("seventh simultaneous range class did not upgrade")
	}
	for anchor := uintptr(3000); anchor < 3003; anchor++ {
		descriptor, ok := palette.descriptor(anchor)
		if !ok || descriptor.history.writePC != 0x11100 {
			t.Fatalf("range anchor %d descriptor=(%+v,%v)", anchor, descriptor, ok)
		}
	}
}

func TestCompactPaletteRangeCapacityReservesCachedTarget(t *testing.T) {
	groups := newCompactGroups()
	palette := new(compactPalette)
	groups.palette.Store(palette)
	groups.activate()
	current := epoch.NewEpoch(8, 4)
	lifecycle := groups.ensureLifecycle()
	var first compactHistoryDescriptor
	for i := 0; i < compactPaletteMaxShapes-1; i++ {
		descriptor := compactHistoryDescriptor{
			history: compactHistoryKey{
				write:           current,
				exclusiveWriter: 8,
				writePC:         uintptr(0xa000 + i),
				writeCount:      1,
			},
			lifecycle: lifecycle,
		}
		installPaletteShape(t, palette, uintptr(i), descriptor)
		if i == 0 {
			first = descriptor
		}
	}
	first.history, _ = first.history.afterRead(current, compactTestClock(current), 0xb000)
	cachedShape, _, _ := paletteShapeFromDescriptor(first)
	cachedOwner := palette.ensureShape(cachedShape)
	if cachedOwner == 0 || palette.shapeRecord(cachedOwner, false).members != 0 {
		t.Fatalf("cached target setup owner=%d", cachedOwner)
	}

	beforeRevision := groups.revision.Load()
	beforeFirst, _ := palette.descriptor(0)
	beforeSecond, _ := palette.descriptor(1)
	beforeFirstOwner, beforeSecondOwner := palette.owner(0), palette.owner(1)
	if groups.tryRange(0, 2, nil, current, compactTestClock(current), 0xb000, false) {
		t.Fatal("range unexpectedly fit a missing target beside the only cached target")
	}
	afterFirst, _ := palette.descriptor(0)
	afterSecond, _ := palette.descriptor(1)
	if groups.revision.Load() != beforeRevision || palette.owner(0) != beforeFirstOwner ||
		palette.owner(1) != beforeSecondOwner || afterFirst != beforeFirst || afterSecond != beforeSecond ||
		palette.findShape(cachedShape) != cachedOwner {
		t.Fatal("failed capacity preflight mutated or recycled the cached target")
	}
}

func TestCompactPaletteMigrationRejectsUnsupportedGroups(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*compactGroup)
	}{
		{name: "nonjoinable", mutate: func(group *compactGroup) { group.joinable = false }},
		{name: "descriptor mismatch", mutate: func(group *compactGroup) { group.descriptor.history.writePC++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			groups := newCompactGroups()
			current := epoch.NewEpoch(12, 5)
			state, ok := groups.tryWrite(37, current, compactTestClock(current), 0xc000)
			if !ok {
				t.Fatal("setup write failed")
			}
			group, overlap := groups.lookupGroup(37)
			if group == nil || overlap {
				t.Fatalf("setup group=%p overlap=%v", group, overlap)
			}
			test.mutate(group)
			if got := groups.upgradePalette(); got != nil || groups.palette.Load() != nil {
				t.Fatalf("unsupported migration installed palette %p/%p", got, groups.palette.Load())
			}
			if allocs := testing.AllocsPerRun(100, func() {
				if groups.upgradePalette() != nil {
					panic("unsupported migration installed a palette")
				}
			}); allocs != 0 {
				t.Fatalf("unsupported migration allocated %.2f objects", allocs)
			}
			if groups.groups[0].Load() != group || groups.lookup(37) != state ||
				state.GetW() != current || state.GetWritePC() != 0xc000 {
				t.Fatal("rejected migration changed the bitmap representation or exact history")
			}
		})
	}
}

func TestCompactPaletteClockHighBoundaryAndPackedNeighbors(t *testing.T) {
	groups := paletteTestGroups(t)
	palette := groups.palette.Load()
	anchors := []uintptr{3075, 3076, 3079, 3080, 3199, 3200, 3327, 3328}
	for i, anchor := range anchors {
		current := epoch.NewEpoch(11, uint64(0xffff+i))
		if _, ok := groups.tryWrite(anchor, current, compactTestClock(current), 0x8111); !ok {
			t.Fatalf("anchor %d transition failed", anchor)
		}
	}
	for i, anchor := range anchors {
		descriptor, ok := palette.descriptor(anchor)
		want := epoch.NewEpoch(11, uint64(0xffff+i))
		if !ok || descriptor.history.write != want {
			t.Fatalf("packed anchor %d = %v,%v want %v", anchor, descriptor.history.write, ok, want)
		}
	}
}

func TestCompactPaletteRangeSplitsSameEpochWriteBranch(t *testing.T) {
	groups := paletteTestGroups(t)
	for i, clockValue := range []uint64{7, 8} {
		current := epoch.NewEpoch(13, clockValue)
		if _, ok := groups.tryWrite(uintptr(2000+i), current, compactTestClock(current), 0x9000); !ok {
			t.Fatal("setup write failed")
		}
	}
	current := epoch.NewEpoch(13, 8)
	if !groups.tryRange(2000, 2, nil, current, compactTestClock(current), 0x9001, true) {
		t.Fatal("two-branch range failed")
	}
	first, _ := groups.palette.Load().descriptor(2000)
	second, _ := groups.palette.Load().descriptor(2001)
	if first.history.writeCount != 2 || second.history.writeCount != 1 ||
		first.history.write != current || second.history.write != current {
		t.Fatalf("branch histories: first=%+v second=%+v", first.history, second.history)
	}
}

func TestCompactPaletteConflictLeavesExactBitsUnchanged(t *testing.T) {
	groups := paletteTestGroups(t)
	palette := groups.palette.Load()
	writer := epoch.NewEpoch(21, 3)
	if _, ok := groups.tryWrite(3000, writer, compactTestClock(writer), 0xa001); !ok {
		t.Fatal("setup write failed")
	}
	beforeRevision := groups.revision.Load()
	beforeOwner := palette.owner(3000)
	beforeWrite, beforeRead := palette.lows(3000)
	reader := epoch.NewEpoch(22, 1)
	if groups.tryRange(3000, 1, nil, reader, compactTestClock(reader), 0xa002, false) {
		t.Fatal("conflicting range succeeded")
	}
	afterWrite, afterRead := palette.lows(3000)
	if groups.revision.Load() != beforeRevision || palette.owner(3000) != beforeOwner ||
		afterWrite != beforeWrite || afterRead != beforeRead {
		t.Fatal("failed range mutated dense publication")
	}
}

func TestCompactPaletteUniformSameReaderUpdatesOnlySelectedLows(t *testing.T) {
	groups := newCompactGroups()
	palette := new(compactPalette)
	groups.palette.Store(palette)
	groups.activate()

	const (
		first  = uintptr(100)
		count  = uintptr(16)
		start  = first + 4
		size   = uintptr(8)
		readPC = uintptr(0xa101)
	)
	write := epoch.NewEpoch(9, 11)
	previous := epoch.NewEpoch(7, 0x10002)
	current := epoch.NewEpoch(7, 0x10003)
	descriptor := compactHistoryDescriptor{
		history: compactHistoryKey{
			write:           write,
			read:            previous,
			exclusiveWriter: 9,
			writePC:         0xa100,
			readPC:          readPC,
			writeCount:      1,
		},
		lifecycle: groups.ensureLifecycle(),
	}
	owner := uint8(0)
	for anchor := first; anchor < first+count; anchor++ {
		got := installPaletteShape(t, palette, anchor, descriptor)
		if owner == 0 {
			owner = got
		} else if got != owner {
			t.Fatalf("anchor %d owner=%d, want %d", anchor, got, owner)
		}
	}
	shape := palette.shapeRecord(owner, false).shape
	beforeRevision := groups.revision.Load()
	if !groups.tryRange(start, size, nil, current, compactTestClock(write, current), readPC, false) {
		t.Fatal("same-reader uniform transition fell back")
	}
	if groups.revision.Load() != beforeRevision+2 {
		t.Fatalf("revision=%d, want %d", groups.revision.Load(), beforeRevision+2)
	}
	if record := palette.shapeRecord(owner, false); record == nil || record.shape != shape || record.members != uint16(count) {
		t.Fatalf("same-reader transition changed shape record: %+v", record)
	}
	if palette.findShape(shape) != owner {
		t.Fatal("same-reader transition changed the palette hash binding")
	}
	for anchor := first; anchor < first+count; anchor++ {
		got, ok := palette.descriptor(anchor)
		wantRead := previous
		if anchor >= start && anchor < start+size {
			wantRead = current
		}
		if !ok || got.history.write != write || got.history.read != wantRead || got.history.readPC != readPC {
			t.Fatalf("anchor %d descriptor=%+v ok=%v, want write=%v read=%v pc=%#x",
				anchor, got, ok, write, wantRead, readPC)
		}
	}
	beforeNoopRevision := groups.revision.Load()
	if !groups.tryRange(start, size, nil, current, compactTestClock(write, current), readPC, false) {
		t.Fatal("same-reader uniform no-op fell back")
	}
	if groups.revision.Load() != beforeNoopRevision {
		t.Fatalf("same-reader no-op revision=%d, want %d", groups.revision.Load(), beforeNoopRevision)
	}
}

func TestCompactPaletteUniformSameReaderRejectsUnorderedWrite(t *testing.T) {
	groups := newCompactGroups()
	palette := new(compactPalette)
	groups.palette.Store(palette)
	groups.activate()

	write := epoch.NewEpoch(9, 11)
	previous := epoch.NewEpoch(7, 2)
	current := epoch.NewEpoch(7, 3)
	descriptor := compactHistoryDescriptor{
		history: compactHistoryKey{
			write:           write,
			read:            previous,
			exclusiveWriter: 9,
			writePC:         0xa200,
			readPC:          0xa201,
			writeCount:      1,
		},
		lifecycle: groups.ensureLifecycle(),
	}
	for anchor := uintptr(200); anchor < 208; anchor++ {
		installPaletteShape(t, palette, anchor, descriptor)
	}
	beforeRevision := groups.revision.Load()
	beforeWrite, beforeRead := palette.lows(200)
	if groups.tryRange(200, 8, nil, current, compactTestClock(current), descriptor.history.readPC, false) {
		t.Fatal("same-reader transition ignored an unordered write")
	}
	afterWrite, afterRead := palette.lows(200)
	if groups.revision.Load() != beforeRevision || afterWrite != beforeWrite || afterRead != beforeRead {
		t.Fatal("rejected same-reader transition changed dense publication")
	}
}

func TestCompactPaletteGetMaterializesExactWord(t *testing.T) {
	pt := NewPageTableShadow()
	const base = uintptr(0x8800000)
	current := epoch.NewEpoch(17, 0x10002)
	clock := compactTestClock(current)
	for i := 0; i <= compactGroupCapacity; i++ {
		if !pt.TryCompactWrite(base+uintptr(i*17), current, clock, uintptr(0xb000+i)) {
			t.Fatalf("history %d failed", i)
		}
	}
	view, _ := pt.blockFor(base, false)
	if view.history.compact.Load().palette.Load() == nil {
		t.Fatal("dense palette not installed")
	}
	addr := base + uintptr(compactGroupCapacity*17)
	state := pt.Get(addr)
	if state == nil || state.GetW() != current || state.GetWritePC() != 0xb000+compactGroupCapacity {
		t.Fatalf("materialized state = %v", state)
	}
	if pt.GetSlot(addr) == nil {
		t.Fatal("Get did not publish the exact word slot")
	}
}

func TestCompactPaletteFullBlockClearAndReset(t *testing.T) {
	groups := paletteTestGroups(t)
	current := epoch.NewEpoch(9, 2)
	clock := compactTestClock(current)
	if !groups.tryRange(0, rangeBlockSize, nil, current, clock, 0xc001, false) {
		t.Fatal("full-block dense transition failed")
	}
	groups.clearRange(7, 2)
	if groups.palette.Load().owner(7) != compactPaletteTombstone || groups.palette.Load().owner(8) != compactPaletteTombstone {
		t.Fatal("partial clear did not publish dense tombstones")
	}
	groups.reset()
	for _, anchor := range []uintptr{0, 7, 8, rangeBlockSize - 1} {
		if groups.palette.Load().owner(anchor) != compactPaletteDefault {
			t.Fatalf("reset retained owner at %d", anchor)
		}
	}
}

func TestCompactPaletteFullBlockNoopDoesNotPublish(t *testing.T) {
	groups := paletteTestGroups(t)
	current := epoch.NewEpoch(9, 2)
	clock := compactTestClock(epoch.NewEpoch(9, 1), current)
	if !groups.tryRange(0, rangeBlockSize, nil, current, clock, 0xd001, false) {
		t.Fatal("full-block dense setup transition failed")
	}
	palette := groups.palette.Load()
	beforeRevision := groups.revision.Load()
	var before [4]compactHistoryDescriptor
	for i, anchor := range []uintptr{0, 127, 128, rangeBlockSize - 1} {
		before[i], _ = palette.descriptor(anchor)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if !groups.tryRange(0, rangeBlockSize, nil, current, clock, 0xd001, false) {
			panic("full-block dense no-op fell back")
		}
	}); allocs != 0 {
		t.Fatalf("full-block dense no-op allocated %.2f objects", allocs)
	}
	if groups.revision.Load() != beforeRevision {
		t.Fatalf("full-block no-op revision=%d, want %d", groups.revision.Load(), beforeRevision)
	}
	for i, anchor := range []uintptr{0, 127, 128, rangeBlockSize - 1} {
		after, ok := palette.descriptor(anchor)
		if !ok || after != before[i] {
			t.Fatalf("full-block no-op changed anchor %d: after=%+v ok=%v before=%+v", anchor, after, ok, before[i])
		}
	}
}

func BenchmarkCompactPaletteFullBlockNoop(b *testing.B) {
	groups := newCompactGroups()
	current := epoch.NewEpoch(9, 2)
	clock := compactTestClock(epoch.NewEpoch(9, 1), current)
	for i := 0; i <= compactGroupCapacity; i++ {
		if _, ok := groups.tryWrite(uintptr(i*17), epoch.NewEpoch(9, 1), clock, uintptr(100+i)); !ok {
			b.Fatal("palette setup failed")
		}
	}
	if !groups.tryRange(0, rangeBlockSize, nil, current, clock, 0xd101, false) {
		b.Fatal("full-block setup transition failed")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !groups.tryRange(0, rangeBlockSize, nil, current, clock, 0xd101, false) {
			b.Fatal("full-block no-op fell back")
		}
	}
}

func BenchmarkCompactPaletteUniformScalarReadWrite(b *testing.B) {
	groups := newCompactGroups()
	current := epoch.NewEpoch(9, 2)
	clock := compactTestClock(current)
	for i := 0; i <= compactGroupCapacity; i++ {
		if _, ok := groups.tryWrite(uintptr(i*17), current, clock, uintptr(100+i)); !ok {
			b.Fatal("palette setup failed")
		}
	}
	const (
		anchor  = uintptr(3000)
		writePC = uintptr(0xd201)
		readPC  = uintptr(0xd202)
	)
	if !groups.tryRange(anchor, 8, nil, current, clock, writePC, true) {
		b.Fatal("uniform scalar setup failed")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !groups.tryRange(anchor, 8, nil, current, clock, readPC, false) ||
			!groups.tryRange(anchor, 8, nil, current, clock, writePC, true) {
			b.Fatal("uniform scalar transition fell back")
		}
	}
}
