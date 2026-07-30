package stackdepot

import (
	"strings"
	"sync"
	"testing"
	"unsafe"
)

func TestCaptureStackAndExactDeduplication(t *testing.T) {
	Reset()

	var first, second uint64
	for i := 0; i < 2; i++ {
		id := CaptureStack()
		if i == 0 {
			first = id
		} else {
			second = id
		}
	}
	if first == 0 || second == 0 {
		t.Fatalf("CaptureStack returned unavailable IDs: %x, %x", first, second)
	}
	if first != second {
		t.Fatalf("identical stacks received different IDs: %x, %x", first, second)
	}
	stack := GetStack(first)
	if stack == nil || stack.PC[0] == 0 {
		t.Fatalf("GetStack(%x) did not return the captured stack", first)
	}
	if GetStack(first) != stack {
		t.Fatal("canonical ID did not retain a stable StackTrace pointer")
	}
	if unique, _ := Stats(); unique != 1 {
		t.Fatalf("Stats reported %d unique stacks, want 1", unique)
	}
}

func TestForcedHashCollisionUsesExactFramesAndCount(t *testing.T) {
	Reset()
	const collision = uint64(0xfeedface)

	short := []uintptr{11, 22}
	long := []uintptr{11, 22, 33}
	other := []uintptr{11, 44}
	shortID := storeStack(short, collision)
	longID := storeStack(long, collision)
	otherID := storeStack(other, collision)
	if shortID == 0 || longID == 0 || otherID == 0 {
		t.Fatal("collision inserts unexpectedly exhausted the depot")
	}
	if shortID == longID || shortID == otherID || longID == otherID {
		t.Fatalf("distinct colliding stacks aliased: %x %x %x", shortID, longID, otherID)
	}
	if got := storeStack(short, collision); got != shortID {
		t.Fatalf("exact colliding stack got ID %x, want canonical %x", got, shortID)
	}

	assertPCs(t, shortID, short)
	assertPCs(t, longID, long)
	assertPCs(t, otherID, other)
	if unique, _ := Stats(); unique != 3 {
		t.Fatalf("Stats reported %d colliding stacks, want 3", unique)
	}
}

func TestCollisionChainBeyondEightRecords(t *testing.T) {
	Reset()
	const collision = uint64(7)
	var ids [17]uint64
	for i := range ids {
		pcs := []uintptr{uintptr(i + 1), uintptr(1000 + i)}
		ids[i] = storeStack(pcs, collision)
		if ids[i] == 0 {
			t.Fatalf("insert %d failed in collision chain", i)
		}
	}
	for i, id := range ids {
		pcs := []uintptr{uintptr(i + 1), uintptr(1000 + i)}
		if got := storeStack(pcs, collision); got != id {
			t.Fatalf("collision-chain entry %d got ID %x, want %x", i, got, id)
		}
		assertPCs(t, id, pcs)
	}
}

func TestDepotSaturationDoesNotOverwriteIssuedIDs(t *testing.T) {
	Reset()
	var witnesses [4]struct {
		id uint64
		pc uintptr
	}
	witnessAt := [4]int{0, depotSize / 3, depotSize / 2, depotSize - 1}
	nextWitness := 0
	for i := 0; i < depotSize; i++ {
		pc := uintptr(i + 1)
		id := storeStack([]uintptr{pc}, uint64(i))
		if id == 0 {
			t.Fatalf("depot saturated early at record %d", i)
		}
		if nextWitness < len(witnessAt) && i == witnessAt[nextWitness] {
			witnesses[nextWitness] = struct {
				id uint64
				pc uintptr
			}{id: id, pc: pc}
			nextWitness++
		}
	}
	if got := storeStack([]uintptr{depotSize + 1}, depotSize+1); got != 0 {
		t.Fatalf("saturated depot returned ID %x, want unavailable ID 0", got)
	}
	// A canonical lookup still succeeds at capacity and must not consume or
	// overwrite a record.
	if got := storeStack([]uintptr{1}, 0); got != witnesses[0].id {
		t.Fatalf("canonical saturated lookup got %x, want %x", got, witnesses[0].id)
	}
	for _, witness := range witnesses {
		assertPCs(t, witness.id, []uintptr{witness.pc})
	}
	unique, memory := Stats()
	if unique != depotSize {
		t.Fatalf("Stats reported %d records at saturation, want %d", unique, depotSize)
	}
	wantMemory := int64(depotSize) * int64(unsafe.Sizeof(depotRecord{}))
	if memory != wantMemory {
		t.Fatalf("Stats reported %d bytes at saturation, want %d", memory, wantMemory)
	}
}

func TestConcurrentCanonicalization(t *testing.T) {
	Reset()
	pcs := []uintptr{101, 202, 303, 404}
	const hash = uint64(0x12345678)
	const goroutines = 128

	start := make(chan struct{})
	ids := make(chan uint64, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			ids <- storeStack(pcs, hash)
		}()
	}
	close(start)
	wg.Wait()
	close(ids)

	var canonical uint64
	for id := range ids {
		if id == 0 {
			t.Fatal("concurrent canonicalization returned unavailable ID")
		}
		if canonical == 0 {
			canonical = id
		} else if id != canonical {
			t.Fatalf("concurrent canonicalization returned %x and %x", canonical, id)
		}
	}
	assertPCs(t, canonical, pcs)
	if unique, _ := Stats(); unique != 1 {
		t.Fatalf("concurrent canonicalization published %d records, want 1", unique)
	}
}

func TestResetAdvancesGenerationAndRejectsStaleIDs(t *testing.T) {
	Reset()
	oldID := storeStack([]uintptr{1, 2, 3}, 9)
	if oldID == 0 || GetStack(oldID) == nil {
		t.Fatal("failed to create pre-reset stack ID")
	}
	oldGeneration := oldID >> recordIndexBits

	Reset()
	if got := GetStack(oldID); got != nil {
		t.Fatalf("stale ID %x resolved after Reset", oldID)
	}
	newID := storeStack([]uintptr{8, 9}, 10)
	if newID == 0 {
		t.Fatal("failed to create post-reset stack ID")
	}
	if newID&recordIndexMask != oldID&recordIndexMask {
		t.Fatalf("test expected record-index reuse, got old %x new %x", oldID, newID)
	}
	if newID>>recordIndexBits == oldGeneration {
		t.Fatalf("Reset did not advance generation: old %x new %x", oldID, newID)
	}
	assertPCs(t, newID, []uintptr{8, 9})
	if got := GetStack(oldID); got != nil {
		t.Fatalf("reused record made stale ID %x resolve", oldID)
	}
}

func TestGenerationExhaustionDoesNotWrapOrReviveIDs(t *testing.T) {
	// Put the depot at its last issuable generation without iterating through
	// the enormous generation space. Restore ordinary state for later tests.
	depotGeneration.Store(maxGeneration - 1)
	for i := range stackBuckets {
		stackBuckets[i].head.Store(0)
	}
	recordCount.Store(0)
	defer func() {
		depotGeneration.Store(0)
		Reset()
	}()

	lastID := storeStack([]uintptr{77}, 77)
	if lastID == 0 || GetStack(lastID) == nil {
		t.Fatal("last issuable generation did not produce an ID")
	}
	Reset()
	if GetStack(lastID) != nil {
		t.Fatal("generation exhaustion revived the last issued ID")
	}
	if got := storeStack([]uintptr{88}, 88); got != 0 {
		t.Fatalf("generation-exhausted depot returned ID %x, want 0", got)
	}
	Reset()
	if got := storeStack([]uintptr{99}, 99); got != 0 {
		t.Fatalf("terminal generation wrapped and returned ID %x", got)
	}
}

func TestGetStackRejectsUnavailableAndMalformedIDs(t *testing.T) {
	Reset()
	if GetStack(0) != nil {
		t.Fatal("GetStack(0) returned a stack")
	}
	generation := currentGeneration()
	for _, id := range []uint64{
		generation << recordIndexBits,                     // zero record index
		generation<<recordIndexBits | uint64(depotSize+1), // out of range
		(generation+1)<<recordIndexBits | 1,               // future generation
		generation<<recordIndexBits | 1,                   // unpublished record
	} {
		if got := GetStack(id); got != nil {
			t.Fatalf("GetStack(%x) returned an invalid stack", id)
		}
	}
}

func TestFormatStack(t *testing.T) {
	Reset()
	id := CaptureStack()
	stack := GetStack(id)
	if stack == nil {
		t.Fatal("CaptureStack did not produce a formattable stack")
	}
	formatted := stack.FormatStack()
	for _, want := range []string{"TestFormatStack", "stackdepot_test.go", "()", ":"} {
		if !strings.Contains(formatted, want) {
			t.Fatalf("formatted stack does not contain %q:\n%s", want, formatted)
		}
	}

	var nilStack *StackTrace
	if got := nilStack.FormatStack(); got != "  <unknown>\n" {
		t.Fatalf("nil FormatStack = %q, want unknown", got)
	}
	if got := (&StackTrace{}).FormatStack(); got != "  <runtime internal>\n" {
		t.Fatalf("empty FormatStack = %q, want runtime internal", got)
	}
}

func TestStatsTracksExactOccupiedRecords(t *testing.T) {
	Reset()
	if unique, memory := Stats(); unique != 0 || memory != 0 {
		t.Fatalf("empty Stats = (%d, %d), want (0, 0)", unique, memory)
	}
	first := storeStack([]uintptr{1}, 1)
	if duplicate := storeStack([]uintptr{1}, 1); duplicate != first {
		t.Fatalf("duplicate ID = %x, want %x", duplicate, first)
	}
	_ = storeStack([]uintptr{2, 3}, 2)

	unique, memory := Stats()
	if unique != 2 {
		t.Fatalf("Stats unique = %d, want 2", unique)
	}
	wantMemory := int64(2 * unsafe.Sizeof(depotRecord{}))
	if memory != wantMemory {
		t.Fatalf("Stats memory = %d, want %d", memory, wantMemory)
	}
	Reset()
	if unique, memory := Stats(); unique != 0 || memory != 0 {
		t.Fatalf("post-reset Stats = (%d, %d), want (0, 0)", unique, memory)
	}
}

func assertPCs(t *testing.T, id uint64, want []uintptr) {
	t.Helper()
	stack := GetStack(id)
	if stack == nil {
		t.Fatalf("GetStack(%x) returned nil", id)
	}
	for i, pc := range want {
		if stack.PC[i] != pc {
			t.Fatalf("GetStack(%x).PC[%d] = %d, want %d", id, i, stack.PC[i], pc)
		}
	}
	for i := len(want); i < MaxFrames; i++ {
		if stack.PC[i] != 0 {
			t.Fatalf("GetStack(%x).PC[%d] = %d beyond exact frame count", id, i, stack.PC[i])
		}
	}
}

func BenchmarkCaptureStack(b *testing.B) {
	Reset()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CaptureStack()
	}
}

func BenchmarkStoreStackCanonical(b *testing.B) {
	Reset()
	pcs := []uintptr{1, 2, 3, 4, 5, 6, 7, 8}
	hash := hashStack(pcs)
	if id := storeStack(pcs, hash); id == 0 {
		b.Fatal("setup store failed")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = storeStack(pcs, hash)
	}
}

func BenchmarkGetStack(b *testing.B) {
	Reset()
	id := storeStack([]uintptr{1, 2, 3, 4}, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = GetStack(id)
	}
}

func BenchmarkFormatStack(b *testing.B) {
	Reset()
	stack := GetStack(CaptureStack())
	if stack == nil {
		b.Fatal("setup capture failed")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = stack.FormatStack()
	}
}
