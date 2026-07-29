//go:build amd64 || arm64

package shadowmem

import (
	"sync"
	"testing"

	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/vectorclock"
)

const primaryMemoryTestBase = uintptr(1) << 40

func primaryMemoryTestBlock(t *testing.T, pt *PageTableShadow, addr uintptr) (*shadowPage, uintptr, blockView) {
	t.Helper()
	base := pt.base.Load()
	if base == 0 || addr < base || addr-base >= ptTotalCoverage {
		t.Fatalf("address %#x is not in initialized primary window [0x%x, 0x%x)", addr, base, base+ptTotalCoverage)
	}
	offset := addr - base
	page := pt.pages[offset>>l1Shift].Load()
	if page == nil {
		t.Fatalf("primary page for %#x is absent", addr)
	}
	blockIdx := (offset >> rangeBlockShift) & (rangeBlocksPerPage - 1)
	view, ok := pt.blockFor(addr, false)
	if !ok || view.slotTableOwner != &page.slotTables[blockIdx] {
		t.Fatalf("primary block binding for %#x is not page directory entry %d", addr, blockIdx)
	}
	return page, blockIdx, view
}

func publishedPrimaryTables(page *shadowPage) int {
	count := 0
	for i := range page.slotTables {
		if page.slotTables[i].Load() != nil {
			count++
		}
	}
	return count
}

func TestPrimaryFullBlockDefaultDoesNotAllocateSlotTable(t *testing.T) {
	pt := NewPageTableShadow()
	pt.AccessRange(primaryMemoryTestBase, rangeBlockSize, func(_ uintptr, _ uint8, _ *VarState) {})

	page, blockIdx, _ := primaryMemoryTestBlock(t, pt, primaryMemoryTestBase)
	if table := page.slotTables[blockIdx].Load(); table != nil {
		t.Fatalf("full-block default allocated slot table %p", table)
	}
	if got := publishedPrimaryTables(page); got != 0 {
		t.Fatalf("full-block default published %d primary slot tables, want 0", got)
	}
}

func TestPrimaryCompactOnlyAccessDoesNotAllocateSlotTable(t *testing.T) {
	pt := NewPageTableShadow()
	current := epoch.NewEpoch(3, 7)
	clock := vectorclock.New()
	clock.Set(3, 7)
	if !pt.TryCompactWrite(primaryMemoryTestBase+123, current, clock, 0x1234) {
		t.Fatal("compact-only setup did not remain compact")
	}

	page, blockIdx, _ := primaryMemoryTestBlock(t, pt, primaryMemoryTestBase+123)
	if table := page.slotTables[blockIdx].Load(); table != nil {
		t.Fatalf("compact-only access allocated slot table %p", table)
	}
	if got := publishedPrimaryTables(page); got != 0 {
		t.Fatalf("compact-only access published %d primary slot tables, want 0", got)
	}
}

func TestPrimaryFirstMaterializationPublishesOneSlotTable(t *testing.T) {
	pt := NewPageTableShadow()
	addr := primaryMemoryTestBase + 123
	pt.GetOrCreate(addr)

	page, blockIdx, _ := primaryMemoryTestBlock(t, pt, addr)
	if table := page.slotTables[blockIdx].Load(); table == nil {
		t.Fatal("first materialization did not publish its block slot table")
	}
	if got := publishedPrimaryTables(page); got != 1 {
		t.Fatalf("first materialization published %d primary slot tables, want 1", got)
	}
}

func TestPrimaryMaterializationsReuseBlockSlotTable(t *testing.T) {
	pt := NewPageTableShadow()
	firstAddr := primaryMemoryTestBase + 8
	secondAddr := primaryMemoryTestBase + 80
	firstSlot := pt.GetOrCreateSlot(firstAddr)
	page, blockIdx, _ := primaryMemoryTestBlock(t, pt, firstAddr)
	table := page.slotTables[blockIdx].Load()
	secondSlot := pt.GetOrCreateSlot(secondAddr)

	if got := page.slotTables[blockIdx].Load(); got != table {
		t.Fatalf("second materialization replaced block table: got %p, want %p", got, table)
	}
	if firstSlot == secondSlot {
		t.Fatal("different words unexpectedly share one shadow slot")
	}
	if got := table.slots[(firstAddr>>3)&rangeBlockWordMask].Load(); got != firstSlot {
		t.Fatalf("first word table entry = %p, want %p", got, firstSlot)
	}
	if got := table.slots[(secondAddr>>3)&rangeBlockWordMask].Load(); got != secondSlot {
		t.Fatalf("second word table entry = %p, want %p", got, secondSlot)
	}
}

func TestPrimaryBlocksPublishDistinctSlotTables(t *testing.T) {
	pt := NewPageTableShadow()
	firstAddr := primaryMemoryTestBase + 17
	secondAddr := primaryMemoryTestBase + rangeBlockSize + 17
	pt.GetOrCreate(firstAddr)
	pt.GetOrCreate(secondAddr)

	page, firstBlock, _ := primaryMemoryTestBlock(t, pt, firstAddr)
	secondPage, secondBlock, _ := primaryMemoryTestBlock(t, pt, secondAddr)
	if page != secondPage {
		t.Fatal("adjacent test blocks unexpectedly landed in different primary pages")
	}
	firstTable := page.slotTables[firstBlock].Load()
	secondTable := page.slotTables[secondBlock].Load()
	if firstTable == nil || secondTable == nil || firstTable == secondTable {
		t.Fatalf("adjacent block tables = (%p, %p), want distinct non-nil tables", firstTable, secondTable)
	}
}

func TestPrimaryConcurrentMaterializationPublishesSingleSlotTable(t *testing.T) {
	pt := NewPageTableShadow()
	const workers = 32
	start := make(chan struct{})
	slots := make([]*ShadowSlot, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			slots[i] = pt.GetOrCreateSlot(primaryMemoryTestBase + uintptr(i)*8)
		}(i)
	}
	close(start)
	wg.Wait()

	page, blockIdx, _ := primaryMemoryTestBlock(t, pt, primaryMemoryTestBase)
	table := page.slotTables[blockIdx].Load()
	if table == nil || publishedPrimaryTables(page) != 1 {
		t.Fatalf("concurrent materialization table=%p published=%d, want one", table, publishedPrimaryTables(page))
	}
	for i, slot := range slots {
		if slot == nil || table.slots[i].Load() != slot {
			t.Fatalf("worker %d slot=%p table entry=%p", i, slot, table.slots[i].Load())
		}
	}
}

func TestPrimaryMaterializedSlotPreservesExactEightByteLanes(t *testing.T) {
	pt := NewPageTableShadow()
	word := primaryMemoryTestBase + 64
	states := make([]*VarState, shadowSlotLanes)
	for lane := uintptr(0); lane < shadowSlotLanes; lane++ {
		states[lane] = pt.GetOrCreate(word + lane)
	}
	for lane, want := range states {
		if got := pt.Get(word + uintptr(lane)); got != want {
			t.Fatalf("lane %d lookup = %p, want %p", lane, got, want)
		}
		for other, state := range states {
			if lane != other && want == state {
				t.Fatalf("lanes %d and %d share exact-address state %p", lane, other, want)
			}
		}
	}
}

func TestPrimarySlotTableRetainedAcrossFullBlockClear(t *testing.T) {
	pt := NewPageTableShadow()
	addr := primaryMemoryTestBase + 123
	slot := pt.GetOrCreateSlot(addr)
	page, blockIdx, _ := primaryMemoryTestBlock(t, pt, addr)
	table := page.slotTables[blockIdx].Load()

	pt.ClearRange(primaryMemoryTestBase, rangeBlockSize)

	if got := page.slotTables[blockIdx].Load(); got != table {
		t.Fatalf("full clear changed published block table: got %p, want %p", got, table)
	}
	if got := pt.GetSlot(addr); got != slot {
		t.Fatalf("full clear detached materialized slot: got %p, want %p", got, slot)
	}
	for lane := uint8(0); lane < shadowSlotLanes; lane++ {
		if state := slot.State(lane); state != nil {
			t.Fatalf("full clear retained lane %d state %p", lane, state)
		}
	}
}
