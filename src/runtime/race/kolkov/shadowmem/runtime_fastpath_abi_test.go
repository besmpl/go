//go:build amd64 || arm64

package shadowmem

import (
	"internal/runtime/atomic"
	"sync"
	"testing"
	"unsafe"

	"runtime/race/kolkov/epoch"
	raceg "runtime/race/kolkov/goroutine"
	"runtime/race/kolkov/vectorclock"
)

// TestRuntimeFastPathABI protects the private layout and lookup policy mirrored
// by runtime/race_kolkov.go to keep compiler-generated hooks call-free. Update
// the runtime fast path and this owner package together when any value changes.
func TestRuntimeFastPathABI(t *testing.T) {
	var pt PageTableShadow
	var page shadowPage
	var block rangeBlock
	var compact compactGroups
	var slot shadowSlot
	var externalCell externalBlockCell
	var externalBlock externalShadowBlock
	var externalSlots externalSlotTable
	var state VarState
	var ctx raceg.RaceContext

	checks := []struct {
		name string
		got  uintptr
		want uintptr
	}{
		{"PageTableShadow.base", unsafe.Offsetof(pt.base), 0},
		{"PageTableShadow.pages", unsafe.Offsetof(pt.pages), 8},
		{"PageTableShadow.external", unsafe.Offsetof(pt.external), 524296},
		{"shadowPage.slots", unsafe.Offsetof(page.slots), 0},
		{"shadowPage.blocks", unsafe.Offsetof(page.blocks), 2097152},
		{"rangeBlock.mu", unsafe.Offsetof(block.mu), 0},
		{"rangeBlock.state", unsafe.Offsetof(block.state), 8},
		{"rangeBlock.compact", unsafe.Offsetof(block.compact), 16},
		{"compactGroups.active", unsafe.Offsetof(compact.active), 0},
		{"externalBlockCell.base", unsafe.Offsetof(externalCell.base), 0},
		{"externalBlockCell.next", unsafe.Offsetof(externalCell.next), 8},
		{"externalBlockCell.block", unsafe.Offsetof(externalCell.block), 16},
		{"externalShadowBlock.slots", unsafe.Offsetof(externalBlock.slots), 0},
		{"externalShadowBlock.history", unsafe.Offsetof(externalBlock.history), 8},
		{"externalSlotTable.slots", unsafe.Offsetof(externalSlots.slots), 0},
		{"shadowSlot.states", unsafe.Offsetof(slot.states), 0},
		{"VarState.W", unsafe.Offsetof(state.W), 0},
		{"VarState.readEpoch0", unsafe.Offsetof(state.readEpoch0), 32},
		{"VarState.readerState", unsafe.Offsetof(state.readerState), 40},
		{"RaceContext.ReadCacheInvalidatedClock", unsafe.Offsetof(ctx.ReadCacheInvalidatedClock), 4},
		{"RaceContext.Epoch", unsafe.Offsetof(ctx.Epoch), 16},
		{"RaceContext.ReadCache", unsafe.Offsetof(ctx.ReadCache), 24},
		{"RaceContext.ReadCacheStates", unsafe.Offsetof(ctx.ReadCacheStates), 56},
		{"RaceContext.ReadCacheWidths", unsafe.Offsetof(ctx.ReadCacheWidths), 88},
	}
	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("runtime/race_kolkov.go ABI dependency %s: offset=%d, want %d", check.name, check.got, check.want)
		}
	}

	if got := unsafe.Sizeof(uintptr(0)); got != 8 {
		t.Fatalf("runtime/race_kolkov.go ABI dependency pointer width=%d, want 8", got)
	}
	if got := unsafe.Sizeof(atomic.Pointer[externalBlockCell]{}); got != 8 {
		t.Errorf("runtime/race_kolkov.go ABI dependency atomic pointer width=%d, want 8", got)
	}
	if got := unsafe.Sizeof(compact.active); got != 4 {
		t.Errorf("runtime/race_kolkov.go ABI dependency compact active width=%d, want 4", got)
	}
	if got := unsafe.Sizeof(ctx.ReadCacheInvalidatedClock); got != 4 {
		t.Errorf("runtime/race_kolkov.go read-cache invalidation clock width=%d, want 4", got)
	}
	if got := unsafe.Sizeof(block); got != 24 {
		t.Errorf("runtime/race_kolkov.go ABI dependency rangeBlock size=%d, want 24", got)
	}
	if got := unsafe.Sizeof(pt); got != 1048584 {
		t.Errorf("runtime/race_kolkov.go ABI dependency PageTableShadow size=%d, want 1048584", got)
	}
	if got := unsafe.Sizeof(externalBlock); got != 32 {
		t.Errorf("runtime/race_kolkov.go ABI dependency externalShadowBlock size=%d, want 32", got)
	}
	if got := unsafe.Sizeof(externalCell); got != 48 {
		t.Errorf("runtime/race_kolkov.go ABI dependency externalBlockCell size=%d, want 48", got)
	}
	if got := unsafe.Sizeof(externalSlots); got != 4096 {
		t.Errorf("runtime/race_kolkov.go ABI dependency externalSlotTable size=%d, want 4096", got)
	}
	if raceg.ReadCacheSlots != 4 || unsafe.Sizeof(ctx.ReadCache) != 4*unsafe.Sizeof(uintptr(0)) {
		t.Errorf("runtime/race_kolkov.go read-cache ABI changed: slots=%d size=%d", raceg.ReadCacheSlots, unsafe.Sizeof(ctx.ReadCache))
	}
	if unsafe.Sizeof(ctx.ReadCacheStates) != 4*unsafe.Sizeof(unsafe.Pointer(nil)) {
		t.Errorf("runtime/race_kolkov.go read-cache state ABI changed: size=%d", unsafe.Sizeof(ctx.ReadCacheStates))
	}
	if unsafe.Sizeof(ctx.ReadCacheWidths) != 4 {
		t.Errorf("runtime/race_kolkov.go read-cache width ABI changed: size=%d", unsafe.Sizeof(ctx.ReadCacheWidths))
	}
	if raceg.ReadCacheWeakWidth != 1<<7 {
		t.Errorf("runtime/race_kolkov.go weak-width marker changed: %#x", raceg.ReadCacheWeakWidth)
	}
	if l1Size != 65536 || l1Shift != 21 || l2Mask != 0x3FFFF || ptTotalCoverage != 1<<37 {
		t.Errorf("runtime/race_kolkov.go page-table policy changed: size=%d shift=%d mask=%#x coverage=%d", l1Size, l1Shift, l2Mask, ptTotalCoverage)
	}
	if rangeBlockShift != 12 || rangeBlockWords != 512 || rangeBlocksPerPage != 512 {
		t.Errorf("runtime/race_kolkov.go range-block policy changed: shift=%d words=%d blocks/page=%d", rangeBlockShift, rangeBlockWords, rangeBlocksPerPage)
	}
	if externalBlockBuckets != 65536 || externalBlockMask != 0xFFFF {
		t.Errorf("runtime/race_kolkov.go external-directory policy changed: buckets=%d mask=%#x", externalBlockBuckets, externalBlockMask)
	}
	first := pt.GetOrCreate(0x1000)
	adjacentField := pt.GetOrCreate(0x1004)
	if first == adjacentField {
		t.Fatal("adjacent sub-word field addresses unexpectedly share VarState")
	}
	if got := pt.Get(0x1000); got != first {
		t.Fatalf("exact lookup at 0x1000 returned %p, want %p", got, first)
	}
	if got := pt.Get(0x1004); got != adjacentField {
		t.Fatalf("exact lookup at 0x1004 returned %p, want %p", got, adjacentField)
	}
	for _, addr := range []uintptr{0, 0x1000, 0x123456789ABCDEF0} {
		want := (uint64(addr) * uint64(0x9E3779B97F4A7C15)) >> 48
		if got := fastHash(addr); got != want {
			t.Errorf("runtime/race_kolkov.go external hash policy for %#x: got %#x, want %#x", addr, got, want)
		}
	}
}

// mirroredRuntimeExternalState reproduces runtime/race_kolkov.go's bounded
// sparse-block pointer walk using literal ABI offsets. matched distinguishes an
// authoritative nil materialized lane from a directory miss for test purposes;
// either case makes the runtime take the sound detector slow path.
func mirroredRuntimeExternalState(pt *PageTableShadow, addr uintptr) (state *VarState, matched bool) {
	const (
		runtimeExternalOffset           = uintptr(524296)
		runtimeExternalBucketMask       = uintptr(0xFFFF)
		runtimeExternalBlockMask        = uintptr(4095)
		runtimeExternalWordMask         = uintptr(511)
		runtimeExternalCellNextOffset   = uintptr(8)
		runtimeExternalCellSlotsOffset  = uintptr(16)
		runtimeExternalHistoryStateOff  = uintptr(32)
		runtimeExternalCompactOffset    = uintptr(40)
		runtimeExternalMaxChainFastPath = 8
		runtimeHashMultiplier           = uint64(0x9E3779B97F4A7C15)
	)

	blockBase := addr &^ runtimeExternalBlockMask
	hash := (uint64(blockBase) * runtimeHashMultiplier) >> 48
	bucket := uintptr(hash) & runtimeExternalBucketMask
	cell := (*atomic.Pointer[externalBlockCell])(unsafe.Add(unsafe.Pointer(pt),
		runtimeExternalOffset+bucket*8)).Load()
	for range runtimeExternalMaxChainFastPath {
		if cell == nil {
			return nil, false
		}
		cellPtr := unsafe.Pointer(cell)
		if *(*uintptr)(cellPtr) == blockBase {
			wordIdx := (addr >> 3) & runtimeExternalWordMask
			slots := (*atomic.Pointer[externalSlotTable])(unsafe.Add(cellPtr, runtimeExternalCellSlotsOffset)).Load()
			if slots != nil {
				slot := (*atomic.Pointer[shadowSlot])(unsafe.Add(unsafe.Pointer(slots), wordIdx*8)).Load()
				if slot != nil {
					return (*atomic.Pointer[VarState])(unsafe.Add(unsafe.Pointer(slot), (addr&7)*8)).Load(), true
				}
			}
			if compact := (*atomic.Pointer[compactGroups])(unsafe.Add(cellPtr, runtimeExternalCompactOffset)).Load(); compact != nil {
				if compact.active.Load() != 0 {
					return nil, true
				}
			}
			return (*atomic.Pointer[VarState])(unsafe.Add(cellPtr, runtimeExternalHistoryStateOff)).Load(), true
		}
		cell = (*atomic.Pointer[externalBlockCell])(unsafe.Add(cellPtr, runtimeExternalCellNextOffset)).Load()
	}
	return nil, false
}

// mirroredRuntimePrimaryState reproduces the call-free primary-page pointer
// walk in runtime/race_kolkov.go using its literal ABI offsets. Keeping this
// helper independent of field selection makes the test fail if either package
// changes the private layout without updating the other.
func mirroredRuntimePrimaryState(page *shadowPage, pageOffset uintptr) *VarState {
	const (
		runtimeSlotsOffset      = uintptr(0)
		runtimeBlocksOffset     = uintptr(2097152)
		runtimeBlockShift       = 12
		runtimeBlockMask        = uintptr(511)
		runtimeBlockSize        = uintptr(24)
		runtimeBlockStateOffset = uintptr(8)
		runtimeBlockCompactOff  = uintptr(16)
	)

	pagePtr := unsafe.Pointer(page)
	wordIdx := (pageOffset >> 3) & l2Mask
	slot := (*atomic.Pointer[shadowSlot])(unsafe.Add(pagePtr, runtimeSlotsOffset+wordIdx*8)).Load()
	if slot != nil {
		return (*atomic.Pointer[VarState])(unsafe.Add(unsafe.Pointer(slot), (pageOffset&7)*8)).Load()
	}
	blockIdx := (pageOffset >> runtimeBlockShift) & runtimeBlockMask
	blockPtr := unsafe.Add(pagePtr, runtimeBlocksOffset+blockIdx*runtimeBlockSize)
	if compact := (*atomic.Pointer[compactGroups])(unsafe.Add(blockPtr, runtimeBlockCompactOff)).Load(); compact != nil {
		if compact.active.Load() != 0 {
			return nil
		}
	}
	return (*atomic.Pointer[VarState])(unsafe.Add(blockPtr, runtimeBlockStateOffset)).Load()
}

func TestRuntimeFastPathBlockDefaultAndTombstonePrecedence(t *testing.T) {
	pt := NewPageTableShadow()
	const appBase = uintptr(1) << 40
	write := epoch.NewEpoch(7, 19)
	pt.AccessRange(appBase, rangeBlockSize, func(_ uintptr, _ uint8, state *VarState) {
		state.SetW(write)
	})

	base := pt.base.Load()
	offset := appBase - base
	page := pt.pages[offset>>l1Shift].Load()
	if page == nil {
		t.Fatal("full-block access did not create its primary shadow page")
	}
	pageOffset := offset & (uintptr(1)<<l1Shift - 1)
	if slot := pt.GetSlot(appBase + 123); slot != nil {
		t.Fatalf("full-block default unexpectedly materialized slot %p", slot)
	}
	view, ok := pt.blockFor(appBase+123, false)
	if !ok {
		t.Fatal("full-block default lost its primary block")
	}
	if compact := view.history.compact.Load(); compact == nil || compact.active.Load() != 0 {
		t.Fatalf("default clear header = %p active=%v, want eager inactive header", compact, compact != nil && compact.active.Load() != 0)
	}
	if got := mirroredRuntimePrimaryState(page, pageOffset+123); got == nil || got.GetW() != write {
		t.Fatalf("mirrored block-default lookup = %v, want W=%v", got, write)
	}

	// Allocator clear cannot allocate. A partial clear publishes an exact
	// tombstone in the eager header; activating that header conservatively gates
	// every unmaterialized runtime lookup in the block.
	pt.ClearRange(appBase+123, 1)
	if slot := pt.GetSlot(appBase + 123); slot != nil {
		t.Fatalf("partial clear allocated materialized slot %p", slot)
	}
	if compact := view.history.compact.Load(); compact == nil || compact.active.Load() == 0 {
		t.Fatal("partial clear did not activate its permanent compact gate")
	}
	if got := mirroredRuntimePrimaryState(page, pageOffset+123); got != nil {
		t.Fatalf("mirrored cleared-lane lookup = %v, want conservative nil", got)
	}
	if got := mirroredRuntimePrimaryState(page, pageOffset+122); got != nil {
		t.Fatalf("mirrored adjacent lookup after active gate = %v, want conservative nil", got)
	}
	if got := pt.Get(appBase + 122); got == nil || got.GetW() != write {
		t.Fatalf("exact adjacent lookup = %v, want retained W=%v", got, write)
	}
}

func TestRuntimeFastPathExternalBlockDefaultAndTombstonePrecedence(t *testing.T) {
	pt := NewPageTableShadow()
	// Fix the immutable primary window low, leaving appBase in the sparse
	// directory. This models platforms whose executable globals precede heap use.
	pt.GetOrCreate(0x1003)
	const appBase = uintptr(1) << 40
	write := epoch.NewEpoch(8, 23)
	pt.AccessRange(appBase, rangeBlockSize, func(_ uintptr, _ uint8, state *VarState) {
		state.SetW(write)
	})

	if slot := pt.GetSlot(appBase + 123); slot != nil {
		t.Fatalf("full external block default unexpectedly materialized slot %p", slot)
	}
	view, ok := pt.blockFor(appBase+123, false)
	if !ok {
		t.Fatal("full-block default lost its external block")
	}
	if compact := view.history.compact.Load(); compact == nil || compact.active.Load() != 0 {
		t.Fatalf("external default clear header = %p active=%v, want eager inactive header", compact, compact != nil && compact.active.Load() != 0)
	}
	if got, matched := mirroredRuntimeExternalState(pt, appBase+123); !matched || got == nil || got.GetW() != write {
		t.Fatalf("mirrored external default lookup = (%v, %v), want W=%v", got, matched, write)
	}

	pt.ClearRange(appBase+123, 1)
	if slot := pt.GetSlot(appBase + 123); slot != nil {
		t.Fatalf("partial external clear allocated slot %p", slot)
	}
	if compact := view.history.compact.Load(); compact == nil || compact.active.Load() == 0 {
		t.Fatal("partial external clear did not activate its permanent compact gate")
	}
	if got, matched := mirroredRuntimeExternalState(pt, appBase+123); !matched || got != nil {
		t.Fatalf("mirrored external cleared-lane lookup = (%v, %v), want (nil, true)", got, matched)
	}
	if got, matched := mirroredRuntimeExternalState(pt, appBase+122); !matched || got != nil {
		t.Fatalf("mirrored external adjacent lookup after active gate = (%v, %v), want (nil, true)", got, matched)
	}
	if got := pt.Get(appBase + 122); got == nil || got.GetW() != write {
		t.Fatalf("external exact adjacent lookup = %v, want retained W=%v", got, write)
	}
}

func TestRuntimeFastPathPrimaryCompactGateAndHotPromotion(t *testing.T) {
	pt := NewPageTableShadow()
	const addr = uintptr(1)<<40 + 0x123
	current := epoch.NewEpoch(15, 43)
	clock := vectorclock.New()
	clock.Set(15, 43)

	if !pt.TryCompactWrite(addr, current, clock, 0x1234) {
		t.Fatal("first simple write did not enter compact history")
	}
	view, ok := pt.blockFor(addr, false)
	if !ok {
		t.Fatal("compact write did not publish its primary block")
	}
	compact := view.history.compact.Load()
	if compact == nil || compact.active.Load() == 0 || pt.GetSlot(addr) != nil {
		t.Fatalf("compact setup = (%p, slot %p), want permanent gate and nil slot", compact, pt.GetSlot(addr))
	}
	before := pt.Get(addr)
	if before == nil || before.GetW() != current {
		t.Fatalf("compact state = %v, want W=%v", before, current)
	}

	base := pt.base.Load()
	offset := addr - base
	page := pt.pages[offset>>l1Shift].Load()
	pageOffset := offset & (uintptr(1)<<l1Shift - 1)
	if got := mirroredRuntimePrimaryState(page, pageOffset); got != nil {
		t.Fatalf("runtime compact gate = %p, want conservative nil", got)
	}
	if pt.TryCompactWrite(addr, current, clock, 0x1234) {
		t.Fatal("true compact no-op stayed gated instead of requesting hot promotion")
	}

	promoted := pt.GetOrCreate(addr)
	if promoted == nil {
		t.Fatal("hot promotion returned nil state")
	}
	if promoted.GetW() != current || promoted.GetLifecycleID() != before.GetLifecycleID() {
		t.Fatalf("promoted state = %v lifecycle %d, want W=%v lifecycle %d", promoted, promoted.GetLifecycleID(), current, before.GetLifecycleID())
	}
	if got := view.history.compact.Load(); got != compact {
		t.Fatalf("hot promotion replaced permanent compact gate: got %p, want %p", got, compact)
	}
	if got := mirroredRuntimePrimaryState(page, pageOffset); got != promoted {
		t.Fatalf("materialized slot precedence = %p, want %p", got, promoted)
	}

	pt.ClearRange(addr&^(rangeBlockSize-1), rangeBlockSize)
	if got := view.history.compact.Load(); got != compact {
		t.Fatalf("full clear replaced permanent compact gate: got %p, want %p", got, compact)
	}
	if compact.active.Load() == 0 {
		t.Fatal("full clear deactivated permanent compact gate")
	}
	if got := mirroredRuntimePrimaryState(page, pageOffset); got != nil {
		t.Fatalf("runtime mapping after full clear = %p, want authoritative nil", got)
	}
}

func TestRuntimeFastPathExternalCompactGateAndHotPromotion(t *testing.T) {
	pt := NewPageTableShadow()
	pt.GetOrCreate(0x1003) // Pin the primary window below addr.
	const addr = uintptr(1)<<40 + 0x223
	current := epoch.NewEpoch(16, 47)
	clock := vectorclock.New()
	clock.Set(16, 47)

	if !pt.TryCompactWrite(addr, current, clock, 0x5678) {
		t.Fatal("first external simple write did not enter compact history")
	}
	view, ok := pt.blockFor(addr, false)
	if !ok {
		t.Fatal("compact write did not publish its external block")
	}
	compact := view.history.compact.Load()
	if compact == nil || compact.active.Load() == 0 || pt.GetSlot(addr) != nil {
		t.Fatalf("external compact setup = (%p, slot %p), want permanent gate and nil slot", compact, pt.GetSlot(addr))
	}
	before := pt.Get(addr)
	if got, matched := mirroredRuntimeExternalState(pt, addr); !matched || got != nil {
		t.Fatalf("external runtime compact gate = (%p, %v), want (nil, true)", got, matched)
	}
	if pt.TryCompactWrite(addr, current, clock, 0x5678) {
		t.Fatal("true external compact no-op stayed gated instead of requesting hot promotion")
	}

	promoted := pt.GetOrCreate(addr)
	if promoted == nil {
		t.Fatal("external hot promotion returned nil state")
	}
	if promoted.GetW() != current || promoted.GetLifecycleID() != before.GetLifecycleID() {
		t.Fatalf("external promoted state = %v, want preserved compact history", promoted)
	}
	if got := view.history.compact.Load(); got != compact {
		t.Fatalf("external hot promotion replaced compact gate: got %p, want %p", got, compact)
	}
	if got, matched := mirroredRuntimeExternalState(pt, addr); !matched || got != promoted {
		t.Fatalf("external materialized precedence = (%p, %v), want (%p, true)", got, matched, promoted)
	}

	pt.ClearRange(addr&^(rangeBlockSize-1), rangeBlockSize)
	if got := view.history.compact.Load(); got != compact {
		t.Fatalf("external full clear replaced compact gate: got %p, want %p", got, compact)
	}
	if compact.active.Load() == 0 {
		t.Fatal("external full clear deactivated permanent compact gate")
	}
	if got, matched := mirroredRuntimeExternalState(pt, addr); !matched || got != nil {
		t.Fatalf("external mapping after full clear = (%p, %v), want (nil, true)", got, matched)
	}
}

func TestRuntimeReadCacheStateIdentityRejectsClearedPrimaryLane(t *testing.T) {
	pt := NewPageTableShadow()
	const addr = uintptr(1)<<40 + 0x123
	cached := pt.GetOrCreate(addr)
	if cached == nil {
		t.Fatal("setup did not materialize cached state")
	}

	base := pt.base.Load()
	offset := addr - base
	page := pt.pages[offset>>l1Shift].Load()
	pageOffset := offset & (uintptr(1)<<l1Shift - 1)
	if got := mirroredRuntimePrimaryState(page, pageOffset); got != cached {
		t.Fatalf("initial runtime mapping = %p, want cached state %p", got, cached)
	}

	pt.ClearRange(addr, 1)
	if got := mirroredRuntimePrimaryState(page, pageOffset); got == cached {
		t.Fatalf("cleared runtime mapping revived cached state %p", cached)
	}
	replacement := pt.GetOrCreate(addr)
	if replacement == nil || replacement == cached {
		t.Fatalf("replacement state = %p, cached generation = %p", replacement, cached)
	}
	if got := mirroredRuntimePrimaryState(page, pageOffset); got != replacement {
		t.Fatalf("reused runtime mapping = %p, want replacement %p", got, replacement)
	}
}

func TestRuntimeReadCacheStateIdentityRejectsClearedExternalLane(t *testing.T) {
	pt := NewPageTableShadow()
	pt.GetOrCreate(0x1003) // Pin the primary window below addr.
	const addr = uintptr(1)<<40 + 0x223
	cached := pt.GetOrCreate(addr)
	if got, matched := mirroredRuntimeExternalState(pt, addr); !matched || got != cached {
		t.Fatalf("initial external mapping = (%p, %v), want cached state %p", got, matched, cached)
	}

	pt.ClearRange(addr, 1)
	if got, matched := mirroredRuntimeExternalState(pt, addr); !matched || got == cached {
		t.Fatalf("cleared external mapping = (%p, %v), cached state %p", got, matched, cached)
	}
	replacement := pt.GetOrCreate(addr)
	if replacement == nil || replacement == cached {
		t.Fatalf("external replacement = %p, cached generation = %p", replacement, cached)
	}
	if got, matched := mirroredRuntimeExternalState(pt, addr); !matched || got != replacement {
		t.Fatalf("reused external mapping = (%p, %v), want replacement %p", got, matched, replacement)
	}
}

func TestRuntimeFastPathExternalCollisionBound(t *testing.T) {
	pt := NewPageTableShadow()
	pt.GetOrCreate(0x1003) // Pin the primary window below the test addresses.

	const (
		start      = uintptr(1) << 40
		chainLimit = 8
	)
	targetBucket := externalBlockHash(start)
	addresses := make([]uintptr, 0, chainLimit+1)
	for addr := start; len(addresses) < cap(addresses); addr += rangeBlockSize {
		if externalBlockHash(addr) == targetBucket {
			addresses = append(addresses, addr)
		}
	}

	write := epoch.NewEpoch(9, 29)
	for _, addr := range addresses[:chainLimit] {
		pt.AccessRange(addr, rangeBlockSize, func(_ uintptr, _ uint8, state *VarState) {
			state.SetW(write)
		})
	}
	// Cells are prepended. The oldest target is exactly the eighth and final
	// cell inspected by the runtime bound.
	if got, matched := mirroredRuntimeExternalState(pt, addresses[0]+7); !matched || got == nil || got.GetW() != write {
		t.Fatalf("external lookup at bound = (%v, %v), want W=%v", got, matched, write)
	}
	pt.AccessRange(addresses[chainLimit], rangeBlockSize, func(_ uintptr, _ uint8, state *VarState) {
		state.SetW(write)
	})
	// Prepending one more cell puts the target beyond the bound. Missing it is
	// intentional and sound: the hook takes the full detector path.
	if got, matched := mirroredRuntimeExternalState(pt, addresses[0]+7); matched || got != nil {
		t.Fatalf("external lookup beyond bound = (%v, %v), want slow-path miss", got, matched)
	}
	if got := pt.Get(addresses[0] + 7); got == nil || got.GetW() != write {
		t.Fatalf("full sparse lookup beyond runtime bound = %v, want W=%v", got, write)
	}

	pt.Reset()
	if got, matched := mirroredRuntimeExternalState(pt, addresses[chainLimit]+7); matched || got != nil {
		t.Fatalf("external lookup after reset = (%v, %v), want miss", got, matched)
	}
}

func TestRuntimeFastPathExternalClearReuseLifecycle(t *testing.T) {
	pt := NewPageTableShadow()
	pt.GetOrCreate(0x1003)
	const appBase = uintptr(1) << 40

	firstWrite := epoch.NewEpoch(10, 31)
	pt.AccessRange(appBase, rangeBlockSize, func(_ uintptr, _ uint8, state *VarState) {
		state.SetW(firstWrite)
	})
	cell := pt.externalBlock(appBase, false)
	if cell == nil {
		t.Fatal("external setup did not publish a block cell")
	}

	pt.ClearRange(appBase, rangeBlockSize)
	if got := pt.externalBlock(appBase, false); got != cell {
		t.Fatalf("full clear replaced sparse cell: got %p, want %p", got, cell)
	}
	if got, matched := mirroredRuntimeExternalState(pt, appBase+255); !matched || got != nil {
		t.Fatalf("external lookup after full clear = (%v, %v), want authoritative empty cell", got, matched)
	}
	if cell.history.state.Load() != nil {
		t.Fatal("full clear retained external block default")
	}
	if slots := cell.slots.Load(); slots != nil {
		for i := range slots.slots {
			if slot := slots.slots[i].Load(); slot != nil {
				t.Fatalf("full clear retained external slot %d: %p", i, slot)
			}
		}
	}

	secondWrite := epoch.NewEpoch(10, 37)
	pt.AccessRange(appBase, rangeBlockSize, func(_ uintptr, _ uint8, state *VarState) {
		state.SetW(secondWrite)
	})
	if got, matched := mirroredRuntimeExternalState(pt, appBase+255); !matched || got == nil || got.GetW() != secondWrite {
		t.Fatalf("external lookup after reuse = (%v, %v), want W=%v", got, matched, secondWrite)
	}
}

func TestRuntimeFastPathExternalRandomizedMatchesGet(t *testing.T) {
	pt := NewPageTableShadow()
	pt.GetOrCreate(0x1003)

	type record struct {
		addr  uintptr
		state *VarState
	}
	records := make([]record, 0, 512)
	used := make(map[uintptr]bool)
	random := uint64(0x6a09e667f3bcc909)
	for len(records) < cap(records) {
		random ^= random << 13
		random ^= random >> 7
		random ^= random << 17
		blockBase := (uintptr(1) << 40) + (uintptr(random&((1<<31)-1)) &^ (rangeBlockSize - 1))
		if used[blockBase] {
			continue
		}
		used[blockBase] = true
		addr := blockBase + uintptr((random>>32)&(uint64(rangeBlockSize)-1))
		var state *VarState
		if len(records)&1 == 0 {
			write := epoch.NewEpoch(11, uint64(len(records)+1))
			pt.AccessRange(blockBase, rangeBlockSize, func(_ uintptr, _ uint8, defaultState *VarState) {
				defaultState.SetW(write)
				state = defaultState
			})
		} else {
			state = pt.GetOrCreate(addr)
			state.SetW(epoch.NewEpoch(12, uint64(len(records)+1)))
		}
		records = append(records, record{addr: addr, state: state})
	}

	for i, record := range records {
		oracle := pt.Get(record.addr)
		got, matched := mirroredRuntimeExternalState(pt, record.addr)
		if !matched || got != oracle || got != record.state {
			t.Fatalf("record %d addr %#x: mirrored=(%p, %v), Get=%p, want=%p", i, record.addr, got, matched, oracle, record.state)
		}
	}
}

func TestPageTableConcurrentExternalPublicationIsExact(t *testing.T) {
	pt := NewPageTableShadow()
	pt.GetOrCreate(0x1003)
	const (
		start = uintptr(1) << 40
		count = 32
	)
	targetBucket := externalBlockHash(start)
	addresses := make([]uintptr, 0, count)
	for addr := start; len(addresses) < cap(addresses); addr += rangeBlockSize {
		if externalBlockHash(addr) == targetBucket {
			addresses = append(addresses, addr)
		}
	}

	states := make([]*VarState, len(addresses))
	startWork := make(chan struct{})
	var wg sync.WaitGroup
	for i, addr := range addresses {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startWork
			state := pt.GetOrCreate(addr + uintptr(i&7))
			state.SetW(epoch.NewEpoch(13, uint64(i+1)))
			states[i] = state
		}()
	}
	close(startWork)
	wg.Wait()

	for i, addr := range addresses {
		exact := addr + uintptr(i&7)
		if got := pt.Get(exact); got != states[i] {
			t.Fatalf("cell %d addr %#x: Get=%p, want %p", i, exact, got, states[i])
		}
		got, matched := mirroredRuntimeExternalState(pt, exact)
		if matched && got != states[i] {
			t.Fatalf("cell %d addr %#x: bounded lookup returned other state %p, want %p", i, exact, got, states[i])
		}
		if !matched && got != nil {
			t.Fatalf("cell %d addr %#x: bounded miss returned state %p", i, exact, got)
		}
	}
}

func TestPageTableExactAddressClearRange(t *testing.T) {
	pt := NewPageTableShadow()
	first := pt.GetOrCreate(0x1000)
	adjacent := pt.GetOrCreate(0x1004)

	pt.ClearRange(0x1000, 4)
	if got := pt.Get(0x1000); got != nil {
		t.Fatalf("cleared state = %p, want nil", got)
	}
	if got := pt.Get(0x1004); got != adjacent {
		t.Fatalf("adjacent live state = %p, want %p", got, adjacent)
	}
	if first == adjacent {
		t.Fatal("test setup unexpectedly shared exact-address state")
	}
}

func TestPageTableLowAddressUsesSparseExactBlock(t *testing.T) {
	pt := NewPageTableShadow()
	const low = uintptr(0x1003)
	state := pt.GetOrCreate(low)
	if base := pt.base.Load(); base != uintptr(1)<<l1Shift {
		t.Fatalf("low-address initialization base=%#x, want aligned sentinel window base %#x", base, uintptr(1)<<l1Shift)
	}
	if got := pt.Get(low); got != state {
		t.Fatalf("sparse exact lookup = %p, want %p", got, state)
	}
	if got := pt.Get(low + 1); got != nil {
		t.Fatalf("adjacent sparse lane unexpectedly aliased low address: %p", got)
	}
}

func TestPageTableImmutableBasePreservesSequentialRoutes(t *testing.T) {
	pt := NewPageTableShadow()
	const (
		low  = uintptr(1) << 32
		high = (uintptr(1) << 35) + low
	)
	lowState := pt.GetOrCreate(low)
	base := pt.base.Load()
	highState := pt.GetOrCreate(high)
	if got := pt.base.Load(); got != base {
		t.Fatalf("primary base changed from %#x to %#x", base, got)
	}
	if base == 0 || base&((uintptr(1)<<l1Shift)-1) != 0 {
		t.Fatalf("immutable base=%#x, want nonzero 2MiB alignment", base)
	}
	if got := pt.Get(low); got != lowState {
		t.Fatalf("low state after later overlapping-window address = %p, want %p", got, lowState)
	}
	if got := pt.Get(high); got != highState {
		t.Fatalf("high state after immutable initialization = %p, want %p", got, highState)
	}
}

func TestPageTableBaseInitializationPreservesBlockDefault(t *testing.T) {
	pt := NewPageTableShadow()
	const (
		lowBlock = ptTotalCoverage / 4
		high     = ptTotalCoverage / 2
	)
	write := epoch.NewEpoch(14, 41)
	pt.AccessRange(lowBlock, rangeBlockSize, func(_ uintptr, _ uint8, state *VarState) {
		state.SetW(write)
	})

	before := pt.Get(lowBlock + 123)
	if before == nil || before.GetW() != write {
		t.Fatalf("setup block history = %v, want W=%v", before, write)
	}
	base := pt.base.Load()
	pt.GetOrCreate(high)
	if got := pt.base.Load(); got != base {
		t.Fatalf("base changed from %#x to %#x", base, got)
	}
	if after := pt.Get(lowBlock + 123); after != before {
		t.Fatalf("block default changed after later address: got %p, want %p", after, before)
	}
}

func TestPageTablePrimaryExternalBoundaries(t *testing.T) {
	pt := NewPageTableShadow()
	pt.GetOrCreate(0x1003)
	base := pt.base.Load()
	if base != uintptr(1)<<l1Shift {
		t.Fatalf("base=%#x, want %#x", base, uintptr(1)<<l1Shift)
	}

	addresses := []struct {
		addr     uintptr
		external bool
	}{
		{base - 1, true},
		{base, false},
		{base + ptTotalCoverage - 1, false},
		{base + ptTotalCoverage, true},
	}
	for _, test := range addresses {
		state := pt.GetOrCreate(test.addr)
		if got := pt.Get(test.addr); got != state {
			t.Errorf("addr %#x: Get=%p, want %p", test.addr, got, state)
		}
		external := pt.externalBlock(test.addr, false)
		if test.external && external == nil {
			t.Errorf("addr %#x: expected sparse external route", test.addr)
		}
		if !test.external && external != nil {
			t.Errorf("addr %#x: unexpected sparse external cell %p", test.addr, external)
		}
	}
	if got := pt.base.Load(); got != base {
		t.Fatalf("boundary accesses changed base from %#x to %#x", base, got)
	}
}

func TestPageTableConcurrentInitializationPreservesWinnerRoutes(t *testing.T) {
	const (
		low  = uintptr(1) << 32
		high = uintptr(1) << 40
	)
	for iteration := range 100 {
		pt := NewPageTableShadow()
		start := make(chan struct{})
		var lowState, highState *VarState
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			lowState = pt.GetOrCreate(low)
		}()
		go func() {
			defer wg.Done()
			<-start
			highState = pt.GetOrCreate(high)
		}()
		close(start)
		wg.Wait()

		base := pt.base.Load()
		if base == 0 || base&((uintptr(1)<<l1Shift)-1) != 0 {
			t.Fatalf("iteration %d: concurrent base=%#x, want nonzero 2MiB alignment", iteration, base)
		}
		if got := pt.Get(low); got != lowState {
			t.Fatalf("iteration %d: concurrent initialization lost low state: got %p, want %p", iteration, got, lowState)
		}
		if got := pt.Get(high); got != highState {
			t.Fatalf("iteration %d: concurrent initialization lost high state: got %p, want %p", iteration, got, highState)
		}
	}
}
