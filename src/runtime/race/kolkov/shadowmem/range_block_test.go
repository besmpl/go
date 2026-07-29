//go:build amd64 || arm64

package shadowmem

import (
	"sync"
	"testing"

	"runtime/race/kolkov/epoch"
)

func TestPageTableFullRangesUseBlockDefaults(t *testing.T) {
	pt := NewPageTableShadow()
	const (
		base = uintptr(1) << 40
		size = uintptr(1) << 20
	)
	write := epoch.NewEpoch(7, 11)
	visits := 0
	pt.AccessRange(base, size, func(_ uintptr, _ uint8, state *VarState) {
		visits++
		state.SetW(write)
	})

	wantBlocks := int(size / rangeBlockSize)
	if visits != wantBlocks {
		t.Fatalf("full-range visits = %d, want one per block (%d)", visits, wantBlocks)
	}
	for offset := uintptr(0); offset < size; offset += rangeBlockSize {
		if slot := pt.GetSlot(base + offset); slot != nil {
			t.Fatalf("block %#x materialized word slot %p", offset, slot)
		}
		state := pt.Get(base + offset)
		if state == nil || state.GetW() != write {
			t.Fatalf("block %#x default = %v, want W=%v", offset, state, write)
		}
	}
	if first, second := pt.Get(base), pt.Get(base+rangeBlockSize); first == second {
		t.Fatal("adjacent blocks unexpectedly share mutable default state")
	}
}

func TestPageTableWordMaterializationClonesCompleteBlockDefault(t *testing.T) {
	pt := NewPageTableShadow()
	const base = uintptr(1) << 40
	initial := epoch.NewEpoch(3, 17)
	pt.AccessRange(base, rangeBlockSize, func(_ uintptr, _ uint8, state *VarState) {
		state.SetW(initial)
		state.SetWritePC(0x1234)
		state.IncrementWriteCount()
	})
	defaultState := pt.Get(base)

	const offset = uintptr(123)
	slot := pt.GetOrCreateSlot(base + offset)
	clone := slot.State(0)
	if clone == nil || clone == defaultState {
		t.Fatalf("materialized clone = %p, default = %p", clone, defaultState)
	}
	for lane := uint8(1); lane < shadowSlotLanes; lane++ {
		if got := slot.State(lane); got != clone {
			t.Fatalf("lane %d state = %p, want whole-word clone %p", lane, got, clone)
		}
	}
	if clone.GetW() != initial || clone.GetWritePC() != 0x1234 || clone.GetWriteCount() != 1 {
		t.Fatalf("materialized state lost default history: %s", clone)
	}

	isolated := slot.Isolate(uint8((base + offset) & 7))
	isolated.SetW(epoch.NewEpoch(9, 23))
	isolated.UnlockAccess()
	if defaultState.GetW() != initial {
		t.Fatalf("scalar override contaminated block default: W=%v", defaultState.GetW())
	}
}

func TestPageTablePartialClearDetachesFromBlockDefault(t *testing.T) {
	pt := NewPageTableShadow()
	const base = uintptr(1) << 40
	write := epoch.NewEpoch(5, 29)
	pt.AccessRange(base, rangeBlockSize, func(_ uintptr, _ uint8, state *VarState) {
		state.SetW(write)
	})

	const offset = uintptr(123)
	pt.ClearRange(base+offset, 1)
	if got := pt.Get(base + offset); got != nil {
		t.Fatalf("cleared lane retained block history: %p", got)
	}
	if got := pt.Get(base + offset - 1); got == nil || got.GetW() != write {
		t.Fatalf("adjacent lane lost block history: %v", got)
	}
	if got := pt.Get(base + offset + 1); got == nil || got.GetW() != write {
		t.Fatalf("adjacent lane lost block history: %v", got)
	}
}

func TestPageTableSparseBlocksAvoidCASWordChains(t *testing.T) {
	pt := NewPageTableShadow()
	// Establish a primary window far away from the range below.
	pt.GetOrCreate(uintptr(1) << 40)
	const (
		base = uintptr(1) << 42
		size = uintptr(1) << 20
	)
	visits := 0
	pt.AccessRange(base, size, func(_ uintptr, _ uint8, state *VarState) {
		visits++
		state.SetW(epoch.NewEpoch(1, 1))
	})
	if want := int(size / rangeBlockSize); visits != want {
		t.Fatalf("sparse range visits = %d, want %d", visits, want)
	}
	cells := 0
	for i := range pt.external {
		for cell := pt.external[i].Load(); cell != nil; cell = cell.next {
			cells++
			if slots := cell.block.slots.Load(); slots != nil {
				t.Fatalf("sparse full range allocated slot table %p", slots)
			}
		}
	}
	if want := int(size / rangeBlockSize); cells != want {
		t.Fatalf("sparse block cells = %d, want %d", cells, want)
	}
}

func TestPageTableMaterializationSerializesWithBlockRange(t *testing.T) {
	const base = uintptr(1) << 40
	for iteration := 0; iteration < 100; iteration++ {
		pt := NewPageTableShadow()
		pt.AccessRange(base, rangeBlockSize, func(_ uintptr, _ uint8, state *VarState) {
			state.SetW(epoch.NewEpoch(1, 1))
		})

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var scalar *VarState
		go func() {
			defer wg.Done()
			<-start
			scalar = pt.GetOrCreateSlot(base + 19).Isolate(3)
			scalar.SetW(epoch.NewEpoch(2, 2))
			scalar.UnlockAccess()
		}()
		go func() {
			defer wg.Done()
			<-start
			pt.AccessRange(base, rangeBlockSize, func(_ uintptr, _ uint8, state *VarState) {
				state.SetW(epoch.NewEpoch(3, 3))
			})
		}()
		close(start)
		wg.Wait()

		if got := pt.Get(base + 19); got != scalar {
			t.Fatalf("iteration %d: scalar state orphaned: got %p want %p", iteration, got, scalar)
		}
	}
}
