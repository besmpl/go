package shadowmem

import (
	"sync"
	"testing"
	"unsafe"

	"runtime/race/kolkov/epoch"
)

func TestShadowSlotGetOrCreateSerializesWithGroupPublication(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		var slot ShadowSlot
		const lane = uint8(3)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)

		var scalar *VarState
		go func() {
			defer wg.Done()
			<-start
			scalar = slot.GetOrCreateLane(lane)
		}()
		go func() {
			defer wg.Done()
			<-start
			slot.AccessGroups(0xff, func(_ uint8, state *VarState) {
				state.SetW(epoch.NewEpoch(1, 1))
			})
		}()

		close(start)
		wg.Wait()
		if got := slot.State(lane); scalar != got {
			t.Fatalf("iteration %d: scalar setup returned orphan %p; lane publishes %p", iteration, scalar, got)
		}
	}
}

func TestCloneOrdinaryLockedCopiesCompleteState(t *testing.T) {
	state := NewVarState()
	state.SetW(epoch.NewEpoch(7, 91))
	state.SetExclusiveWriter(-1)
	state.SetWritePC(0x1111)
	state.SetReadPC(0x2222)
	state.SetWriteStack(0x3333)
	state.SetReadStack(0x4444)
	state.IncrementWriteCount()
	state.SetReadEpoch(epoch.NewEpoch(2, 17))
	state.PromoteToReadClock(epoch.NewEpoch(3, 29), nil)

	state.LockAccess()
	clone := state.CloneOrdinaryLocked()
	state.UnlockAccess()
	if clone == nil {
		t.Fatal("ordinary clone unexpectedly rejected")
	}
	if clone.GetW() != state.GetW() || clone.GetExclusiveWriter() != -1 {
		t.Fatalf("write state not cloned: W=%v owner=%d", clone.GetW(), clone.GetExclusiveWriter())
	}
	if clone.GetWritePC() != 0x1111 || clone.GetReadPC() != 0x2222 {
		t.Fatalf("PC state not cloned: write=%#x read=%#x", clone.GetWritePC(), clone.GetReadPC())
	}
	if clone.GetWriteStack() != 0x3333 || clone.GetReadStack() != 0x4444 {
		t.Fatal("stack metadata not cloned")
	}
	if clone.GetWriteCount() != 1 || !clone.IsPromoted() {
		t.Fatalf("protected state not cloned: count=%d promoted=%v", clone.GetWriteCount(), clone.IsPromoted())
	}
	if clone.GetReadClock() == state.GetReadClock() {
		t.Fatal("promoted read clock was shallow-copied")
	}
	if got := clone.GetReadClock().Get(3); got != 29 {
		t.Fatalf("cloned read clock[3]=%d, want 29", got)
	}

	marker := new(byte)
	state.LockAccess()
	state.SetAtomicState(unsafe.Pointer(marker))
	atomicClone := state.CloneOrdinaryLocked()
	state.UnlockAccess()
	if got := atomicClone.GetAtomicState(); got != unsafe.Pointer(marker) {
		t.Fatalf("clone atomic overlay = %p, want shared %p", got, marker)
	}
}

func TestShadowSlotSplitOnlyCOWProperty(t *testing.T) {
	var slot ShadowSlot
	var wantCount [8]uint32
	var wantWrite [8]epoch.Epoch
	seed := uint32(0x91e10da5)

	for step := uint64(1); step <= 1000; step++ {
		seed = seed*1664525 + 1013904223
		mask := uint8(seed >> 24)
		if mask == 0 {
			mask = 1 << (seed & 7)
		}
		write := epoch.NewEpoch(1, step)
		slot.AccessGroups(mask, func(_ uint8, state *VarState) {
			state.IncrementWriteCount()
			state.SetW(write)
		})
		for lane := 0; lane < 8; lane++ {
			if mask&(1<<lane) != 0 {
				wantCount[lane]++
				wantWrite[lane] = write
			}
			state := slot.State(uint8(lane))
			if wantCount[lane] == 0 {
				if state != nil {
					t.Fatalf("step %d lane %d unexpectedly materialized", step, lane)
				}
				continue
			}
			if state == nil {
				t.Fatalf("step %d lane %d has nil state", step, lane)
			}
			if state.GetWriteCount() != wantCount[lane] || state.GetW() != wantWrite[lane] {
				t.Fatalf("step %d lane %d got count=%d W=%v; want count=%d W=%v",
					step, lane, state.GetWriteCount(), state.GetW(), wantCount[lane], wantWrite[lane])
			}
		}
	}
}

func TestShadowSlotPartialGroupClonesAndScalarIsolates(t *testing.T) {
	var slot ShadowSlot
	initialWrite := epoch.NewEpoch(1, 10)
	slot.AccessGroups(0x0f, func(_ uint8, state *VarState) {
		state.SetW(initialWrite)
		state.SetReadEpoch(epoch.NewEpoch(2, 20))
		state.SetWritePC(0x1234)
		state.IncrementWriteCount()
	})
	shared := slot.State(0)
	for lane := uint8(1); lane < 4; lane++ {
		if slot.State(lane) != shared {
			t.Fatalf("initial lane %d did not join zero-state group", lane)
		}
	}

	isolated := slot.Isolate(1)
	if isolated == shared {
		t.Fatal("scalar lane retained shared state")
	}
	if isolated.GetW() != initialWrite || isolated.GetReadEpoch() != epoch.NewEpoch(2, 20) || isolated.GetWritePC() != 0x1234 || isolated.GetWriteCount() != 1 {
		t.Fatalf("isolated state did not clone complete ordinary history: %s", isolated)
	}
	isolated.SetW(epoch.NewEpoch(3, 30))
	isolated.UnlockAccess()

	if got := slot.State(0).GetW(); got != initialWrite {
		t.Fatalf("scalar mutation contaminated adjacent group: W=%v", got)
	}
	if slot.State(2) != shared || slot.State(3) != shared {
		t.Fatal("unaffected lanes were redirected")
	}
}

func TestShadowSlotNeverMergesExistingGroups(t *testing.T) {
	var slot ShadowSlot
	first := slot.Isolate(0)
	first.SetW(epoch.NewEpoch(1, 1))
	first.UnlockAccess()
	second := slot.Isolate(1)
	second.SetW(epoch.NewEpoch(1, 1))
	second.UnlockAccess()
	if first == second {
		t.Fatal("test setup unexpectedly shared states")
	}

	slot.AccessGroups(0x03, func(_ uint8, state *VarState) {
		state.SetW(epoch.NewEpoch(2, 2))
	})
	if slot.State(0) != first || slot.State(1) != second {
		t.Fatal("range operation merged distinct equivalence groups")
	}
}

func TestShadowSlotLockGroupsLocksEachEquivalenceGroupOnce(t *testing.T) {
	var slot ShadowSlot
	marker := new(byte)
	slot.AccessGroups(0xff, func(_ uint8, state *VarState) {
		state.SetW(epoch.NewEpoch(1, 9))
		state.SetAtomicState(unsafe.Pointer(marker))
	})

	visits := 0
	var locked *VarState
	slot.LockGroups(0xff, func(mask uint8, state *VarState) {
		visits++
		locked = state
		if mask != 0xff {
			t.Fatalf("locked mask = %#02x, want 0xff", mask)
		}
	})
	if visits != 1 {
		t.Fatalf("full shared group visits = %d, want 1", visits)
	}
	locked.UnlockAccess()

	var partial *VarState
	slot.LockGroups(0x0f, func(mask uint8, state *VarState) {
		partial = state
		if mask != 0x0f {
			t.Fatalf("partial mask = %#02x, want 0x0f", mask)
		}
	})
	if partial == locked {
		t.Fatal("partial atomic transaction did not split ordinary group")
	}
	if partial.GetW() != epoch.NewEpoch(1, 9) || partial.GetAtomicState() != unsafe.Pointer(marker) {
		t.Fatal("partial group did not retain ordinary state and shared atomic overlay")
	}
	partial.UnlockAccess()
	if slot.State(0) != partial || slot.State(3) != partial || slot.State(4) != locked {
		t.Fatal("partial transaction redirected the wrong lanes")
	}
}
