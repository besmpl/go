//go:build amd64 || arm64

package shadowmem

import (
	"runtime"
	"testing"
	"time"
	"unsafe"

	"runtime/race/kolkov/epoch"
)

func TestAtomicStateUsesExistingVarStatePadding(t *testing.T) {
	var state VarState
	if got, want := unsafe.Sizeof(state), uintptr(128); got != want {
		t.Fatalf("VarState size = %d, want %d", got, want)
	}
	if got, want := unsafe.Offsetof(state.atomicState), uintptr(120); got != want {
		t.Fatalf("atomic state offset = %d, want %d", got, want)
	}
	if got, want := unsafe.Offsetof(state.W), uintptr(0); got != want {
		t.Fatalf("runtime-mirrored W offset = %d, want %d", got, want)
	}
	if got, want := unsafe.Offsetof(state.readEpoch0), uintptr(32); got != want {
		t.Fatalf("runtime-mirrored read epoch offset = %d, want %d", got, want)
	}
	if got, want := unsafe.Offsetof(state.readerState), uintptr(40); got != want {
		t.Fatalf("runtime-mirrored reader state offset = %d, want %d", got, want)
	}
	if got, want := unsafe.Sizeof(ShadowSlot{}), uintptr(72); got != want {
		t.Fatalf("ShadowSlot size = %d, want %d; atomic fast metadata must remain out-of-line", got, want)
	}
	if got, want := unsafe.Sizeof(AtomicFastPath{}), uintptr(56); got != want {
		t.Fatalf("atomic-only sidecar size = %d, want %d", got, want)
	}
}

func TestAtomicStateResetDropsOpaqueHistory(t *testing.T) {
	state := NewVarState()
	marker := new(byte)
	state.LockAccess()
	state.SetAtomicState(unsafe.Pointer(marker))
	state.UnlockAccess()

	state.Reset()
	if got := state.GetAtomicState(); got != nil {
		t.Fatalf("atomic history after Reset = %p, want nil", got)
	}
}

func TestAtomicStateCloneBalancesSidecarOwnership(t *testing.T) {
	state := NewVarState()
	overlay := unsafe.Pointer(new(byte))
	retains, releases := 0, 0
	retain := func(p unsafe.Pointer) {
		if p != overlay {
			t.Fatalf("retained overlay %p, want %p", p, overlay)
		}
		retains++
	}
	release := func(p unsafe.Pointer) {
		if p != overlay {
			t.Fatalf("released overlay %p, want %p", p, overlay)
		}
		releases++
	}
	state.LockAccess()
	state.SetAtomicStateOwned(overlay, retain, release)
	clone := state.CloneOrdinaryLocked()
	state.UnlockAccess()
	if retains != 2 || releases != 0 {
		t.Fatalf("sidecar ownership after clone = retains %d releases %d, want 2/0", retains, releases)
	}

	state.Reset()
	if clone.GetAtomicState() != overlay || releases != 1 {
		t.Fatalf("first detach changed surviving clone: overlay=%p releases=%d", clone.GetAtomicState(), releases)
	}
	clone.Reset()
	if retains != 2 || releases != 2 {
		t.Fatalf("balanced sidecar ownership = retains %d releases %d, want 2/2", retains, releases)
	}
}

func enrolledAtomicFastPathForTest(t *testing.T, mask uint8) (*ShadowSlot, *AtomicFastPath) {
	t.Helper()
	slot := new(ShadowSlot)
	return slot, enrollAtomicFastPathInSlotForTest(t, slot, mask)
}

func enrollAtomicFastPathInSlotForTest(t *testing.T, slot *ShadowSlot, mask uint8) *AtomicFastPath {
	t.Helper()
	var binding *AtomicFastPath
	slot.LockAtomicGroups(mask, true, func(gotMask uint8, state *VarState) {
		if gotMask != mask {
			t.Fatalf("enrollment group mask = %#x, want %#x", gotMask, mask)
		}
		marker := new(byte)
		state.SetAtomicState(unsafe.Pointer(marker))
		binding = state.atomicState.Load()
		if binding == nil {
			t.Fatal("visitor did not attach an atomic binding")
		}
		if enrolled := binding.enrolled.Load(); enrolled != 0 {
			t.Fatalf("visitor binding enrolled=%d, want unpublished capability", enrolled)
		}
	})
	if binding == nil || binding.enrolled.Load() == 0 {
		t.Fatal("compatible slow setup did not enroll its attached binding")
	}
	binding.state.UnlockAccess()
	return binding
}

func TestAtomicFastPathRetainWinsBeforeEscape(t *testing.T) {
	slot, binding := enrolledAtomicFastPathForTest(t, 0xff)
	if !binding.TryRetain(0xff) {
		t.Fatal("cached capability did not retain directly")
	}

	done := make(chan struct{})
	go func() {
		slot.ClearMask(0xff)
		close(done)
	}()
	deadline := time.After(time.Second)
	for binding.users.Load()&atomicFastEscaped == 0 {
		select {
		case <-deadline:
			t.Fatal("clear did not close retained capability")
		default:
		}
	}
	select {
	case <-done:
		t.Fatal("escape completed before retained transaction released")
	default:
	}

	binding.Release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("escape did not complete after retained transaction released")
	}
}

func TestAtomicFastPathEscapeWinsBeforeRetain(t *testing.T) {
	slot, binding := enrolledAtomicFastPathForTest(t, 0xff)
	slot.AccessGroups(0xff, func(_ uint8, _ *VarState) {})
	if got := binding.users.Load(); got != atomicFastEscaped {
		t.Fatalf("escaped capability gate = %#x, want %#x", got, atomicFastEscaped)
	}
	if fast := slot.TryAtomicFast(0xff); fast != nil {
		fast.Release()
		t.Fatal("ordinary access lost the escape-before-retain race")
	}
	if binding.TryRetain(0xff) {
		binding.Release()
		t.Fatal("escaped cached capability retained directly")
	}
}

func TestAtomicFastPathDirectRetainRejectsWrongMask(t *testing.T) {
	_, binding := enrolledAtomicFastPathForTest(t, 0xff)
	if binding.TryRetain(0x0f) {
		binding.Release()
		t.Fatal("cached capability retained an incompatible mask")
	}
	if got := binding.users.Load(); got != 0 {
		t.Fatalf("wrong-mask retain left users=%#x, want 0", got)
	}
	if !binding.TryRetain(0xff) {
		t.Fatal("wrong-mask miss damaged the exact cached capability")
	}
	binding.Release()
}

func TestAtomicFastPathReenrollmentWaitsAndUsesFreshGeneration(t *testing.T) {
	slot, old := enrolledAtomicFastPathForTest(t, 0xff)
	held := slot.TryAtomicFast(0xff)
	if held != old {
		t.Fatalf("retained capability = %p, want old generation %p", held, old)
	}
	oldState, oldOverlay := old.state, old.overlay
	oldLifecycle := old.lifecycle
	oldMask, oldOrdinaryMask := old.mask, old.ordinaryMask

	done := make(chan *AtomicFastPath, 1)
	go func() {
		// The ordinary transaction closes old and cannot enter its visitor until
		// held drains. Only then may two consecutive compatible setups arm and
		// publish a replacement.
		slot.AccessGroups(0xff, func(_ uint8, _ *VarState) {})
		for attempt := 0; attempt < 2; attempt++ {
			var state *VarState
			slot.LockAtomicGroups(0xff, true, func(_ uint8, locked *VarState) {
				state = locked
			})
			state.UnlockAccess()
		}
		done <- slot.TryAtomicFast(0xff)
	}()

	deadline := time.Now().Add(time.Second)
	for old.users.Load()&atomicFastEscaped == 0 {
		if time.Now().After(deadline) {
			held.Release()
			t.Fatal("ordinary transaction did not close retained generation")
		}
		runtime.Gosched()
	}
	if old.TryRetain(0xff) {
		old.Release()
		held.Release()
		t.Fatal("closed generation accepted a late retain")
	}
	select {
	case fresh := <-done:
		if fresh != nil {
			fresh.Release()
		}
		held.Release()
		t.Fatal("replacement published before retained old token drained")
	default:
	}

	held.Release()
	var fresh *AtomicFastPath
	select {
	case fresh = <-done:
	case <-time.After(time.Second):
		t.Fatal("compatible setup did not publish after old token drained")
	}
	if fresh == nil {
		t.Fatal("compatible setup published no replacement capability")
	}
	defer fresh.Release()
	if fresh == old {
		t.Fatal("compatible setup reopened the escaped capability object")
	}
	if got := old.users.Load(); got != atomicFastEscaped {
		t.Fatalf("old generation gate = %#x, want permanently escaped %#x", got, atomicFastEscaped)
	}
	if old.TryRetain(0xff) {
		old.Release()
		t.Fatal("old generation reopened after replacement publication")
	}
	if !fresh.TryRetain(0xff) {
		t.Fatal("fresh generation could not be retained directly")
	}
	fresh.Release()
	if old.state != oldState || old.overlay != oldOverlay || old.lifecycle != oldLifecycle || old.mask != oldMask || old.ordinaryMask != oldOrdinaryMask {
		t.Fatal("replacement repurposed immutable fields of the escaped descriptor")
	}
}

func TestAtomicFastPathRearmProbationAvoidsGeneralRetryAllocation(t *testing.T) {
	slot, old := enrolledAtomicFastPathForTest(t, 0xff)

	// Model the first general fallback after an enrolled operation. It closes the
	// generation and clears any prior probation marker.
	var initialState *VarState
	slot.LockAtomicGroups(0xff, false, func(_ uint8, state *VarState) {
		initialState = state
	})
	initialState.UnlockAccess()
	if got := old.users.Load(); got != atomicFastEscaped {
		t.Fatalf("initial general transaction gate = %#x, want escaped", got)
	}

	var plainState, generalState *VarState
	allocs := testing.AllocsPerRun(100, func() {
		// The enrolled-mode setup is the first compatible hit after fallback. It remains slow
		// and arms probation without allocating a descriptor.
		slot.LockAtomicGroups(0xff, true, func(_ uint8, state *VarState) {
			plainState = state
		})
		plainState.UnlockAccess()
		if fast := slot.TryAtomicFast(0xff); fast != nil {
			fast.Release()
			t.Fatal("first compatible hit unexpectedly published a capability")
		}

		// The following general transaction is incompatible and clears probation.
		// Repeating this pair must not allocate unreachable descriptors.
		slot.LockAtomicGroups(0xff, false, func(_ uint8, state *VarState) {
			generalState = state
		})
		generalState.UnlockAccess()
	})
	if allocs != 0 {
		t.Fatalf("warmed general-retry capability churn allocated %.2f objects/op, want 0", allocs)
	}

	// Two compatible hits without an intervening escape still restore the fast
	// path, and publication uses a fresh object rather than reopening old.
	for attempt := 0; attempt < 2; attempt++ {
		slot.LockAtomicGroups(0xff, true, func(_ uint8, state *VarState) {
			plainState = state
		})
		plainState.UnlockAccess()
	}
	fresh := slot.TryAtomicFast(0xff)
	if fresh == nil {
		t.Fatal("second consecutive compatible hit did not publish a capability")
	}
	defer fresh.Release()
	if fresh == old {
		t.Fatal("probation reopened the escaped descriptor")
	}
}

func waitForClearToRetainSlot(t *testing.T, slot *ShadowSlot, done <-chan struct{}) {
	t.Helper()
	deadline := time.After(time.Second)
	for slot.mu.state.Load()&1 == 0 {
		select {
		case <-done:
			t.Fatal("clear returned before draining the retained access transaction")
		case <-deadline:
			t.Fatal("clear did not acquire the shadow-slot lifecycle lock")
		default:
			runtime.Gosched()
		}
	}
	select {
	case <-done:
		t.Fatal("clear returned while the slot lifecycle lock was observed retained")
	default:
	}
}

func TestClearMaskDrainsRetainedSlowAccess(t *testing.T) {
	slot := new(ShadowSlot)
	state := slot.Isolate(0)
	locked := true
	defer func() {
		if locked {
			state.UnlockAccess()
		}
	}()

	done := make(chan struct{})
	go func() {
		slot.ClearMask(1)
		close(done)
	}()
	waitForClearToRetainSlot(t, slot, done)
	if got := slot.State(0); got != state {
		t.Fatalf("clear detached retained state %p before its transaction ended; got %p", state, got)
	}
	// Model an already-started scalar visitor attaching its first atomic
	// overlay after clear acquired s.mu but while clear waited for accessMu.
	// The second escape pass must close this late binding before detach.
	state.SetAtomicState(unsafe.Pointer(new(byte)))
	lateBinding := state.atomicState.Load()

	state.UnlockAccess()
	locked = false
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("clear did not finish after the retained access transaction ended")
	}
	if got := slot.State(0); got != nil {
		t.Fatalf("clear retained state %p after draining it", got)
	}
	if got := lateBinding.users.Load(); got != atomicFastEscaped {
		t.Fatalf("late binding gate = %#x, want permanently escaped", got)
	}
}

func TestPageTableClearDrainsRetainedSlowAccess(t *testing.T) {
	const blockBase = uintptr(0x500000)
	for _, test := range []struct {
		name string
		addr uintptr
		size uintptr
	}{
		{name: "partial", addr: blockBase + 3, size: 1},
		{name: "full block", addr: blockBase, size: rangeBlockSize},
	} {
		t.Run(test.name, func(t *testing.T) {
			pt := NewPageTableShadow()
			slot := pt.GetOrCreateSlot(test.addr)
			lane := uint8(test.addr & 7)
			state := slot.Isolate(lane)
			locked := true
			defer func() {
				if locked {
					state.UnlockAccess()
				}
			}()

			done := make(chan struct{})
			go func() {
				pt.ClearRange(test.addr, test.size)
				close(done)
			}()
			waitForClearToRetainSlot(t, slot, done)
			if got := slot.State(lane); got != state {
				t.Fatalf("page clear detached retained state %p before its transaction ended; got %p", state, got)
			}

			state.UnlockAccess()
			locked = false
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("page clear did not finish after the retained access transaction ended")
			}
			if got := pt.Get(test.addr); got != nil {
				t.Fatalf("page clear retained state %p after draining it", got)
			}
		})
	}
}

func TestAtomicFastPathGeneralSetupEscapesNewBinding(t *testing.T) {
	slot := new(ShadowSlot)
	var binding *AtomicFastPath
	slot.AccessGroups(0xff, func(_ uint8, state *VarState) {
		state.SetAtomicState(unsafe.Pointer(new(byte)))
		binding = state.atomicState.Load()
		if got := binding.users.Load(); got != 0 {
			t.Fatalf("new binding escaped before visitor completed: gate=%#x", got)
		}
	})
	if binding == nil {
		t.Fatal("general setup did not attach an atomic binding")
	}
	if gate := binding.users.Load(); gate != atomicFastEscaped {
		t.Fatalf("general setup gate=%#x, want permanently escaped", gate)
	}
	if fast := slot.TryAtomicFast(0xff); fast != nil {
		fast.Release()
		t.Fatal("general setup enrolled its newly attached binding")
	}
}

func TestAtomicFastPathFullBlockClearKeepsResolvedSlotAuthoritative(t *testing.T) {
	pt := NewPageTableShadow()
	const blockBase = uintptr(0x400000)
	target := pt.GetOrCreateSlot(blockBase)
	blocker := pt.GetOrCreateSlot(blockBase + 8)
	blockerBinding := enrollAtomicFastPathInSlotForTest(t, blocker, 0xff)
	heldBlocker := blocker.TryAtomicFast(0xff)
	if heldBlocker != blockerBinding {
		t.Fatalf("retained blocker = %p, want %p", heldBlocker, blockerBinding)
	}
	defer func() {
		if heldBlocker != nil {
			heldBlocker.Release()
		}
	}()

	clearDone := make(chan struct{})
	go func() {
		pt.ClearRange(blockBase, rangeBlockSize)
		close(clearDone)
	}()
	deadline := time.After(time.Second)
	for blockerBinding.users.Load()&atomicFastEscaped == 0 {
		select {
		case <-deadline:
			t.Fatal("full-block clear did not reach the retained blocker slot")
		default:
			runtime.Gosched()
		}
	}
	select {
	case <-clearDone:
		t.Fatal("full-block clear completed while the blocker was retained")
	default:
	}

	// Clear has already processed target and is blocked on the next word. Model
	// AtomicBegin resolving target before clear and acquiring its slot lock only
	// now. This access linearizes after target's clear point and must remain in
	// the page table; detaching the empty slot would orphan it.
	staleBinding := enrollAtomicFastPathInSlotForTest(t, target, 0xff)
	heldTarget := target.TryAtomicFast(0xff)
	if heldTarget != staleBinding {
		t.Fatalf("post-clear retained target = %p, want %p", heldTarget, staleBinding)
	}
	defer heldTarget.Release()
	if got := pt.GetSlot(blockBase); got != target {
		t.Fatalf("resolved slot detached during clear: got %p, want %p", got, target)
	}

	heldBlocker.Release()
	heldBlocker = nil
	select {
	case <-clearDone:
	case <-time.After(time.Second):
		t.Fatal("full-block clear did not finish after blocker release")
	}
	if got := pt.GetSlot(blockBase); got != target {
		t.Fatalf("post-clear slot = %p, want authoritative resolved slot %p", got, target)
	}
	if got := pt.Get(blockBase); got != staleBinding.state {
		t.Fatalf("post-clear state = %p, want post-clear transaction state %p", got, staleBinding.state)
	}
}

func lockInitializedAtomicGroupsForTest(t *testing.T, slot *ShadowSlot, mask uint8, overlay unsafe.Pointer) ([]*VarState, *AtomicFastPath) {
	t.Helper()
	var states []*VarState
	slot.LockAtomicGroups(mask, true, func(_ uint8, state *VarState) {
		state.SetAtomicState(overlay)
		states = append(states, state)
	})
	for i := len(states) - 1; i >= 0; i-- {
		states[i].UnlockAccess()
	}
	return states, slot.TryAtomicFast(mask)
}

func TestAtomicFastPathEnrollsOneFrozenOrdinaryGroup(t *testing.T) {
	slot := new(ShadowSlot)
	initialized := slot.Isolate(0)
	wantW := epoch.NewEpoch(41, 7)
	initialized.SetW(wantW)
	initialized.SetWritePC(0x7a01)
	initialized.UnlockAccess()

	states, fast := lockInitializedAtomicGroupsForTest(t, slot, 0xff, unsafe.Pointer(new(byte)))
	if len(states) != 2 {
		t.Fatalf("initialized exact mask produced %d ordinary groups, want 2", len(states))
	}
	if fast == nil {
		t.Fatal("one initialized group plus empty siblings did not enroll")
	}
	defer fast.Release()
	if fast.State() != initialized || fast.OrdinaryMask() != 0x01 || fast.Mask() != 0xff {
		t.Fatalf("capability descriptor = state %p ordinary %#x mask %#x, want %p/0x01/0xff",
			fast.State(), fast.OrdinaryMask(), fast.Mask(), initialized)
	}
	if initialized.GetW() != wantW || initialized.GetWritePC() != 0x7a01 {
		t.Fatalf("enrollment changed frozen ordinary history: W=%v PC=%#x", initialized.GetW(), initialized.GetWritePC())
	}
	for lane := uint8(0); lane < shadowSlotLanes; lane++ {
		state := slot.State(lane)
		if state == nil || state.atomicState.Load() != fast {
			t.Fatalf("lane %d binding = state %p capability %p, want shared %p", lane, state, state.atomicState.Load(), fast)
		}
	}
}

func TestAtomicFastPathRejectsMultipleOrdinaryHistories(t *testing.T) {
	for _, test := range []struct {
		name             string
		differentOverlay bool
	}{
		{name: "two nonempty groups"},
		{name: "different overlays", differentOverlay: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			slot := new(ShadowSlot)
			first := slot.Isolate(0)
			first.SetW(epoch.NewEpoch(51, 3))
			first.UnlockAccess()
			second := slot.Isolate(4)
			if !test.differentOverlay {
				second.SetReadEpoch(epoch.NewEpoch(52, 5))
			}
			second.UnlockAccess()

			shared := unsafe.Pointer(new(byte))
			var locked []*VarState
			slot.LockAtomicGroups(0xff, true, func(mask uint8, state *VarState) {
				overlay := shared
				if test.differentOverlay && mask&0xf0 != 0 {
					overlay = unsafe.Pointer(new(byte))
				}
				state.SetAtomicState(overlay)
				locked = append(locked, state)
			})
			for i := len(locked) - 1; i >= 0; i-- {
				locked[i].UnlockAccess()
			}
			if fast := slot.TryAtomicFast(0xff); fast != nil {
				fast.Release()
				t.Fatal("unsupported ordinary shape enrolled a fast capability")
			}
		})
	}
}

func TestSharedAtomicFastPathPartialClearDrainsEveryLane(t *testing.T) {
	slot := new(ShadowSlot)
	initialized := slot.Isolate(0)
	initialized.SetW(epoch.NewEpoch(61, 9))
	initialized.UnlockAccess()
	_, fast := lockInitializedAtomicGroupsForTest(t, slot, 0xff, unsafe.Pointer(new(byte)))
	if fast == nil {
		t.Fatal("initialized exact mask did not enroll")
	}

	done := make(chan struct{})
	go func() {
		slot.ClearMask(0x80)
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for fast.users.Load()&atomicFastEscaped == 0 {
		if time.Now().After(deadline) {
			fast.Release()
			t.Fatal("partial clear did not close the shared capability")
		}
		runtime.Gosched()
	}
	select {
	case <-done:
		fast.Release()
		t.Fatal("partial clear completed before retained capability release")
	default:
	}
	if got := slot.State(7); got == nil {
		fast.Release()
		t.Fatal("partial clear detached its lane before draining the shared capability")
	}

	fast.Release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("partial clear did not finish after capability release")
	}
	if got := slot.State(7); got != nil {
		t.Fatalf("cleared lane retained state %p", got)
	}
	if got := slot.State(0); got != initialized {
		t.Fatalf("partial clear changed initialized sibling: got %p, want %p", got, initialized)
	} else if got.GetW() == 0 {
		t.Fatal("partial clear erased initialized sibling history")
	}
	if probe := slot.TryAtomicFast(0xff); probe != nil {
		probe.Release()
		t.Fatal("partial clear left the shared capability reusable")
	}
}

func TestAtomicFastPathRetainFreezesNonFirstLaneMutation(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(slot *ShadowSlot, state *VarState)
	}{
		{
			name: "isolate",
			mutate: func(slot *ShadowSlot, _ *VarState) {
				isolated := slot.Isolate(7)
				isolated.UnlockAccess()
			},
		},
		{
			name: "reset",
			mutate: func(_ *ShadowSlot, state *VarState) {
				state.Reset()
			},
		},
		{
			name: "set atomic state",
			mutate: func(_ *ShadowSlot, state *VarState) {
				state.LockAccess()
				state.SetAtomicState(unsafe.Pointer(new(byte)))
				state.UnlockAccess()
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			slot := new(ShadowSlot)
			initialized := slot.Isolate(0)
			initialized.SetW(epoch.NewEpoch(62, 9))
			initialized.UnlockAccess()
			_, fast := lockInitializedAtomicGroupsForTest(t, slot, 0xff, unsafe.Pointer(new(byte)))
			if fast == nil {
				t.Fatal("initialized exact mask did not enroll")
			}
			nonFirst := slot.State(7)
			if nonFirst == nil || nonFirst == initialized {
				fast.Release()
				t.Fatalf("non-first lane state = %p, want distinct empty sibling", nonFirst)
			}

			done := make(chan struct{})
			go func() {
				test.mutate(slot, nonFirst)
				close(done)
			}()
			deadline := time.Now().Add(time.Second)
			for fast.users.Load()&atomicFastEscaped == 0 {
				if time.Now().After(deadline) {
					fast.Release()
					t.Fatal("non-first-lane mutation did not close retained capability")
				}
				runtime.Gosched()
			}
			select {
			case <-done:
				fast.Release()
				t.Fatal("non-first-lane mutation completed before retained capability release")
			default:
			}

			fast.Release()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("non-first-lane mutation did not finish after capability release")
			}
			if probe := slot.TryAtomicFast(0xff); probe != nil {
				probe.Release()
				t.Fatal("old capability retained after non-first-lane mutation")
			}
		})
	}
}

func TestAtomicFastPathMutationCancelsNonFirstLaneProbation(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(state *VarState, overlay unsafe.Pointer)
	}{
		{
			name: "reset",
			mutate: func(state *VarState, _ unsafe.Pointer) {
				state.Reset()
			},
		},
		{
			name: "set atomic state",
			mutate: func(state *VarState, overlay unsafe.Pointer) {
				state.LockAccess()
				state.SetAtomicState(overlay)
				state.UnlockAccess()
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			slot := new(ShadowSlot)
			initialized := slot.Isolate(0)
			initialized.SetW(epoch.NewEpoch(63, 11))
			initialized.UnlockAccess()
			overlay := unsafe.Pointer(new(byte))
			_, old := lockInitializedAtomicGroupsForTest(t, slot, 0xff, overlay)
			if old == nil {
				t.Fatal("initialized exact mask did not enroll")
			}
			old.Release()

			// Close the live generation, then perform exactly one compatible
			// setup so the old descriptor carries an armed full-mask signature.
			slot.AccessGroups(0xff, func(_ uint8, _ *VarState) {})
			var locked [shadowSlotLanes]*VarState
			lockedN := 0
			slot.LockAtomicGroups(0xff, true, func(_ uint8, state *VarState) {
				locked[lockedN] = state
				lockedN++
			})
			for i := lockedN - 1; i >= 0; i-- {
				locked[i].UnlockAccess()
			}
			if got := old.rearm.Load(); got != 0xff {
				t.Fatalf("armed probation signature = %#x, want 0xff", got)
			}
			if probe := slot.TryAtomicFast(0xff); probe != nil {
				probe.Release()
				t.Fatal("first compatible setup published instead of arming probation")
			}

			nonFirst := slot.State(7)
			if nonFirst == nil || nonFirst == initialized {
				t.Fatalf("non-first lane state = %p, want distinct empty sibling", nonFirst)
			}
			test.mutate(nonFirst, overlay)
			if got := old.rearm.Load(); got != 0 {
				t.Fatalf("non-first-lane mutation retained probation signature %#x", got)
			}

			setup := func() *AtomicFastPath {
				lockedN = 0
				slot.LockAtomicGroups(0xff, true, func(_ uint8, state *VarState) {
					if state.GetAtomicState() == nil {
						state.SetAtomicState(overlay)
					}
					locked[lockedN] = state
					lockedN++
				})
				for i := lockedN - 1; i >= 0; i-- {
					locked[i].UnlockAccess()
				}
				return slot.TryAtomicFast(0xff)
			}
			if first := setup(); first != nil {
				first.Release()
				t.Fatal("first compatible setup after mutation bypassed probation")
			}
			fresh := setup()
			if fresh == nil {
				t.Fatal("second compatible setup after mutation did not rearm")
			}
			defer fresh.Release()
			if fresh == old {
				t.Fatal("post-mutation setup reopened escaped descriptor")
			}
		})
	}
}

func TestVarStateResetDrainsFrozenOrdinaryHistory(t *testing.T) {
	slot := new(ShadowSlot)
	initialized := slot.Isolate(0)
	wantW := epoch.NewEpoch(71, 11)
	initialized.SetW(wantW)
	initialized.SetWritePC(0x7b01)
	initialized.UnlockAccess()
	_, fast := lockInitializedAtomicGroupsForTest(t, slot, 0xff, unsafe.Pointer(new(byte)))
	if fast == nil {
		t.Fatal("initialized exact mask did not enroll")
	}
	oldLifecycle := initialized.GetLifecycleID()

	done := make(chan struct{})
	go func() {
		initialized.Reset()
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for fast.users.Load()&atomicFastEscaped == 0 {
		if time.Now().After(deadline) {
			fast.Release()
			t.Fatal("Reset did not close the retained capability")
		}
		runtime.Gosched()
	}
	select {
	case <-done:
		fast.Release()
		t.Fatal("Reset completed before retained capability release")
	default:
	}
	if got := initialized.GetW(); got != wantW {
		fast.Release()
		t.Fatalf("Reset changed frozen W while retained: got %v, want %v", got, wantW)
	}
	if got := initialized.GetWritePC(); got != 0x7b01 {
		fast.Release()
		t.Fatalf("Reset changed frozen PC while retained: got %#x", got)
	}
	if got := initialized.GetLifecycleID(); got != oldLifecycle {
		fast.Release()
		t.Fatalf("Reset changed lifecycle while retained: got %d, want %d", got, oldLifecycle)
	}

	fast.Release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Reset did not finish after capability release")
	}
	if initialized.GetW() != 0 || initialized.GetWritePC() != 0 || initialized.GetAtomicState() != nil {
		t.Fatalf("reset state retained history: W=%v PC=%#x atomic=%p",
			initialized.GetW(), initialized.GetWritePC(), initialized.GetAtomicState())
	}
	if got := initialized.GetLifecycleID(); got == oldLifecycle {
		t.Fatalf("Reset reused lifecycle %d", got)
	}
	if probe := slot.TryAtomicFast(0xff); probe != nil {
		probe.Release()
		t.Fatal("old capability revalidated after Reset")
	}
}
