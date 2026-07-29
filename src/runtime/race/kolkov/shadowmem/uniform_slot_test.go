//go:build amd64 || arm64

package shadowmem

import (
	"testing"
	"time"
	"unsafe"
)

func TestLockReadHintSlotRangeRequiresExactWithinWordMask(t *testing.T) {
	pt := NewPageTableShadow()
	const addr = uintptr(0x710002)
	pt.AccessRange(addr, 4, func(_ uintptr, _ uint8, _ *VarState) {})

	state, ok := pt.LockReadHintSlotRange(addr, 4)
	if !ok || state == nil {
		t.Fatal("exact four-byte range did not lock")
	}
	state.UnlockAccess()

	if state, ok := pt.LockReadHintSlotRange(addr, 2); ok {
		state.UnlockAccess()
		t.Fatal("partial reference mask was accepted")
	}
	pt.AccessRange(addr+1, 1, func(_ uintptr, _ uint8, state *VarState) {
		state.IncrementWriteCount()
	})
	if state, ok := pt.LockReadHintSlotRange(addr, 4); ok {
		state.UnlockAccess()
		t.Fatal("non-uniform lane mapping was accepted")
	}
	if state, ok := pt.LockReadHintSlotRange(addr+5, 4); ok {
		state.UnlockAccess()
		t.Fatal("word-straddling range was accepted")
	}
}

func TestReadHintCandidateWaitDoesNotRetainSlotLock(t *testing.T) {
	pt := NewPageTableShadow()
	const addr = uintptr(0x720000)
	pt.AccessRange(addr, 4, func(_ uintptr, _ uint8, _ *VarState) {})

	slot, state, mask, ok := pt.readHintSlotCandidate(addr, 4)
	if !ok {
		t.Fatal("exact candidate unavailable")
	}
	state.LockAccess()

	type result struct {
		locked bool
		retry  bool
	}
	started := make(chan struct{})
	done := make(chan result, 1)
	go func() {
		close(started)
		locked, retry := pt.lockReadHintSlotCandidate(addr, slot, state, mask)
		done <- result{locked: locked, retry: retry}
	}()
	<-started

	// A disjoint lane must remain isolatable while the hinted candidate waits
	// on accessMu. Holding slot.mu across that wait would deadlock this call.
	isolateDone := make(chan *VarState, 1)
	go func() {
		isolateDone <- slot.Isolate(6)
	}()
	select {
	case disjoint := <-isolateDone:
		disjoint.UnlockAccess()
	case <-time.After(2 * time.Second):
		// Release both transactions before failing so a lock-order regression
		// cannot strand the package test process.
		state.UnlockAccess()
		got := <-done
		if got.locked {
			state.UnlockAccess()
		}
		disjoint := <-isolateDone
		disjoint.UnlockAccess()
		t.Fatal("disjoint lane isolation waited for the hinted candidate")
	}
	state.UnlockAccess()

	got := <-done
	if !got.locked || got.retry {
		t.Fatalf("candidate result = %+v, want locked", got)
	}
	state.UnlockAccess()
}

func TestReadHintCandidateRejectsRedirects(t *testing.T) {
	for _, test := range []struct {
		name     string
		redirect func(*PageTableShadow, uintptr)
	}{
		{
			name: "clear",
			redirect: func(pt *PageTableShadow, addr uintptr) {
				pt.ClearRange(addr, 4)
			},
		},
		{
			name: "copy-on-write",
			redirect: func(pt *PageTableShadow, addr uintptr) {
				pt.AccessRange(addr, 2, func(_ uintptr, _ uint8, state *VarState) {
					state.IncrementWriteCount()
				})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			pt := NewPageTableShadow()
			const addr = uintptr(0x730000)
			pt.AccessRange(addr, 4, func(_ uintptr, _ uint8, _ *VarState) {})
			slot, state, mask, ok := pt.readHintSlotCandidate(addr, 4)
			if !ok {
				t.Fatal("exact candidate unavailable")
			}

			test.redirect(pt, addr)
			if locked, retry := pt.lockReadHintSlotCandidate(addr, slot, state, mask); locked || !retry {
				if locked {
					state.UnlockAccess()
				}
				t.Fatalf("stale candidate = (locked %t, retry %t), want (false, true)", locked, retry)
			}
		})
	}
}

func TestLockReadHintSlotRangeRejectsAtomicOverlay(t *testing.T) {
	pt := NewPageTableShadow()
	const addr = uintptr(0x740001)
	pt.AccessRange(addr, 4, func(_ uintptr, _ uint8, _ *VarState) {})
	state := pt.Get(addr)
	state.LockAccess()
	overlay := new(byte)
	state.SetAtomicState(unsafe.Pointer(overlay))
	state.UnlockAccess()

	if state, ok := pt.LockReadHintSlotRange(addr, 4); ok {
		state.UnlockAccess()
		t.Fatal("state with atomic overlay was accepted")
	}
}
