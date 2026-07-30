package api

import (
	"testing"
	"unsafe"
)

func TestAtomicBridgeDispatchesPlainAndRMWFastModes(t *testing.T) {
	Reset()
	defer Reset()
	Enable()
	const addr = uintptr(0x7f1000)

	var token [8]unsafe.Pointer
	ctx := raceAtomicBeginPlain(addr, 8, 0, false, &token)
	if ctx <= 1 {
		t.Fatalf("plain bridge returned invalid context %#x", ctx)
	}
	if token[0] == nil || token[1] != nil {
		t.Fatalf("first plain bridge token = [%p %p], want converted fast enrollment", token[0], token[1])
	}
	raceAtomicEnd(addr, 8, 0x6100, ctx, &token, true, true)

	ctx = raceAtomicBeginPlain(addr, 8, ctx, true, &token)
	if token[0] == nil || token[1] != nil {
		t.Fatalf("second plain bridge token = [%p %p], want fast capability", token[0], token[1])
	}
	escaped := token[0]
	raceAtomicEnd(addr, 8, 0x6101, ctx, &token, false, true)

	ctx = raceAtomicBeginRMW(addr, 8, ctx, true, &token)
	if token[0] != escaped || token[1] != nil {
		t.Fatalf("failed-CAS bridge token = [%p %p], want existing capability %p", token[0], token[1], escaped)
	}
	raceAtomicEnd(addr, 8, 0x6102, ctx, &token, false, true)

	ctx = raceAtomicBeginRMW(addr, 8, ctx, true, &token)
	if token[0] != escaped || token[1] != nil {
		t.Fatalf("successful RMW bridge token = [%p %p], want existing capability %p", token[0], token[1], escaped)
	}
	raceAtomicEnd(addr, 8, 0x6103, ctx, &token, true, true)

	// The general bridge is retained for ignored operations and RMW reuse misses.
	// Even with the same compatible width it must close the capability.
	ctx = raceAtomicBegin(addr, 8, ctx, true, &token)
	if token[0] == nil || token[1] == nil {
		t.Fatalf("general bridge token = [%p %p], want fully locked transaction", token[0], token[1])
	}
	raceAtomicEnd(addr, 8, 0x6104, ctx, &token, false, true)

	ctx = raceAtomicBeginPlain(addr, 8, ctx, true, &token)
	if token[0] == nil || token[1] == nil {
		t.Fatalf("first plain bridge token after general access = [%p %p], want probationary slow transaction", token[0], token[1])
	}
	raceAtomicEnd(addr, 8, 0x6105, ctx, &token, false, true)

	ctx = raceAtomicBeginPlain(addr, 8, ctx, true, &token)
	if token[0] == nil || token[1] != nil {
		t.Fatalf("second plain bridge token after general access = [%p %p], want fresh fast capability", token[0], token[1])
	}
	if token[0] == escaped {
		t.Fatalf("plain bridge reopened escaped capability %p", escaped)
	}
	raceAtomicEnd(addr, 8, 0x6106, ctx, &token, false, true)
}

func TestAtomicRMWBridgeMissNeverEnrolls(t *testing.T) {
	Reset()
	defer Reset()
	Enable()
	const addr = uintptr(0x7f2000)

	var token [8]unsafe.Pointer
	var ctx uintptr
	for operation := 0; operation < 3; operation++ {
		ctx = raceAtomicBeginRMW(addr, 8, ctx, true, &token)
		if ctx <= 1 {
			t.Fatalf("RMW bridge operation %d returned invalid context %#x", operation, ctx)
		}
		if token[0] == nil || token[1] == nil {
			t.Fatalf("RMW bridge miss operation %d token = [%p %p], want fully locked transaction", operation, token[0], token[1])
		}
		raceAtomicEnd(addr, 8, 0x6200+uintptr(operation), ctx, &token, true, true)
	}
}

func TestAtomicRMWCooperativeBridgeContentionReturnsCleanRetry(t *testing.T) {
	Reset()
	defer Reset()
	Enable()
	const addr = uintptr(0x7f3000)

	var token [8]unsafe.Pointer
	ctx := raceAtomicBeginPlain(addr, 8, 0, false, &token)
	raceAtomicEnd(addr, 8, 0x6300, ctx, &token, true, true)

	ctx = raceAtomicBeginRMW(addr, 8, ctx, true, &token)
	if token[0] == nil || token[1] != nil {
		t.Fatalf("blocking owner token = [%p %p], want exact capability", token[0], token[1])
	}
	var contender [8]unsafe.Pointer
	contender[0] = unsafe.Pointer(new(byte))
	gotContext, retry := raceAtomicBeginRMWCooperative(addr, 8, ctx, true, &contender)
	if gotContext != ctx {
		t.Fatalf("contention context = %#x, want %#x", gotContext, ctx)
	}
	if !retry {
		t.Fatal("cooperative bridge contention did not request a retry")
	}
	for i, retained := range contender {
		if retained != nil {
			t.Fatalf("contention retained token[%d] = %p", i, retained)
		}
	}
	raceAtomicEnd(addr, 8, 0x6301, ctx, &token, true, true)

	gotContext, retry = raceAtomicBeginRMWCooperative(addr, 8, ctx, true, &contender)
	if gotContext != ctx || retry {
		t.Fatalf("uncontended cooperative begin = (%#x, %v), want (%#x, false)", gotContext, retry, ctx)
	}
	if contender[0] == nil || contender[1] != nil {
		t.Fatalf("uncontended cooperative token = [%p %p], want exact capability", contender[0], contender[1])
	}
	raceAtomicEnd(addr, 8, 0x6302, ctx, &contender, true, true)

	var miss [8]unsafe.Pointer
	gotContext, retry = raceAtomicBeginRMWCooperative(addr+8, 8, ctx, true, &miss)
	if gotContext != ctx || retry {
		t.Fatalf("general cooperative miss = (%#x, %v), want (%#x, false)", gotContext, retry, ctx)
	}
	if miss[0] == nil || miss[1] == nil {
		t.Fatalf("general cooperative miss token = [%p %p], want blocking transaction", miss[0], miss[1])
	}
	raceAtomicEnd(addr+8, 8, 0x6303, ctx, &miss, true, true)
}
