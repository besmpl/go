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
