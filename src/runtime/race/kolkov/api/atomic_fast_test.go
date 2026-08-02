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

	// The general bridge is retained for ignored and otherwise general operations.
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

func TestAtomicRMWBridgeMissEnrollsAndReuses(t *testing.T) {
	Reset()
	defer Reset()
	Enable()
	const addr = uintptr(0x7f2000)

	var token [8]unsafe.Pointer
	ctx := raceAtomicBeginRMW(addr, 8, 0, true, &token)
	if ctx <= 1 || token[0] == nil || token[1] != nil {
		t.Fatalf("first RMW bridge begin = (%#x, [%p %p]), want valid context and converted enrollment", ctx, token[0], token[1])
	}
	capability := token[0]
	raceAtomicEnd(addr, 8, 0x6200, ctx, &token, true, true)

	ctx = raceAtomicBeginRMW(addr, 8, ctx, true, &token)
	if token[0] != capability || token[1] != nil {
		t.Fatalf("second RMW bridge token = [%p %p], want enrolled capability %p", token[0], token[1], capability)
	}
	raceAtomicEnd(addr, 8, 0x6201, ctx, &token, true, true)
}

func TestAtomicRMWCooperativeBridgeContentionReturnsCleanRetry(t *testing.T) {
	Reset()
	defer Reset()
	Enable()
	const addr = uintptr(0x7f3000)

	var token [8]unsafe.Pointer
	ctx := raceAtomicBeginRMW(addr, 8, 0, true, &token)
	raceAtomicEnd(addr, 8, 0x6300, ctx, &token, true, true)

	ctx = raceAtomicBeginRMW(addr, 8, ctx, true, &token)
	if token[0] == nil || token[1] != nil {
		t.Fatalf("blocking owner token = [%p %p], want exact capability", token[0], token[1])
	}
	var contender [8]unsafe.Pointer
	contender[0] = unsafe.Pointer(new(byte))
	gotContext, retry, _, _, _ := raceAtomicBeginRMWCooperative(addr, 8, ctx, true, &contender)
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

	gotContext, retry, _, _, _ = raceAtomicBeginRMWCooperative(addr, 8, ctx, true, &contender)
	if gotContext != ctx || retry {
		t.Fatalf("uncontended cooperative begin = (%#x, %v), want (%#x, false)", gotContext, retry, ctx)
	}
	if contender[0] == nil || contender[1] != nil {
		t.Fatalf("uncontended cooperative token = [%p %p], want exact capability", contender[0], contender[1])
	}
	raceAtomicEnd(addr, 8, 0x6302, ctx, &contender, true, true)

	var miss [8]unsafe.Pointer
	gotContext, retry, _, _, _ = raceAtomicBeginRMWCooperative(addr+8, 8, ctx, true, &miss)
	if gotContext != ctx || retry {
		t.Fatalf("cooperative RMW miss = (%#x, %v), want (%#x, false)", gotContext, retry, ctx)
	}
	if miss[0] == nil || miss[1] != nil {
		t.Fatalf("cooperative RMW miss token = [%p %p], want converted enrollment", miss[0], miss[1])
	}
	raceAtomicEnd(addr+8, 8, 0x6303, ctx, &miss, true, true)
}

func TestAtomicLoadStoreCooperativeBridgesReturnCleanRetry(t *testing.T) {
	Reset()
	defer Reset()
	Enable()
	const addr = uintptr(0x7f4000)

	var token [8]unsafe.Pointer
	ctx := raceAtomicBeginPlain(addr, 8, 0, false, &token)
	raceAtomicEnd(addr, 8, 0x6400, ctx, &token, true, true)
	ctx = raceAtomicBeginRMW(addr, 8, ctx, true, &token)
	if token[0] == nil || token[1] != nil {
		t.Fatalf("blocking owner token = [%p %p], want exact capability", token[0], token[1])
	}

	tests := []struct {
		name  string
		begin func(*[8]unsafe.Pointer) (uintptr, bool)
		end   func(uintptr, *[8]unsafe.Pointer)
	}{
		{
			name: "load",
			begin: func(candidate *[8]unsafe.Pointer) (uintptr, bool) {
				return raceAtomicBeginLoadCooperative(addr, 8, ctx, candidate)
			},
			end: func(context uintptr, candidate *[8]unsafe.Pointer) {
				raceAtomicLoadEnd(addr, 8, 0x6402, context, candidate)
			},
		},
		{
			name: "store",
			begin: func(candidate *[8]unsafe.Pointer) (uintptr, bool) {
				return raceAtomicBeginStoreCooperative(addr, 8, ctx, candidate)
			},
			end: func(context uintptr, candidate *[8]unsafe.Pointer) {
				raceAtomicEnd(addr, 8, 0x6403, context, candidate, true, true)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name+" contention", func(t *testing.T) {
			var candidate [8]unsafe.Pointer
			candidate[0] = unsafe.Pointer(new(byte))
			gotContext, retry := test.begin(&candidate)
			if gotContext != ctx || !retry {
				t.Fatalf("cooperative contention = (%#x, %v), want (%#x, true)", gotContext, retry, ctx)
			}
			for lane, retained := range candidate {
				if retained != nil {
					t.Fatalf("contention retained token[%d] = %p", lane, retained)
				}
			}
		})
	}
	raceAtomicEnd(addr, 8, 0x6401, ctx, &token, true, true)

	for _, test := range tests {
		t.Run(test.name+" success", func(t *testing.T) {
			var candidate [8]unsafe.Pointer
			gotContext, retry := test.begin(&candidate)
			if gotContext != ctx || retry {
				t.Fatalf("uncontended cooperative begin = (%#x, %v), want (%#x, false)", gotContext, retry, ctx)
			}
			if candidate[0] == nil || candidate[1] != nil {
				t.Fatalf("uncontended token = [%p %p], want exact capability", candidate[0], candidate[1])
			}
			test.end(gotContext, &candidate)
		})
	}

	var miss [8]unsafe.Pointer
	gotContext, retry := raceAtomicBeginLoadCooperative(addr+8, 8, ctx, &miss)
	if gotContext != ctx || retry || miss[0] == nil {
		t.Fatalf("load enrollment miss = (%#x, %v, %p), want blocking token", gotContext, retry, miss[0])
	}
	raceAtomicLoadEnd(addr+8, 8, 0x6404, gotContext, &miss)
	gotContext, retry = raceAtomicBeginStoreCooperative(addr+16, 8, ctx, &miss)
	if gotContext != ctx || retry || miss[0] == nil {
		t.Fatalf("store enrollment miss = (%#x, %v, %p), want blocking token", gotContext, retry, miss[0])
	}
	raceAtomicEnd(addr+16, 8, 0x6405, gotContext, &miss, true, true)
}
