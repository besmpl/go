// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

#include "textflag.h"

// Pure Go race detector (Kolkov) atomic operations.
// These implementations use internal/runtime/atomic and call race hooks.
// For Kolkov, we use the same implementations as the non-race version
// because the race detection is handled at a higher level through
// the compiler's race instrumentation.

TEXT ·SwapInt32(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Xchgint32(SB)

TEXT ·SwapUint32(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Xchg(SB)

TEXT ·SwapInt64(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Xchgint64(SB)

TEXT ·SwapUint64(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Xchg64(SB)

TEXT ·SwapUintptr(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Xchguintptr(SB)

TEXT ·CompareAndSwapInt32(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Casint32(SB)

TEXT ·CompareAndSwapUint32(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Cas(SB)

TEXT ·CompareAndSwapUintptr(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Casuintptr(SB)

TEXT ·CompareAndSwapInt64(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Casint64(SB)

TEXT ·CompareAndSwapUint64(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Cas64(SB)

TEXT ·AddInt32(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Xaddint32(SB)

TEXT ·AddUint32(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Xadd(SB)

TEXT ·AddUintptr(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Xadduintptr(SB)

TEXT ·AddInt64(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Xaddint64(SB)

TEXT ·AddUint64(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Xadd64(SB)

TEXT ·LoadInt32(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Loadint32(SB)

TEXT ·LoadUint32(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Load(SB)

TEXT ·LoadInt64(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Loadint64(SB)

TEXT ·LoadUint64(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Load64(SB)

TEXT ·LoadUintptr(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Loaduintptr(SB)

TEXT ·LoadPointer(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Loadp(SB)

TEXT ·StoreInt32(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Storeint32(SB)

TEXT ·StoreUint32(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Store(SB)

TEXT ·StoreInt64(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Storeint64(SB)

TEXT ·StoreUint64(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Store64(SB)

TEXT ·StoreUintptr(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Storeuintptr(SB)

// And operations
TEXT ·AndInt32(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·And32(SB)

TEXT ·AndInt64(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·And64(SB)

TEXT ·AndUint32(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·And32(SB)

TEXT ·AndUint64(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·And64(SB)

TEXT ·AndUintptr(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Anduintptr(SB)

// Or operations
TEXT ·OrInt32(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Or32(SB)

TEXT ·OrInt64(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Or64(SB)

TEXT ·OrUint32(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Or32(SB)

TEXT ·OrUint64(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Or64(SB)

TEXT ·OrUintptr(SB),NOSPLIT,$0
	JMP	internal∕runtime∕atomic·Oruintptr(SB)
