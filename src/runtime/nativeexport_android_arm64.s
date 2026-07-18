// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#include "textflag.h"
#include "cgo/abi_arm64.h"

// nativeExportCall transitions from the Android native ABI to cgocallback.
// R0 is an ABIInternal func(frame unsafe.Pointer) entry PC and R1 points to
// the caller-owned argument/result frame.
TEXT runtime·nativeExportCall(SB),NOSPLIT|NOFRAME,$0
	MOVD	$runtime·nativeExportReady(SB), R2
wait_runtime:
	LDARW	(R2), R3
	CBNZ	R3, runtime_ready
	WORD	$0xd503203f // YIELD
	B	wait_runtime

runtime_ready:
	// Match runtime/cgo's arm64 crosscall2 transition, but pass no cgo
	// context and depend only on runtime-owned callback machinery.
	SUB	$(8*24), RSP
	STP	(R0, R1), (8*1)(RSP)
	MOVD	ZR, (8*3)(RSP)

	SAVE_R19_TO_R28(8*4)
	SAVE_F8_TO_F15(8*14)
	STP	(R29, R30), (8*22)(RSP)

	BL	runtime·load_g(SB)
	BL	runtime·cgocallback(SB)

	RESTORE_R19_TO_R28(8*4)
	RESTORE_F8_TO_F15(8*14)
	LDP	(8*22)(RSP), (R29, R30)

	ADD	$(8*24), RSP
	RET
