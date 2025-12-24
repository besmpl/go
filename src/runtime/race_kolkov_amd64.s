// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

#include "go_asm.h"
#include "go_tls.h"
#include "funcdata.h"
#include "textflag.h"

// Pure-Go race detector assembly stubs for AMD64.
//
// These functions provide the low-level interface between compiler-generated
// instrumentation and the pure-Go race detector. They handle:
//   - Efficient caller PC capture
//   - ABIInternal calling convention compliance
//   - Fast path for hot functions (raceread/racewrite)

// func runtime·raceread(addr uintptr)
// Called from instrumented code with ABIInternal convention.
// AX = addr, return address on stack at (SP)
TEXT	runtime·raceread<ABIInternal>(SB), NOSPLIT, $0-8
	// AX already contains addr (ABIInternal passes first arg in AX)
	// Capture caller PC from stack
	MOVQ	(SP), CX	// CX = caller PC
	// Call kolkovOnRead(addr, pc)
	MOVQ	AX, DI		// first arg: addr
	MOVQ	CX, SI		// second arg: pc
	JMP	runtime·kolkovOnReadAsm(SB)

// func runtime·racewrite(addr uintptr)
// Called from instrumented code with ABIInternal convention.
// AX = addr, return address on stack at (SP)
TEXT	runtime·racewrite<ABIInternal>(SB), NOSPLIT, $0-8
	// AX already contains addr
	// Capture caller PC from stack
	MOVQ	(SP), CX	// CX = caller PC
	// Call kolkovOnWrite(addr, pc)
	MOVQ	AX, DI		// first arg: addr
	MOVQ	CX, SI		// second arg: pc
	JMP	runtime·kolkovOnWriteAsm(SB)

// func runtime·racereadpc(addr unsafe.Pointer, callpc, pc uintptr)
// Called with explicit PCs.
TEXT	runtime·racereadpc(SB), NOSPLIT, $0-24
	MOVQ	addr+0(FP), DI
	MOVQ	pc+16(FP), SI		// Use pc, not callpc
	JMP	runtime·kolkovOnReadAsm(SB)

// func runtime·racewritepc(addr unsafe.Pointer, callpc, pc uintptr)
// Called with explicit PCs.
TEXT	runtime·racewritepc(SB), NOSPLIT, $0-24
	MOVQ	addr+0(FP), DI
	MOVQ	pc+16(FP), SI		// Use pc, not callpc
	JMP	runtime·kolkovOnWriteAsm(SB)

// func runtime·racereadrange(addr, size uintptr)
// Called from instrumented code with ABIInternal convention.
// AX = addr, BX = size, return address on stack at (SP)
TEXT	runtime·racereadrange<ABIInternal>(SB), NOSPLIT, $0-16
	// AX = addr, BX = size (ABIInternal)
	MOVQ	(SP), CX	// CX = caller PC
	// For ranges, we call kolkovOnRead for each 8-byte chunk
	// Simple approach: just call once for the base address
	MOVQ	AX, DI		// first arg: addr
	MOVQ	CX, SI		// second arg: pc
	JMP	runtime·kolkovOnReadAsm(SB)

// func runtime·racewriterange(addr, size uintptr)
// Called from instrumented code with ABIInternal convention.
// AX = addr, BX = size, return address on stack at (SP)
TEXT	runtime·racewriterange<ABIInternal>(SB), NOSPLIT, $0-16
	// AX = addr, BX = size (ABIInternal)
	MOVQ	(SP), CX	// CX = caller PC
	// Simple approach: call once for base address
	MOVQ	AX, DI		// first arg: addr
	MOVQ	CX, SI		// second arg: pc
	JMP	runtime·kolkovOnWriteAsm(SB)

// func runtime·racereadrangepc1(addr, size, pc uintptr)
TEXT	runtime·racereadrangepc1(SB), NOSPLIT, $0-24
	MOVQ	addr+0(FP), DI
	MOVQ	pc+16(FP), SI
	JMP	runtime·kolkovOnReadAsm(SB)

// func runtime·racewriterangepc1(addr, size, pc uintptr)
TEXT	runtime·racewriterangepc1(SB), NOSPLIT, $0-24
	MOVQ	addr+0(FP), DI
	MOVQ	pc+16(FP), SI
	JMP	runtime·kolkovOnWriteAsm(SB)

// func runtime·racefuncenter(callpc uintptr)
// Called from instrumented code at function entry.
TEXT	runtime·racefuncenter(SB), NOSPLIT, $0-8
	// Function enter tracking is optional for MVP
	// Could be used for call stack building
	RET

// func runtime·racefuncenterfp(fp uintptr)
// Called from instrumented code at function entry using frame pointer.
TEXT	runtime·racefuncenterfp(SB), NOSPLIT, $0-8
	RET

// func runtime·racefuncexit()
// Called from instrumented code at function exit.
TEXT	runtime·racefuncexit(SB), NOSPLIT, $0-0
	RET

// kolkovOnReadAsm is the assembly entry point for read tracking.
// DI = addr, SI = pc (from raceread)
// Converts to ABIInternal convention and tail-calls kolkovOnReadGo.
// Must be NOFRAME to avoid stackmap issues during stack copy.
// CRITICAL: Use <ABIInternal> suffix to pass args in registers (AX, BX).
TEXT	runtime·kolkovOnReadAsm(SB), NOSPLIT|NOFRAME, $0-0
	// Convert System V ABI (DI, SI) to Go ABIInternal (AX, BX)
	MOVQ	DI, AX		// addr -> first arg
	MOVQ	SI, BX		// pc -> second arg
	JMP	runtime·kolkovOnReadGo<ABIInternal>(SB)

// kolkovOnWriteAsm is the assembly entry point for write tracking.
// DI = addr, SI = pc (from racewrite)
// Converts to ABIInternal convention and tail-calls kolkovOnWriteGo.
// Must be NOFRAME to avoid stackmap issues during stack copy.
// CRITICAL: Use <ABIInternal> suffix to pass args in registers (AX, BX).
TEXT	runtime·kolkovOnWriteAsm(SB), NOSPLIT|NOFRAME, $0-0
	// Convert System V ABI (DI, SI) to Go ABIInternal (AX, BX)
	MOVQ	DI, AX		// addr -> first arg
	MOVQ	SI, BX		// pc -> second arg
	JMP	runtime·kolkovOnWriteGo<ABIInternal>(SB)
