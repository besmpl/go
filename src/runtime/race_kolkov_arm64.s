// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

#include "go_asm.h"
#include "funcdata.h"
#include "textflag.h"

// Pure-Go race detector assembly stubs for ARM64.
//
// ARM64 ABI: arguments in R0-R7, return in R0/R1
// Link register: R30 (LR), stack pointer: RSP
// ABIInternal: first arg in R0

// func runtime·raceread(addr uintptr)
// R0 = addr (ABIInternal)
TEXT	runtime·raceread<ABIInternal>(SB), NOSPLIT, $0-8
	// R0 = addr
	// R30 (LR) = return address = caller PC
	MOVD	R30, R1		// R1 = caller PC
	// Call kolkovOnRead(addr, pc)
	B	runtime·kolkovOnReadAsm(SB)

// func runtime·racewrite(addr uintptr)
// R0 = addr (ABIInternal)
TEXT	runtime·racewrite<ABIInternal>(SB), NOSPLIT, $0-8
	MOVD	R30, R1		// R1 = caller PC
	B	runtime·kolkovOnWriteAsm(SB)

// func runtime·racereadpc(addr unsafe.Pointer, callpc, pc uintptr)
TEXT	runtime·racereadpc(SB), NOSPLIT, $0-24
	MOVD	addr+0(FP), R0
	MOVD	pc+16(FP), R1
	B	runtime·kolkovOnReadAsm(SB)

// func runtime·racewritepc(addr unsafe.Pointer, callpc, pc uintptr)
TEXT	runtime·racewritepc(SB), NOSPLIT, $0-24
	MOVD	addr+0(FP), R0
	MOVD	pc+16(FP), R1
	B	runtime·kolkovOnWriteAsm(SB)

// func runtime·racereadrange(addr, size uintptr)
// R0 = addr, R1 = size (ABIInternal)
TEXT	runtime·racereadrange<ABIInternal>(SB), NOSPLIT, $0-16
	// R0 = addr, use LR for PC
	MOVD	R30, R1
	B	runtime·kolkovOnReadAsm(SB)

// func runtime·racewriterange(addr, size uintptr)
// R0 = addr, R1 = size (ABIInternal)
TEXT	runtime·racewriterange<ABIInternal>(SB), NOSPLIT, $0-16
	MOVD	R30, R1
	B	runtime·kolkovOnWriteAsm(SB)

// func runtime·racereadrangepc1(addr, size, pc uintptr)
TEXT	runtime·racereadrangepc1(SB), NOSPLIT, $0-24
	MOVD	addr+0(FP), R0
	MOVD	pc+16(FP), R1
	B	runtime·kolkovOnReadAsm(SB)

// func runtime·racewriterangepc1(addr, size, pc uintptr)
TEXT	runtime·racewriterangepc1(SB), NOSPLIT, $0-24
	MOVD	addr+0(FP), R0
	MOVD	pc+16(FP), R1
	B	runtime·kolkovOnWriteAsm(SB)

// func runtime·racefuncenter(callpc uintptr)
TEXT	runtime·racefuncenter(SB), NOSPLIT, $0-8
	RET

// func runtime·racefuncenterfp(fp uintptr)
TEXT	runtime·racefuncenterfp(SB), NOSPLIT, $0-8
	RET

// func runtime·racefuncexit()
TEXT	runtime·racefuncexit(SB), NOSPLIT, $0-0
	RET

// kolkovOnReadAsm - R0 = addr, R1 = pc
TEXT	runtime·kolkovOnReadAsm(SB), NOSPLIT, $32-0
	MOVD	R0, 8(RSP)	// addr
	MOVD	R1, 16(RSP)	// pc
	BL	runtime·kolkovOnReadGo(SB)
	RET

// kolkovOnWriteAsm - R0 = addr, R1 = pc
TEXT	runtime·kolkovOnWriteAsm(SB), NOSPLIT, $32-0
	MOVD	R0, 8(RSP)	// addr
	MOVD	R1, 16(RSP)	// pc
	BL	runtime·kolkovOnWriteGo(SB)
	RET
