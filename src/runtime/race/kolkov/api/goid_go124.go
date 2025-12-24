// Copyright 2025 The racedetector Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build go1.24 && !go1.25 && (amd64 || arm64)

// Go 1.24 specific goid extraction.
//
// In Go 1.24, the gobuf struct is 56 bytes (7 pointers including 'ret'),
// placing goid at offset 160 - same as Go 1.23.
//
// g struct layout (Go 1.24):
//
//	Field          Size    Offset
//	-----          ----    ------
//	stack          16      0
//	stackguard0    8       16
//	stackguard1    8       24
//	_panic         8       32
//	_defer         8       40
//	m              8       48
//	sched (gobuf)  56      56   (7 pointers: sp, pc, g, ctxt, ret, lr, bp)
//	syscallsp      8       112
//	syscallpc      8       120
//	syscallbp      8       128
//	stktopsp       8       136
//	param          8       144
//	atomicstatus   4       152
//	stackLock      4       156
//	goid           8       160  <- TARGET

package api

import "unsafe"

// goidOffset for Go 1.24 is 160 bytes (same as Go 1.23, gobuf still has 'ret' field).
const goidOffset = 160

// getg returns the current goroutine's g struct pointer.
// Implemented in assembly (goid_amd64.s or goid_arm64.s).
//
//go:noescape
func getg() uintptr

// getGoroutineIDFast extracts the goroutine ID using assembly fast path.
//
//go:nosplit
//go:nocheckptr
func getGoroutineIDFast() int64 {
	gptr := getg()
	if gptr == 0 {
		return getGoroutineIDSlow()
	}

	//nolint:gosec // G103: Intentional unsafe pointer arithmetic for runtime access
	goid := *(*uint64)(unsafe.Pointer(gptr + goidOffset))

	//nolint:gosec // G115: Safe conversion - goid values never exceed int64 max
	return int64(goid)
}
