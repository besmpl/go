// Copyright 2025 The racedetector Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build go1.25 && !go1.26 && (amd64 || arm64)

// Go 1.25 specific goid extraction.
//
// In Go 1.25, the gobuf struct is 48 bytes (6 pointers) - 'ret' field was REMOVED.
// This places goid at offset 152 (8 bytes earlier than Go 1.23/1.24).
//
// g struct layout (Go 1.25):
//
//	Field          Size    Offset
//	-----          ----    ------
//	stack          16      0
//	stackguard0    8       16
//	stackguard1    8       24
//	_panic         8       32
//	_defer         8       40
//	m              8       48
//	sched (gobuf)  48      56   (6 pointers: sp, pc, g, ctxt, lr, bp - NO 'ret'!)
//	syscallsp      8       104
//	syscallpc      8       112
//	syscallbp      8       120
//	stktopsp       8       128
//	param          8       136
//	atomicstatus   4       144
//	stackLock      4       148
//	goid           8       152  <- TARGET

package api

import "unsafe"

// goidOffset for Go 1.25 is 152 bytes.
const goidOffset = 152

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
