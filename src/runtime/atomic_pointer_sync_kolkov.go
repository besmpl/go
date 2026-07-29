// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

package runtime

import (
	"internal/goexperiment"
	"internal/runtime/sys"
	"unsafe"
)

// These pointer entry points retain the write-barrier and cgo-pointer checks
// of the standard implementations, but pass their true public caller PC to the
// pure-Go detector instead of re-entering through a sync/atomic uintptr symbol.

//go:linkname sync_atomic_StorePointer sync/atomic.StorePointer
//go:nosplit
//go:nocheckptr
func sync_atomic_StorePointer(ptr *unsafe.Pointer, new unsafe.Pointer) {
	pc := sys.GetCallerPC()
	fp := getfp()
	if goexperiment.CgoCheck2 {
		cgoCheckPtrWrite(ptr, new)
	}
	kolkovAtomicStorePointer(ptr, new, kolkovAtomicUserPC(pc, fp))
	KeepAlive(ptr)
	KeepAlive(new)
}

//go:linkname sync_atomic_SwapPointer sync/atomic.SwapPointer
//go:nosplit
//go:nocheckptr
func sync_atomic_SwapPointer(ptr *unsafe.Pointer, new unsafe.Pointer) unsafe.Pointer {
	pc := sys.GetCallerPC()
	fp := getfp()
	if goexperiment.CgoCheck2 {
		cgoCheckPtrWrite(ptr, new)
	}
	old := kolkovAtomicSwapPointer(ptr, new, kolkovAtomicUserPC(pc, fp))
	KeepAlive(ptr)
	KeepAlive(new)
	KeepAlive(old)
	return old
}

//go:linkname sync_atomic_CompareAndSwapPointer sync/atomic.CompareAndSwapPointer
//go:nosplit
//go:nocheckptr
func sync_atomic_CompareAndSwapPointer(ptr *unsafe.Pointer, old, new unsafe.Pointer) bool {
	pc := sys.GetCallerPC()
	fp := getfp()
	if goexperiment.CgoCheck2 {
		cgoCheckPtrWrite(ptr, new)
	}
	swapped := kolkovAtomicCASPointer(ptr, old, new, kolkovAtomicUserPC(pc, fp))
	KeepAlive(ptr)
	KeepAlive(old)
	KeepAlive(new)
	return swapped
}
