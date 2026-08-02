// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race

package race

import (
	"internal/abi"
	"unsafe"
)

const Enabled = true

// Functions below pushed from runtime. Synchronization hooks use addr as an
// identity; neither detector backend retains it as a Go pointer or liveness
// reference, although address-keyed detector metadata may outlive the call.

//go:linkname Acquire
//go:noescape
func Acquire(addr unsafe.Pointer)

//go:linkname Release
//go:noescape
func Release(addr unsafe.Pointer)

//go:linkname ReleaseMerge
//go:noescape
func ReleaseMerge(addr unsafe.Pointer)

//go:linkname Disable
func Disable()

//go:linkname Enable
func Enable()

//go:linkname Read
func Read(addr unsafe.Pointer)

//go:linkname ReadPC
func ReadPC(addr unsafe.Pointer, callerpc, pc uintptr)

//go:linkname ReadObjectPC
func ReadObjectPC(t *abi.Type, addr unsafe.Pointer, callerpc, pc uintptr)

//go:linkname Write
func Write(addr unsafe.Pointer)

//go:linkname WritePC
func WritePC(addr unsafe.Pointer, callerpc, pc uintptr)

//go:linkname WriteObjectPC
func WriteObjectPC(t *abi.Type, addr unsafe.Pointer, callerpc, pc uintptr)

//go:linkname ReadRange
func ReadRange(addr unsafe.Pointer, len int)

//go:linkname WriteRange
func WriteRange(addr unsafe.Pointer, len int)

//go:linkname Errors
func Errors() int
