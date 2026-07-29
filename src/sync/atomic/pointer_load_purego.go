// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

package atomic

import "unsafe"

// Load atomically loads and returns the value stored in x.
func (x *Pointer[T]) Load() *T { return (*T)(loadPointer(&x.v)) }

// Generic Pointer methods instantiated in a no-race package can otherwise be
// inlined there and lower LoadPointer directly to a hardware load. Keep this
// seam out of line so the pure-Go detector observes the acquire operation.
//
//go:noinline
//go:norace
func loadPointer(addr *unsafe.Pointer) unsafe.Pointer {
	return LoadPointer(addr)
}
