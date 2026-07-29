// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build !race || cgo

package atomic

// Load atomically loads and returns the value stored in x.
// Keep the normal and TSAN body identical to the upstream direct call so both
// the generic method and LoadPointer remain eligible for their usual inlining.
func (x *Pointer[T]) Load() *T { return (*T)(LoadPointer(&x.v)) }
