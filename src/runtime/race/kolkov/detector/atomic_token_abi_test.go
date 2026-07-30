// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package detector

import (
	"testing"
	"unsafe"
)

func TestAtomicTokenABI(t *testing.T) {
	var token AtomicToken
	want := uintptr(AtomicTokenSlots) * unsafe.Sizeof(unsafe.Pointer(nil))
	if got := unsafe.Sizeof(token); got != want {
		t.Fatalf("AtomicToken size = %d, want %d (%d pointer slots)", got, want, AtomicTokenSlots)
	}
	if got, want := unsafe.Alignof(token), unsafe.Alignof(unsafe.Pointer(nil)); got != want {
		t.Fatalf("AtomicToken alignment = %d, want pointer alignment %d", got, want)
	}
}
