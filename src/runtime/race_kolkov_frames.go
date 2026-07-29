// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race

package runtime

import _ "unsafe" // for go:linkname

// kolkovFramesNext exposes only the stable values needed by the pure-Go race
// detector. Mirroring runtime.Frame in another package is ABI-fragile because
// it contains private fields whose layout may change between Go versions.
//
//go:linkname kolkovFramesNext
func kolkovFramesNext(frames *Frames) (pc uintptr, function, file string, line int, more bool) {
	frame, more := frames.Next()
	return frame.PC, frame.Function, frame.File, frame.Line, more
}

// kolkovPCFunctionHasPrefix reports whether prefix matches the innermost
// logical function at pc. It mirrors the first-frame symbol selection in
// Frames.Next without constructing a Frames value, so detector classification
// misses do not allocate. pc is a return PC, as produced by the runtime race
// hooks, and is adjusted to the call instruction before inline unwinding.
//
//go:linkname kolkovPCFunctionHasPrefix
func kolkovPCFunctionHasPrefix(pc uintptr, prefix string) bool {
	f := findfunc(pc)
	if !f.valid() {
		return false
	}
	if entry := f.entry(); pc > entry {
		pc--
	}
	u, uf := newInlineUnwinder(f, pc)
	name := u.srcFunc(uf).name()
	return len(name) >= len(prefix) && name[:len(prefix)] == prefix
}
