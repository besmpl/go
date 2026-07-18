// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import _ "unsafe" // for go:linkname

// nativeExportCall follows the Android native ABI, not a Go ABI. Native-export
// assembly stubs call it with the callback entry PC in R0 and frame pointer in
// R1. The declaration supplies the linker handshake for that cross-package
// assembly reference.
//
//go:linkname nativeExportCall
func nativeExportCall()
