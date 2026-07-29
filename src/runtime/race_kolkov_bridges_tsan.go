// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && cgo

package runtime

import _ "unsafe" // for go:linkname

// The Kolkov packages can be tested directly in a TSAN race build even though
// runtime/race selects the native backend there. Keep their narrow runtime
// bridges linkable without enabling or mutating the pure-Go detector state.

//go:linkname kolkovIncrementErrors
func kolkovIncrementErrors() {}

//go:linkname kolkovReportDone
func kolkovReportDone() {}

//go:linkname kolkovGetGoid
//go:nosplit
func kolkovGetGoid() int64 {
	gp := getg()
	if gp.m != nil && gp.m.curg != nil {
		return int64(gp.m.curg.goid)
	}
	return int64(gp.goid)
}
