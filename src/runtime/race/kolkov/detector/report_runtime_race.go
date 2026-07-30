// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race

package detector

import _ "unsafe" // for go:linkname

// Runtime functions used by the race-enabled report path. Frames.Next stays
// behind a narrow bridge because runtime.Frames contains private ABI state.

//go:linkname runtimeCallersReport runtime.Callers
func runtimeCallersReport(skip int, pc []uintptr) int

//go:linkname runtimeCallersFramesReport runtime.CallersFrames
func runtimeCallersFramesReport(callers []uintptr) *runtimeFramesReport

type runtimeFramesReport struct{}

//go:linkname runtimeFramesNextReport runtime.kolkovFramesNext
func runtimeFramesNextReport(f *runtimeFramesReport) (pc uintptr, function, file string, line int, more bool)

//go:linkname kolkovIncrementErrors runtime.kolkovIncrementErrors
func kolkovIncrementErrors()

//go:linkname kolkovReportDone runtime.kolkovReportDone
func kolkovReportDone()

//go:linkname runtimePCFunctionHasPrefix runtime.kolkovPCFunctionHasPrefix
func runtimePCFunctionHasPrefix(pc uintptr, prefix string) bool
