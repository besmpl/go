// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build !race

package detector

import (
	"runtime"
	"strings"
)

// Ordinary unit tests use only public runtime behavior. These implementations
// deliberately avoid pulling Kolkov bridge state into the runtime package.

func runtimeCallersReport(skip int, pc []uintptr) int {
	return runtime.Callers(skip+1, pc)
}

type runtimeFramesReport = runtime.Frames

func runtimeCallersFramesReport(callers []uintptr) *runtimeFramesReport {
	return runtime.CallersFrames(callers)
}

func runtimeFramesNextReport(frames *runtimeFramesReport) (pc uintptr, function, file string, line int, more bool) {
	frame, more := frames.Next()
	return frame.PC, frame.Function, frame.File, frame.Line, more
}

func kolkovIncrementErrors() {}

func kolkovReportDone() {}

func runtimePCFunctionHasPrefix(pc uintptr, prefix string) bool {
	function := runtime.FuncForPC(pc)
	if function == nil {
		return false
	}
	return strings.HasPrefix(function.Name(), prefix)
}
