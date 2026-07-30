//go:build race

package stackdepot

import _ "unsafe"

// The race runtime supplies a narrow Frames.Next bridge because mirroring the
// private layout of runtime.Frame is ABI-fragile.

//go:linkname runtimeCallers runtime.Callers
func runtimeCallers(skip int, pc []uintptr) int

//go:linkname runtimeCallersFrames runtime.CallersFrames
func runtimeCallersFrames(callers []uintptr) *runtimeFrames

type runtimeFrames struct{}

//go:linkname runtimeFramesNext runtime.kolkovFramesNext
func runtimeFramesNext(f *runtimeFrames) (pc uintptr, function, file string, line int, more bool)
