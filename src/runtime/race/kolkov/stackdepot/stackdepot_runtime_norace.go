//go:build !race

package stackdepot

import "runtime"

type runtimeFrames = runtime.Frames

func runtimeCallers(skip int, pc []uintptr) int {
	// Account for this wrapper so CaptureStack observes the same callers as the
	// direct race-runtime linkname implementation.
	return runtime.Callers(skip+1, pc)
}

func runtimeCallersFrames(callers []uintptr) *runtimeFrames {
	return runtime.CallersFrames(callers)
}

func runtimeFramesNext(f *runtimeFrames) (pc uintptr, function, file string, line int, more bool) {
	frame, more := f.Next()
	return frame.PC, frame.Function, frame.File, frame.Line, more
}
