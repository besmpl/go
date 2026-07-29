// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build !race

// Dummy race detection API, used when not built with -race.

package runtime

import (
	"unsafe"
)

const raceenabled = false

// Because raceenabled is false, none of these functions should be called.

func raceReadObjectPC(t *_type, addr unsafe.Pointer, callerpc, pc uintptr)  { throw("race") }
func raceWriteObjectPC(t *_type, addr unsafe.Pointer, callerpc, pc uintptr) { throw("race") }
func raceinit() (uintptr, uintptr)                                          { throw("race"); return 0, 0 }
func racefini()                                                             { throw("race") }
func raceproccreate() uintptr                                               { throw("race"); return 0 }
func raceprocdestroy(ctx uintptr)                                           { throw("race") }
func racemapshadow(addr unsafe.Pointer, size uintptr)                       { throw("race") }
func racewritepc(addr unsafe.Pointer, callerpc, pc uintptr)                 { throw("race") }
func racereadpc(addr unsafe.Pointer, callerpc, pc uintptr)                  { throw("race") }
func racereadrangepc(addr unsafe.Pointer, sz, callerpc, pc uintptr)         { throw("race") }
func racewriterangepc(addr unsafe.Pointer, sz, callerpc, pc uintptr)        { throw("race") }
func raceacquire(addr unsafe.Pointer)                                       { throw("race") }
func raceacquireg(gp *g, addr unsafe.Pointer)                               { throw("race") }
func raceacquirectx(racectx uintptr, addr unsafe.Pointer)                   { throw("race") }
func racerelease(addr unsafe.Pointer)                                       { throw("race") }
func racereleaseg(gp *g, addr unsafe.Pointer)                               { throw("race") }
func racereleaseacquire(addr unsafe.Pointer)                                { throw("race") }
func racereleaseacquireg(gp *g, addr unsafe.Pointer)                        { throw("race") }
func racereleasemerge(addr unsafe.Pointer)                                  { throw("race") }
func racereleasemergeg(gp *g, addr unsafe.Pointer)                          { throw("race") }
func racefingo()                                                            { throw("race") }
func racemalloc(p unsafe.Pointer, sz uintptr)                               { throw("race") }
func racefree(p unsafe.Pointer, sz uintptr)                                 { throw("race") }
func raceheapspanfree(p unsafe.Pointer, size uintptr)                       {}
func racegostart(pc uintptr) uintptr                                        { throw("race"); return 0 }
func racegosetchildid(childGoid uint64, spawnctx uintptr) uintptr           { throw("race"); return 0 }
func racegoend()                                                            { throw("race") }
func racectxstart(pc, spawnctx uintptr) uintptr                             { throw("race"); return 0 }
func racectxend(racectx uintptr)                                            { throw("race") }

// The Kolkov packages have ordinary unit tests that run without -race. Keep
// their narrow runtime bridges available in non-race binaries without
// enabling detector state or changing the public race API.

//go:linkname kolkovIncrementErrors
func kolkovIncrementErrors() {}

//go:linkname kolkovReportDone
func kolkovReportDone() {}

//go:linkname kolkovFramesNext
func kolkovFramesNext(frames *Frames) (pc uintptr, function, file string, line int, more bool) {
	frame, more := frames.Next()
	return frame.PC, frame.Function, frame.File, frame.Line, more
}

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

//go:linkname kolkovGetGoid
//go:nosplit
func kolkovGetGoid() int64 {
	gp := getg()
	if gp.m != nil && gp.m.curg != nil {
		return int64(gp.m.curg.goid)
	}
	return int64(gp.goid)
}
