// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Runtime-bridge goroutine ID extraction.
//
// This file provides fast goroutine ID extraction by calling directly into
// the Go runtime via linkname. Since this package is compiled as part of
// the Go runtime (runtime/race/kolkov/api), the runtime bridge function
// kolkovGetGoid is always available.
//
// This replaces the previous approach of:
//   - Assembly TLS access (goid_amd64.s, goid_arm64.s)
//   - Hardcoded struct offsets (goid_go123.go, goid_go124.go, goid_go125.go)
//   - runtime.Stack parsing fallback (goid_fallback.go)
//
// The runtime bridge uses getg().goid directly, which is:
//   - Version-independent (no struct offset maintenance per Go release)
//   - Platform-independent (no per-arch assembly)
//   - Zero-cost (getg() is a compiler intrinsic)
//
// As recommended by @randall77: "use getg().goid for goroutine ID"

package api

import _ "unsafe" // for go:linkname

// kolkovGetGoid returns the current goroutine's ID via runtime bridge.
// The implementation is in runtime/race_kolkov_detector.go and uses
// the getg() compiler intrinsic to access goid directly.
//
//go:linkname kolkovGetGoid runtime.kolkovGetGoid
//go:nosplit
func kolkovGetGoid() int64

// getGoroutineIDFast extracts the goroutine ID via runtime bridge.
//
// Performance: ~0ns (compiler intrinsic, no TLS/assembly/parsing).
//
// This replaces the previous assembly-based approach which required:
//   - Per-version struct offset files (goid_go123.go, goid_go124.go, goid_go125.go)
//   - Per-architecture assembly files (goid_amd64.s, goid_arm64.s)
//   - Fallback runtime.Stack parsing (~1500ns)
//
//go:nosplit
func getGoroutineIDFast() int64 {
	return kolkovGetGoid()
}
