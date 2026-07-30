// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build !race

package api

const goroutineIDUsesRuntimeBridge = false

// Non-race builds need goroutine identity only for the Kolkov packages' unit
// tests. Use the existing runtime.Stack parser instead of retaining a private
// runtime linkname in every ordinary binary.
func getGoroutineIDFast() int64 {
	return getGoroutineIDSlow()
}
