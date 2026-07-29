// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

// The pure-Go race build provides sync/atomic's public symbols as
// ABIInternal Go functions in runtime/race_kolkov_atomic.go. This empty
// assembly marker keeps the package's external declarations buildable without
// introducing ABI0 trampolines.
