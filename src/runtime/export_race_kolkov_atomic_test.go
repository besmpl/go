// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

package runtime

var RaceKolkovAtomicRMWRetry = kolkovAtomicRMWRetry
var RaceKolkovAtomicRMWWake = kolkovAtomicRMWWake
var RaceKolkovAtomicSyncFrame = kolkovAtomicSyncFrame
var RaceKolkovAtomicSyncFrameUncached = kolkovAtomicSyncFrameUncached
var RaceKolkovProcPin = procPin
var RaceKolkovProcUnpin = procUnpin
