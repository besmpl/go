// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

// This file imports the pure-Go Kolkov race detector when CGO is disabled.
// When CGO_ENABLED=0 and -race is used, this provides the race detection
// implementation instead of ThreadSanitizer.

package race

import _ "runtime/race/kolkov/api"
