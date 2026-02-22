// Copyright 2025 The racedetector Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Common goroutine ID extraction utilities.
//
// This file contains the main entry point and slow-path fallback for
// goroutine ID extraction.
//
// The fast path (getGoroutineIDFast) is provided by goid_runtime.go
// which uses a runtime bridge to access getg().goid directly — zero cost,
// version-independent, platform-independent.
//
// The slow path (getGoroutineIDSlow) parses runtime.Stack output and
// is kept for testing/validation only.
//
// API:
//   - getGoroutineID(): Main entry point, delegates to getGoroutineIDFast()
//   - getGoroutineIDFast(): Runtime bridge via getg().goid (~0ns)
//   - getGoroutineIDSlow(): runtime.Stack parsing (~1500ns, testing only)
//   - parseGID(): Parses goroutine ID from stack trace bytes

package api

// getGoroutineID returns the current goroutine ID.
//
// This is the main entry point for goroutine ID extraction. It delegates
// to getGoroutineIDFast() which calls into the runtime via linkname bridge
// to access getg().goid directly (~0ns, zero allocations).
//
// Returns:
//   - int64: Goroutine ID (always positive, unique per goroutine)
func getGoroutineID() int64 {
	return getGoroutineIDFast()
}

// getGoroutineIDSlow extracts goroutine ID by parsing runtime.Stack output.
//
// This is the slow-path method used only for testing and validation.
// It parses the first line of the stack trace to extract the goroutine ID.
//
// Stack trace format: "goroutine 123 [running]:\n..."
//
// Performance: ~1500ns per call (dominated by runtime.Stack allocation).
//
// Returns:
//   - int64: Goroutine ID (always positive), or 0 if parsing fails
func getGoroutineIDSlow() int64 {
	// Allocate buffer for stack trace.
	// We only need the first line, so 64 bytes is sufficient.
	// Format: "goroutine 123 [running]:\n..."
	var buf [64]byte

	// Get stack trace for current goroutine only (all=false).
	n := runtimeStack(buf[:], false)

	// Parse goroutine ID from the buffer.
	return parseGID(buf[:n])
}

// parseGID extracts the goroutine ID from stack trace bytes.
//
// Expected format: "goroutine 123 [running]:..."
// Returns the numeric ID (123 in this example) or 0 if parsing fails.
//
// This function is optimized for minimal allocations:
//   - No string conversion
//   - No regex
//   - Direct byte parsing
//
// Parameters:
//   - buf: Stack trace bytes from runtime.Stack
//
// Returns:
//   - int64: Parsed goroutine ID, or 0 if format is invalid
func parseGID(buf []byte) int64 {
	// Expected prefix: "goroutine "
	const prefix = "goroutine "
	const prefixLen = 10 // len("goroutine ")

	// Verify buffer has expected prefix.
	if len(buf) < prefixLen {
		return 0
	}

	// Fast prefix check (uses string conversion but avoids regex).
	// Safe: we already verified len(buf) >= prefixLen above.
	if string(buf[:prefixLen]) != prefix {
		return 0
	}

	// Parse numeric goroutine ID.
	// Format after prefix: "123 [running]:..."
	var gid int64
	for i := prefixLen; i < len(buf); i++ {
		//nolint:gosec // G602: i is always < len(buf) due to loop condition
		c := buf[i]
		if c >= '0' && c <= '9' {
			gid = gid*10 + int64(c-'0')
		} else {
			// Non-digit terminates the ID (usually space before "[running]")
			break
		}
	}

	return gid
}
