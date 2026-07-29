// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package race_test

import (
	"sync"
	"testing"
)

func TestNoRaceLogicalIDsPastDenseLimit(t *testing.T) {
	const workers = 1200
	values := make([]int, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := range values {
		go func(i int) {
			defer wg.Done()
			<-start
			values[i] = i
		}(i)
	}
	close(start)
	wg.Wait()
	for i, value := range values {
		if value != i {
			t.Fatalf("values[%d] = %d", i, value)
		}
	}
}

func TestRaceLogicalIDsPastDenseLimit(t *testing.T) {
	// Retire enough contexts that both conflicting goroutines necessarily use
	// the sparse logical-ID tier even when this test runs by itself.
	for i := 0; i < 1100; i++ {
		done := make(chan struct{})
		go func() { close(done) }()
		<-done
	}

	var value int
	done := make(chan struct{}, 2)
	go func() {
		value = 1
		done <- struct{}{}
	}()
	go func() {
		value = 2
		done <- struct{}{}
	}()
	<-done
	<-done
	_ = value
}
