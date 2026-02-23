package main

import "sync"

func main() {
	x := 0
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		x = 1 // WRITE — race with main goroutine
	}()
	x = 2 // WRITE — race with goroutine above
	wg.Wait()
	_ = x
}
