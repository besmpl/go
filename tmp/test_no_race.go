package main

import "sync"

func main() {
	x := 0
	var wg sync.WaitGroup
	wg.Add(1)
	x = 42 // WRITE before go — should happen-before child's read
	go func() {
		defer wg.Done()
		_ = x // READ — no race, parent wrote before fork
	}()
	wg.Wait()
	_ = x // READ after Wait — no race
}
