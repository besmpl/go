package main

func main() {
	x := 0
	ch := make(chan struct{})
	go func() {
		x = 1 // WRITE in child
		ch <- struct{}{} // signal: done writing
	}()
	<-ch   // wait for child to finish writing
	x = 2  // WRITE in main — no race, channel sync establishes HB
	_ = x
}
