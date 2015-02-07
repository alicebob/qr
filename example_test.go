package qr_test

import (
	"fmt"
	"github.com/alicebob/qr"
)

func Example() {
	q := qr.New("/tmp/", "example", qr.OptionBuffer(100))
	defer q.Close()
	go func() {
		for e := range q.Dequeue() {
			fmt.Printf("We got: %v\n", e)
		}
	}()

	// elsewhere:
	q.Enqueue("aap")
	q.Enqueue("noot")
}
