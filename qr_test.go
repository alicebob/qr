package qr_test

import (
	"testing"
	"time"

	"github.com/alicebob/qr"
)

func TestBasic(t *testing.T) {

	qr := qr.New()
	for i := 0; i < 1000; i++ {
		qr.Enqueue(i)
	}

	ret := make([]int, 1000)
	for i := range ret {
		select {
		case ii := <-qr.Dequeue():
			t.Logf("Got a : %#v", ii)
			ret[i] = ii.(int)
		case <-time.After(2 * time.Second):
			t.Fatalf("q should not be empty")
		}
	}

	select {
	case e := <-qr.Dequeue():
		t.Fatalf("q should be empty, got a %#v", e)
	default:
		// ok
	}
}
