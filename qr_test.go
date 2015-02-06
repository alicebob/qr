package qr_test

import (
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/qr"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func setupDataDir() string {
	os.RemoveAll("./d")
	if err := os.Mkdir("./d/", 0700); err != nil {
		panic(fmt.Sprintf("Can't make ./d/: %v", err))
	}
	return "./d"
}

func TestBasic(t *testing.T) {
	d := setupDataDir()

	q := qr.New(d, "test")
	defer q.Close()
	for i := 0; i < 1000; i++ {
		q.Enqueue(i)
	}

	ret := make([]int, 1000)
	for i := range ret {
		select {
		case ii := <-q.Dequeue():
			t.Logf("Got a : %#v", ii)
			ret[i] = ii.(int)
		case <-time.After(2 * time.Second):
			t.Fatalf("q should not be empty")
		}
	}

	select {
	case e := <-q.Dequeue():
		t.Fatalf("q should be empty, got a %#v", e)
	default:
		// ok
	}
}

func TestBlock(t *testing.T) {
	// Read should block until there is something.
	d := setupDataDir()
	q := qr.New(d, "test")
	defer q.Close()

	ready := make(chan struct{})

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		ready <- struct{}{}
		if got := <-q.Dequeue(); got != "hello world" {
			t.Errorf("Want hello, got %#v", got)
		}
	}()
	<-ready

	q.Enqueue("hello world")

	wg.Wait()
}

func TestBig(t *testing.T) {
	// Queue a lot of elements.
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	d := setupDataDir()

	q := qr.New(d, "events")

	eventCount := 10000
	for i := 0; i < eventCount; i++ {
		q.Enqueue(fmt.Sprintf("Event %d: %s", i, strings.Repeat("0xDEADBEEF", 300)))
	}
	for i := 0; i < eventCount; i++ {
		want := fmt.Sprintf("Event %d: %s", i, strings.Repeat("0xDEADBEEF", 300))
		if got := <-q.Dequeue(); want != got {
			t.Fatalf("Want for %d: %#v, got %#v", i, want, got)
		}
	}

	q.Close()
	// Everything is processed. All files should be gone.
	if got, want := fileCount(d), 0; got != want {
		t.Fatalf("Wrong number of files: got %d, want %d", got, want)
	}
}

func TestAsync(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	// Random sleep readers and writers.
	d := setupDataDir()
	q := qr.New(d, "events")
	var (
		eventCount = 10000
		payload    = strings.Repeat("0xDEADBEEF", 300)
		wg         = sync.WaitGroup{}
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < eventCount; i++ {
			q.Enqueue(payload)
			time.Sleep(time.Duration(rand.Intn(100)) * time.Microsecond)
		}
	}()
	// Reader is a little slower.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < eventCount; i++ {
			if have, want := <-q.Dequeue(), payload; have != want {
				t.Fatalf("have %#v, want %#v", have, want)
			}
			time.Sleep(time.Duration(rand.Intn(150)) * time.Microsecond)
		}
	}()
	wg.Wait()
	q.Close()

	// Everything is processed. All files should be gone.
	if got, want := fileCount(d), 0; got != want {
		t.Fatalf("Wrong number of files: got %d, want %d", got, want)
	}
}

func TestMany(t *testing.T) {
	// Read and write a lot of messages, as fast as possible.
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var (
		eventCount = 1000000
		clients    = 10
		payload    = strings.Repeat("0xDEADBEEF", 30)
		d          = setupDataDir()
	)

	q := qr.New(d, "events")
	wg := sync.WaitGroup{}

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < eventCount/clients; j++ {
				q.Enqueue(payload)
			}
		}()
	}
	wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < eventCount; i++ {
			if got := <-q.Dequeue(); payload != got {
				t.Fatalf("Want for %d: %#v, got %#v", i, payload, got)
			}
		}
	}()
	wg.Wait()

	q.Close()
	// Everything is processed. All files should be gone.
	if got, want := fileCount(d), 0; got != want {
		t.Fatalf("Wrong number of files: got %d, want %d", got, want)
	}
}

func TestReopen(t *testing.T) {
	// Simple reopening.
	d := setupDataDir()
	q := qr.New(d, "events")
	q.Enqueue("Message 1")
	q.Enqueue("Message 2")
	q.Close()

	q = qr.New(d, "events")
	select {
	case <-q.Dequeue():
	case <-time.After(10 * time.Millisecond):
		t.Fatalf("nothing to read")
	}
	<-q.Dequeue()
	q.Close()
	// Everything is processed. All files should be gone.
	if got, want := fileCount(d), 0; got != want {
		t.Fatalf("Wrong number of files: got %d, want %d", got, want)
	}
}

func TestReadOnly(t *testing.T) {
	// Only reading doesn't block the close.
	d := setupDataDir()
	q := qr.New(d, "i")
	select {
	case v := <-q.Dequeue():
		t.Fatalf("Impossible read: %v", v)
	default:
	}
	q.Close()
}

func TestStruct(t *testing.T) {
	d := setupDataDir()
	q := qr.New(d, "events")
	defer q.Close()
	type s struct {
		X string
		Y int
	}
	gob.Register(s{})
	data := []s{
		{"Event", 1},
		{"alice", 2},
		{"bob", 3},
	}
	for _, d := range data {
		q.Enqueue(d)
	}
	for i := 0; i < 20; i++ {
		q.Enqueue(s{"dummy", i})
	}
	for _, want := range data {
		if got := <-q.Dequeue(); want != got {
			t.Errorf("Want %#v, got %#v", want, got)
		}
	}
}

func TestTwoStructs(t *testing.T) {
	d := setupDataDir()
	q1 := qr.New(d, "s1")
	defer q1.Close()
	q2 := qr.New(d, "s2")
	defer q2.Close()

	type s1 struct {
		X string
		Y int
	}
	gob.Register(s1{})

	type s2 struct {
		A float64
		B string
	}
	gob.Register(s2{})

	data1 := []s1{
		{"Event", 1},
		{"alice", 2},
		{"bob", 3},
	}

	data2 := []s2{
		{3.14, "pi"},
		{2.72, "e"},
	}

	for _, d1 := range data1 {
		q1.Enqueue(d1)
	}
	for _, d2 := range data2 {
		q2.Enqueue(d2)
	}
	for i := 0; i < 20; i++ {
		q1.Enqueue(s1{"dummy", i})
		q2.Enqueue(s2{1.0, "one"})
	}

	for _, want := range data1 {
		if got := <-q1.Dequeue(); want != got {
			t.Errorf("Want %#v, got %#v", want, got)
		}
	}
	for _, want := range data2 {
		if got := <-q2.Dequeue(); want != got {
			t.Errorf("Want %#v, got %#v", want, got)
		}
	}
}

// fileCount is a helper to count files in a directory.
func fileCount(dir string) int {
	fh, _ := os.Open(dir)
	defer fh.Close()
	n, err := fh.Readdirnames(-1)
	if err != nil {
		panic(err)
	}
	return len(n)
}
