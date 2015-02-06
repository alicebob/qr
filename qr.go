package qr

// Queue with disk based overflow. Element order is not strictly preserved.
//
// When everything is fine elements flow over Qr.q. This is a simple channel
// directly connecting the producer(s) and the consumer(s).
// If that channel is full elements are written to the Qr.planb channel. The
// loop() will write all elements from Qr.planb to disk. That file is closed
// after `timeout` (no matter how many elements ended up in the file). At the
// same time loop will try to handle old files: if there is at least a single
// completed file, loop() will open that file and try to write the elements to
// Qr.q.
//
//   ---> Enqueue()    ---------   .q  --------->     Dequeue() --->
//             \                                           ^
//            .planb                                     .q
//               \--> swapout() --> fs() --> swapin() --/
//

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"time"
)

const (
	fileExtension = ".q"
	timeout       = time.Second // TODO: configurable.
)

type Qr struct {
	q      chan interface{} // the main channel.
	planb  chan interface{} // to disk, used when q is full.
	base   string
	prefix string
}

func New() *Qr {
	qr := Qr{
		q:      make(chan interface{}, 40), // TODO: size config
		planb:  make(chan interface{}),
		base:   "./",
		prefix: "testqr",
	}
	// TODO: look at the FS for existing Q files
	var (
		filesToDisk   = make(chan string)
		filesFromDisk = make(chan string)
	)
	go qr.swapout(filesToDisk)
	go qr.fs(filesToDisk, filesFromDisk)
	go qr.swapin(filesFromDisk)
	return &qr
}

// Enqueue adds something in the queue. This never blocks, and is safe to be
// called by different goroutines.
func (qr *Qr) Enqueue(e interface{}) {
	select {
	case qr.q <- e:
		return
	default:
	}
	// fmt.Printf("full q\n")
	qr.planb <- e
}

// Dequeue is the channel where elements come from. It'll be closed when we
// shut down.
func (qr *Qr) Dequeue() <-chan interface{} {
	return qr.q
}

// Quit shuts down all Go routines and closes the Dequeue() channel. It'll
// write all in-flight entries to disk. Calling Enqueue() after Quit will
// panic.
func (qr *Qr) Quit() {
	// Closing planb triggers a cascade closing of all go-s and channels.
	close(qr.planb)

	// Store the in-flight ones for next time.
	// TODO: could be there is nothing.
	filename := qr.batchFilename(0) // special filename
	fh, err := os.Create(filename)
	if err != nil {
		fmt.Printf("create err: %v\n", err)
		return
	}
	defer fh.Close()
	enc := gob.NewEncoder(fh)
	for e := range qr.q {
		if err = enc.Encode(&e); err != nil {
			fmt.Printf("Encode error: %v\n", err)
		}
	}
}

func (qr *Qr) swapout(files chan string) {
	var (
		enc      *gob.Encoder
		filename string
		fh       io.WriteCloser
		t        = time.NewTimer(100 * time.Minute)
	)
	defer func() {
		if enc != nil {
			fh.Close()
			files <- filename
		}
		close(files)
	}()
	var err error
	for {
		select {
		case e, ok := <-qr.planb:
			if !ok {
				return
			}
			if enc == nil {
				// open file
				filename = qr.batchFilename(time.Now().UnixNano())
				fmt.Printf("new file %s...\n", filename)
				fh, err = os.Create(filename)
				if err != nil {
					fmt.Printf("create err: %v\n", err)
					panic("todo")
					return // TODO
				}
				enc = gob.NewEncoder(fh)
				t.Reset(timeout)
			}
			if err = enc.Encode(&e); err != nil {
				fmt.Printf("Encode error: %v\n", err)
			}
		case <-t.C:
			// time to close our file.
			fmt.Printf("closing file %s...\n", filename)
			fh.Close()
			enc = nil
			files <- filename
		}
	}
}

func (qr *Qr) swapin(files chan string) {
	defer close(qr.q)
	for filename := range files {
		fh, err := os.Open(filename)
		fmt.Printf("open %s: %v\n", filename, err)
		if err != nil {
			fmt.Printf("open err: %v\n", err)
			continue
		}
		os.Remove(filename)
		dec := gob.NewDecoder(fh)
		for {
			var next interface{}
			if err = dec.Decode(&next); err != nil {
				if err != io.EOF {
					fmt.Printf("decode err: %v\n", err)
					// TODO
					break
				}
			}
			qr.q <- next
		}
	}
}

// fs keeps a list of all files on disk. swapout() will add filenames, and
// swapin asks it for filenames. It returns when in is closed.
func (qr *Qr) fs(in, out chan string) {
	defer close(out)
	var (
		filenames []string
		checkOut  chan string
		next      string
	)
	for {
		select {
		case f, ok := <-in:
			if !ok {
				return
			}
			filenames = append(filenames, f)
			checkOut = out
			next = filenames[0]
		case checkOut <- next:
			// case disabled if there is no file
			filenames = filenames[1:]
		}
	}
}

func (qr *Qr) batchFilename(id int64) string {
	return fmt.Sprintf("%s/%s-%020d%s", qr.base, qr.prefix, id, fileExtension)
}
