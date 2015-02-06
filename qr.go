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
//   ---> Enqueue()     -------   .q   ------->     Dequeue() --->
//             \                                        ^
//            .planb                                  .q
//               \--> swapout() -> disk -> swapin() --/
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
	files := make(chan string)
	go qr.swapin(files)
	go qr.swapout(files)
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

func (qr *Qr) Quit() {
	// close(qr.file)
	// todo: wait for swapout
	// close(qr.q)
	// todo: empty q to a file
}

func (qr *Qr) swapout(files chan string) {
	var (
		enc      *gob.Encoder
		filename string
		fh       io.WriteCloser
		t        = time.NewTimer(100 * time.Minute)
	)
	var err error
	for {
		select {
		case e := <-qr.planb:
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
			fh = nil
			enc = nil
			files <- filename
		}
	}
}

func (qr *Qr) swapin(newFiles chan string) {
	var (
		files    []string
		fh       io.ReadCloser
		filename string
		q        chan interface{}
		dec      *gob.Decoder
		next     interface{}
	)
	var setupNext func()
	setupNext = func() {
		var err error
		// fmt.Printf("in setupNext\n")
		if dec == nil {
		again:
			// no decoder openend.
			if len(files) == 0 {
				// Nothing to do. Just wait.
				fmt.Printf("nothing to do\n")
				q = nil
				return
			}
			filename, files = files[0], files[1:]
			fh, err = os.Open(filename)
			fmt.Printf("open %s: %v\n", filename, err)
			if err != nil {
				fmt.Printf("open err: %v\n", err)
				goto again
			}
			dec = gob.NewDecoder(fh)
		}
		err = dec.Decode(&next)
		if err != nil {
			if err == io.EOF {
				fmt.Printf("done reading %s\n", filename)
				fh.Close()
				os.Remove(filename)
				q = nil
				dec = nil
				setupNext()
			} else {
				fmt.Printf("decode err: %v\n", err)
				// TODO
			}
		}
		q = qr.q
	}
	for {
		select {
		case f := <-newFiles:
			files = append(files, f)
			setupNext()
		case q <- next:
			// case is disabled if there is nothing to send
			// fmt.Printf("wrote from disk to main q\n")
			setupNext()
		}
	}
}

func (qr *Qr) batchFilename(id int64) string {
	return fmt.Sprintf("%s/%s-%020d%s", qr.base, qr.prefix, id, fileExtension)
}
