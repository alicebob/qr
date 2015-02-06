package qr

// Queue with disk based overflow. Order is not strictly preserved.
//
// When everything is fine elements flow over Qr.q. This is a simple channel
// directly connecting the producer(s) and the consumer(s).
// If that channel is full elements are written to the Qr.file channel. The loop() will
// write all elements from Qr.file to disk. That file is closed after `timeout`
// (no matter how many elements ended up in the file). At the same time loop
// will try to handle old files: if there is at least a single completed file,
// loop() will open that file and try to write the elements to Qr.q.
//
//   ---> Enqueue()   --- .q --->   Dequeue() --->
//             \                       ^
//            .file                   /
//               \--> disk []files --/
//                     (loop())

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
	file   chan interface{} // to disk, used when q is full.
	base   string
	prefix string
}

func New() *Qr {
	qr := Qr{
		q:      make(chan interface{}, 40), // TODO: size config
		file:   make(chan interface{}),
		base:   "./",
		prefix: "testqr",
	}
	// TODO: look at the FS for existing Q files
	go qr.loop()
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
	qr.file <- e
}

// Dequeue is the channel where elements come from. It'll be closed when we
// shut down.
func (qr *Qr) Dequeue() <-chan interface{} {
	return qr.q
}

func (qr *Qr) Quit() {
	// close(qr.files)
	// todo: wait for loop
	// close(qr.q)
	// todo: empty q to a file
	// todo: be sure the value in `next` is kept
}

func (qr *Qr) loop() {
	defer fmt.Printf("loop done\n")
	var (
		wfh       io.WriteCloser
		enc       *gob.Encoder
		files     []string
		writefile string
		readfile  string
		q         chan interface{}
		rfh       io.ReadCloser
		dec       *gob.Decoder
		next      interface{}
		t         = time.NewTimer(100 * time.Minute)
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
			readfile, files = files[0], files[1:]
			rfh, err = os.Open(readfile)
			fmt.Printf("open %s: %v\n", readfile, err)
			if err != nil {
				fmt.Printf("open err: %v\n", err)
				goto again
			}
			dec = gob.NewDecoder(rfh)
		}
		err = dec.Decode(&next)
		if err != nil {
			if err == io.EOF {
				fmt.Printf("done reading %s\n", readfile)
				rfh.Close()
				os.Remove(readfile)
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

	var err error
	for {
		select {
		case e := <-qr.file:
			if enc == nil {
				// open file
				writefile = qr.batchFilename(time.Now().UnixNano())
				fmt.Printf("new file %s...\n", writefile)
				wfh, err = os.Create(writefile)
				if err != nil {
					fmt.Printf("create err: %v\n", err)
					panic("todo")
					return // TODO
				}
				enc = gob.NewEncoder(wfh)
				t.Reset(timeout)
			}
			if err = enc.Encode(&e); err != nil {
				fmt.Printf("Encode error: %v\n", err)
			}
		case <-t.C:
			// time to close our file.
			fmt.Printf("closing file %s...\n", writefile)
			wfh.Close()
			wfh = nil
			dec = nil
			files = append(files, writefile)
			if q == nil {
				setupNext()
			}
		case q <- next:
			// fmt.Printf("wrote from disk to main q\n")
			setupNext()
		}
	}
}

func (qr *Qr) batchFilename(id int64) string {
	return fmt.Sprintf("%s/%s-%020d%s", qr.base, qr.prefix, id, fileExtension)
}
