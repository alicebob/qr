package qr

// In process queue with disk based overflow. Element order is not strictly
// preserved.
//
// When everything is fine elements flow over Qr.q. This is a simple channel
// directly connecting the producer(s) and the consumer(s).
// If that channel is full elements are written to the Qr.planb channel.
// swapout() will write all elements from Qr.planb to disk. That file is closed
// after `timeout`. At the same time swapin() will try to process old files: if
// there is at least a single completed file, swapin() will open that file and
// try to write the elements to Qr.q.
//
//   ---> Enqueue()    ---------   .q  --------->     Dequeue() --->
//             \                                           ^
//            .planb                                     .q
//               \--> swapout() --> fs() --> swapin() --/
//
//
// Usage:
// q := New("/mnt/queues/", "demo", OptionBuffer(100))
// defer q.Close()
// go func() {
//     for e := range q.Dequeue() {
//        fmt.Printf("We got: %v\n", e)
//     }
// }
// // elsewhere:
// q.Enqueue("aap")
// q.Enqueue("noot")

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	// DefaultTimeout can be changed with OptionTimeout.
	DefaultTimeout = 10 * time.Second
	// DefaultBuffer can be changed with OptionBuffer.
	DefaultBuffer = 1000

	fileExtension = ".qr"
)

type Qr struct {
	q          chan interface{} // the main channel.
	planb      chan interface{} // to disk, used when q is full.
	dir        string
	prefix     string
	timeout    time.Duration
	bufferSize int
}

// Option is an option to New(), which can change some settings.
type Option func(qr *Qr)

// OptionTimeout is an option for New(). It specifies the time after which a queue
// file is closed. Smaller means more files.
func OptionTimeout(t time.Duration) Option {
	return func(qr *Qr) {
		qr.timeout = t
	}
}

// OptionBuffer is an option for New(). It specifies the in-memory size of the
// queue. Smaller means the disk will be used sooner, larger means more memory.
func OptionBuffer(n int) Option {
	return func(qr *Qr) {
		qr.bufferSize = n
	}
}

// New starts a Queue which stores files in <dir>/<prefix>-.<timestamp>.qr
func New(dir, prefix string, options ...Option) *Qr {
	qr := Qr{
		planb:      make(chan interface{}),
		dir:        dir,
		prefix:     prefix,
		timeout:    DefaultTimeout,
		bufferSize: DefaultBuffer,
	}
	for _, cb := range options {
		cb(&qr)
	}
	qr.q = make(chan interface{}, qr.bufferSize)

	var (
		filesToDisk   = make(chan string)
		filesFromDisk = make(chan string)
	)
	go qr.swapout(filesToDisk)
	go qr.fs(filesToDisk, filesFromDisk)
	go qr.swapin(filesFromDisk)
	for _, f := range qr.findOld() {
		filesFromDisk <- f
	}
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

// Close shuts down all Go routines and closes the Dequeue() channel. It'll
// write all in-flight entries to disk. Calling Enqueue() after Close will
// panic.
func (qr *Qr) Close() {
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
	enc := gob.NewEncoder(fh)
	count := 0
	for e := range qr.q {
		count++
		if err = enc.Encode(&e); err != nil {
			fmt.Printf("Encode error: %v\n", err)
		}
	}
	fh.Close()

	if count == 0 {
		// all this work, and there was nothing to queue...
		os.Remove(filename)
	}
}

func (qr *Qr) swapout(files chan string) {
	defer fmt.Printf("swapout out\n")
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
				fmt.Printf("swapout %s\n", filename)
				fh, err = os.Create(filename)
				if err != nil {
					fmt.Printf("create err: %v\n", err)
					panic("todo")
					return // TODO
				}
				enc = gob.NewEncoder(fh)
				t.Reset(qr.timeout)
			}
			if err = enc.Encode(&e); err != nil {
				fmt.Printf("Encode error: %v\n", err)
			}
		case <-t.C:
			// time to close our file.
			// fmt.Printf("closing file %s...\n", filename)
			fh.Close()
			enc = nil
			files <- filename
		}
	}
}

func (qr *Qr) swapin(files chan string) {
	defer fmt.Printf("swapin out\n")
	defer close(qr.q)
	for filename := range files {
		fh, err := os.Open(filename)
		fmt.Printf("swapin %s\n", filename)
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
				}
				fh.Close()
				fh = nil
				break
			}
			// fmt.Printf("swapin write: %v\n", next)
			qr.q <- next
		}
		fmt.Printf("swapin done with %s\n", filename)
	}
}

// fs keeps a list of all files on disk. swapout() will add filenames, and
// swapin asks it for filenames. It returns when in is closed.
func (qr *Qr) fs(in, out chan string) {
	defer fmt.Printf("fs out\n")
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
			if checkOut == nil {
				checkOut = out
				next = f
			} else {
				filenames = append(filenames, f)
			}
		case checkOut <- next:
			if len(filenames) > 0 {
				next, filenames = filenames[0], filenames[1:]
			} else {
				// case disabled since there is no file
				checkOut = nil
			}
		}
	}
}

func (qr *Qr) batchFilename(id int64) string {
	return fmt.Sprintf("%s/%s-%020d%s", qr.dir, qr.prefix, id, fileExtension)
}

// findOld finds .qr files from a previous run.
func (qr *Qr) findOld() []string {
	f, err := os.Open(qr.dir)
	if err != nil {
		return nil
	}
	defer f.Close()
	var existing []string
	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil
	}
	for _, name := range names {
		if !strings.HasPrefix(name, qr.prefix+"-") || !strings.HasSuffix(name, fileExtension) {
			continue
		}
		existing = append(existing, filepath.Join(qr.dir, name))
	}
	sort.Strings(existing)
	return existing
}
