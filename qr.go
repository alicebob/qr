package qr

// In process queue with disk based overflow. Element order is not strictly
// preserved.
//
// When everything is fine elements flow over Qr.q. This is a simple channel
// directly connecting the producer(s) and the consumer(s).
// If that channel is full elements are written to the Qr.planb channel.
// swapout() will write all elements from Qr.planb to disk. It makes a new file
// every `timeout`. At the same time swapin() will deal with completed files.
// swapin() will open the oldest file and write the elements to Qr.q.
//
//   ---> Enqueue()   ------   .q   ----->    merge() -> .out -> Dequeue() --->
//            \                                 ^
//          .planb                         .confluence
//             \                               /
//              \--> swapout()     swapin() --/
//                      \             ^
//                       \--> fs() --/

//
//
// Usage:
//    q := New("/mnt/queues/", "demo", OptionBuffer(100))
//    defer q.Close()
// 	  if err := q.Test("your datatype"); err != nil {
//        panic(err)
// 	  }
//    go func() {
//        for e := range q.Dequeue() {
//           fmt.Printf("We got: %v\n", e)
//        }
//    }()
//
//    // elsewhere:
//    q.Enqueue("aap")
//    q.Enqueue("noot")
//
//
// Gob is used to serialize entries; custom types should be registered using
// gob.Register().

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
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

var (
	// ErrInvalidPrefix is potentially returned by New.
	ErrInvalidPrefix = errors.New("invalid prefix")
)

// Qr is a disk-based queue. Create one with New().
type Qr struct {
	q          chan interface{} // the main channel.
	planb      chan interface{} // to disk, used when q is full.
	confluence chan interface{} // from disk to merge()
	out        chan interface{}
	dir        string
	prefix     string
	timeout    time.Duration
	bufferSize int
	logf       func(string, ...interface{}) // Printf() style
}

// Option is an option to New(), which can change some settings.
type Option func(qr *Qr) error

// OptionTimeout is an option for New(). It specifies the time after which a queue
// file is closed. Smaller means more files.
func OptionTimeout(t time.Duration) Option {
	return func(qr *Qr) error {
		qr.timeout = t
		return nil
	}
}

// OptionBuffer is an option for New(). It specifies the in-memory size of the
// queue. Smaller means the disk will be used sooner, larger means more memory.
func OptionBuffer(n int) Option {
	return func(qr *Qr) error {
		qr.bufferSize = n
		return nil
	}
}

// OptionLogger is an option for New(). Is sets the logger, the default is
// log.Printf, but glog.Errorf would also work.
func OptionLogger(l func(string, ...interface{})) Option {
	return func(qr *Qr) error {
		qr.logf = l
		return nil
	}
}

// OptionTest is an option for New(). It tests that the given sample item can
// be serialized to disk and deserialized successfully. This verifies that disk
// access works, and that the type can be fully serialized and deserialized
// with gob. The option can be repeated.
func OptionTest(t interface{}) Option {
	return func(qr *Qr) error {
		return qr.test(t)
	}
}

// New starts a Queue which stores files in <dir>/<prefix>-.<timestamp>.qr
// 'prefix' must be a simple ASCII string.
func New(dir, prefix string, options ...Option) (*Qr, error) {
	if len(prefix) == 0 || strings.ContainsAny(prefix, ":-/") {
		return nil, ErrInvalidPrefix
	}

	qr := Qr{
		planb:      make(chan interface{}),
		confluence: make(chan interface{}),
		out:        make(chan interface{}),
		dir:        dir,
		prefix:     prefix,
		timeout:    DefaultTimeout,
		bufferSize: DefaultBuffer,
		logf:       log.Printf,
	}
	for _, cb := range options {
		if err := cb(&qr); err != nil {
			return nil, err
		}
	}

	qr.q = make(chan interface{}, qr.bufferSize)

	var (
		filesToDisk   = make(chan string)
		filesFromDisk = make(chan string)
	)
	go qr.merge()
	go qr.swapout(filesToDisk)
	go qr.fs(filesToDisk, filesFromDisk)
	go qr.swapin(filesFromDisk)
	for _, f := range qr.findOld() {
		filesToDisk <- f
	}
	return &qr, nil
}

// Enqueue adds something in the queue. This never blocks, and is safe to be
// called by different goroutines.
func (qr *Qr) Enqueue(e interface{}) {
	select {
	case qr.q <- e:
	default:
		qr.planb <- e
	}
}

// Dequeue is the channel where elements come out the queue. It'll be closed
// on Close().
func (qr *Qr) Dequeue() <-chan interface{} {
	return qr.out
}

// Close shuts down all Go routines and closes the Dequeue() channel. It'll
// write all in-flight entries to disk. Calling Enqueue() after Close will
// panic.
func (qr *Qr) Close() {
	close(qr.q)
	// Closing planb triggers a cascade closing of all go-s and channels.
	close(qr.planb)

	// Store the in-flight entries for next time.
	filename := qr.batchFilename(0) // special filename
	fh, err := os.Create(filename)
	if err != nil {
		qr.logf("QR create err: %v", err)
		return
	}
	enc := gob.NewEncoder(fh)
	count := 0
	for e := range qr.out {
		count++
		if err = enc.Encode(&e); err != nil {
			qr.logf("QR encode err: %v", err)
		}
	}
	fh.Close()
	if count == 0 {
		// there was nothing to queue
		os.Remove(filename)
	}
}

// test tests that the given sample item can be serialized to disk and
// deserialized successfully. This verifies that disk access works, and that
// the type can be fully serialized and deserialized.
func (qr *Qr) test(i interface{}) error {
	filename := qr.testBatchFilename()

	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("create err: %v", err)
	}
	defer os.Remove(filename)
	defer f.Close()
	enc := gob.NewEncoder(f)
	if err := enc.Encode(&i); err != nil {
		return err
	}

	if f, err = os.Open(filename); err != nil {
		return fmt.Errorf("create err: %v", err)
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	var c interface{}
	if err = dec.Decode(&c); err != nil {
		return err
	}
	if !reflect.DeepEqual(i, c) {
		return fmt.Errorf("deserialization error: have %#v, want %#v", c, i)
	}
	return nil
}

func (qr *Qr) merge() {
	defer func() {
		for e := range qr.q {
			qr.out <- e
		}
		for e := range qr.confluence {
			qr.out <- e
		}
		close(qr.out)
	}()

	// read q and planb, and write them to out
	for {
		// prefer to read from Q
		select {
		case e, ok := <-qr.q:
			if !ok {
				return
			}
			qr.out <- e
			continue
		default:
		}

		// otherwise try both
		select {
		case e, ok := <-qr.q:
			if !ok {
				return
			}
			qr.out <- e
		case e, ok := <-qr.confluence:
			if !ok {
				return
			}
			qr.out <- e
		}
	}
}

func (qr *Qr) swapout(files chan<- string) {
	var (
		enc      *gob.Encoder
		filename string
		fh       io.WriteCloser
		tc       <-chan time.Time
		t        = time.NewTimer(0)
		err      error
	)
	defer func() {
		if enc != nil {
			fh.Close()
			files <- filename
		}
		close(files)
		t.Stop()
	}()
	for {
		select {
		case e, ok := <-qr.planb:
			if !ok {
				return
			}
			if enc == nil {
				filename = qr.batchFilename(time.Now().UnixNano())
				fh, err = os.Create(filename)
				if err != nil {
					// TODO: sure we return?
					qr.logf("QR create err: %v", err)
					return
				}
				enc = gob.NewEncoder(fh)
				t.Reset(qr.timeout)
				tc = t.C
			}
			if err = enc.Encode(&e); err != nil {
				qr.logf("QR encode err: %v", err)
			}
		case <-tc:
			fh.Close()
			files <- filename
			enc = nil
			tc = nil
		}
	}
}

func (qr *Qr) swapin(files <-chan string) {
	defer close(qr.confluence)
	for filename := range files {
		fh, err := os.Open(filename)
		if err != nil {
			qr.logf("QR open err: %v", err)
			continue
		}
		os.Remove(filename)
		dec := gob.NewDecoder(fh)
		for {
			var next interface{}
			if err = dec.Decode(&next); err != nil {
				if err != io.EOF {
					qr.logf("QR decode err: %v", err)
				}
				fh.Close()
				fh = nil
				break
			}
			qr.confluence <- next
		}
	}
}

func (qr *Qr) fs(in <-chan string, out chan<- string) {
	defer close(out)
	var (
		filenames []string
		checkOut  chan<- string
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

func (qr *Qr) batchFilename(n int64) string {
	return fmt.Sprintf("%s/%s-%020d%s",
		qr.dir,
		qr.prefix,
		n,
		fileExtension,
	)
}

func (qr *Qr) testBatchFilename() string {
	return fmt.Sprintf("%s/%s-test%s", qr.dir, qr.prefix, fileExtension)
}

// findOld finds .qr files from a previous run.
func (qr *Qr) findOld() []string {
	f, err := os.Open(qr.dir)
	if err != nil {
		return nil
	}
	defer f.Close()

	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil
	}

	var existing []string
	for _, n := range names {
		if !strings.HasPrefix(n, qr.prefix+"-") ||
			!strings.HasSuffix(n, fileExtension) ||
			strings.HasSuffix(n, "-test"+fileExtension) {
			continue
		}
		existing = append(existing, filepath.Join(qr.dir, n))
	}

	sort.Strings(existing)

	return existing
}
