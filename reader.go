package stream

import (
	"io"
	"sync"
)

// Reader is a concurrent-safe Stream Reader.
type Reader struct {
	s         *Stream
	file      File
	mu        sync.Mutex
	readOff   int64
	closeOnce onceWithErr
}

// Name returns the name of the underlying File in the FileSystem.
func (r *Reader) Name() string { return r.file.Name() }

// ReadAt lets you Read from specific offsets in the Stream.
// ReadAt blocks while waiting for the requested section of the Stream to be written,
// unless the Stream is closed in which case it will always return immediately.
func (r *Reader) ReadAt(p []byte, off int64) (n int, err error) {
	return r.read(p, func(p []byte) (int, error) {
		return r.file.ReadAt(p, off)
	}, &off)
}

// Read reads from the Stream. If the end of an open Stream is reached, Read
// blocks until more data is written or the Stream is Closed.
func (r *Reader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.read(p, r.file.Read, &r.readOff)
}

type readerFunc func([]byte) (int, error)

func (r *Reader) read(p []byte, readFunc readerFunc, off *int64) (n int, err error) {
	for {
		var m int
		m, err = r.s.b.UseHandle(func() (int, error) {
			return readFunc(p[n:])
		})
		n += m
		*off += int64(m)

		switch {
		case n != 0 && err == nil:
			return n, nil

		case err == io.EOF:
			if err := r.s.b.Wait(r, *off); err != nil {
				return n, r.checkErr(err)
			}

		case err != nil:
			return n, r.checkErr(err)
		}
	}
}

func (r *Reader) checkErr(err error) error {
	switch err {
	case ErrCanceled:
		r.Close()
	}
	return err
}

// Close closes this Reader on the Stream. This must be called when done with the
// Reader or else the Stream cannot be Removed.
func (r *Reader) Close() error {
	return r.closeOnce.Do(func() (err error) {
		err = r.file.Close()
		r.s.b.DropReader(r)
		return err
	})
}

// Size returns the current size of the entire stream (not the remaining bytes to be read),
// and true iff the size is valid (not canceled), and final (won't change).
// Can be safely called concurrently with all other methods.
func (r *Reader) Size() (int64, bool) {
	return r.s.b.Size()
}
