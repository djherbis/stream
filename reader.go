package stream

import (
	"io"
	"sync"
)

// Reader is a concurrent-safe Stream Reader.
type Reader struct {
	s       *Stream
	file    File
	mu      sync.Mutex
	readOff int64
}

// Name returns the name of the underlying File in the FileSystem.
func (r *Reader) Name() string { return r.file.Name() }

// ReadAt lets you Read from specific offsets in the Stream.
// ReadAt blocks while waiting for the requested section of the Stream to be written,
// unless the Stream is closed in which case it will always return immediately.
func (r *Reader) ReadAt(p []byte, off int64) (n int, err error) {
	var m int

	for {

		m, err = r.file.ReadAt(p[n:], off)
		n += m
		off += int64(m)

		switch {
		case n != 0 && err == nil:
			return n, err
		case err == io.EOF:
			if v, open := r.s.b.Wait(off); v == 0 && !open {
				return n, io.EOF
			}
		case err != nil:
			return n, err
		}

	}
}

// Read reads from the Stream. If the end of an open Stream is reached, Read
// blocks until more data is written or the Stream is Closed.
func (r *Reader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var m int

	for {
		m, err = r.file.Read(p[n:])
		n += m
		r.readOff += int64(m)

		switch {
		case n != 0 && err == nil:
			return n, nil
		case err == io.EOF:
			if v, open := r.s.b.Wait(r.readOff); v == 0 && !open {
				return n, io.EOF
			}
		case err != nil:
			return n, err
		}
	}
}

// Close closes this Reader on the Stream. This must be called when done with the
// Reader or else the Stream cannot be Removed.
func (r *Reader) Close() error {
	defer r.s.dec()
	return r.file.Close()
}

// Size returns the current size of the entire stream (not the remaining bytes to be read),
// and true iff the the stream writer has been closed. If closed, the size will no longer change.
// Can be safely called concurrently with all other methods.
func (r *Reader) Size() (int64, bool) {
	return r.s.b.Size()
}
