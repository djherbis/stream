package stream

import (
	"errors"
	"io"
	"sync"
)

// ErrRemoving is returned when reading a Reader on a Stream which is being Removed.
var ErrCanceled = errors.New("reader has been canceled")

// Reader is a concurrent-safe Stream Reader.
type Reader struct {
	id       int64
	s        *Stream
	file     File
	mu       sync.Mutex
	readOff  int64
	close    bool
	cancel   bool
	closing  bool
	closeErr error
}

// Name returns the name of the underlying File in the FileSystem.
func (r *Reader) Name() string { return r.file.Name() }

// ReadAt lets you Read from specific offsets in the Stream.
// ReadAt blocks while waiting for the requested section of the Stream to be written,
// unless the Stream is closed in which case it will always return immediately.
func (r *Reader) ReadAt(p []byte, off int64) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var m int

	for {
		if r.cancel {
			return 0, ErrCanceled
		}

		if r.closing {
			return 0, nil
		}

		m, err = r.file.ReadAt(p[n:], off)
		n += m
		off += int64(m)

		switch {
		case n != 0 && err == nil:
			return n, err
		case err == io.EOF:
			if v, open, canceled := r.s.b.Wait(off, r.id); v == 0 {
				if canceled || r.cancel {
					r.cancel = true
					r.closeUnsafe()
					return 0, ErrCanceled
				} else if !open || r.close {
					r.closeUnsafe()
					return n, io.EOF
				}
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
		if r.cancel {
			return 0, ErrCanceled
		}

		if r.closing {
			return 0, io.EOF
		}

		m, err = r.file.Read(p[n:])
		n += m
		r.readOff += int64(m)

		switch {
		case n != 0 && err == nil:
			return n, nil
		case err == io.EOF:
			if v, open, canceled := r.s.b.Wait(r.readOff, r.id); v == 0 {
				if canceled || r.cancel {
					r.cancel = true
					r.closeUnsafe()
					return 0, ErrCanceled
				} else if !open || r.close {
					r.closeUnsafe()
					return n, io.EOF
				}
			}
		case err != nil:
			return n, err
		}
	}
}

// Close closes this Reader on the Stream. This must be called when done with the
// Reader or else the Stream cannot be Removed.
func (r *Reader) Close() error {
	r.close = true
	r.s.b.Unblock(r.id)
	r.mu.Lock()
	r.mu.Unlock()
	return r.closeUnsafe()
}

// Cancel closes this Reader on the Stream and fails next reads from reader.
// It immediately cancels all pending reads
func (r *Reader) Cancel() error {
	r.cancel = true
	r.s.b.Unblock(r.id)
	r.mu.Lock()
	r.mu.Unlock()
	return r.closeUnsafe()
}

// This should be called inside r.mu.Lock()
func (r *Reader) closeUnsafe() error {
	if r.closing {
		return r.closeErr
	}
	r.closing = true
	defer r.s.dec()
	r.closeErr = r.file.Close()
	r.s.b.Closed(r.id)
	return r.closeErr
}

// Size returns the current size of the entire stream (not the remaining bytes to be read),
// and true iff the the stream writer has been closed. If closed, the size will no longer change.
// Can be safely called concurrently with all other methods.
func (r *Reader) Size() (int64, bool) {
	return r.s.b.Size()
}
