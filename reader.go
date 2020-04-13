package stream

import (
	"errors"
	"io"
	"sync"
)

// Reader is a concurrent-safe Stream Reader.
type Reader struct {
	s         *Stream
	file      File
	fileMu    sync.RWMutex
	readMu    sync.Mutex
	readOff   int64
	closeOnce onceWithErr
}

// Name returns the name of the underlying File in the FileSystem.
func (r *Reader) Name() string { return r.file.Name() }

// ReadAt lets you Read from specific offsets in the Stream.
// ReadAt blocks while waiting for the requested section of the Stream to be written,
// unless the Stream is closed in which case it will always return immediately.
func (r *Reader) ReadAt(p []byte, off int64) (n int, err error) {
	return r.read(p, &off)
}

// Read reads from the Stream. If the end of an open Stream is reached, Read
// blocks until more data is written or the Stream is Closed.
func (r *Reader) Read(p []byte) (n int, err error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	return r.read(p, &r.readOff)
}

func (r *Reader) read(p []byte, off *int64) (n int, err error) {
	for {
		var m int
		m, err = r.s.b.UseHandle(func() (int, error) {
			r.fileMu.RLock()
			defer r.fileMu.RUnlock()
			return r.file.ReadAt(p[n:], *off)
		})
		n += m
		*off += int64(m)

		switch {
		case n != 0 && (err == nil || err == io.EOF):
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
		r.fileMu.Lock()
		err = r.file.Close()
		r.fileMu.Unlock()
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

// Seek changes the offset of the next Read in the stream.
// Seeking to Start/Current does not block for the stream to reach that position,
// so it cannot guarantee that position exists.
// Seeking to End will block until the stream is closed and then seek to that position.
// Seek is safe to call concurrently with all other methods, though calling it
// concurrently with Read will lead to an undefined order of the calls
// (ex. may Seek then Read or Read than Seek, changing which bytes are Read).
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()

	switch whence {
	default:
		return 0, errWhence
	case io.SeekStart:
	case io.SeekCurrent:
		offset += r.readOff
	case io.SeekEnd:
		if err := r.s.b.Wait(r, maxInt64); err != nil && err != io.EOF {
			return 0, err
		}
		size, _ := r.s.b.Size() // we most be closed to reach here due to ^
		offset += size
	}
	if offset < 0 {
		return 0, errOffset
	}
	r.readOff = offset
	return r.readOff, nil
}

var (
	errWhence = errors.New("Seek: invalid whence")
	errOffset = errors.New("Seek: invalid offset")
)

const maxInt64 = 1<<63 - 1
