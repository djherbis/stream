// Package stream provides a way to read and write to a synchronous buffered pipe, with multiple reader support.
package stream

import (
	"errors"
	"math/rand"
	"sync"
)

// ErrRemoving is returned when requesting a Reader on a Stream which is being Removed.
var ErrRemoving = errors.New("cannot open a new reader while removing file")
var ErrCanceling = errors.New("cannot open a new reader while canceling stream")
var ErrWriteAfterClosing = errors.New("cannot write to stream after closing file")

// Stream is used to concurrently Write and Read from a File.
type Stream struct {
	mu        sync.Mutex
	grp       sync.WaitGroup
	b         *broadcaster
	file      File
	fs        FileSystem
	canceling bool
	removing  bool
	closing   bool
}

// New creates a new Stream from the StdFileSystem with Name "name".
func New(name string) (*Stream, error) {
	return NewStream(name, StdFileSystem)
}

// NewStream creates a new Stream with Name "name" in FileSystem fs.
func NewStream(name string, fs FileSystem) (*Stream, error) {
	f, err := fs.Create(name)
	sf := &Stream{
		file: f,
		fs:   fs,
		b:    newBroadcaster(),
	}
	sf.inc()
	return sf, err
}

// NewMemStream creates an in-memory stream with no name, and no underlying fs.
// This should replace uses of NewStream("name", NewMemFs()).
// Remove() is unsupported as there is no fs to remove it from.
func NewMemStream() *Stream {
	f := newMemFile("")
	s := &Stream{
		file: f,
		fs:   singletonFs{f},
		b:    newBroadcaster(),
	}
	s.inc()
	return s
}

type singletonFs struct {
	file *memFile
}

func (fs singletonFs) Create(key string) (File, error) { return nil, errors.New("unsupported") }

func (fs singletonFs) Open(key string) (File, error) { return &memReader{memFile: fs.file}, nil }

func (fs singletonFs) Remove(key string) error { return errors.New("unsupported") }

// Name returns the name of the underlying File in the FileSystem.
func (s *Stream) Name() string { return s.file.Name() }

// Write writes p to the Stream. It's concurrent safe to be called with Stream's other methods.
func (s *Stream) Write(p []byte) (int, error) {
	if s.closing {
		return 0, ErrWriteAfterClosing
	}
	n, err := s.file.Write(p)
	s.b.Wrote(n)
	return n, err
}

func (s *Stream) performClose(canceling bool) error {
	if s.closing {
		return nil
	}
	s.closing = true
	err := s.file.Close()
	if err != nil {
		s.closing = false
		return err
	}
	s.b.Close(canceling)
	s.dec()
	return nil
}

// Close will close the active stream. This will cause Readers to return EOF once they have
// read the entire stream.
func (s *Stream) Close() error {
	// If stream is removing then lock is already acquired by remove and remove is waiting for close
	if s.removing {
		return s.performClose(false)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.performClose(false)
}

// Remove will block until the Stream and all its Readers have been Closed,
// at which point it will delete the underlying file. NextReader() will return
// ErrRemoving if called after Remove.
func (s *Stream) Remove() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.canceling {
		return nil
	}
	return s.remove()
}

// This should be called inside s.mu.Lock()
func (s *Stream) remove() error {
	if s.removing {
		return nil
	}
	s.removing = true
	s.grp.Wait()
	if err := s.fs.Remove(s.file.Name()); err != nil {
		s.removing = false
		return err
	}
	return nil
}

// Cancel will close Stream and all it's Readers
// at which point it will delete the underlying file.
// NextReader() will return ErrCanceling if canceling.
func (s *Stream) Cancel() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.canceling {
		return nil
	}
	s.canceling = true
	if err := s.performClose(true); err != nil {
		s.canceling = false
		return err
	}
	if err := s.remove(); err != nil {
		s.canceling = false
		return err
	}
	return nil
}

// NextReader will return a concurrent-safe Reader for this stream. Each Reader will
// see a complete and independent view of the stream, and can Read while the stream
// is written to.
func (s *Stream) NextReader() (*Reader, error) {
	s.inc()

	if s.canceling {
		s.dec()
		return nil, ErrCanceling
	}

	if s.removing {
		s.dec()
		return nil, ErrRemoving
	}

	file, err := s.fs.Open(s.file.Name())
	if err != nil {
		s.dec()
		return nil, err
	}

	return &Reader{file: file, s: s, id: rand.Int63()}, nil
}

func (s *Stream) inc() { s.grp.Add(1) }
func (s *Stream) dec() { s.grp.Done() }
