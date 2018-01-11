// Package stream provides a way to read and write to a synchronous buffered pipe, with multiple reader support.
package stream

import (
	"errors"
	"sync"
)

// ErrUnsupported is returned when an operation is not supported.
var ErrUnsupported = errors.New("unsupported")

// Stream is used to concurrently Write and Read from a File.
type Stream struct {
	mu        sync.Mutex
	b         *broadcaster
	file      File
	fs        FileSystem
	closeOnce onceWithErr
}

// New creates a new Stream from the StdFileSystem with Name "name".
func New(name string) (*Stream, error) {
	return NewStream(name, StdFileSystem)
}

// NewStream creates a new Stream with Name "name" in FileSystem fs.
func NewStream(name string, fs FileSystem) (*Stream, error) {
	f, err := fs.Create(name)
	return newStream(f, fs), err
}

// NewMemStream creates an in-memory stream with no name, and no underlying fs.
// This should replace uses of NewStream("name", NewMemFs()).
// Remove() is unsupported as there is no fs to remove it from.
func NewMemStream() *Stream {
	f := newMemFile("")
	return newStream(f, singletonFs{f})
}

func newStream(file File, fs FileSystem) *Stream {
	return &Stream{
		file: file,
		fs:   fs,
		b:    newBroadcaster(),
	}
}

type singletonFs struct {
	file *memFile
}

func (fs singletonFs) Create(key string) (File, error) { return nil, ErrUnsupported }

func (fs singletonFs) Open(key string) (File, error) { return &memReader{memFile: fs.file}, nil }

func (fs singletonFs) Remove(key string) error { return ErrUnsupported }

// Name returns the name of the underlying File in the FileSystem.
func (s *Stream) Name() string { return s.file.Name() }

// Write writes p to the Stream. It's concurrent safe to be called with Stream's other methods.
func (s *Stream) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, err := s.file.Write(p)
	s.b.Wrote(n)
	return n, err
}

// Close will close the active stream. This will cause Readers to return EOF once they have
// read the entire stream.
func (s *Stream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeOnce.Do(func() (err error) {
		err = s.file.Close()
		s.b.Close()
		return err
	})
}

// Remove will block until the Stream and all its Readers have been Closed,
// at which point it will delete the underlying file. NextReader() will return
// ErrRemoving if called after Remove.
func (s *Stream) Remove() error {
	s.ShutdownWithErr(ErrRemoving)
	return s.fs.Remove(s.file.Name())
}

// ShutdownWithErr causes NextReader to stop creating new Readers and instead return err, this
// method also blocks until all Readers and the Writer have closed.
func (s *Stream) ShutdownWithErr(err error) {
	if err == nil {
		return
	}
	s.b.PreventNewHandles(err) // no new readers can be created, but existing ones can finish, same with the writer
	s.b.WaitForZeroHandles()   // wait for exiting handles to finish up
}

// Cancel signals that this Stream is forcibly ending, NextReader() will fail, existing readers will fail Reads, all Readers & Writer are Closed.
// This call is non-blocking, and Remove() after this call is non-blocking.
func (s *Stream) Cancel() error {
	s.b.Cancel()     // all existing reads are canceled, no new reads will occur, all readers closed
	return s.Close() // all writes are stopped
}

// NextReader will return a concurrent-safe Reader for this stream. Each Reader will
// see a complete and independent view of the stream, and can Read while the stream
// is written to.
func (s *Stream) NextReader() (*Reader, error) {
	return s.b.NewReader(func() (*Reader, error) {
		file, err := s.fs.Open(s.file.Name())
		if err != nil {
			return nil, err
		}
		return &Reader{file: file, s: s}, nil
	})
}
