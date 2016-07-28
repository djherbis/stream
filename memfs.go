package stream

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// ErrNotFoundInMem is returned when an in-memory FileSystem cannot find a file.
var ErrNotFoundInMem = errors.New("not found")

type memfs struct {
	mu    sync.RWMutex
	files map[string]*memFile
}

// NewMemFS returns a New in-memory FileSystem
func NewMemFS() FileSystem {
	return &memfs{
		files: make(map[string]*memFile),
	}
}

func (fs *memfs) Create(key string) (File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	file := &memFile{
		name: key,
		r:    bytes.NewBuffer(nil),
	}
	file.buf.Store([]byte(nil))
	file.memReader.memFile = file
	fs.files[key] = file
	return file, nil
}

func (fs *memfs) Open(key string) (File, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if f, ok := fs.files[key]; ok {
		return &memReader{memFile: f}, nil
	}
	return nil, ErrNotFoundInMem
}

func (fs *memfs) Remove(key string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	delete(fs.files, key)
	return nil
}

type memFile struct {
	mu   sync.Mutex
	name string
	r    *bytes.Buffer
	buf  atomic.Value
	memReader
}

func (f *memFile) Name() string {
	return f.name
}

func (f *memFile) Write(p []byte) (int, error) {
	if len(p) > 0 {
		f.mu.Lock()
		n, err := f.r.Write(p)
		f.buf.Store(f.r.Bytes())
		f.mu.Unlock()
		return n, err
	}
	return len(p), nil
}

func (f *memFile) Bytes() []byte {
	return f.buf.Load().([]byte)
}

func (f *memFile) Close() error {
	return nil
}

type memReader struct {
	*memFile
	n int
}

func (r *memReader) ReadAt(p []byte, off int64) (n int, err error) {
	data := r.Bytes()
	if int64(len(data)) < off {
		return 0, io.EOF
	}
	n, err = bytes.NewReader(data[off:]).ReadAt(p, 0)
	return n, err
}

func (r *memReader) Read(p []byte) (n int, err error) {
	n, err = bytes.NewReader(r.Bytes()[r.n:]).Read(p)
	r.n += n
	return n, err
}

func (r *memReader) Close() error {
	return nil
}
