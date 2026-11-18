package stream_test

import (
	"archive/zip"
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/djherbis/stream"
)

// Pause a real short ReadAt after it has observed the incomplete file. This
// lets the writer append the rest before the captured result reaches Reader.
type shortReadFS struct {
	stream.FileSystem
	armed    chan struct{}
	observed chan struct{}
	resume   chan struct{}
	once     sync.Once
}

type shortReadFile struct {
	stream.File
	fs *shortReadFS
}

func (fs *shortReadFS) Open(name string) (stream.File, error) {
	f, err := fs.FileSystem.Open(name)
	if err != nil {
		return nil, err
	}
	return &shortReadFile{File: f, fs: fs}, nil
}

func (f *shortReadFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off)
	select {
	case <-f.fs.armed:
	default:
		return n, err
	}
	if n > 0 && err == io.EOF {
		f.fs.once.Do(func() {
			close(f.fs.observed)
			<-f.fs.resume
		})
	}
	return n, err
}

func TestReaderAtGrowingZip(t *testing.T) {
	var archive bytes.Buffer
	zw := zip.NewWriter(&archive)
	w, err := zw.Create("payload.txt")
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("the complete archived contents")
	if _, err := w.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	data := archive.Bytes()

	for name, baseFS := range map[string]stream.FileSystem{
		"memory": stream.NewMemFS(),
		"file":   stream.StdFileSystem,
	} {
		t.Run(name, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "stream-readat")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			fs := &shortReadFS{FileSystem: baseFS, armed: make(chan struct{}), observed: make(chan struct{}), resume: make(chan struct{})}
			s, err := stream.NewStream(filepath.Join(dir, "archive.zip"), fs)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Cancel()
			r, err := s.NextReader()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			var resumeOnce sync.Once
			resume := func() { resumeOnce.Do(func() { close(fs.resume) }) }
			defer resume()

			split := len(data) / 2
			if _, err := s.Write(data[:split]); err != nil {
				t.Fatal(err)
			}
			// Read must still return the available prefix promptly on an open stream.
			prefix := make([]byte, len(data))
			if n, err := r.Read(prefix); n != split || err != nil || !bytes.Equal(prefix[:n], data[:split]) {
				t.Fatalf("Read = (%d, %v), want the available %d-byte prefix", n, err, split)
			}
			if _, err := r.Seek(1, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			type result struct {
				archive *zip.Reader
				err     error
			}
			close(fs.armed)
			done := make(chan result, 1)
			go func() {
				zr, err := zip.NewReader(r, int64(len(data)))
				done <- result{zr, err}
			}()
			select {
			case <-fs.observed:
			case <-time.After(5 * time.Second):
				t.Fatal("ZIP reader did not reach the incomplete file")
			}
			if _, err := s.Write(data[split:]); err != nil {
				t.Fatal(err)
			}
			resume()
			var got result
			select {
			case got = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("ZIP reader did not finish after the remaining bytes were written")
			}
			if got.err != nil {
				t.Fatal(got.err)
			}
			if len(got.archive.File) != 1 || got.archive.File[0].Name != "payload.txt" {
				t.Fatal("unexpected ZIP directory")
			}
			content, err := got.archive.File[0].Open()
			if err != nil {
				t.Fatal(err)
			}
			gotPayload, err := ioutil.ReadAll(content)
			content.Close()
			if err != nil || !bytes.Equal(gotPayload, payload) {
				t.Fatalf("ZIP payload = %q, %v", gotPayload, err)
			}
			one := make([]byte, 1)
			if n, err := r.Read(one); n != 1 || err != nil || one[0] != data[1] {
				t.Fatalf("ReadAt changed sequential position: Read = (%d, %v), data = %q", n, err, one)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			last := make([]byte, 4)
			if n, err := r.ReadAt(last, int64(len(data)-2)); n != 2 || err != io.EOF || !bytes.Equal(last[:n], data[len(data)-2:]) {
				t.Fatalf("short final ReadAt = (%d, %v), data = %q", n, err, last[:n])
			}
		})
	}
}

func TestReaderEmptyRead(t *testing.T) {
	for _, at := range []bool{false, true} {
		name := "Read"
		if at {
			name = "ReadAt"
		}
		t.Run(name, func(t *testing.T) {
			s := stream.NewMemStream()
			defer s.Cancel()
			r, err := s.NextReader()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			done := make(chan error, 1)
			go func() {
				var err error
				if at {
					_, err = r.ReadAt(nil, 0)
				} else {
					_, err = r.Read(nil)
				}
				done <- err
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				s.Cancel()
				<-done
				t.Fatal("empty read did not return")
			}
		})
	}
}
