package stream

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"
)

var (
	testdata = []byte("hello\nworld\n")
	errFail  = errors.New("fail")
)

type badFs struct {
	readers []File
}
type badFile struct{ name string }

func (r badFile) Name() string                            { return r.name }
func (r badFile) Read(p []byte) (int, error)              { return 0, errFail }
func (r badFile) ReadAt(p []byte, off int64) (int, error) { return 0, errFail }
func (r badFile) Write(p []byte) (int, error)             { return 0, errFail }
func (r badFile) Close() error                            { return errFail }

func (fs badFs) Create(name string) (File, error) { return os.Create(name) }
func (fs badFs) Open(name string) (File, error) {
	if len(fs.readers) > 0 {
		f := fs.readers[len(fs.readers)-1]
		fs.readers = fs.readers[:len(fs.readers)-1]
		return f, nil
	}
	return nil, errFail
}
func (fs badFs) Remove(name string) error { return os.Remove(name) }

func TestMemFs(t *testing.T) {
	fs := NewMemFS()
	if _, err := fs.Open("not found"); err != ErrNotFoundInMem {
		t.Error(err)
		t.FailNow()
	}
}

func TestSingletonFs(t *testing.T) {
	var fs singletonFs
	if _, err := fs.Create(""); err == nil {
		t.Errorf("expected unsupported, got %s", err)
	}
}

func TestBadFile(t *testing.T) {
	fs := badFs{readers: make([]File, 0, 1)}
	fs.readers = append(fs.readers, badFile{name: "test"})
	f, err := NewStream("test", fs)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer cleanup(f, t)
	defer f.Close()

	r, err := f.NextReader()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer r.Close()
	if r.Name() != "test" {
		t.Errorf("expected name to to be 'test' got %s", r.Name())
		t.FailNow()
	}
	if _, err := r.ReadAt(nil, 0); err == nil {
		t.Error("expected ReadAt error")
		t.FailNow()
	}
	if _, err := r.Read(nil); err == nil {
		t.Error("expected Read error")
		t.FailNow()
	}
}

func TestBadFs(t *testing.T) {
	f, err := NewStream("test", badFs{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer cleanup(f, t)
	defer f.Close()

	r, err := f.NextReader()
	if err == nil {
		t.Error("expected open error")
		t.FailNow()
	} else {
		return
	}
	r.Close()
}

func TestStd(t *testing.T) {
	f, err := New(t.Name() + ".txt")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if f.Name() != t.Name()+".txt" {
		t.Errorf("expected name to be %s.txt: %s", t.Name(), f.Name())
	}
	testFile(f, t)
}

func TestMem(t *testing.T) {
	f, err := NewStream(t.Name()+".txt", NewMemFS())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	f.Write(nil)
	testFile(f, t)
}

func TestCancelClosesAll(t *testing.T) {
	f, err := New(t.Name() + ".txt")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	for i := 0; i < 10; i++ {
		_, err := f.NextReader() // we won't even grab the reader
		if err != nil {
			t.Error(err)
			t.FailNow()
		}
	}
	f.Cancel()    // Closes all the idle Readers
	cleanup(f, t) // this will deadlock if any arn't Closed
}

func TestCloseUnblocksBlockingRead(t *testing.T) {
	f, err := New(t.Name() + ".txt")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	r, err := f.NextReader()
	go func() {
		_, err := ioutil.ReadAll(r)
		if err == nil || err == io.EOF {
			t.Error("exected an error on a blocking Read for a closed Reader")
		}
	}()
	time.AfterFunc(100*time.Millisecond, func() {
		r.Close()
		f.Close()
	})
	cleanup(f, t) // this will deadlock if any arn't Closed
}

func TestCancelBeforeClose(t *testing.T) {
	f, err := New(t.Name() + ".txt")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	f.Write([]byte("Hello"))
	r, err := f.NextReader() // blocking reader
	if err != nil {
		t.Error("error creating new reader: ", err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_, err := ioutil.ReadAll(r)
		if err != ErrCanceled {
			t.Error("Read after cancel should return an error")
		}
		wg.Done()
	}()
	<-time.After(100 * time.Millisecond) // give Reader time to block, this tests it unblocks

	// When canceling writer, reader is closed, so writer unblocks and test passes
	f.Cancel()

	// ReadAt after cancel
	_, err = ioutil.ReadAll(io.NewSectionReader(r, 0, 1))
	if err != ErrCanceled {
		t.Error("ReadAt after cancel should return an error")
	}

	// NextReader should fail as well
	_, err = f.NextReader()
	if err != ErrCanceled {
		t.Error("NextReader should be canceled, but got: ", err)
	}

	n, err := f.Write([]byte("world"))
	// Writer is closed as well
	if err == nil {
		t.Error("expected write after canceling to fail")
	}
	if n != 0 {
		t.Error("expected write after canceling to not write anything")
	}
	wg.Wait()
	cleanup(f, t)
}

func TestCancelAfterClose(t *testing.T) {
	f, err := New(t.Name() + ".txt")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	f.Write([]byte("Hello"))
	f.Close()

	r, _ := f.NextReader()

	// When canceling writer, reader is unblocked, so writer unblocks and test passes
	f.Cancel()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_, err := ioutil.ReadAll(r)
		if err != ErrCanceled {
			t.Error("Read after canceling (even after close) should fail")
		}
		wg.Done()
	}()

	wg.Wait()

	cleanup(f, t)
}

func TestMemStreams(t *testing.T) {
	testFile(NewMemStream(), t)
}

func TestReadAtWait(t *testing.T) {
	f, err := NewStream(t.Name()+".txt", NewMemFS())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	r, err := f.NextReader()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	io.WriteString(f, "hello")
	go func() {
		<-time.After(100 * time.Millisecond)
		io.WriteString(f, " world")
		f.Close()
	}()
	data := make([]byte, 11)
	_, err = r.ReadAt(data, 0)
	if err != nil {
		t.Error(err)
	}
	if string(data) != "hello world" {
		t.Error("expected to read 'hello world' got ", string(data))
	}
}

func TestRemove(t *testing.T) {
	f, err := NewStream(t.Name()+".txt", NewMemFS())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	r, err := f.NextReader()
	if err != nil {
		t.Error("should return valid reader before remove")
	}
	fmt.Fprintf(f, "Hello")
	go cleanup(f, t)
	<-time.After(100 * time.Millisecond)
	waitForReadToFinish := make(chan struct{})
	go func() {
		b, err := ioutil.ReadAll(r)
		r.Close()
		if err != nil {
			t.Errorf("reader should continue as if nothing happens after remove, got: %s", err.Error())
		}
		if string(b) != "Hello World" {
			t.Errorf("reader should read all written data aven after remove, got: %s", string(b))
		}
		close(waitForReadToFinish)
	}()
	r2, err := f.NextReader()
	switch err {
	case ErrRemoving:
	case nil:
		t.Error("expected error on NextReader()")
		r2.Close()
	default:
		t.Error("expected diff error on NextReader()", err)
	}

	// It should still succeed as we didn't closed stream yet what causes a remove
	fmt.Fprintf(f, " World")

	f.Close()

	n, err := fmt.Fprintf(f, " War")

	if err == nil {
		t.Errorf("Write to stream after closing should fail, has: %v", err)
	}

	if n != 0 {
		t.Error("expected write after closing to not write anything")
	}
	<-waitForReadToFinish
}

func testFile(f *Stream, t *testing.T) {

	for i := 0; i < 10; i++ {
		go testReader(f, t)
	}

	for i := 0; i < 10; i++ {
		f.Write(testdata[:10])
		<-time.After(10 * time.Millisecond)
		f.Write(testdata[10:])
	}

	if err := f.Close(); err != nil {
		t.Error("expected succesful close, got ", err)
	}
	testReader(f, t)
	cleanup(f, t)
}

func testReader(f *Stream, t *testing.T) {
	r, err := f.NextReader()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	defer func() {
		err = r.Close()
		if err != nil {
			t.Error("expected successful close, got ", err)
		}

		_, err = ioutil.ReadAll(r)
		if err == nil {
			t.Error("Read after Reader.Close() should return an error")
		}

		_, err = ioutil.ReadAll(io.NewSectionReader(r, 0, 1))
		if err == nil {
			t.Error("ReadAt after Reader.Close() should return an error")
		}
	}()

	buf := bytes.NewBuffer(nil)
	sr := io.NewSectionReader(r, 1+int64(len(testdata)*5), 5)
	io.CopyBuffer(buf, sr, make([]byte, 3)) // force multiple reads
	if !bytes.Equal(buf.Bytes(), testdata[1:6]) {
		t.Errorf("unequal %s", buf.Bytes())
		return
	}

	buf.Reset()
	io.CopyBuffer(buf, r, make([]byte, 3)) // force multiple reads
	if !bytes.Equal(buf.Bytes(), bytes.Repeat(testdata, 10)) {
		t.Errorf("unequal %s", buf.Bytes())
		return
	}
	l, closed := r.Size()
	if !closed {
		t.Errorf("expected writer to be closed, but was open")
	}
	if l != int64(len(testdata)*10) {
		t.Errorf("expected size to be %d but got %d", len(testdata)*10, l)
	}
}

func cleanup(f *Stream, t *testing.T) {
	if err := f.Remove(); err != nil && err != ErrUnsupported {
		t.Error("error while removing file: ", err)
	}
}
