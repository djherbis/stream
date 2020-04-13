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

type slowFs struct {
	fs FileSystem
}
type slowFile struct {
	file File
}

func (r slowFile) Name() string { return r.file.Name() }
func (r slowFile) Read(p []byte) (int, error) {
	time.Sleep(5 * time.Millisecond)
	return r.file.Read(p)
}
func (r slowFile) ReadAt(p []byte, off int64) (int, error) {
	time.Sleep(5 * time.Millisecond)
	return r.file.ReadAt(p, off)
}
func (r slowFile) Write(p []byte) (int, error) {
	time.Sleep(10 * time.Millisecond)
	return r.file.Write(p)
}
func (r slowFile) Close() error {
	time.Sleep(25 * time.Millisecond)
	return r.file.Close()
}

func (fs slowFs) Create(name string) (File, error) {
	time.Sleep(5 * time.Millisecond)
	file, err := fs.fs.Create(name)
	if err != nil {
		return nil, err
	}
	return &slowFile{file}, nil
}
func (fs slowFs) Open(name string) (File, error) {
	time.Sleep(5 * time.Millisecond)
	file, err := fs.fs.Open(name)
	if err != nil {
		return nil, err
	}
	return &slowFile{file}, nil
}
func (fs slowFs) Remove(name string) error {
	time.Sleep(25 * time.Millisecond)
	return fs.fs.Remove(name)
}

func GetFilesystems() []FileSystem {
	return []FileSystem{
		NewMemFS(),
		&slowFs{NewMemFS()},
		StdFileSystem,
	}
}

func TestMemFs(t *testing.T) {
	fs := NewMemFS()
	if _, err := fs.Open("not found"); err != ErrNotFoundInMem {
		t.Error(err)
		t.FailNow()
	}


	want := "hello"
	w, _ := fs.Create("file")
	io.WriteString(w, want)

	r, _ := fs.Open("file")
	data, _ := ioutil.ReadAll(r)
	got := string(data)

	if want != got {
		t.Errorf("Want/got: %v/%v", want, got)
	}

	r.Close()
	if _, err := ioutil.ReadAll(r); err != os.ErrClosed {
		t.Errorf("wanted ErrClosed got %v", err)
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

// Extra test for slow filesystem
func TestSlowMem(t *testing.T) {
	f, err := NewStream(t.Name()+".txt", &slowFs{NewMemFS()})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	f.Write(nil)
	testFile(f, t)
}

func TestCancelClosesAll(t *testing.T) {
	for _, fs := range GetFilesystems() {
		func() {
			f, err := NewStream(t.Name()+".txt", fs)
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
			f.Cancel() // Closes all the idle Readers
			// Double cancel should not affect the outcome
			f.Cancel()
			cleanup(f, t) // this will deadlock if any arn't Closed
		}()
	}
}

func TestCloseUnblocksBlockingRead(t *testing.T) {
	for _, fs := range GetFilesystems() {
		testCloseUnblocksBlockingRead(t, fs)
	}
}

func testCloseUnblocksBlockingRead(t *testing.T, fs FileSystem) {
	f, err := NewStream(t.Name()+".txt", fs)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	r, err := f.NextReader()
	if err != nil {
		t.Error(err)
	}
	go func() {
		_, err := ioutil.ReadAll(r)
		if err == nil || err == io.EOF {
			t.Error("expected an error on a blocking Read for a closed Reader")
		}
	}()
	<-time.After(50 * time.Millisecond) // wait for blocking read
	r.Close()

	<-time.After(50 * time.Millisecond) // wait until read has been unblocked
	f.Close()

	cleanup(f, t) // this will deadlock if any arn't Closed
}

func TestCancelBeforeClose(t *testing.T) {
	for _, fs := range GetFilesystems() {
		testCancelBeforeClose(t, fs)
	}
}

func testCancelBeforeClose(t *testing.T, fs FileSystem) {
	f, err := NewStream(t.Name()+".txt", fs)
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
	<-time.After(50 * time.Millisecond) // give Reader time to block, this tests it unblocks

	// When canceling writer, reader is closed, so writer unblocks and test passes
	f.Cancel()
	// Double cancel should not affect the outcome
	f.Cancel()
	// Close after cancel should not affect the outcome
	f.Close()

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
	for _, fs := range GetFilesystems() {
		testCancelAfterClose(t, fs)
	}
}

func testCancelAfterClose(t *testing.T, fs FileSystem) {
	f, err := NewStream(t.Name()+".txt", fs)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	r, _ := f.NextReader()

	wg := sync.WaitGroup{}

	wg.Add(2)

	f.Write([]byte("Hello"))
	f.Close()

	// This unblocks and cancels any future reads
	f.Cancel()

	go func() {
		time.Sleep(50 * time.Millisecond)
		_, err := f.NextReader()
		if err != ErrCanceled {
			t.Error("Opening new reader after canceling should fail")
		}
		wg.Done()
	}()

	go func() {
		time.Sleep(50 * time.Millisecond)
		_, err := ioutil.ReadAll(r)
		if err != ErrCanceled {
			t.Error("If canceling after closing, already opened readers should finish")
		}
		wg.Done()
	}()

	wg.Wait()

	cleanup(f, t)
}

func TestShutdownAfterClose(t *testing.T) {
	for _, fs := range GetFilesystems() {
		testShutdownAfterClose(t, fs)
	}
}

func testShutdownAfterClose(t *testing.T, fs FileSystem) {
	f, err := NewStream(t.Name()+".txt", fs)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	r, _ := f.NextReader()

	wg := sync.WaitGroup{}

	wg.Add(3)
	er := errors.New("shutdown")

	go func() {
		f.Write([]byte("Hello"))
		f.Close()
		f.ShutdownWithErr(er)
		wg.Done()
	}()

	go func() {
		time.Sleep(50 * time.Millisecond)
		_, err := f.NextReader()
		if err != er {
			t.Error("Opening new reader after canceling should fail")
		}
		wg.Done()
	}()

	go func() {
		time.Sleep(50 * time.Millisecond)
		defer r.Close()
		_, err := ioutil.ReadAll(r)
		if err != nil {
			t.Error("Shutdown should allow for any already created readers to finish reading")
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
	for _, fs := range GetFilesystems() {
		testReadAtWait(t, fs)
	}
}

func testReadAtWait(t *testing.T, fs FileSystem) {
	f, err := NewStream(t.Name()+".txt", fs)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer f.Remove()
	r, err := f.NextReader()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer r.Close()
	io.WriteString(f, "hello")
	go func() {
		<-time.After(50 * time.Millisecond)
		io.WriteString(f, " world")
		f.Close()
	}()
	data := make([]byte, 5)
	_, err = r.ReadAt(data, 6)
	if err != nil {
		t.Error(err)
	}
	want := "world"
	if string(data) != want {
		t.Errorf("expected to read %q got %q", want, string(data))
	}
}

func TestShutdown(t *testing.T) {
	f, err := NewStream(t.Name()+".txt", NewMemFS())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	r, err := f.NextReader()
	if err != nil {
		t.Error("should return valid reader before remove")
	}
	io.WriteString(f, "Hello")

	f.ShutdownWithErr(nil) // noop
	n, err := r.Read(make([]byte, 1))
	if err != nil || n != 1 {
		t.Errorf("expected successful read, got %s, bytes=%d.", err, n)
	}

	er := errors.New("shutdown")

	go func() {
		_, err := f.NextReader()
		if err != er {
			t.Errorf("expected %s, got %s", er, err)
		}

		f.Close()
		io.Copy(ioutil.Discard, r)
		r.Close()
	}()

	f.ShutdownWithErr(er)
}

func TestRemove(t *testing.T) {
	for _, fs := range GetFilesystems() {
		testRemove(t, fs)
	}
}

func testRemove(t *testing.T, fs FileSystem) {
	f, err := NewStream(t.Name()+".txt", fs)
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
	<-time.After(50 * time.Millisecond)
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
		<-time.After(5 * time.Millisecond)
		f.Write(testdata[10:])
	}

	if err := f.Close(); err != nil {
		t.Error("expected successful close, got ", err)
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

func TestSeeker(t *testing.T) {
	for _, fs := range GetFilesystems() {
		testSeeker(t, fs)
	}
}

func testSeeker(t *testing.T, fs FileSystem) {
	f, err := NewStream(t.Name()+".txt", fs)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer f.Remove()
	output := "012345"
	go func() {
		io.WriteString(f, output)
		<-time.After(5 * time.Millisecond)
		f.Close()
	}()
	r, _ := f.NextReader()

	n := int64(len(output))
	for i := int64(-1); i >= -n; i-- {
		off, err := r.Seek(i, io.SeekEnd)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := off, n+i; want != got {
			t.Errorf("Want/got wrong offset: %v, %v", want, got)
		}
		data, err := ioutil.ReadAll(io.LimitReader(r, 1))
		if err != nil {
			t.Fatal(err)
		}
		if got, want := string(data), output[n+i:n+i+1]; want != got {
			t.Errorf("Want/got wrong: %v, %v", want, got)
		}
	}

	for i := int64(0); i < n; i++ {
		off, err := r.Seek(i, io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := off, i; want != got {
			t.Errorf("Want/got wrong offset: %v, %v", want, got)
		}
		data, err := ioutil.ReadAll(io.LimitReader(r, 1))
		if err != nil {
			t.Fatal(err)
		}
		if got, want := string(data), output[i:i+1]; want != got {
			t.Errorf("Want/got wrong: %v, %v", want, got)
		}
	}

	r.Seek(0, io.SeekStart)

	for i := int64(1); i < n; i += 2 {
		off, err := r.Seek(1, io.SeekCurrent)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := off, i; want != got {
			t.Errorf("Want/got wrong offset: %v, %v", want, got)
		}
		data, err := ioutil.ReadAll(io.LimitReader(r, 1))
		if err != nil {
			t.Fatal(err)
		}
		if got, want := string(data), output[i:i+1]; want != got {
			t.Errorf("Want/got wrong: %v, %v", want, got)
		}
	}

	if _, err := r.Seek(0, 100); err != errWhence {
		t.Errorf("Expected errWhence")
	}

	if _, err := r.Seek(-1, io.SeekStart); err != errOffset {
		t.Errorf("Expected errOffset")
	}

	f.Cancel()
	if _, err := r.Seek(0, io.SeekEnd); err != ErrCanceled {
		t.Fatal(err)
	}
}
