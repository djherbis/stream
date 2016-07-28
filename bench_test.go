package stream

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"sync"
	"testing"
)

const testDataSize = 10 * 1024 * 1024

func BenchmarkInMemoryStream(b *testing.B) {
	benchmarkStream(NewMemFS(), b)
}

func BenchmarkOnDiskStream(b *testing.B) {
	benchmarkStream(StdFileSystem, b)
}

func benchmarkStream(fs FileSystem, b *testing.B) {
	b.ReportAllocs()

	// load random bytes
	rnd := io.LimitReader(rand.New(rand.NewSource(0)), testDataSize)
	buf := bytes.NewBuffer(nil)
	io.Copy(buf, rnd)

	// allocate stream
	w, err := NewStream("hello", fs)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Remove()

	// test parallel writer/reader speed
	var once sync.Once
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// fill writer
			once.Do(func() {
				go func() {
					io.Copy(w, buf)
					w.Close()
				}()
			})

			// read all
			r, _ := w.NextReader()
			io.Copy(ioutil.Discard, r)
			r.Close()
		}
	})
}
