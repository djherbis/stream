stream 
==========

[![GoDoc](https://godoc.org/github.com/djherbis/stream?status.svg)](https://godoc.org/github.com/djherbis/stream)
[![Software License](https://img.shields.io/badge/license-MIT-brightgreen.svg)](LICENSE.txt)
[![Build Status](https://travis-ci.org/djherbis/stream.svg?branch=master)](https://travis-ci.org/djherbis/stream)
[![Coverage Status](https://coveralls.io/repos/djherbis/stream/badge.svg?branch=master)](https://coveralls.io/r/djherbis/stream?branch=master)

Usage
------------

Write and Read concurrently, and independently.

```go
import(
	"io"
	"log"
	"os"
	"time"

	"github.com/djherbis/stream"
)

func main(){
	w, err := stream.New("mystream")
	if err != nil {
		log.Fatal(err)
	}

	go func(){
		io.WriteString(w, "Hello World!")
		<-time.After(time.Second)
		io.WriteString(w, "Streaming updates...")
		w.Close()
	}()

	waitForReader := make(chan struct{})
	go func(){
		// Read from the stream
		r, err := w.NextReader()
		if err != nil {
			log.Fatal(err)
		}
		io.Copy(os.Stdout, r) // Hello World! (1 second) Streaming updates...
		r.Close()
		close(waitForReader)
	}()

  // Full copy of the stream!
	r, err := w.NextReader() 
	if err != nil {
		log.Fatal(err)
	}
	io.Copy(os.Stdout, r) // Hello World! (1 second) Streaming updates...

	// r supports io.ReaderAt too.
	p := make([]byte, 4)
	r.ReadAt(p, 1) // Read "ello" into p

	r.Close()

	<-waitForReader // don't leave main before go-routine finishes
}
```

Installation
------------
```sh
go get github.com/djherbis/stream
```
