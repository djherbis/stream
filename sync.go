package stream

import (
	"errors"
	"io"
	"os"
	"sync"
)

// ErrRemoving is returned when requesting a Reader on a Stream which is being Removed.
var ErrRemoving = errors.New("cannot open a new reader while removing file")

// ErrCanceled indicates that stream has been canceled.
var ErrCanceled = errors.New("stream has been canceled")

type streamState int

const (
	openState streamState = iota
	closedState
	canceledState
)

type broadcaster struct {
	mu           sync.RWMutex
	cond         *sync.Cond
	state        streamState
	size         int64
	newHandleErr error
	rs           *readerSet
	fileInUse    sync.WaitGroup
}

func newBroadcaster() *broadcaster {
	var b broadcaster
	b.cond = sync.NewCond(b.mu.RLocker())
	b.rs = newReaderSet()
	b.addHandle()
	return &b
}

// Wait blocks until we've written past the given offset, or until closed.
func (b *broadcaster) Wait(r *Reader, off int64) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for b.state == openState && off >= b.size && b.rs.has(r) {
		b.cond.Wait()
	}

	switch b.state {
	case canceledState:
		return ErrCanceled

	case closedState:
		if off >= b.size {
			return io.EOF
		}
	}

	if !b.rs.has(r) {
		return os.ErrClosed
	}

	return nil
}

func (b *broadcaster) Wrote(n int) {
	if n > 0 {
		b.mu.Lock()
		b.size += int64(n)
		b.mu.Unlock()
		b.cond.Broadcast()
	}
}

func (b *broadcaster) Close() (err error) {
	b.mu.Lock()
	b.setState(closedState)
	b.mu.Unlock()

	b.dropHandle()
	return nil
}

func (b *broadcaster) Cancel() (err error) {
	b.mu.Lock()
	b.setState(canceledState)
	b.preventNewHandles(ErrCanceled)
	readersToClose := b.rs.dropAll()
	b.mu.Unlock()

	for _, r := range readersToClose {
		r.Close()
	}

	return nil
}

func (b *broadcaster) PreventNewHandles(err error) {
	b.mu.Lock()
	b.preventNewHandles(err)
	b.mu.Unlock()
}

func (b *broadcaster) preventNewHandles(err error) {
	if b.newHandleErr == nil {
		b.newHandleErr = err
	}
}

func (b *broadcaster) WaitForZeroHandles() {
	b.fileInUse.Wait()
}

func (b *broadcaster) UseHandle(do func() (int, error)) (int, error) {
	b.mu.RLock()
	switch b.state {
	case canceledState:
		b.mu.RUnlock()
		return 0, ErrCanceled
	}
	b.mu.RUnlock()

	// if !b.rs.has(r) =>  file is closed, => read will fail anyway

	// let's not hold the read lock while doing the Read, we're not reading any state on the broadcaster there.
	return do()
}

func (b *broadcaster) setState(s streamState) {
	switch b.state {
	case canceledState:

	default:
		b.state = s
		b.cond.Broadcast()
	}
}

func (b *broadcaster) Size() (size int64, isClosed bool) {
	b.mu.RLock()
	size = b.size
	isClosed = b.state == closedState
	b.mu.RUnlock()
	return size, isClosed
}

func (b *broadcaster) addHandle() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.newHandleErr != nil {
		return b.newHandleErr
	}

	b.fileInUse.Add(1)
	return nil
}

func (b *broadcaster) dropHandle() { b.fileInUse.Done() }

func (b *broadcaster) NewReader(createReader func() (*Reader, error)) (*Reader, error) {
	if err := b.addHandle(); err != nil {
		return nil, err
	}

	r, err := createReader()
	if err != nil {
		b.dropHandle()
		return nil, err
	}

	b.mu.Lock()
	b.rs.add(r)
	b.mu.Unlock()

	return r, nil
}

func (b *broadcaster) DropReader(r *Reader) {
	b.mu.Lock()
	b.rs.drop(r)
	isCanceled := b.state == canceledState
	b.mu.Unlock()

	b.dropHandle()

	if isCanceled {
		// we've canceled, either we've already broadcasted from Stream.Cancel() or will.
		// if we have => there will be no more blocking reads, no need to broadcast here.
		// if we haven't yet => we will, so no need to broadcast here.
		// this avoids a broadcast storm on Cancel() when all readers call Close()
		return
	}
	b.cond.Broadcast()
}

type onceWithErr struct {
	once sync.Once
	err  error
}

func (co *onceWithErr) Do(closeFunc func() error) error {
	co.once.Do(func() {
		co.err = closeFunc()
	})
	return co.err
}
