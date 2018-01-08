package stream

import (
	"errors"
	"io"
	"sync"
)

// ErrRemoving is returned when requesting a Reader on a Stream which is being Removed.
var ErrRemoving = errors.New("cannot open a new reader while removing file")

// ErrCanceled indicates that stream has been canceled.
var ErrCanceled = errors.New("stream has been canceled")

var errReaderIsClosed = errors.New("closed Reader")

type streamState int

const (
	openState streamState = iota
	closedState
	canceledState
)

type broadcaster struct {
	mu       sync.RWMutex
	cond     *sync.Cond
	state    streamState
	size     int64
	removing bool
	rs       *readerSet
	grp      sync.WaitGroup
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
		remaining := b.size - off
		if remaining == 0 {
			return io.EOF
		}
	}

	if !b.rs.has(r) {
		return errReaderIsClosed
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
	err = b.setState(closedState)
	b.mu.Unlock()

	b.dropHandle()
	return err
}

func (b *broadcaster) Cancel() (err error) {
	b.mu.Lock()
	err = b.setState(canceledState)
	readersToClose := b.rs.dropAll()
	b.mu.Unlock()

	for _, r := range readersToClose {
		r.Close()
	}

	return err
}

func (b *broadcaster) PreventNewHandles() {
	b.mu.Lock()
	b.removing = true
	b.mu.Unlock()
}

func (b *broadcaster) WaitForZeroHandles() {
	b.grp.Wait()
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

func (b *broadcaster) setState(s streamState) error {
	switch b.state {
	case canceledState:
		// do nothing

	default:
		b.state = s
		b.cond.Broadcast()
	}
	return nil
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
	if b.state == canceledState {
		return ErrCanceled
	}

	if b.removing {
		return ErrRemoving
	}

	b.grp.Add(1)
	return nil
}

func (b *broadcaster) dropHandle() { b.grp.Done() }

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
	b.mu.Unlock()

	b.dropHandle()

	// in case this Reader is blocking, wake all Readers up.
	// would be better if we could wake up just this reader, but not possible at this time.
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
