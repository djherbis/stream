package stream

import (
	"sync"
)

type broadcaster struct {
	mu        sync.RWMutex
	cond      *sync.Cond
	closed    bool
	canceled  bool
	size      int64
	unblocked map[int64]bool
}

func newBroadcaster() *broadcaster {
	var b broadcaster
	b.cond = sync.NewCond(b.mu.RLocker())
	b.unblocked = make(map[int64]bool)
	return &b
}

// Wait blocks until we've written past the given offset, or until closed.
func (b *broadcaster) Wait(off int64, id int64) (n int64, open bool, canceled bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for !b.closed && off >= b.size && !b.unblocked[id] {
		b.cond.Wait()
	}
	return b.size - off, !b.closed, b.canceled
}

func (b *broadcaster) Wrote(n int) {
	if n > 0 {
		b.mu.Lock()
		b.size += int64(n)
		b.mu.Unlock()
		b.cond.Broadcast()
	}
}

func (b *broadcaster) Unblock(id int64) {
	b.mu.Lock()
	b.unblocked[id] = true
	b.mu.Unlock()
	b.cond.Broadcast()
}

func (b *broadcaster) Closed(id int64) {
	b.mu.Lock()
	delete(b.unblocked, id)
	b.mu.Unlock()
}

func (b *broadcaster) Close(canceled bool) error {
	b.mu.Lock()
	b.closed = true
	b.canceled = canceled
	b.mu.Unlock()
	b.cond.Broadcast()
	return nil
}

func (b *broadcaster) Size() (s int64, c bool) {
	b.mu.RLock()
	s = b.size
	c = b.closed
	b.mu.RUnlock()
	return s, c
}
