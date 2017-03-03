package stream

import (
	"sync"
)

type broadcaster struct {
	mu     sync.RWMutex
	cond   *sync.Cond
	closed bool
	size   int64
}

func newBroadcaster() *broadcaster {
	var b broadcaster
	b.cond = sync.NewCond(b.mu.RLocker())
	return &b
}

// Wait blocks until we've written past the given offset, or until closed.
func (b *broadcaster) Wait(off int64) (n int64, open bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for !b.closed && off >= b.size {
		b.cond.Wait()
	}
	return b.size - off, !b.closed
}

func (b *broadcaster) Wrote(n int) {
	if n > 0 {
		b.mu.Lock()
		b.size += int64(n)
		b.mu.Unlock()
		b.cond.Broadcast()
	}
}

func (b *broadcaster) Close() error {
	b.mu.Lock()
	b.closed = true
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
