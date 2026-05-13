package gosnowflake

import (
	"sync"
	"sync/atomic"
)

type blockingReadCloser struct {
	payload             []byte
	firstChunkSize      int
	blockingReadStarted chan struct{}
	unblockRead         chan struct{}
	terminalErr         error

	blockOnce sync.Once
	closeOnce sync.Once
	closed    atomic.Bool
	sentBody  bool
}

func newBlockingReadCloser(payload []byte, terminalErr error) *blockingReadCloser {
	return &blockingReadCloser{
		payload:             append([]byte(nil), payload...),
		firstChunkSize:      len(payload),
		blockingReadStarted: make(chan struct{}),
		unblockRead:         make(chan struct{}),
		terminalErr:         terminalErr,
	}
}

func (b *blockingReadCloser) Read(p []byte) (int, error) {
	if !b.sentBody {
		b.sentBody = true
		chunk := b.payload
		if b.firstChunkSize >= 0 && b.firstChunkSize < len(chunk) {
			chunk = chunk[:b.firstChunkSize]
		}
		copy(p, chunk)
		return len(chunk), nil
	}
	b.blockOnce.Do(func() {
		close(b.blockingReadStarted)
	})
	<-b.unblockRead
	return 0, b.terminalErr
}

func (b *blockingReadCloser) Close() error {
	b.closed.Store(true)
	b.closeOnce.Do(func() {
		close(b.unblockRead)
	})
	return nil
}
