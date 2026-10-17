package ws

import "sync"

// outbox is an ordered, bounded buffer of frames waiting to be written to the
// current physical connection. Frames produced while disconnected are kept
// here and replayed on reconnect.
type outbox struct {
	mu     sync.Mutex
	frames [][]byte
	size   int
	max    int
	closed bool
	notify chan struct{}
}

func newOutbox(max int) *outbox {
	if max <= 0 {
		max = 8 << 20
	}
	return &outbox{max: max, notify: make(chan struct{}, 1)}
}

// push appends a frame. It returns false when the outbox is closed or the
// frame would exceed the size budget.
func (o *outbox) push(frame []byte) bool {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return false
	}
	if o.size+len(frame) > o.max && len(o.frames) > 0 {
		o.mu.Unlock()
		return false
	}
	o.frames = append(o.frames, frame)
	o.size += len(frame)
	o.mu.Unlock()

	select {
	case o.notify <- struct{}{}:
	default:
	}
	return true
}

// requeue prepends frames that were drained but not delivered. It is used
// when a write fails or a physical connection is replaced mid-batch.
func (o *outbox) requeue(frames [][]byte) {
	if len(frames) == 0 {
		return
	}
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return
	}
	o.frames = append(frames, o.frames...)
	for _, frame := range frames {
		o.size += len(frame)
	}
	// Shed the newest frames if requeueing pushed us over budget.
	for o.size > o.max && len(o.frames) > 0 {
		last := len(o.frames) - 1
		o.size -= len(o.frames[last])
		o.frames = o.frames[:last]
	}
	o.mu.Unlock()

	select {
	case o.notify <- struct{}{}:
	default:
	}
}

// drain removes and returns all queued frames.
func (o *outbox) drain() [][]byte {
	o.mu.Lock()
	frames := o.frames
	o.frames = nil
	o.size = 0
	o.mu.Unlock()
	return frames
}

func (o *outbox) signal() <-chan struct{} { return o.notify }

func (o *outbox) close() {
	o.mu.Lock()
	o.closed = true
	o.mu.Unlock()
	select {
	case o.notify <- struct{}{}:
	default:
	}
}
