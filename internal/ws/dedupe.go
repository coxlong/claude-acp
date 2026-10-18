package ws

import (
	"encoding/json"
	"sync"

	"github.com/coxlong/claude-acp/internal/rpc"
)

// dedupe remembers recently completed request responses so that a client
// replaying its outbox after a reconnect does not cause a request to run
// twice. Only success responses are cached: a transient error must be
// retryable, so its entry is dropped and a replay re-executes the request.
type dedupe struct {
	mu      sync.Mutex
	max     int
	order   []string
	entries map[string]*dedupeEntry
}

type dedupeEntry struct {
	response []byte
}

func newDedupe(max int) *dedupe {
	if max <= 0 {
		max = 256
	}
	return &dedupe{max: max, entries: make(map[string]*dedupeEntry)}
}

// begin records that a request is being handled.
//
// known tells whether this id was already seen on this logical client (an
// in-flight duplicate, or a replay of a request from an earlier connection).
// When known is false — a brand-new id that happens to collide with one left
// over from a previous client lifetime — any stale entry is discarded so the
// fresh request is executed instead of being answered (or swallowed) by the
// previous life's state.
func (d *dedupe) begin(key string, known bool) (cached []byte, duplicate bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if e := d.entries[key]; e != nil {
		if !known {
			d.removeLocked(key)
		} else if e.response != nil {
			return e.response, true
		} else {
			return nil, true
		}
	}
	d.entries[key] = &dedupeEntry{}
	d.order = append(d.order, key)
	d.evictLocked()
	return nil, false
}

// recordResponse stores the serialized response for a completed request.
// Error responses are not cached — the entry is removed so a replayed
// request can be retried.
func (d *dedupe) recordResponse(resp *rpc.Response) {
	if len(resp.ID) == 0 {
		return
	}
	key := string(resp.ID)

	if resp.Error != nil {
		d.mu.Lock()
		d.removeLocked(key)
		d.mu.Unlock()
		return
	}

	b, err := json.Marshal(resp)
	if err != nil {
		return
	}

	d.mu.Lock()
	e := d.entries[key]
	if e == nil {
		e = &dedupeEntry{}
		d.entries[key] = e
		d.order = append(d.order, key)
	}
	e.response = b
	d.evictLocked()
	d.mu.Unlock()
}

func (d *dedupe) removeLocked(key string) {
	if _, ok := d.entries[key]; !ok {
		return
	}
	delete(d.entries, key)
	for i, k := range d.order {
		if k == key {
			d.order = append(d.order[:i], d.order[i+1:]...)
			break
		}
	}
}

func (d *dedupe) evictLocked() {
	for len(d.order) > d.max {
		oldest := d.order[0]
		d.order = d.order[1:]
		delete(d.entries, oldest)
	}
}
