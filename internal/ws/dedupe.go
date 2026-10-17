package ws

import (
	"encoding/json"
	"sync"

	"github.com/coxlong/claude-acp/internal/rpc"
)

// dedupe remembers recently completed request responses so that a client
// replaying its outbox after a reconnect does not cause a request to run
// twice.
type dedupe struct {
	mu      sync.Mutex
	max     int
	order   []string
	entries map[string]*dedupeEntry
}

type dedupeEntry struct {
	response []byte
	inflight bool
}

func newDedupe(max int) *dedupe {
	if max <= 0 {
		max = 256
	}
	return &dedupe{max: max, entries: make(map[string]*dedupeEntry)}
}

// begin records that a request is being handled. It returns a cached
// response if the request already completed, or duplicate=true if it is
// still in flight.
func (d *dedupe) begin(key string) (cached []byte, duplicate bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if e := d.entries[key]; e != nil {
		if e.response != nil {
			return e.response, true
		}
		return nil, true
	}
	d.entries[key] = &dedupeEntry{inflight: true}
	d.order = append(d.order, key)
	d.evictLocked()
	return nil, false
}

// recordResponse stores the serialized response for a completed request.
func (d *dedupe) recordResponse(resp *rpc.Response) {
	if len(resp.ID) == 0 {
		return
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return
	}
	key := string(resp.ID)

	d.mu.Lock()
	e := d.entries[key]
	if e == nil {
		e = &dedupeEntry{}
		d.entries[key] = e
		d.order = append(d.order, key)
	}
	e.response = b
	e.inflight = false
	d.evictLocked()
	d.mu.Unlock()
}

func (d *dedupe) evictLocked() {
	for len(d.order) > d.max {
		oldest := d.order[0]
		d.order = d.order[1:]
		delete(d.entries, oldest)
	}
}
