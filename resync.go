package xds

import (
	"maps"
	"sync"
	"time"

	statuspb "google.golang.org/genproto/googleapis/rpc/status"
)

// ResyncAfterNack resyncs an Envoy a while after it NACKs, so a fixed config gets through without a reconnect loop.
// If it NACKs again after the resync, the next wait doubles, up to the maximum. Once an Envoy goes twice that without
// a NACK, it starts again from the initial wait. Use its Nacked and Disconnected as your Events'.
//
// While it waits, that Envoy gets no changes, so the maximum is the longest you'll have it serve stale config, e.g.
// endpoints for pods that have gone.
type ResyncAfterNack[D any] struct {
	initial, maxWait time.Duration

	mu       sync.Mutex
	pending  map[*Client[D]]*time.Timer
	backoffs map[string]backoff // by node ID, not client: a resync is a new stream, so a new client
}

type backoff struct {
	delay    time.Duration
	lastNack time.Time
}

// NewResyncAfterNack waits initial before the first resync, and never more than maxWait.
func NewResyncAfterNack[D any](initial, maxWait time.Duration) *ResyncAfterNack[D] {
	return &ResyncAfterNack[D]{initial: initial, maxWait: maxWait, pending: map[*Client[D]]*time.Timer{}, backoffs: map[string]backoff{}}
}

// Nacked schedules a resync, on a stream's first NACK.
func (r *ResyncAfterNack[D]) Nacked(c *Client[D], _ TypeURL, _ *statuspb.Status) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.pending[c]; ok {
		return
	}
	// Stays pending until the stream it ends disconnects, so its later NACKs don't resync it again.
	r.pending[c] = time.AfterFunc(r.next(c.Node.GetId()), c.Resync)
}

// Disconnected cancels a stream's resync.
func (r *ResyncAfterNack[D]) Disconnected(c *Client[D]) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.pending[c]; ok {
		t.Stop()
		delete(r.pending, c)
	}
}

func (r *ResyncAfterNack[D]) next(id string) time.Duration {
	now := time.Now()
	// Forget Envoys that have gone quiet, which would start from initial anyway: node IDs come and go with pods.
	// ponytail: scans every node on each NACK; fine while NACKs are rare, sweep on a timer if they aren't.
	maps.DeleteFunc(r.backoffs, func(_ string, b backoff) bool { return now.Sub(b.lastNack) > 2*r.maxWait })
	b, ok := r.backoffs[id]
	if ok {
		b.delay = min(2*b.delay, r.maxWait)
	} else {
		b.delay = r.initial
	}
	b.lastNack = now
	r.backoffs[id] = b
	return b.delay
}
